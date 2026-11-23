/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.datasource.transactions.xa;

import org.apache.flink.api.common.JobID;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.core.datastream.sink.writer.JdbcWriterState;
import org.apache.flink.connector.jdbc.core.datastream.sink.writer.JdbcWriterStateSerializer;
import org.apache.flink.connector.jdbc.datasource.connections.xa.SimpleXaConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.connections.xa.XaConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.domain.TransactionId;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.exceptions.TransientXaException;
import org.apache.flink.connector.jdbc.testutils.DatabaseTest;
import org.apache.flink.connector.jdbc.testutils.TableManaged;
import org.apache.flink.connector.jdbc.testutils.tables.templates.BooksTable;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;
import org.apache.flink.util.FlinkRuntimeException;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.event.Level;

import javax.annotation.Nullable;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.StringUtils.hexStringToByte;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link XaTransaction} against a database with two-phase commit. */
public abstract class XaTransactionTest implements DatabaseTest {

    @RegisterExtension
    final LoggerAuditingExtension log =
            new LoggerAuditingExtension(XaTransaction.class, Level.WARN);

    private static final BooksTable TEST_TABLE = new BooksTable("XaTransactionTable");

    private static final JobID VERSION_2_JOB_ID =
            JobID.fromHexString("6b64d8a9a951e2e8767ae952ad951706");

    /**
     * Version 2 of the writer state, from the writer of 4.1.0 (subtask 1 of 2 of job {@link
     * #VERSION_2_JOB_ID}, restored at checkpoint 1000) after the commit of its transaction for
     * checkpoint 1000 failed transiently: prepared [1000], hanging [1001].
     */
    private static final String VERSION_2_STATE =
            "00000001446b64d8a9a951e2e8767ae952ad951706000000010000000200000000000003"
                    + "e90000000000000000000000000000000000000000000000000000000000000000000000"
                    + "0000000001446b64d8a9a951e2e8767ae952ad9517060000000100000002000000000000"
                    + "03e800000001000000000000000000000000000000000000000000000000000000000000"
                    + "0000";

    /** As {@link #VERSION_2_STATE}, after the commits for 1000 and 1001: prepared [1000, 1001]. */
    private static final String VERSION_2_STATE_TWO_PREPARED =
            "00000001446b64d8a9a951e2e8767ae952ad951706000000010000000200000000000003"
                    + "ea0000000000000000000000000000000000000000000000000000000000000000000000"
                    + "0000000002446b64d8a9a951e2e8767ae952ad9517060000000100000002000000000000"
                    + "03e800000002000000000000000000000000000000000000000000000000000000000000"
                    + "0000446b64d8a9a951e2e8767ae952ad951706000000010000000200000000000003e900"
                    + "000000000000000000000000000000000000000000000000000000000000000000000000"
                    + "000000000000000000000000000000000000000000000000000000000000000000000000"
                    + "000000000000000000000000000000000000000000000000000000000000000000000000"
                    + "00000000";

    @Override
    public List<TableManaged> getManagedTables() {
        return Collections.singletonList(TEST_TABLE);
    }

    /** A failed test can leave a prepared transaction behind, which holds its locks. */
    @AfterEach
    void rollbackPreparedTransactions() throws Exception {
        try (SimpleXaConnectionProvider xa = newXaConnectionProvider()) {
            xa.open();
            for (Xid xid : xa.recover()) {
                xa.rollback(xid);
            }
        }
    }

    @Test
    void testRollbackPreparedTxsFromCheckpoint() throws Exception {
        byte[] jobId = new JobID().getBytes();
        TransactionId earlier = prepare(jobId, 0, 1, 2, 1);
        TransactionId leftover = prepare(jobId, 0, 1, 3, 2);

        rollbackPreparedTxs(jobId, 0, 1, 3);

        assertThat(isPrepared(earlier)).isTrue();
        assertThat(isPrepared(leftover)).isFalse();
    }

    /**
     * A writer that can't list the prepared transactions fails instead of leaving a leftover that
     * holds its locks.
     */
    @Test
    void testRollbackPreparedTxsFailsIfItCantList() throws Exception {
        try (XaTransaction transaction =
                new XaTransaction(
                        JdbcExactlyOnceOptions.builder().withRecoveredAndRollback(false).build(),
                        TransactionId.create(new JobID().getBytes(), 0, 1),
                        failingRecover(newXaConnectionProvider()))) {
            transaction.open(JdbcWriterState.empty());

            assertThatThrownBy(() -> transaction.rollbackPreparedTxs(5))
                    .isInstanceOf(FlinkRuntimeException.class)
                    .hasMessageContaining("XA_RECOVER_ADMIN");
        }
    }

    /** Another exactly-once sink of the same job writes with the same job id and subtask index. */
    @Test
    void testRollbackPreparedTxsKeepsOtherParallelism() throws Exception {
        byte[] jobId = new JobID().getBytes();
        TransactionId otherSink = prepare(jobId, 0, 4, 5, 1);

        rollbackPreparedTxs(jobId, 0, 2, 5);

        assertThat(isPrepared(otherSink)).isTrue();
    }

    @Test
    void testRollbackPreparedTxsKeepsOtherSubtask() throws Exception {
        byte[] jobId = new JobID().getBytes();
        TransactionId otherSubtask = prepare(jobId, 1, 2, 5, 1);

        rollbackPreparedTxs(jobId, 0, 2, 5);

        assertThat(isPrepared(otherSubtask)).isTrue();
    }

    /**
     * A job restored from a savepoint while the job that took it still runs would roll back the
     * transaction that job's committer is about to commit.
     */
    @Test
    void testOpenKeepsRestoredHangingOfEarlierJob() throws Exception {
        byte[] earlierJobId = new JobID().getBytes();
        TransactionId hanging = prepare(earlierJobId, 0, 1, 5, 1);

        rollbackPreparedTxs(new JobID().getBytes(), 0, 1, 5, restoredState(hanging));

        assertThat(isPrepared(hanging)).isTrue();
    }

    /** The state of a failed attempt of the same job, here before an upscale. */
    @Test
    void testOpenRollsBackRestoredHangingOfSameJob() throws Exception {
        byte[] jobId = new JobID().getBytes();
        TransactionId hanging = prepare(jobId, 0, 1, 5, 1);

        rollbackPreparedTxs(jobId, 0, 2, 5, restoredState(hanging));

        assertThat(isPrepared(hanging)).isFalse();
    }

    /**
     * At another parallelism nothing else rolls back the restored transaction, so a writer that
     * can't roll it back fails, and the restart tries again.
     */
    @Test
    void testOpenFailsIfRollbackOfRestoredHangingFailsTransiently() throws Exception {
        byte[] jobId = new JobID().getBytes();
        TransactionId hanging = prepare(jobId, 0, 1, 5, 1);

        try (XaTransaction transaction =
                new XaTransaction(
                        JdbcExactlyOnceOptions.defaults(),
                        TransactionId.create(jobId, 0, 2),
                        SimpleXaConnectionProvider.from(
                                failingFirstRollback(getMetadata().buildXaDataSource()), null))) {
            assertThatThrownBy(() -> transaction.open(restoredState(hanging)))
                    .isInstanceOf(FlinkRuntimeException.class)
                    .hasCauseInstanceOf(TransientXaException.class);
        }
        assertThat(isPrepared(hanging)).isTrue();

        rollbackPreparedTxs(jobId, 0, 2, 5, restoredState(hanging));
        assertThat(isPrepared(hanging)).isFalse();
    }

    /**
     * At another parallelism the writer only rolls back the transaction that the restored state
     * names, not those that the failed attempt prepared for later checkpoints.
     */
    @Test
    void testOpenAtOtherParallelismKeepsLaterLeftovers() throws Exception {
        byte[] jobId = new JobID().getBytes();
        TransactionId hanging = prepare(jobId, 0, 1, 5, 1);
        TransactionId later = prepare(jobId, 0, 1, 6, 2);

        rollbackPreparedTxs(jobId, 0, 2, 5, restoredState(hanging));

        assertThat(isPrepared(hanging)).isFalse();
        assertThat(isPrepared(later)).isTrue();
    }

    /**
     * The writer of 4.1 committed at the barrier and kept a transaction whose commit failed
     * transiently in its state. A restored writer commits it.
     */
    @Test
    void testOpenCommitsPreparedOfVersion2State() throws Exception {
        JdbcWriterState state = version2State(VERSION_2_STATE);
        TransactionId prepared = prepare(VERSION_2_JOB_ID.getBytes(), 1, 2, 1000, 1);
        assertThat(state.getPrepared()).containsExactly(prepared);

        try (XaTransaction ignored = newTransaction(null, new JobID().getBytes(), 1, 2, state)) {
            assertThat(isPrepared(prepared)).isFalse();
        }

        assertThat(TEST_TABLE.selectAllTable(getMetadata())).hasSize(1);
    }

    @Test
    void testOpenCommitsAllPreparedOfVersion2State() throws Exception {
        JdbcWriterState state = version2State(VERSION_2_STATE_TWO_PREPARED);
        TransactionId first = prepare(VERSION_2_JOB_ID.getBytes(), 1, 2, 1000, 1);
        TransactionId second = prepare(VERSION_2_JOB_ID.getBytes(), 1, 2, 1001, 2);
        assertThat(state.getPrepared()).containsExactly(first, second);

        try (XaTransaction ignored = newTransaction(null, new JobID().getBytes(), 1, 2, state)) {
            assertThat(isPrepared(first)).isFalse();
            assertThat(isPrepared(second)).isFalse();
        }

        assertThat(TEST_TABLE.selectAllTable(getMetadata())).hasSize(2);
    }

    /**
     * The writer of 4.1 may have committed the transaction on a retry before the job failed, so the
     * database no longer knows it.
     */
    @Test
    void testOpenIgnoresCommittedPreparedOfVersion2State() throws Exception {
        try (XaTransaction ignored =
                newTransaction(
                        null, new JobID().getBytes(), 1, 2, version2State(VERSION_2_STATE))) {
            assertThat(TEST_TABLE.selectAllTable(getMetadata())).isEmpty();
        }
    }

    private static JdbcWriterState version2State(String hex) throws IOException {
        return new JdbcWriterStateSerializer().deserialize(2, hexStringToByte(hex));
    }

    @Test
    void testRollbackPreparedTxsKeepsEarlierJobWithoutPrefix() throws Exception {
        TransactionId earlierJob = prepare(new JobID().getBytes(), 0, 1, 5, 1);

        rollbackPreparedTxs(new JobID().getBytes(), 0, 1, 0);

        assertThat(isPrepared(earlierJob)).isTrue();
    }

    /** A restore from a savepoint into a new job, or a new job after a lost one. */
    @Test
    void testRollbackPreparedTxsOfEarlierJobWithPrefix() throws Exception {
        byte[] earlierJobId = new JobID().getBytes();
        TransactionId committed = prepare("books", earlierJobId, 0, 1, 4, 1);
        TransactionId leftover = prepare("books", earlierJobId, 0, 1, 5, 2);

        rollbackPreparedTxs("books", new JobID().getBytes(), 0, 1, 5, JdbcWriterState.empty());

        assertThat(isPrepared(committed)).isTrue();
        assertThat(isPrepared(leftover)).isFalse();
    }

    @Test
    void testRollbackPreparedTxsKeepsOtherPrefix() throws Exception {
        TransactionId otherSink = prepare("authors", new JobID().getBytes(), 0, 1, 5, 1);

        rollbackPreparedTxs("books", new JobID().getBytes(), 0, 1, 0, JdbcWriterState.empty());

        assertThat(isPrepared(otherSink)).isTrue();
    }

    /** Each subtask owns the subtask indexes equal to its own modulo the parallelism. */
    @Test
    void testRollbackPreparedTxsOfOtherParallelismWithPrefix() throws Exception {
        byte[] jobId = new JobID().getBytes();
        TransactionId owned = prepare("books", jobId, 3, 4, 5, 1);
        TransactionId notOwned = prepare("books", jobId, 2, 4, 5, 2);

        rollbackPreparedTxs("books", jobId, 1, 2, 5, JdbcWriterState.empty());

        assertThat(isPrepared(owned)).isFalse();
        assertThat(isPrepared(notOwned)).isTrue();
    }

    /**
     * In BATCH a writer can restart while the others have already prepared their transactions for
     * the committer.
     */
    @Test
    void testRollbackPreparedTxsKeepsOtherSubtaskWithPrefix() throws Exception {
        byte[] jobId = new JobID().getBytes();
        TransactionId otherSubtask = prepare("books", jobId, 1, 2, 0, 1);

        rollbackPreparedTxs("books", jobId, 0, 2, 0, JdbcWriterState.empty());

        assertThat(isPrepared(otherSubtask)).isTrue();
    }

    /**
     * A job restored from a savepoint with another prefix, or with one where the job that took it
     * had none, leaves that job alone, as it may still run.
     */
    @Test
    void testRollbackPreparedTxsKeepsRestoredHangingOfOtherPrefix() throws Exception {
        byte[] earlierJobId = new JobID().getBytes();
        TransactionId blue = prepare("blue", earlierJobId, 0, 1, 5, 1);
        TransactionId none = prepare(earlierJobId, 1, 2, 5, 2);
        JdbcWriterState state =
                JdbcWriterState.of(Collections.emptyList(), Arrays.asList(blue, none));

        rollbackPreparedTxs("green", new JobID().getBytes(), 0, 1, 5, state);

        assertThat(isPrepared(blue)).isTrue();
        assertThat(isPrepared(none)).isTrue();
    }

    /** A transaction that is gone by the time the writer rolls it back is no failure. */
    @Test
    void testRollbackPreparedTxsOfTransactionRolledBackMeanwhile() throws Exception {
        TransactionId leftover = prepare("books", new JobID().getBytes(), 0, 1, 5, 1);
        XaConnectionProvider racing =
                proxy(
                        newXaConnectionProvider(),
                        (delegate, method, args) -> {
                            if (method.getName().equals("rollback")) {
                                // another writer gets there first
                                try (SimpleXaConnectionProvider other = newXaConnectionProvider()) {
                                    other.open();
                                    other.rollback((Xid) args[0]);
                                }
                            }
                            return method.invoke(delegate, args);
                        });

        try (XaTransaction transaction =
                new XaTransaction(
                        JdbcExactlyOnceOptions.defaults(),
                        TransactionId.create("books", new JobID().getBytes(), 0, 1),
                        racing)) {
            transaction.open(JdbcWriterState.empty());
            transaction.rollbackPreparedTxs(5);
        }

        assertThat(isPrepared(leftover)).isFalse();
    }

    /**
     * A job that adds a prefix and restarts under the same job id rolls back the transaction that
     * its failed attempt started without one.
     */
    @Test
    void testOpenRollsBackRestoredHangingOfSameJobWithoutPrefix() throws Exception {
        byte[] jobId = new JobID().getBytes();
        TransactionId hanging = prepare(jobId, 0, 1, 5, 1);

        try (XaTransaction ignored = newTransaction("books", jobId, 0, 1, restoredState(hanging))) {
            assertThat(isPrepared(hanging)).isFalse();
        }
    }

    /** As above, for a transaction that the failed attempt prepared after the restored one. */
    @Test
    void testRollbackPreparedTxsOfSameJobWithoutPrefix() throws Exception {
        byte[] jobId = new JobID().getBytes();
        TransactionId leftover = prepare(jobId, 0, 1, 5, 1);

        rollbackPreparedTxs("books", jobId, 0, 1, 5, JdbcWriterState.empty());

        assertThat(isPrepared(leftover)).isFalse();
    }

    /**
     * Which leftovers of the given checkpoint a writer (subtask 1 of 2) with or without the prefix
     * "books" rolls back, from the restored state or from the listing at the restored checkpoint 5.
     * Under a prefix only the writers with that prefix, each those of the subtasks equal to its own
     * modulo the parallelism; without a prefix only the writers of the same job, from the listing
     * only those of their own subtask and parallelism.
     */
    @ParameterizedTest(name = "prefix={0}, restored={1}, checkpoint={2}")
    @CsvSource({
        ",false,4", ",false,5", ",true,4", ",true,5",
        "books,false,4", "books,false,5", "books,true,4", "books,true,5"
    })
    void testOwnership(@Nullable String prefix, boolean restored, long checkpointId)
            throws Exception {
        byte[] jobId = new JobID().getBytes();
        byte[] otherJobId = new JobID().getBytes();
        int[][] subtaskAndParallelism = {{1, 2}, {0, 2}, {1, 4}, {3, 4}, {2, 4}};
        Map<TransactionId, Boolean> expected = new LinkedHashMap<>();
        int id = 0;
        for (String xidPrefix : new String[] {null, "books", "authors"}) {
            for (byte[] xidJobId : new byte[][] {jobId, otherJobId}) {
                for (int[] sp : subtaskAndParallelism) {
                    boolean owned;
                    if (xidPrefix != null) {
                        owned = xidPrefix.equals(prefix) && sp[0] % 2 == 1;
                    } else {
                        owned = xidJobId == jobId && (restored || (sp[0] == 1 && sp[1] == 2));
                    }
                    boolean rolledBack = owned && (restored || checkpointId >= 5);
                    expected.put(
                            prepare(xidPrefix, xidJobId, sp[0], sp[1], checkpointId, id++),
                            rolledBack);
                }
            }
        }
        JdbcWriterState state =
                restored
                        ? JdbcWriterState.of(
                                Collections.emptyList(), new ArrayList<>(expected.keySet()))
                        : JdbcWriterState.empty();

        try (XaTransaction transaction = newTransaction(prefix, jobId, 1, 2, state)) {
            if (!restored) {
                transaction.rollbackPreparedTxs(5);
            }
        }

        List<String> wrong = new ArrayList<>();
        for (Map.Entry<TransactionId, Boolean> entry : expected.entrySet()) {
            if (isPrepared(entry.getKey()) == entry.getValue()) {
                wrong.add(
                        (entry.getValue() ? "kept " : "rolled back ")
                                + describe(entry.getKey(), jobId));
            }
        }
        assertThat(wrong).isEmpty();
    }

    private static String describe(TransactionId transactionId, byte[] jobId) {
        return String.format(
                "prefix=%s job=%s subtask=%d/%d checkpoint=%d",
                transactionId.getPrefix().orElse(null),
                Arrays.equals(transactionId.getJobId(), jobId) ? "same" : "other",
                transactionId.getSubtaskId(),
                transactionId.getNumberOfSubtasks(),
                transactionId.getCheckpointId());
    }

    @Test
    void testWarnAboutLeftoversOfOtherJobWithoutPrefix() throws Exception {
        TransactionId leftover = prepare(new JobID().getBytes(), 0, 1, 5, 1);

        rollbackPreparedTxs(new JobID().getBytes(), 0, 1, 5);

        assertThat(log.getMessages())
                .singleElement(InstanceOfAssertFactories.STRING)
                .contains("left 1 prepared transactions", "withTransactionIdPrefix(\"my-sink\")");
        assertThat(isPrepared(leftover)).isTrue();
    }

    /**
     * The transactions of the restored checkpoint belong to the committer, which commits them after
     * the restore.
     */
    @Test
    void testNoWarningAboutTransactionsOfEarlierCheckpoints() throws Exception {
        TransactionId committable = prepare(new JobID().getBytes(), 0, 1, 4, 1);

        rollbackPreparedTxs(new JobID().getBytes(), 0, 1, 5);

        assertThat(isPrepared(committable)).isTrue();
        assertThat(log.getMessages()).isEmpty();
    }

    /** The other subtasks list the same transactions, so only the first one warns. */
    @Test
    void testWarnAboutLeftoversOnlyOnFirstSubtask() throws Exception {
        prepare(new JobID().getBytes(), 0, 1, 5, 1);

        rollbackPreparedTxs(new JobID().getBytes(), 1, 2, 5);

        assertThat(log.getMessages()).isEmpty();
    }

    /**
     * No warning about the leftovers of the job's other writers or of another sink with a prefix:
     * they are theirs to roll back.
     */
    @Test
    void testNoWarningAboutOtherWritersAndSinks() throws Exception {
        byte[] jobId = new JobID().getBytes();
        prepare(jobId, 1, 2, 5, 1);
        prepare("authors", new JobID().getBytes(), 0, 1, 5, 2);

        rollbackPreparedTxs("books", jobId, 0, 2, 5, JdbcWriterState.empty());

        assertThat(log.getMessages()).isEmpty();
    }

    /** The listing after a failed rollback usually fails as well, and must not hide the cause. */
    @Test
    void testRollbackPreparedTxsKeepsRollbackFailureIfListingFails() throws Exception {
        prepare("books", new JobID().getBytes(), 0, 1, 5, 1);
        SimpleXaConnectionProvider delegate = newXaConnectionProvider();
        AtomicInteger listings = new AtomicInteger();
        XaConnectionProvider failing =
                proxy(
                        XaConnectionProvider.class,
                        (method, args) -> {
                            if (method.getName().equals("rollback")) {
                                throw new FlinkRuntimeException("rollback failed");
                            }
                            if (method.getName().equals("recover")
                                    && listings.incrementAndGet() > 1) {
                                throw new FlinkRuntimeException("recover failed");
                            }
                            return invoke(delegate, method, args);
                        });

        try (XaTransaction transaction =
                new XaTransaction(
                        JdbcExactlyOnceOptions.defaults(),
                        TransactionId.create("books", new JobID().getBytes(), 0, 1),
                        failing)) {
            transaction.open(JdbcWriterState.empty());

            assertThatThrownBy(() -> transaction.rollbackPreparedTxs(5))
                    .hasMessage("rollback failed")
                    .satisfies(
                            e ->
                                    assertThat(e.getSuppressed())
                                            .singleElement()
                                            .extracting(Throwable::getMessage)
                                            .isEqualTo("recover failed"));
        }
    }

    /**
     * After a rescale a writer can restore the hanging transaction of a subtask that another writer
     * owns under the prefix. It leaves it to the owner, and an owner that can't list it fails, so
     * no checkpoint drops it from the state before it is rolled back.
     */
    @Test
    void testRestoredHangingOfOtherSubtaskIsRolledBackByItsOwner() throws Exception {
        byte[] jobId = new JobID().getBytes();
        // subtask 3 of 4 before the downscale to 2, so writer 1 owns it
        TransactionId hanging = prepare("books", new JobID().getBytes(), 3, 4, 5, 1);

        rollbackPreparedTxs("books", jobId, 0, 2, 5, restoredState(hanging));
        assertThat(isPrepared(hanging)).isTrue();

        try (XaTransaction owner =
                new XaTransaction(
                        JdbcExactlyOnceOptions.defaults(),
                        TransactionId.create("books", jobId, 1, 2),
                        failingRecover(newXaConnectionProvider()))) {
            owner.open(JdbcWriterState.empty());
            assertThatThrownBy(() -> owner.rollbackPreparedTxs(5))
                    .isInstanceOf(FlinkRuntimeException.class);
        }
        assertThat(isPrepared(hanging)).isTrue();

        // the restart restores the same state, and now the owner can list
        rollbackPreparedTxs("books", jobId, 0, 2, 5, restoredState(hanging));
        rollbackPreparedTxs("books", jobId, 1, 2, 5, JdbcWriterState.empty());
        assertThat(isPrepared(hanging)).isFalse();
    }

    /** Calls the provider through the handler, which gets the provider to delegate to. */
    private static XaConnectionProvider proxy(
            SimpleXaConnectionProvider delegate, DelegatingHandler handler) {
        InvocationHandler invocationHandler =
                (proxy, method, args) -> {
                    try {
                        return handler.invoke(delegate, method, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                };
        return (XaConnectionProvider)
                Proxy.newProxyInstance(
                        XaTransactionTest.class.getClassLoader(),
                        new Class<?>[] {XaConnectionProvider.class},
                        invocationHandler);
    }

    private interface DelegatingHandler {
        Object invoke(SimpleXaConnectionProvider delegate, Method method, Object[] args)
                throws Throwable;
    }

    private static JdbcWriterState restoredState(TransactionId hanging) {
        return JdbcWriterState.of(Collections.emptyList(), Collections.singletonList(hanging));
    }

    /** Prepares a transaction with one record. */
    private TransactionId prepare(
            byte[] jobId, int subtask, int parallelism, long checkpointId, int id)
            throws Exception {
        return prepare(null, jobId, subtask, parallelism, checkpointId, id);
    }

    private TransactionId prepare(
            @Nullable String prefix,
            byte[] jobId,
            int subtask,
            int parallelism,
            long checkpointId,
            int id)
            throws Exception {
        try (XaTransaction transaction =
                newTransaction(prefix, jobId, subtask, parallelism, JdbcWriterState.empty())) {
            transaction.createTx(checkpointId);
            try (PreparedStatement ps =
                    transaction
                            .getConnectionProvider()
                            .getConnection()
                            .prepareStatement(TEST_TABLE.getInsertIntoQuery())) {
                TEST_TABLE
                        .getStatementBuilder()
                        .accept(ps, new BooksTable.BookEntry(id, "title", "author", 1D, 1));
                ps.execute();
            }
            return transaction.prepareTx().get();
        }
    }

    private void rollbackPreparedTxs(byte[] jobId, int subtask, int parallelism, long checkpointId)
            throws Exception {
        rollbackPreparedTxs(jobId, subtask, parallelism, checkpointId, JdbcWriterState.empty());
    }

    private void rollbackPreparedTxs(
            byte[] jobId, int subtask, int parallelism, long checkpointId, JdbcWriterState state)
            throws Exception {
        rollbackPreparedTxs(null, jobId, subtask, parallelism, checkpointId, state);
    }

    private void rollbackPreparedTxs(
            @Nullable String prefix,
            byte[] jobId,
            int subtask,
            int parallelism,
            long checkpointId,
            JdbcWriterState state)
            throws Exception {
        try (XaTransaction transaction =
                newTransaction(prefix, jobId, subtask, parallelism, state)) {
            transaction.rollbackPreparedTxs(checkpointId);
        }
    }

    private XaTransaction newTransaction(
            @Nullable String prefix,
            byte[] jobId,
            int subtask,
            int parallelism,
            JdbcWriterState state)
            throws Exception {
        XaTransaction transaction =
                new XaTransaction(
                        JdbcExactlyOnceOptions.defaults(),
                        TransactionId.create(prefix, jobId, subtask, parallelism),
                        newXaConnectionProvider());
        transaction.open(state);
        return transaction;
    }

    private boolean isPrepared(TransactionId transactionId) throws Exception {
        try (SimpleXaConnectionProvider xa = newXaConnectionProvider()) {
            xa.open();
            return xa.recover().stream()
                    .anyMatch(
                            xid ->
                                    Arrays.equals(
                                                    xid.getGlobalTransactionId(),
                                                    transactionId.getGlobalTransactionId())
                                            && Arrays.equals(
                                                    xid.getBranchQualifier(),
                                                    transactionId.getBranchQualifier()));
        }
    }

    private SimpleXaConnectionProvider newXaConnectionProvider() {
        return SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource());
    }

    private static XaConnectionProvider failingRecover(SimpleXaConnectionProvider delegate) {
        return proxy(
                XaConnectionProvider.class,
                (method, args) -> {
                    if (method.getName().equals("recover")) {
                        throw new FlinkRuntimeException("XAER_RMERR");
                    }
                    return invoke(delegate, method, args);
                });
    }

    /** Fails the first rollback with XAER_RMFAIL, which pgjdbc returns on a connection error. */
    private static XADataSource failingFirstRollback(XADataSource dataSource) {
        AtomicBoolean failed = new AtomicBoolean();
        return proxy(
                XADataSource.class,
                (method, args) -> {
                    Object result = invoke(dataSource, method, args);
                    if (!method.getName().equals("getXAConnection")) {
                        return result;
                    }
                    XAConnection connection = (XAConnection) result;
                    return proxy(
                            XAConnection.class,
                            (connectionMethod, connectionArgs) -> {
                                Object resource =
                                        invoke(connection, connectionMethod, connectionArgs);
                                if (!connectionMethod.getName().equals("getXAResource")) {
                                    return resource;
                                }
                                return proxy(
                                        XAResource.class,
                                        (resourceMethod, resourceArgs) -> {
                                            if (resourceMethod.getName().equals("rollback")
                                                    && failed.compareAndSet(false, true)) {
                                                throw new XAException(XAException.XAER_RMFAIL);
                                            }
                                            return invoke(resource, resourceMethod, resourceArgs);
                                        });
                            });
                });
    }

    private interface Handler {
        Object handle(Method method, Object[] args) throws Throwable;
    }

    private static <T> T proxy(Class<T> type, Handler handler) {
        return type.cast(
                Proxy.newProxyInstance(
                        XaTransactionTest.class.getClassLoader(),
                        new Class<?>[] {type},
                        (proxy, method, args) -> handler.handle(method, args)));
    }

    private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
