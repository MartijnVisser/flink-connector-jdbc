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

package org.apache.flink.connector.jdbc.core.datastream.sink.committer;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.JobInfoImpl;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.TaskInfoImpl;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSinkBuilder;
import org.apache.flink.connector.jdbc.datasource.connections.xa.SimpleXaConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.connections.xa.XaConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.domain.TransactionId;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.exceptions.TransientXaException;
import org.apache.flink.connector.jdbc.derby.DerbyTestBase;
import org.apache.flink.connector.jdbc.testutils.TableManaged;
import org.apache.flink.connector.jdbc.testutils.tables.templates.BooksTable;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.metrics.groups.InternalSinkCommitterMetricGroup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link JdbcCommitter}. */
class JdbcCommitterTest implements DerbyTestBase {

    private static final BooksTable TEST_TABLE = new BooksTable("CommitterTable");
    private static final BooksTable.BookEntry BOOK =
            new BooksTable.BookEntry(1, "title1", "author1", 100D, 10);

    private final TransactionId transactionId =
            TransactionId.create(new JobID().getBytes(), 0, 1).withBranch(1);

    @Override
    public List<TableManaged> getManagedTables() {
        return Collections.singletonList(TEST_TABLE);
    }

    /** A failed test can leave a prepared transaction behind, which holds its locks. */
    @AfterEach
    void rollbackPreparedTransactions() throws SQLException {
        try (SimpleXaConnectionProvider xa = newXaConnectionProvider()) {
            xa.open();
            for (Xid xid : xa.recover()) {
                xa.rollback(xid);
            }
        }
    }

    @Test
    void testCommitTransactionPreparedOnAnotherConnection() throws Exception {
        prepareTransaction();
        TestingCommitRequest request = new TestingCommitRequest(serializeAndDeserialize());

        try (JdbcCommitter committer = newCommitter(newXaConnectionProvider())) {
            committer.commit(Collections.singletonList(request));
        }

        assertThat(request.getFailure()).isNull();
        assertThat(request.getRetryLaterCalls()).isZero();
        assertThat(recover()).doesNotContain(transactionId);
        assertThat(TEST_TABLE.selectAllTable(getMetadata())).containsExactly(BOOK);
    }

    /** After a restore the committer can get a committable it already committed. */
    @Test
    void testCommitAlreadyCommittedTransactionAfterRestore() throws Exception {
        prepareTransaction();
        JdbcCommitable committable = serializeAndDeserialize();

        try (JdbcCommitter committer = newCommitter(newXaConnectionProvider())) {
            committer.commit(Collections.singletonList(new TestingCommitRequest(committable)));
        }
        TestingCommitRequest request = new TestingCommitRequest(committable);
        try (JdbcCommitter committer =
                newCommitter(
                        newXaConnectionProvider(),
                        OptionalLong.of(transactionId.getCheckpointId() + 1))) {
            committer.commit(Collections.singletonList(request));
        }

        assertThat(request.getFailure()).isNull();
        assertThat(request.getRetryLaterCalls()).isZero();
        assertThat(recover()).doesNotContain(transactionId);
        assertThat(TEST_TABLE.selectAllTable(getMetadata())).containsExactly(BOOK);
    }

    /** A transaction the database doesn't know, and that wasn't restored, was never committed. */
    @Test
    void testFailOnUnknownTransaction() throws Exception {
        TestingCommitRequest request = new TestingCommitRequest(serializeAndDeserialize());

        try (JdbcCommitter committer = newCommitter(newXaConnectionProvider())) {
            assertThatThrownBy(() -> committer.commit(Collections.singletonList(request)))
                    .isInstanceOf(FlinkRuntimeException.class);
        }
    }

    /**
     * In BATCH mode the committer commits at the end of input. After a restart it gets all
     * transactions again, including the ones it committed before the failure.
     */
    @ParameterizedTest
    @CsvSource({"BATCH, true", "BATCH, false", "AUTOMATIC, false"})
    void testCommitAlreadyCommittedTransactionAfterRestartAtEndOfInput(
            RuntimeExecutionMode mode, boolean checkpointing) throws Exception {
        prepareTransaction();
        JdbcSink<BooksTable.BookEntry> sink = translatedSink(mode, checkpointing);
        JdbcCommitable committable = serializeAndDeserialize();

        commit(sink, 0, new TestingCommitRequest(committable));
        TestingCommitRequest request = new TestingCommitRequest(committable);
        commit(sink, 1, request);

        assertThat(request.getFailure()).isNull();
        assertThat(recover()).doesNotContain(transactionId);
        assertThat(TEST_TABLE.selectAllTable(getMetadata())).containsExactly(BOOK);
    }

    /**
     * A restarted committer at the end of input can't tell a transaction the database lost from one
     * it committed before, so it skips that one too.
     */
    @Test
    void testSkipLostTransactionAfterRestartAtEndOfInput() throws Exception {
        JdbcSink<BooksTable.BookEntry> sink = translatedSink(RuntimeExecutionMode.BATCH, false);
        TestingCommitRequest request = new TestingCommitRequest(serializeAndDeserialize());

        commit(sink, 1, request);

        assertThat(request.getFailure()).isNull();
        assertThat(TEST_TABLE.selectAllTable(getMetadata())).isEmpty();
    }

    /** Before its first restart, a committer at the end of input has not committed anything. */
    @ParameterizedTest
    @CsvSource({"BATCH, true", "BATCH, false", "AUTOMATIC, false"})
    void testFailOnUnknownTransactionAtEndOfInput(RuntimeExecutionMode mode, boolean checkpointing)
            throws Exception {
        JdbcSink<BooksTable.BookEntry> sink = translatedSink(mode, checkpointing);
        TestingCommitRequest request = new TestingCommitRequest(serializeAndDeserialize());

        assertThatThrownBy(() -> commit(sink, 0, request))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Unable to commit");
    }

    /**
     * In STREAMING mode a restart also restarts the writers, so a restarted committer without a
     * restored checkpoint has not committed any of its transactions. A job in AUTOMATIC mode with
     * checkpointing that runs as BATCH isn't detected and fails the same way.
     */
    @ParameterizedTest
    @CsvSource({"STREAMING, true", "STREAMING, false", "AUTOMATIC, true"})
    void testFailOnUnknownTransactionAfterRestart(RuntimeExecutionMode mode, boolean checkpointing)
            throws Exception {
        JdbcSink<BooksTable.BookEntry> sink = translatedSink(mode, checkpointing);
        TestingCommitRequest request = new TestingCommitRequest(serializeAndDeserialize());

        assertThatThrownBy(() -> commit(sink, 1, request))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Unable to commit");
    }

    @Test
    void testRetryTransientFailureUntilMaxCommitAttempts() throws Exception {
        JdbcCommitable committable = JdbcCommitable.of(transactionId);
        TestingCommitRequest request = new TestingCommitRequest(committable);

        try (JdbcCommitter committer = newCommitter(new TransientlyFailingXaConnectionProvider())) {
            for (int attempt = 1;
                    attempt <= JdbcExactlyOnceOptions.defaults().getMaxCommitAttempts();
                    attempt++) {
                committer.commit(Collections.singletonList(request));
            }
        }

        assertThat(request.getRetryLaterCalls())
                .isEqualTo(JdbcExactlyOnceOptions.defaults().getMaxCommitAttempts() - 1);
        assertThat(request.getFailure()).isInstanceOf(TransientXaException.class);
    }

    private void prepareTransaction() throws SQLException {
        try (SimpleXaConnectionProvider xa = newXaConnectionProvider()) {
            xa.open();
            xa.start(transactionId);
            try (PreparedStatement ps =
                    xa.getConnection().prepareStatement(TEST_TABLE.getInsertIntoQuery())) {
                TEST_TABLE.getStatementBuilder().accept(ps, BOOK);
                ps.execute();
            }
            xa.endAndPrepare(transactionId);
        }
    }

    private JdbcCommitable serializeAndDeserialize() throws IOException {
        JdbcCommitableSerializer serializer = new JdbcCommitableSerializer();
        return serializer.deserialize(
                serializer.getVersion(), serializer.serialize(JdbcCommitable.of(transactionId)));
    }

    private Collection<Xid> recover() throws SQLException {
        try (SimpleXaConnectionProvider xa = newXaConnectionProvider()) {
            xa.open();
            return xa.recover();
        }
    }

    private SimpleXaConnectionProvider newXaConnectionProvider() {
        return SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource());
    }

    private static JdbcCommitter newCommitter(XaConnectionProvider connectionProvider)
            throws IOException {
        return newCommitter(connectionProvider, OptionalLong.empty());
    }

    private static JdbcCommitter newCommitter(
            XaConnectionProvider connectionProvider, OptionalLong restoredCheckpointId)
            throws IOException {
        return new JdbcCommitter(
                DeliveryGuarantee.EXACTLY_ONCE,
                connectionProvider,
                JdbcExactlyOnceOptions.defaults(),
                restoredCheckpointId,
                false);
    }

    /** Translates a job with the sink and returns the copy of the sink that the committer gets. */
    private JdbcSink<BooksTable.BookEntry> translatedSink(
            RuntimeExecutionMode mode, boolean checkpointing) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(mode);
        if (checkpointing) {
            env.enableCheckpointing(100L);
        }
        JdbcSink<BooksTable.BookEntry> sink =
                new JdbcSinkBuilder<BooksTable.BookEntry>()
                        .withQueryStatement(
                                TEST_TABLE.getInsertIntoQuery(), TEST_TABLE.getStatementBuilder())
                        .buildExactlyOnce(
                                JdbcExactlyOnceOptions.defaults(),
                                getMetadata().getXaSourceSupplier());
        env.fromCollection(Collections.singletonList(BOOK)).sinkTo(sink);
        env.getStreamGraph();
        return InstantiationUtil.clone(sink);
    }

    private static void commit(
            JdbcSink<BooksTable.BookEntry> sink, int attemptNumber, TestingCommitRequest request)
            throws Exception {
        try (Committer<JdbcCommitable> committer =
                sink.createCommitter(new TestingCommitterInitContext(attemptNumber))) {
            committer.commit(Collections.singletonList(request));
        }
    }

    /** The context of a committer that has no checkpoint to restore. */
    private static class TestingCommitterInitContext implements CommitterInitContext {

        private final int attemptNumber;

        TestingCommitterInitContext(int attemptNumber) {
            this.attemptNumber = attemptNumber;
        }

        @Override
        public SinkCommitterMetricGroup metricGroup() {
            return InternalSinkCommitterMetricGroup.wrap(
                    UnregisteredMetricsGroup.createOperatorMetricGroup());
        }

        @Override
        public OptionalLong getRestoredCheckpointId() {
            return OptionalLong.empty();
        }

        @Override
        public JobInfo getJobInfo() {
            return new JobInfoImpl(new JobID(), "job");
        }

        @Override
        public TaskInfo getTaskInfo() {
            return new TaskInfoImpl("committer", 1, 0, 1, attemptNumber);
        }
    }

    /** Answers every commit with a transient failure. */
    private static class TransientlyFailingXaConnectionProvider implements XaConnectionProvider {

        private boolean open;

        @Override
        public void open() {
            open = true;
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() {
            open = false;
        }

        @Override
        public void commit(Xid xid, boolean ignoreUnknown) throws TransientXaException {
            throw new TransientXaException(new XAException(XAException.XA_RETRY));
        }

        @Override
        public void start(Xid xid) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void endAndPrepare(Xid xid) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void rollback(Xid xid) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void failAndRollback(Xid xid) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<Xid> recover() {
            return Collections.emptyList();
        }

        @Override
        public Connection getConnection() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isConnectionValid() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Connection getOrEstablishConnection() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void closeConnection() {
            open = false;
        }

        @Override
        public Connection reestablishConnection() {
            throw new UnsupportedOperationException();
        }
    }
}
