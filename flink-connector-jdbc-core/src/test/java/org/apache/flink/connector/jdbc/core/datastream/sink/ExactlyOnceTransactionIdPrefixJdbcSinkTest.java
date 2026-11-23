/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.core.datastream.sink;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.datasource.connections.xa.SimpleXaConnectionProvider;
import org.apache.flink.connector.jdbc.testutils.TableManaged;
import org.apache.flink.connector.jdbc.testutils.tables.TableRow;
import org.apache.flink.connector.jdbc.testutils.tables.templates.BooksTable;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.testutils.junit.SharedReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Runs the exactly-once sink tests with a transaction id prefix. */
class ExactlyOnceTransactionIdPrefixJdbcSinkTest extends ExactlyOnceJdbcSinkTest {

    private static final TableRow GREEN_TABLE = recoveryTable("GreenSinkTable");

    @Override
    public List<TableManaged> getManagedTables() {
        List<TableManaged> tables = new ArrayList<>(super.getManagedTables());
        tables.add(GREEN_TABLE);
        return tables;
    }

    @Override
    protected <T> JdbcSink<T> finishSink(JdbcSinkBuilder<T> builder) {
        return builder.withExecutionOptions(
                        JdbcExecutionOptions.builder().withMaxRetries(0).build())
                .buildExactlyOnce(
                        JdbcExactlyOnceOptions.builder().withTransactionIdPrefix("books").build(),
                        getMetadata().getXaSourceSupplier());
    }

    /**
     * A job fails after both subtasks let checkpoint 2 expire and one fails a later checkpoint,
     * with transactions prepared for both checkpoints. A new job restores the last completed
     * checkpoint at a lower parallelism and must roll them back, also those its restored state
     * doesn't name.
     */
    @Test
    void testRestoreIntoNewJobAfterAFailure(@TempDir Path checkpoints) throws Exception {
        SharedReference<AtomicInteger> failed = sharedObjects.add(new AtomicInteger());
        Configuration config = new Configuration();
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpoints.toUri().toString());
        config.set(
                CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        RestartStrategyUtils.configureNoRestartStrategy(env);
        env.setParallelism(2);
        env.enableCheckpointing(200L);
        env.getCheckpointConfig().setCheckpointTimeout(300L);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);
        addRecoveryPipeline(env, failed);
        assertThatThrownBy(env::execute).isInstanceOf(JobExecutionException.class);
        assertThat(failed.get().get()).isEqualTo(3);
        try (SimpleXaConnectionProvider xa =
                SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource())) {
            xa.open();
            assertThat(xa.recover()).isNotEmpty();
        }

        config = new Configuration();
        config.set(StateRecoveryOptions.SAVEPOINT_PATH, latestCheckpoint(checkpoints));
        env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        RestartStrategyUtils.configureNoRestartStrategy(env);
        env.setParallelism(1);
        env.enableCheckpointing(200L);
        addRecoveryPipeline(env, failed);
        env.execute();

        assertWrittenOnce(RECOVERY_RECORDS);
    }

    /**
     * A job without a prefix lets checkpoint 2 expire and fails a later one, with transactions
     * prepared for both. It gets a prefix and restarts from its last checkpoint under the same job
     * id, as after a last-state upgrade, and must roll back both: the one its restored state names
     * and the one only the listing finds.
     */
    @Test
    void testRestoreWithNewPrefixUnderSameJobId(@TempDir Path checkpoints) throws Exception {
        JobID jobId = new JobID();
        SharedReference<AtomicInteger> failed = sharedObjects.add(new AtomicInteger());
        Configuration config = pinnedJobId(jobId);
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpoints.toUri().toString());
        config.set(
                CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        RestartStrategyUtils.configureNoRestartStrategy(env);
        env.setParallelism(1);
        env.enableCheckpointing(200L);
        env.getCheckpointConfig().setCheckpointTimeout(CHECKPOINT_TIMEOUT);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);
        addPipeline(
                env,
                // records keep coming while the checkpoint expires
                RateLimiterStrategy.perSecond(40),
                new FailingSnapshotMapper(failed, 2, 2, true),
                sink(RECOVERY_TABLE, null));
        assertThatThrownBy(env::execute).isInstanceOf(JobExecutionException.class);
        assertThat(failed.get().get()).isEqualTo(2);
        try (SimpleXaConnectionProvider xa =
                SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource())) {
            xa.open();
            assertThat(xa.recover()).hasSize(2);
        }

        config = pinnedJobId(jobId);
        config.set(StateRecoveryOptions.SAVEPOINT_PATH, latestCheckpoint(checkpoints));
        env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        RestartStrategyUtils.configureNoRestartStrategy(env);
        env.setParallelism(1);
        env.enableCheckpointing(200L);
        addPipeline(
                env,
                RateLimiterStrategy.perSecond(40),
                new FailingSnapshotMapper(failed, 2, 2, true),
                sink(RECOVERY_TABLE, "books"));
        env.execute();

        assertWrittenOnce(RECOVERY_RECORDS);
    }

    /**
     * Blue and green run at the same time under the same job id, for example because it is pinned,
     * with a prefix each. Green starts from a savepoint of blue while blue holds a transaction
     * prepared for a later checkpoint, which green must leave alone. Blue holds it at the second
     * checkpoint after the savepoint, as a savepoint commits nothing and green commits the
     * transactions that blue prepared for it.
     */
    @Test
    void testBlueGreenWithSameJobId(@TempDir Path savepoints) throws Exception {
        JobID jobId = new JobID();
        SharedReference<AtomicInteger> hold = sharedObjects.add(new AtomicInteger());
        SharedReference<CountDownLatch> held = sharedObjects.add(new CountDownLatch(1));
        SharedReference<CountDownLatch> release = sharedObjects.add(new CountDownLatch(1));
        HoldingSnapshotMapper mapper = new HoldingSnapshotMapper(hold, held, release);

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(pinnedJobId(jobId));
        RestartStrategyUtils.configureNoRestartStrategy(env);
        env.setParallelism(1);
        env.enableCheckpointing(100L);
        addPipeline(
                env, RateLimiterStrategy.perCheckpoint(10), mapper, sink(RECOVERY_TABLE, "blue"));
        JobClient blue = env.executeAsync();
        assertThat(blue.getJobID()).isEqualTo(jobId);
        String savepoint = triggerSavepoint(blue, savepoints);
        hold.get().set(2);
        held.get().await();
        awaitPrepared(1);

        Configuration config = pinnedJobId(jobId);
        config.set(StateRecoveryOptions.SAVEPOINT_PATH, savepoint);
        env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        RestartStrategyUtils.configureNoRestartStrategy(env);
        env.setParallelism(1);
        env.enableCheckpointing(100L);
        addPipeline(env, RateLimiterStrategy.perCheckpoint(10), mapper, sink(GREEN_TABLE, "green"));
        env.execute();
        release.get().countDown();
        blue.getJobExecutionResult().get();

        assertWrittenOnce(RECOVERY_RECORDS);
        List<Integer> green = written(GREEN_TABLE);
        assertThat(green).doesNotHaveDuplicates().contains((int) RECOVERY_RECORDS - 1);
    }

    private void awaitPrepared(int transactions) throws Exception {
        Instant deadline = Instant.now().plusSeconds(60);
        try (SimpleXaConnectionProvider xa =
                SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource())) {
            xa.open();
            while (xa.recover().size() != transactions) {
                assertThat(Instant.now()).isBefore(deadline);
                Thread.sleep(10L);
            }
        }
    }

    private static Configuration pinnedJobId(JobID jobId) {
        Configuration config = new Configuration();
        config.set(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, jobId.toHexString());
        return config;
    }

    private JdbcSink<BooksTable.BookEntry> sink(TableRow table, @Nullable String prefix) {
        JdbcExactlyOnceOptions.JDBCExactlyOnceOptionsBuilder options =
                JdbcExactlyOnceOptions.builder();
        if (prefix != null) {
            options.withTransactionIdPrefix(prefix);
        }
        return recoverySinkBuilder(table)
                .withExecutionOptions(JdbcExecutionOptions.builder().withMaxRetries(0).build())
                .buildExactlyOnce(options.build(), getMetadata().getXaSourceSupplier());
    }

    private static void addPipeline(
            StreamExecutionEnvironment env,
            RateLimiterStrategy rateLimiterStrategy,
            MapFunction<BooksTable.BookEntry, BooksTable.BookEntry> mapper,
            JdbcSink<BooksTable.BookEntry> sink) {
        DataGeneratorSource<BooksTable.BookEntry> books =
                new DataGeneratorSource<>(
                        ExactlyOnceJdbcSinkTest::book,
                        RECOVERY_RECORDS,
                        rateLimiterStrategy,
                        TypeInformation.of(BooksTable.BookEntry.class));
        env.fromSource(books, WatermarkStrategy.noWatermarks(), "books")
                .uid("books")
                .map(mapper)
                .uid("mapper")
                .sinkTo(sink)
                .uid("sink");
    }

    /** Retries until all tasks run, as a savepoint can't be taken before. */
    private static String triggerSavepoint(JobClient client, Path savepoints) throws Exception {
        Instant deadline = Instant.now().plusSeconds(60);
        while (true) {
            try {
                return client.triggerSavepoint(
                                savepoints.toUri().toString(), SavepointFormatType.CANONICAL)
                        .get();
            } catch (ExecutionException e) {
                if (Instant.now().isAfter(deadline)) {
                    throw e;
                }
                Thread.sleep(100L);
            }
        }
    }

    /**
     * Holds the snapshot of the checkpoint that counts {@code hold} down to zero until {@code
     * release}, after the sink has prepared its transaction for that checkpoint.
     */
    private static class HoldingSnapshotMapper
            implements MapFunction<BooksTable.BookEntry, BooksTable.BookEntry>,
                    CheckpointedFunction {

        private final SharedReference<AtomicInteger> hold;
        private final SharedReference<CountDownLatch> held;
        private final SharedReference<CountDownLatch> release;

        HoldingSnapshotMapper(
                SharedReference<AtomicInteger> hold,
                SharedReference<CountDownLatch> held,
                SharedReference<CountDownLatch> release) {
            this.hold = hold;
            this.held = held;
            this.release = release;
        }

        @Override
        public BooksTable.BookEntry map(BooksTable.BookEntry book) {
            return book;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            if (hold.get().getAndUpdate(n -> Math.max(0, n - 1)) == 1) {
                held.get().countDown();
                release.get().await();
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) {}
    }

    private void addRecoveryPipeline(
            StreamExecutionEnvironment env, SharedReference<AtomicInteger> failed) {
        DataGeneratorSource<BooksTable.BookEntry> books =
                new DataGeneratorSource<>(
                        ExactlyOnceJdbcSinkTest::book,
                        RECOVERY_RECORDS,
                        // records keep coming while checkpoints expire
                        RateLimiterStrategy.perSecond(40),
                        TypeInformation.of(BooksTable.BookEntry.class));
        env.fromSource(books, WatermarkStrategy.noWatermarks(), "books")
                .uid("books")
                // both subtasks let checkpoint 2 expire, and one fails a later one
                .map(new FailingSnapshotMapper(failed, 2, 3, true))
                .uid("mapper")
                .sinkTo(recoverySink())
                .uid("sink");
    }

    private static String latestCheckpoint(Path checkpoints) throws IOException {
        try (Stream<Path> files = Files.walk(checkpoints)) {
            return files.filter(path -> path.getFileName().toString().equals("_metadata"))
                    .map(Path::getParent)
                    .max(
                            Comparator.comparingInt(
                                    path ->
                                            Integer.parseInt(
                                                    path.getFileName()
                                                            .toString()
                                                            .substring("chk-".length()))))
                    .orElseThrow(() -> new AssertionError("no completed checkpoint"))
                    .toUri()
                    .toString();
        }
    }
}
