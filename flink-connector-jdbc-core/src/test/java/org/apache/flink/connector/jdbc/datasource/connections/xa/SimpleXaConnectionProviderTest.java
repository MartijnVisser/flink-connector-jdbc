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

package org.apache.flink.connector.jdbc.datasource.connections.xa;

import org.apache.flink.connector.jdbc.datasource.transactions.xa.domain.TransactionId;
import org.apache.flink.connector.jdbc.testutils.DatabaseTest;
import org.apache.flink.connector.jdbc.testutils.TableManaged;
import org.apache.flink.connector.jdbc.testutils.tables.templates.BooksTable;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SimpleXaConnectionProvider} against a database with two-phase commit. */
public abstract class SimpleXaConnectionProviderTest implements DatabaseTest {

    private static final BooksTable TEST_TABLE = new BooksTable("XaTable");
    private static final TransactionId TRANSACTION_BASE = TransactionId.empty();

    /** XA drivers return their own {@link Xid} type from recover(), so compare by value. */
    private static final Comparator<Xid> XID_VALUE_COMPARATOR =
            Comparator.comparingInt(Xid::getFormatId)
                    .thenComparing(Xid::getGlobalTransactionId, Arrays::compare)
                    .thenComparing(Xid::getBranchQualifier, Arrays::compare);

    @Override
    public List<TableManaged> getManagedTables() {
        return Collections.singletonList(TEST_TABLE);
    }

    public void assertBooks(List<BooksTable.BookEntry> expected) throws SQLException {
        try (Connection connection = getMetadata().getConnection()) {
            List<BooksTable.BookEntry> current = TEST_TABLE.selectAllTable(connection);
            assertThat(current.size()).isEqualTo(expected.size());
            assertThat(current).isEqualTo(expected);
        }
    }

    /** A failed test can leave a prepared transaction behind, which holds its locks. */
    @AfterEach
    void rollbackPreparedTransactions() throws SQLException {
        try (SimpleXaConnectionProvider xa =
                SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource())) {
            xa.open();
            for (Xid xid : xa.recover()) {
                xa.rollback(xid);
            }
        }
    }

    @Test
    void testSuccessfulTransaction() throws SQLException {
        List<BooksTable.BookEntry> expected =
                Arrays.asList(
                        new BooksTable.BookEntry(1, "title1", "author1", 100D, 10),
                        new BooksTable.BookEntry(2, "title2", "author2", 111D, 11));

        Xid xid = TRANSACTION_BASE.withBranch(System.currentTimeMillis());
        try (SimpleXaConnectionProvider xa =
                SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource())) {
            xa.open();
            // Start transaction
            xa.start(xid);
            Connection connection = xa.getConnection();
            // Do modifications
            try (PreparedStatement ps =
                    connection.prepareStatement(TEST_TABLE.getInsertIntoQuery())) {
                for (BooksTable.BookEntry book : expected) {
                    TEST_TABLE.getStatementBuilder().accept(ps, book);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            // Prepare the transaction
            xa.endAndPrepare(xid);
            // Commit the transaction
            xa.commit(xid, false);
        }

        assertBooks(expected);
    }

    @Test
    void testSuccessfulTransactionInTwoSteps() throws SQLException {
        List<BooksTable.BookEntry> expected =
                Arrays.asList(
                        new BooksTable.BookEntry(1, "title1", "author1", 100D, 10),
                        new BooksTable.BookEntry(2, "title2", "author2", 111D, 11));

        Xid xid = TRANSACTION_BASE.withBranch(System.currentTimeMillis());
        try (SimpleXaConnectionProvider xa =
                SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource())) {
            xa.open();
            // Start transaction
            xa.start(xid);
            // Do modifications
            try (PreparedStatement ps =
                    xa.getConnection().prepareStatement(TEST_TABLE.getInsertIntoQuery())) {
                for (BooksTable.BookEntry book : expected) {
                    TEST_TABLE.getStatementBuilder().accept(ps, book);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            // Prepare the transaction
            xa.endAndPrepare(xid);
        }

        try (SimpleXaConnectionProvider xa =
                SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource())) {
            xa.open();
            // Commit the transaction
            xa.commit(xid, false);
        }

        assertBooks(expected);
    }

    @Test
    void testRecoverFindsTransactionPreparedOnAnotherConnection() throws SQLException {
        Xid xid = TRANSACTION_BASE.withBranch(System.currentTimeMillis());
        try (SimpleXaConnectionProvider xa =
                SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource())) {
            xa.open();
            xa.start(xid);
            try (PreparedStatement ps =
                    xa.getConnection().prepareStatement(TEST_TABLE.getInsertIntoQuery())) {
                TEST_TABLE
                        .getStatementBuilder()
                        .accept(ps, new BooksTable.BookEntry(1, "title1", "author1", 100D, 10));
                ps.execute();
            }
            xa.endAndPrepare(xid);
        }

        try (SimpleXaConnectionProvider xa =
                SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource())) {
            xa.open();
            assertThat(xa.recover()).usingElementComparator(XID_VALUE_COMPARATOR).contains(xid);
            xa.rollback(xid);
            assertThat(xa.recover())
                    .usingElementComparator(XID_VALUE_COMPARATOR)
                    .doesNotContain(xid);
        }

        assertBooks(new ArrayList<>());
    }

    @Test
    void testEmptyFailAndRollbackTransaction() throws SQLException {

        Xid xid = TRANSACTION_BASE.withBranch(System.currentTimeMillis());
        try (SimpleXaConnectionProvider xa =
                SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource())) {
            xa.open();
            // Start transaction
            xa.start(xid);
            // Rollback the transaction
            xa.failAndRollback(xid);
        }

        assertBooks(new ArrayList<>());
    }

    @Test
    void testFailAndRollbackWithTransaction() throws SQLException {
        List<BooksTable.BookEntry> expected =
                Arrays.asList(
                        new BooksTable.BookEntry(1, "title1", "author1", 100D, 10),
                        new BooksTable.BookEntry(2, "title2", "author2", 111D, 11));

        Xid xid = TRANSACTION_BASE.withBranch(System.currentTimeMillis());
        try (SimpleXaConnectionProvider xa =
                SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource())) {
            xa.open();
            // Start transaction
            xa.start(xid);
            Connection connection = xa.getConnection();
            // Do modifications
            try (PreparedStatement ps =
                    connection.prepareStatement(TEST_TABLE.getInsertIntoQuery())) {
                for (BooksTable.BookEntry book : expected) {
                    TEST_TABLE.getStatementBuilder().accept(ps, book);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            // Rollback the transaction
            xa.failAndRollback(xid);
        }

        assertBooks(new ArrayList<>());
    }

    @Test
    void testFailAndRollbackTransactionPreparedOnAnotherConnection() throws SQLException {
        Xid xid = TRANSACTION_BASE.withBranch(System.currentTimeMillis());
        try (SimpleXaConnectionProvider xa =
                SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource())) {
            xa.open();
            xa.start(xid);
            try (PreparedStatement ps =
                    xa.getConnection().prepareStatement(TEST_TABLE.getInsertIntoQuery())) {
                TEST_TABLE
                        .getStatementBuilder()
                        .accept(ps, new BooksTable.BookEntry(1, "title1", "author1", 100D, 10));
                ps.execute();
            }
            xa.endAndPrepare(xid);
        }

        try (SimpleXaConnectionProvider xa =
                SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource())) {
            xa.open();
            xa.failAndRollback(xid);
            assertThat(xa.recover())
                    .usingElementComparator(XID_VALUE_COMPARATOR)
                    .doesNotContain(xid);
        }

        assertBooks(new ArrayList<>());
    }

    @Test
    void testFailAndRollbackUnknownTransaction() throws SQLException {
        Xid xid = TRANSACTION_BASE.withBranch(System.currentTimeMillis());
        try (SimpleXaConnectionProvider xa =
                SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource())) {
            xa.open();
            xa.failAndRollback(xid);
            assertThat(xa.recover())
                    .usingElementComparator(XID_VALUE_COMPARATOR)
                    .doesNotContain(xid);
        }
    }

    /**
     * A transaction that was completed heuristically holds no locks, but the database reports the
     * heuristic outcome on every rollback until the transaction is forgotten.
     */
    @Test
    void testFailAndRollbackForgetsHeuristicallyCompletedTransaction() throws SQLException {
        Xid xid = TRANSACTION_BASE.withBranch(System.currentTimeMillis());
        try (SimpleXaConnectionProvider xa =
                SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource())) {
            xa.open();
            xa.start(xid);
            try (PreparedStatement ps =
                    xa.getConnection().prepareStatement(TEST_TABLE.getInsertIntoQuery())) {
                TEST_TABLE
                        .getStatementBuilder()
                        .accept(ps, new BooksTable.BookEntry(1, "title1", "author1", 100D, 10));
                ps.execute();
            }
            xa.endAndPrepare(xid);
        }

        AtomicBoolean forgotten = new AtomicBoolean();
        try (SimpleXaConnectionProvider xa =
                SimpleXaConnectionProvider.from(
                        heuristicallyCompleted(getMetadata().buildXaDataSource(), forgotten))) {
            xa.open();
            assertThatThrownBy(() -> xa.failAndRollback(xid))
                    .isInstanceOf(FlinkRuntimeException.class);
            assertThat(forgotten).isTrue();

            xa.failAndRollback(xid);
        }

        assertBooks(new ArrayList<>());
    }

    /**
     * A database may roll a transaction back when it ends it with a rollback code, for example
     * {@link XAException#XA_RBTIMEOUT}, and then no longer know it.
     */
    @Test
    void testFailAndRollbackTransactionRolledBackOnEnd() throws SQLException {
        Xid xid = TRANSACTION_BASE.withBranch(System.currentTimeMillis());
        try (SimpleXaConnectionProvider xa =
                SimpleXaConnectionProvider.from(
                        rolledBackOnEnd(getMetadata().buildXaDataSource()))) {
            xa.open();
            xa.failAndRollback(xid);
        }
    }

    /** Ends every transaction with {@link XAException#XA_RBTIMEOUT}. */
    private static XADataSource rolledBackOnEnd(XADataSource dataSource) {
        return wrapResult(
                XADataSource.class,
                dataSource,
                "getXAConnection",
                connection ->
                        wrapResult(
                                XAConnection.class,
                                connection,
                                "getXAResource",
                                resource ->
                                        proxy(
                                                XAResource.class,
                                                (method, args) -> {
                                                    if (method.getName().equals("end")) {
                                                        throw new XAException(
                                                                XAException.XA_RBTIMEOUT);
                                                    }
                                                    return invoke(resource, method, args);
                                                })));
    }

    /**
     * Rolls the transaction back on the first rollback and then reports {@link
     * XAException#XA_HEURMIX} until it is forgotten, as a database does for a transaction that was
     * completed heuristically.
     */
    private static XADataSource heuristicallyCompleted(
            XADataSource dataSource, AtomicBoolean forgotten) {
        AtomicBoolean completed = new AtomicBoolean();
        return wrapResult(
                XADataSource.class,
                dataSource,
                "getXAConnection",
                connection ->
                        wrapResult(
                                XAConnection.class,
                                connection,
                                "getXAResource",
                                resource ->
                                        proxy(
                                                XAResource.class,
                                                (method, args) -> {
                                                    if (method.getName().equals("forget")) {
                                                        forgotten.set(true);
                                                        return null;
                                                    }
                                                    if (method.getName().equals("rollback")
                                                            && !forgotten.get()) {
                                                        if (completed.compareAndSet(false, true)) {
                                                            invoke(resource, method, args);
                                                        }
                                                        throw new XAException(
                                                                XAException.XA_HEURMIX);
                                                    }
                                                    return invoke(resource, method, args);
                                                })));
    }

    private static <T> T wrapResult(
            Class<T> type, Object target, String methodName, Function<Object, Object> wrap) {
        return proxy(
                type,
                (method, args) -> {
                    Object result = invoke(target, method, args);
                    return method.getName().equals(methodName) ? wrap.apply(result) : result;
                });
    }

    private interface Handler {
        Object handle(Method method, Object[] args) throws Throwable;
    }

    private static <T> T proxy(Class<T> type, Handler handler) {
        return type.cast(
                Proxy.newProxyInstance(
                        SimpleXaConnectionProviderTest.class.getClassLoader(),
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

    @Test
    void testRollbackTransaction() throws SQLException {
        List<BooksTable.BookEntry> expected =
                Arrays.asList(
                        new BooksTable.BookEntry(1, "title1", "author1", 100D, 10),
                        new BooksTable.BookEntry(2, "title2", "author2", 111D, 11));

        Xid xid = TRANSACTION_BASE.withBranch(System.currentTimeMillis());
        try (SimpleXaConnectionProvider xa =
                SimpleXaConnectionProvider.from(getMetadata().buildXaDataSource())) {
            xa.open();
            // Start transaction
            xa.start(xid);
            Connection connection = xa.getConnection();
            // Do modifications
            try (PreparedStatement ps =
                    connection.prepareStatement(TEST_TABLE.getInsertIntoQuery())) {
                for (BooksTable.BookEntry book : expected) {
                    TEST_TABLE.getStatementBuilder().accept(ps, book);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            // Prepare the transaction
            xa.endAndPrepare(xid);
            // Rollback the transaction
            xa.rollback(xid);
        }

        assertBooks(new ArrayList<>());
    }
}
