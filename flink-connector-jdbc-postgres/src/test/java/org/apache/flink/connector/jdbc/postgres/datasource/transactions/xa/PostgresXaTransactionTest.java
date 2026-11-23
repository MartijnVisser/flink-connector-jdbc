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

package org.apache.flink.connector.jdbc.postgres.datasource.transactions.xa;

import org.apache.flink.connector.jdbc.datasource.transactions.xa.XaTransactionTest;
import org.apache.flink.connector.jdbc.postgres.PostgresTestBase;

import org.postgresql.xa.PGXADataSource;

import javax.sql.XADataSource;

import java.sql.Connection;
import java.sql.Statement;

/** The {@link XaTransactionTest} for PostgreSQL. */
class PostgresXaTransactionTest extends XaTransactionTest implements PostgresTestBase {

    /** Only the user that prepared a transaction, or a superuser, can roll it back. */
    @Override
    protected XADataSource xaDataSourceOfAnotherUser() throws Exception {
        try (Connection connection = getMetadata().getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'another')"
                            + " THEN CREATE ROLE another LOGIN PASSWORD 'another'; END IF; END $$");
        }
        PGXADataSource dataSource = new PGXADataSource();
        dataSource.setUrl(getMetadata().getJdbcUrl());
        dataSource.setUser("another");
        dataSource.setPassword("another");
        return dataSource;
    }
}
