/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.adapter.hbase;

import org.apache.calcite.linq4j.function.Function1;
import org.junit.Test;

import java.io.PrintStream;
import java.net.URL;
import java.sql.*;
import java.util.Properties;

public class HBaseTest {

    /**
     * Reads from a table.
     */
    @Test
    public void testSelect() throws SQLException {
        checkSql("model", "select * from test");
    }

    private void checkSql(String model, String sql) throws SQLException {
        checkSql(sql, model, output());
    }

    private Function1<ResultSet, Void> output() {
        return new Function1<ResultSet, Void>() {
            public Void apply(ResultSet resultSet) {
                try {
                    output(resultSet, System.out);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                return null;
            }
        };
    }

    private void output(ResultSet resultSet, PrintStream out)
            throws SQLException {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {
            for (int i = 1;; i++) {
                out.print(resultSet.getString(i));
                if (i < columnCount) {
                    out.print(", ");
                } else {
                    out.println();
                    break;
                }
            }
        }
    }

    private void checkSql(String sql, String model, Function1<ResultSet, Void> fn)
            throws SQLException {
        Connection connection = null;
        Statement statement = null;
        try {
            Properties info = new Properties();
            info.put("model", jsonPath(model));
            connection = DriverManager.getConnection("jdbc:calcite:", info);
            statement = connection.createStatement();
            final ResultSet resultSet =
                    statement.executeQuery(
                            sql);
            fn.apply(resultSet);
        } finally {
            close(connection, statement);
        }
    }

    private String jsonPath(String model) {
        final URL url = HBaseTest.class.getResource("/" + model + ".json");
        String s = url.toString();
        if (s.startsWith("file:")) {
            s = s.substring("file:".length());
        }
        return s;
    }

    private void close(Connection connection, Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                // ignore
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }
}

