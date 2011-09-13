/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.util.db;

import java.sql.Blob;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.db.Schema.Column;
import org.apache.oozie.util.db.Schema.DBType;
import org.apache.oozie.util.db.Schema.Index;
import org.apache.oozie.util.db.Schema.Table;

public class TestSchema extends XTestCase {

    public static Map<Table, List<Column>> TABLE_COLUMNS = new HashMap<Table, List<Column>>();
    private static final String DB_NAME = "testdb";

    static {
        for (Column column : TestColumns.values()) {
            List<Column> tColumns = TABLE_COLUMNS.get(column.table());
            if (tColumns == null) {
                tColumns = new ArrayList<Column>();
                TABLE_COLUMNS.put(column.table(), tColumns);
            }
            tColumns.add(column);
        }
    }

    public static enum TestTable implements Table {
        TEST_TABLE;

        public String toString() {
            return DB_NAME + "." + name();
        }
    }

    public static enum TestColumns implements Column {
        TEST_LONG(TestTable.TEST_TABLE, Long.class, true),
        TEST_STRING(TestTable.TEST_TABLE, String.class, false, 100),
        TEST_TIMESTAMP(TestTable.TEST_TABLE, Timestamp.class, false),
        TEST_BOOLEAN(TestTable.TEST_TABLE, Boolean.class, false),
        TEST_BLOB(TestTable.TEST_TABLE, Blob.class, false);

        final Table table;
        final Class<?> type;
        int length = -1;
        final boolean isPrimaryKey;

        TestColumns(Table table, Class<?> type, boolean isPrimaryKey) {
            this(table, type, isPrimaryKey, -1);
        }

        TestColumns(Table table, Class<?> type, boolean isPrimaryKey, int length) {
            this.table = table;
            this.type = type;
            this.isPrimaryKey = isPrimaryKey;
            this.length = length;
        }

        @Override
        public String asLabel() {
            return columnName();
        }

        @Override
        public String columnName() {
            return name();
        }

        @Override
        public int getLength() {
            return length;
        }

        @Override
        public Class<?> getType() {
            return type;
        }

        @Override
        public boolean isPrimaryKey() {
            return isPrimaryKey;
        }

        @Override
        public Table table() {
            return table;
        }
    }

    public static enum TestIndex implements Index {
        TEST_INDEX(TestColumns.TEST_LONG);

        private Column idxCol;

        private TestIndex(Column idxCol) {
            this.idxCol = idxCol;
        }

        @Override
        public Column column() {
            return idxCol;
        }
    }

    public static void prepareDB(Connection conn) throws SQLException {
        DBType type = DBType.ORACLE;
        if (Schema.isHsqlConnection(conn)) {
            type = DBType.HSQL;
        }
        else {
            if (Schema.isMySqlConnection(conn)) {
                type = DBType.MySQL;
            }
        }

        if (!type.equals(DBType.ORACLE)) {
            conn.prepareStatement(
                    "CREATE " + (type.equals(DBType.MySQL) ? "DATABASE " : "SCHEMA ") + DB_NAME
                            + (type.equals(DBType.HSQL) ? " AUTHORIZATION DBA" : "")).execute();
        }
        for (Table table : TABLE_COLUMNS.keySet()) {
            String createStmt = Schema.generateCreateTableScript(table, type, TABLE_COLUMNS.get(table));
            conn.prepareStatement(createStmt).execute();
        }
    }

    public static void dropSchema(Connection conn) throws SQLException {
        DBType type = DBType.ORACLE;
        if (Schema.isHsqlConnection(conn)) {
            type = DBType.HSQL;
        }
        else {
            if (Schema.isMySqlConnection(conn)) {
                type = DBType.MySQL;
            }
            else {
                // do not drop database for oracle, only drop tables
                dropTables(conn);
                return;
            }
        }

        conn.prepareStatement("DROP " + ((type.equals(DBType.MySQL) || type.equals(DBType.ORACLE)) ? "DATABASE " : "SCHEMA ") +
                ((type.equals(DBType.MySQL) || type.equals(DBType.HSQL)) ? DB_NAME : "") +
                (type.equals(DBType.HSQL) ? " CASCADE" : "")).execute();
    }

    public static void dropTables(Connection conn) throws SQLException {
        DBType type = DBType.ORACLE;
        if (Schema.isHsqlConnection(conn)) {
            type = DBType.HSQL;
        }
        else {
            if (Schema.isMySqlConnection(conn)) {
                type = DBType.MySQL;
            }
        }
        for (Table table : TABLE_COLUMNS.keySet()) {
            String dropStmt = Schema.generateDropTableScript(table, type);
            conn.prepareStatement(dropStmt).execute();
        }
    }

    public void testGenerateCreateScript() throws SQLException {
        Connection conn = getDirectConnection();
        prepareDB(conn);
        ResultSet rs = conn.prepareStatement("SELECT COUNT(*) FROM " + TestTable.TEST_TABLE).executeQuery();
        rs.next();
        assertEquals(0, rs.getInt(1));
        rs.close();
        conn.prepareStatement(
                "INSERT INTO " + TestTable.TEST_TABLE + "(" + TestColumns.TEST_LONG + ", " + TestColumns.TEST_STRING
                        + ")" + " VALUES(1, 'abcd')").executeUpdate();
        rs = conn.prepareStatement("SELECT COUNT(*) FROM " + TestTable.TEST_TABLE).executeQuery();
        rs.next();
        assertEquals(1, rs.getInt(1));
        rs.close();
        boolean pkeyTest = false;
        try {
            conn.prepareStatement(
                    "INSERT INTO " + TestTable.TEST_TABLE + "(" + TestColumns.TEST_LONG + ", "
                            + TestColumns.TEST_STRING + ")" + " VALUES(1, 'abcd')").executeUpdate();
        }
        catch (SQLException e) {
            pkeyTest = true;
        }
        assertEquals(true, pkeyTest);
        String indexStmt = Schema.generateCreateIndexScript(TestIndex.TEST_INDEX, DBType.HSQL);
        conn.prepareStatement(indexStmt).execute();// Will throw an exception if
        // index cant be created
        conn.prepareStatement("DROP TABLE " + TestTable.TEST_TABLE).execute();
        dropSchema(conn);
        conn.close();
    }

    public static Connection getDirectConnection() throws SQLException {
        String driver = "org.hsqldb.jdbcDriver";
        String url = "jdbc:hsqldb:mem:testdb";
        String user = "sa";
        String password = "";
        try {
            Class.forName(driver);
        }
        catch (ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        }
        Connection conn = DriverManager.getConnection(url, user, password);
        conn.setAutoCommit(true);
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        return conn;
    }
}
