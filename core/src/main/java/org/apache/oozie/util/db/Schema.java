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

import java.sql.Connection;
import java.sql.SQLException;

import java.sql.Blob;
import java.sql.Timestamp;
import java.util.List;

public class Schema {
    /**
     * Interface for DB Table
     */
    public static interface Table {
        /**
         * Name of the Table
         *
         * @return
         */
        String name();
    }

    /**
     * Interface or table columns
     */
    public static interface Column {
        /**
         * Table to which the column belongs
         *
         * @return table name
         */
        Table table();

        /**
         * Alias to be used by the select statement for this column
         *
         * @return alias for column
         */
        String asLabel();

        /**
         * Name of the column
         *
         * @return column name
         */
        String columnName();

        /**
         * Returns the datatype of the column
         *
         * @return column type
         */
        Class<?> getType();

        /**
         * Returns the length of the column
         *
         * @return
         */
        int getLength();

        /**
         * Returns if the field is a primary key or not
         *
         * @return true if field is a primary key
         */
        boolean isPrimaryKey();
    }

    /**
     * Interface for Index
     */
    public static interface Index {
        /**
         * Column that is to be indexed
         *
         * @return
         */
        Column column();
    }

    /**
     * DB types
     */
    public static enum DBType {
        HSQL, MySQL, ORACLE;
    }

    //TODO Add the SQL Change catalog for different DBMS.
    /**
     * Returns the appropriate DB type for given column according to the DB Type
     *
     * @param column
     * @param dbType
     * @return column type
     */
    public static String getDbDataType(Column column, DBType dbType) {
        String retVal = null;
        if (String.class.equals(column.getType())) {
            if (column.getLength() < 0) {
                retVal = (dbType.equals(DBType.HSQL) ? "VARCHAR" : (dbType.equals(DBType.ORACLE) ? "CLOB" : "TEXT"));
            }
            else {
                retVal = (dbType.equals(DBType.ORACLE) ? "VARCHAR2(" + column.getLength() + ")" : "VARCHAR(" + column.getLength() + ")");
            }
        }
        else {
            if (Timestamp.class.equals(column.getType())) {
                retVal = (dbType.equals(DBType.ORACLE) ? "DATE" : "DATETIME");
            }
            else {
                if (Boolean.class.equals(column.getType())) {
                    retVal = (dbType.equals(DBType.ORACLE) ? "NUMBER(3, 0)" : "BOOLEAN");
                }
                else {
                    if (Long.class.equals(column.getType())) {
                        retVal = (dbType.equals(DBType.ORACLE) ? "NUMBER (19,0)" : "BIGINT");
                    }
                    else {
                        if (Blob.class.equals(column.getType())) {
                            retVal = (dbType.equals(DBType.MySQL) ? "MEDIUMBLOB" : (dbType.equals(DBType.ORACLE) ? "BLOB" : "LONGVARBINARY"));
                        }
                        else {
                            throw new RuntimeException("Column Type[" + column.getType() + "] not mapped to any DB Data Type !!");
                        }
                    }
                }
            }
        }
        return retVal;
    }

    /**
     * Generates the SQL Statement for creating the table
     *
     * @param table
     * @param dbType
     * @param tableColumns
     * @return CREATE TABLE SQL Statement
     */
    public static String generateCreateTableScript(Table table, DBType dbType, List<Column> tableColumns) {
        StringBuilder sb = new StringBuilder("CREATE TABLE ").append(table).append(" ( ");
        StringBuilder pk = new StringBuilder(", PRIMARY KEY ( ");
        boolean pkFlag = false;
        String sep = "";
        String psep = "";
        for (Column column : tableColumns) {
            sb.append(sep).append(column.columnName() + " ").append(Schema.getDbDataType(column, dbType));
            if (column.isPrimaryKey()) {
                pkFlag = true;
                pk.append(psep).append(column.columnName());
                psep = ", ";
            }
            sep = ", ";
        }
        if (pkFlag) {
            pk.append(" )");
            sb.append(pk.toString());
        }
        sb.append(" )");
        if (dbType == DBType.MySQL) {
            sb.append(" ENGINE=InnoDB");
        }
        return sb.toString();
    }

    /**
     * Generates the SQL Statement for droping the table
     *
     * @param table
     * @param dbType
     * @return DROP TABLE SQL Statement
     */
    public static String generateDropTableScript(Table table, DBType dbType) {
        StringBuilder sb = new StringBuilder("DROP TABLE ").append(table);
        if (dbType == DBType.ORACLE) {
            sb.append(" purge");
        }
        return sb.toString();
    }


    /**
     * Generates the SQL statement for creating the Index
     *
     * @param index
     * @param dbType
     * @return CREATE INDEX SQL Statement
     */
    public static String generateCreateIndexScript(Index index, DBType dbType) {
        StringBuilder sb = new StringBuilder("CREATE INDEX ").append(index).append(" ON ").append(
                index.column().table()).append("( " + index.column().columnName() + " )");
        return sb.toString();
    }

    /**
     * Checks if the given connection's driver is HSQL Database Driver
     *
     * @param conn
     * @return true if the driver is HSQL
     * @throws SQLException
     */
    public static boolean isHsqlConnection(Connection conn) throws SQLException {
        if (conn.getMetaData().getDriverName().toLowerCase().contains(DBType.HSQL.name().toLowerCase())) {
            return true;
        }
        return false;
    }

    /**
     * Checks if the given connection's driver is MySQL Database Driver
     *
     * @param conn
     * @return true if the driver is MySQL
     * @throws SQLException
     */
    public static boolean isMySqlConnection(Connection conn) throws SQLException {
        if (conn.getMetaData().getDriverName().toLowerCase().contains(DBType.MySQL.name().toLowerCase())) {
            return true;
        }
        return false;
    }
}
