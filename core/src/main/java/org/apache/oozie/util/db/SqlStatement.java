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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.LogFactory;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.db.Schema.Column;
import org.apache.oozie.util.db.Schema.Table;

/**
 * The <code>SqlStatement</code> is used to generate SQL Statements. Prepare the generated Statements and also to parse
 * the resultSets
 */
public abstract class SqlStatement {

    private static XLog log = new XLog(LogFactory.getLog(SqlStatement.class));
    protected boolean forUpdate = false;

    /**
     * <code>ResultSetReader</code> is used to parse the result set and gives methods for getting appropriate type of
     * data given the column name
     */
    public static class ResultSetReader {
        final ResultSet rSet;

        private ResultSetReader(ResultSet rSet) {
            this.rSet = rSet;
        }

        /**
         * Move the Result Set to next record
         *
         * @return true if there is a next record
         * @throws SQLException
         */
        public boolean next() throws SQLException {
            return rSet.next();
        }

        /**
         * Close the Result Set
         *
         * @throws SQLException
         */
        public void close() throws SQLException {
            rSet.close();
        }

        /**
         * Get the Column data given its type and name
         *
         * @param <T> Type of the column
         * @param clazz Class of the Type
         * @param col Column name
         * @return Column data
         * @throws SQLException
         */
        @SuppressWarnings("unchecked")
        public <T> T get(Class<T> clazz, Column col) throws SQLException {
            if (clazz.isAssignableFrom(col.getType())) {
                return (T) rSet.getObject(col.asLabel());
            }
            else {
                if (String.class.equals(clazz)) {
                    return (T) ("" + rSet.getObject(col.asLabel()));
                }
                else {
                    throw new RuntimeException("Column Error : Actual Type [" + col.getType() + "]," + " Requested Type ["
                            + clazz + "] !!");
                }
            }
        }

        /**
         * Get the data for columns with blob type
         *
         * @param col Column name
         * @return Column data
         * @throws SQLException
         */
        public byte[] getByteArray(Column col) throws SQLException {
            byte[] bArray = null;
            if (Blob.class.equals(col.getType())) {
                BufferedInputStream bStream = new BufferedInputStream(rSet.getBinaryStream(col.asLabel()));
                byte[] temp = new byte[1024];
                int num = 0;
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                BufferedOutputStream bOut = new BufferedOutputStream(baos);
                try {
                    while ((num = bStream.read(temp)) != -1) {
                        bOut.write(temp, 0, num);
                    }
                    bOut.flush();
                    bOut.close();
                }
                catch (IOException e) {
                    throw new SQLException(e);
                }
                bArray = baos.toByteArray();
            }
            else {
                throw new RuntimeException("Column Error : Actual Type [" + col.getType() + "]," + " Requested Type ["
                        + Blob.class + "] !!");
            }
            return bArray;
        }

        /**
         * Get a String Column
         *
         * @param col Column Name
         * @return Column data
         * @throws SQLException
         */
        public String getString(Column col) throws SQLException {
            return get(String.class, col);
        }

        /**
         * Get the Timestamp Column
         *
         * @param col Column name
         * @return Column data
         * @throws SQLException
         */
        public Timestamp getTimestamp(Column col) throws SQLException {
            return get(Timestamp.class, col);
        }

        /**
         * Get the Boolean Column
         *
         * @param col Column name
         * @return Column data
         * @throws SQLException
         */
        public Boolean getBoolean(Column col) throws SQLException {
            return get(Boolean.class, col);
        }

        /**
         * Get the Numeric data
         *
         * @param col Column name
         * @return Column data
         * @throws SQLException
         */
        public Long getLong(Column col) throws SQLException {
            return get(Long.class, col);
        }

    }

    /**
     * Construct the condition statement that will be used in the where clause
     */
    public static class Condition {

        final Column column;
        protected StringBuilder sb = new StringBuilder("( ");
        protected List<Object> values = new ArrayList<Object>();

        private Condition(Column column) {
            this.column = column;
            if (column != null) {
                sb.append(column);
            }
        }

        /**
         * Return the Condition string
         */
        public String toString() {
            return sb.toString();
        }
    }

    /**
     * NULL/NOT NULL Condition Generator
     */
    static class Null extends Condition {
        Null(boolean isInvert, Column column) {
            super(column);
            sb.append(" IS");
            sb.append(isInvert ? " NOT" : "");
            sb.append(" NULL ");
            sb.append(" )");
        }
    }

    /**
     * Generate condition statement for IS NULL
     *
     * @param column column name
     * @return IS NULL condition statement
     */
    public static Condition isNull(Column column) {
        return new Null(false, column);
    }

    /**
     * Generate condition statement for IS NOT NULL
     *
     * @param column column name
     * @return IS NOT NULL condition statement
     */
    public static Condition isNotNull(Column column) {
        return new Null(true, column);
    }

    /**
     * LIKE/NOT LIKE Condition Generator
     */
    static class Like extends Condition {
        Like(boolean isInvert, Column column, String value) {
            super(column);
            sb.append(isInvert ? " NOT" : "").append(" LIKE ").append("?").append(" )");
            values.add(value);
        }
    }

    /**
     * Generate condition statement for IS LIKE
     *
     * @param column column name
     * @param value value to be checked
     * @return IS LIKE condition statement
     */
    public static Condition isLike(Column column, String value) {
        return new Like(false, column, value);
    }

    /**
     * Generates condition statement for IS NOT LIKE
     *
     * @param column column name
     * @param value value to be checked
     * @return IS NOT LIKE condition statement
     */
    public static Condition isNotLike(Column column, String value) {
        return new Like(true, column, value);
    }

    /**
     * Comparison condition statement generator
     */
    static class Compare extends Condition {
        Compare(String oper, Column column, Object value) {
            super(column);
            if (value instanceof Column) {
                sb.append(oper).append(value).append(" )");
            }
            else {
                sb.append(oper).append("?").append(" )");
                values.add(value);
            }
        }
    }

    /**
     * Generate Condition statement for equality check
     *
     * @param column
     * @param value
     * @return Equality Condition statement
     */
    public static Condition isEqual(Column column, Object value) {
        return new Compare(" = ", column, value);
    }

    /**
     * Generate InEquality Condition statement
     *
     * @param column
     * @param value
     * @return Inequality Condition statement
     */
    public static Condition isNotEqual(Column column, Object value) {
        return new Compare(" <> ", column, value);
    }

    /**
     * Generate Condition statement for LESS THAN condition checking
     *
     * @param column
     * @param value
     * @return less than condition statement
     */
    public static Condition lessThan(Column column, Object value) {
        return new Compare(" < ", column, value);
    }

    /**
     * Generate Condition statement for GREATER THAN condition checking
     *
     * @param column
     * @param value
     * @return greater than condition statement
     */
    public static Condition greaterThan(Column column, Object value) {
        return new Compare(" > ", column, value);
    }

    /**
     * Generate Condition statement for LESS THAN OR EQUAL condition checking
     *
     * @param column
     * @param value
     * @return less than or equal condition statement
     */
    public static Condition lessThanOrEqual(Column column, Object value) {
        return new Compare(" <= ", column, value);
    }

    /**
     * Generate Condition statement for GREATER THAN OR EQUAL condition checking
     *
     * @param column
     * @param value
     * @return greater than or equal condition statement
     */
    public static Condition greaterThanOrEqual(Column column, Object value) {
        return new Compare(" >= ", column, value);
    }

    /**
     * IN/NOT IN condition statement generator for checking multiple values and for sub queries
     */
    static class In extends Condition {
        In(boolean isInvert, Column column, Object... values) {
            super(column);
            sb.append(isInvert ? " NOT" : "").append(" IN (");
            for (Object value : values) {
                sb.append(" ? ").append(",");
                this.values.add(value);
            }
            // remove last comma
            sb.setLength(sb.length() - 1);
            sb.append(") )");
        }

        In(boolean isInvert, Column column, Select select) {
            super(column);
            sb.append(isInvert ? " NOT" : "").append(" IN (");
            for (Object value : select.values) {
                this.values.add(value);
            }
            sb.append(select.toString());
            sb.append(") )");
        }
    }

    /**
     * IN Condition for checking multiple values
     *
     * @param column
     * @param values
     * @return In condition statement
     */
    public static Condition in(Column column, Object... values) {
        return new In(false, column, values);
    }

    /**
     * NOT IN Condition for checking multiple values
     *
     * @param column
     * @param values
     * @return not in condition statement
     */
    public static Condition notIn(Column column, Object... values) {
        return new In(true, column, values);
    }

    /**
     * Sub query with IN condition
     *
     * @param column
     * @param select
     * @return Sub query using in
     */
    public static Condition in(Column column, Select select) {
        return new In(false, column, select);
    }

    /**
     * Sub query with NOT IN condition
     *
     * @param column
     * @param select
     * @return sub query using not in
     */
    public static Condition notIn(Column column, Select select) {
        return new In(true, column, select);
    }

    /**
     * Generate the Range checking statements
     */
    static class Between extends Condition {
        Between(boolean isInvert, Column column, Object lVal, Object rVal) {
            super(column);
            sb.append(isInvert ? " NOT" : "").append(" BETWEEN ");
            sb.append(" ? ");
            values.add(lVal);
            sb.append(" AND ");
            sb.append(" ? ").append(" )");
            values.add(rVal);
        }
    }

    /**
     * BETWEEN range checking statement
     *
     * @param column
     * @param lVal min value for range checking
     * @param rVal max value for range checking
     * @return between condition statement
     */
    public static Condition between(Column column, Object lVal, Object rVal) {
        return new Between(false, column, lVal, rVal);
    }

    /**
     * NOT BETWEEN range checking statement
     *
     * @param column
     * @param lVal min value for range checking
     * @param rVal max value for range checking
     * @return not between condition statement
     */
    public static Condition notBetween(Column column, Object lVal, Object rVal) {
        return new Between(true, column, lVal, rVal);
    }

    /**
     * Logical AND condition Generator
     *
     * @param conds list of conditions for AND
     * @return AND statement
     */
    public static Condition and(Condition... conds) {
        Condition retVal = new Condition(null);
        if (conds.length >= 2) {
            for (int i = 0; i < conds.length; i++) {
                if (i > 0) {
                    retVal.sb.append(" AND ");
                }
                retVal.sb.append(conds[i].sb);
                retVal.values.addAll(conds[i].values);
            }
        }
        else {
            if (conds.length == 1) {
                return conds[0];
            }
        }
        retVal.sb.append(" )");
        return retVal;
    }

    /**
     * Logical OR condition generator
     *
     * @param conds list of conditions for OR
     * @return OR statement
     */
    public static Condition or(Condition... conds) {
        Condition retVal = new Condition(null);
        if (conds.length >= 2) {
            for (int i = 0; i < conds.length; i++) {
                if (i > 0) {
                    retVal.sb.append(" OR ");
                }
                retVal.sb.append(conds[i].sb);
                retVal.values.addAll(conds[i].values);
            }
        }
        else {
            if (conds.length == 1) {
                return conds[0];
            }
        }
        retVal.sb.append(" )");
        return retVal;
    }

    protected StringBuilder sb = new StringBuilder("");
    protected List<Object> values = new ArrayList<Object>();

    /**
     * Select Statement generator. Generate the SQL Statement for select statements. Provide methods to add WHERE
     * clause, ORDER BY clause, FOR UPDATE clause.
     */
    public static class Select extends SqlStatement {
        private Condition condition;
        private boolean isOdered = false;
        private Column[] orderby = null;
        private boolean[] isAscending = null;
        private boolean isLimitSet = false;

        private Select(Select other) {
            this.condition = other.condition;
            this.sb.append(other.sb);
            this.values.addAll(other.values);
            this.isOdered = other.isOdered;
            this.isLimitSet = other.isLimitSet;
            this.orderby = other.orderby;
            this.isAscending = other.isAscending;
            this.forUpdate = other.forUpdate;
        }

        Select(boolean count, Table... tables) {
            String temp = count ? "COUNT(*)" : "*";
            this.sb.append("SELECT " + temp + " FROM");
            if ((tables != null) && (tables.length > 0)) {
                for (Table table : tables) {
                    this.sb.append(" " + table + " ,");
                }
                // remove comma
                this.sb.setLength(sb.length() - 1);
            }
            else {
                throw new RuntimeException("Need atleast 1 Table !!");
            }
        }

        Select(Column... columns) {
            this.sb.append("SELECT");
            if ((columns != null) && (columns.length > 0)) {
                Set<Table> tables = new HashSet<Table>();
                for (Column column : columns) {
                    tables.add(column.table());
                    this.sb.append(" " + column + " AS " + column.asLabel() + " ,");
                }
                // remove comma
                this.sb.setLength(sb.length() - 1);
                this.sb.append(" FROM");
                for (Table table : tables) {
                    this.sb.append(" " + table + " ,");
                }
                // remove comma
                this.sb.setLength(sb.length() - 1);
            }
            else {
                throw new RuntimeException("Need atleast 1 Column !!");
            }
        }

        /**
         * Set the condition for where clause
         *
         * @param condition condition for where clause
         * @return <code>Select</code> for cascading
         */
        public Select where(Condition condition) {
            Select retVal = new Select(this);
            retVal.condition = condition;
            retVal.values.addAll(condition.values);
            return retVal;
        }

        /**
         * Sets the column to sort and the order of sort
         *
         * @param column column to sort
         * @param order true = ascending
         * @return <code>Select</code> for cascading
         */
        public Select orderBy(Column column, boolean order) {
            if (!isOdered) {
                Select retVal = new Select(this);
                retVal.orderby = new Column[]{column};
                retVal.isAscending = new boolean[]{order};
                retVal.isOdered = true;
                return retVal;
            }
            return this;
        }

        /**
         * To sort 2 columns
         *
         * @param column0 First column to be sorted
         * @param order0 true = ascending
         * @param column1 Second column to be sorted
         * @param order1 true = ascending
         * @return <code>Select</code> for cascading
         */
        public Select orderBy(Column column0, boolean order0, Column column1, boolean order1) {
            if (!isOdered) {
                Select retVal = new Select(this);
                retVal.orderby = new Column[]{column0, column1};
                retVal.isAscending = new boolean[]{order0, order1};
                retVal.isOdered = true;
                return retVal;
            }
            return this;
        }

        /**
         * Setting the offset and limit for LIMIT clause
         *
         * @param offset
         * @param limit
         * @return <code>Select</code> for cascading
         */
        public Select limit(int offset, int limit) {
            if (isOdered) {
                Select retVal = new Select(this);
                retVal.values.add(limit);
                retVal.values.add(offset);
                retVal.isLimitSet = true;
                return retVal;
            }
            return this;
        }

        /**
         * Set the "for update" flag to lock the rows for updating
         *
         * @return <code>Select</code> for cascading
         */
        // TODO Not working for hsql
        public Select forUpdate() {
            Select retVal = new Select(this);
            retVal.forUpdate = true;
            return retVal;
        }

        /**
         * Generate the SQL Select Statement with conditions and other clauses that were set
         */
        public String toString() {
            String oBy = "";
            if ((orderby != null) && (isAscending != null) && (orderby.length == isAscending.length)) {
                StringBuffer osb = new StringBuffer(" ORDER BY ");
                int i = 0;
                for (Column column : orderby) {
                    osb.append(column.asLabel()).append(isAscending[i] ? " ASC ," : " DESC ,");
                }
                osb.setLength(osb.length() - 1);
                if (isLimitSet) {
                    osb.append("LIMIT ").append("?").append(" OFFSET ").append("?").append(" ");
                }
                oBy = osb.toString();
            }
            return sb.toString() + ((condition != null) ? "WHERE " + condition.toString() : "") + oBy;
        }
    }

    /**
     * SQL Statement generator for DELETE Statements
     */
    public static class Delete extends SqlStatement {
        Condition condition;
        final Table table;

        Delete(Table table) {
            this.table = table;
            this.sb.append("DELETE FROM " + table + " ");
        }

        private Delete(Delete other) {
            this.table = other.table;
            this.condition = other.condition;
            this.sb.append(other.sb);
            this.values.addAll(other.values);
        }

        /**
         * Set the where clause for DELETE
         *
         * @param condition condition for where clause
         * @return <code>Delete</code> for cascading
         */
        public Delete where(Condition condition) {
            Delete retVal = new Delete(this);
            retVal.condition = condition;
            retVal.values.addAll(condition.values);
            return retVal;
        }

        /**
         * Return the DELETE Statement
         */
        public String toString() {
            return sb.toString() + ((condition != null) ? "WHERE " + condition.toString() : "");
        }
    }

    /**
     * UPDATE SQL Statement generator
     */
    public static class Update extends SqlStatement {
        Condition condition;
        final Table table;

        Update(Table table) {
            this.table = table;
            this.sb.append("UPDATE " + table + " SET ");
        }

        private Update(Update other) {
            this.table = other.table;
            this.condition = other.condition;
            this.sb.append(other.sb);
            this.values.addAll(other.values);
        }

        /**
         * SET clause for update statement
         *
         * @param column column name
         * @param value A temporary place holder which can be replaced while preparing
         * @return <code>Update</code> for cascading
         */
        public Update set(Column column, Object value) {
            Update retVal = new Update(this);
            retVal.sb.append((values.isEmpty() ? "" : ", ") + column + " = ? ");
            retVal.values.add(value);
            return retVal;
        }

        /**
         * Set condition for updating
         *
         * @param condition condition for where clause
         * @return <code>Update</code> for cascading
         */
        public Update where(Condition condition) {
            Update retVal = new Update(this);
            retVal.condition = condition;
            retVal.values.addAll(condition.values);
            return retVal;
        }

        /**
         * Return the UPDATE statement
         */
        public String toString() {
            return sb.toString() + ((condition != null) ? " WHERE " + condition.toString() : "");
        }
    }

    /**
     * INSERT Statement generator
     */
    public static class Insert extends SqlStatement {
        StringBuilder sbCol = new StringBuilder("");
        StringBuilder sbVal = new StringBuilder("");
        boolean isFirst = true;
        final Table table;

        Insert(Table table) {
            this.table = table;
            this.sbCol.append("INSERT INTO " + table + " ( )");
            this.sbVal.append("VALUES ( )");
        }

        private Insert(Insert other) {
            this.table = other.table;
            this.sbCol.append(other.sbCol);
            this.sbVal.append(other.sbVal);
            this.values.addAll(other.values);
            if (other.isFirst) {
                this.isFirst = false;
            }
        }

        /**
         * Set the VALUES that are to be inserted
         *
         * @param column
         * @param value A temporary place holder which will be replaced while preparing
         * @return
         */
        public Insert value(Column column, Object value) {
            Insert retVal = new Insert(this);
            retVal.sbCol.setLength(retVal.sbCol.length() - 1);
            retVal.sbVal.setLength(retVal.sbVal.length() - 1);
            retVal.values.add(value);
            retVal.sbCol.append(((isFirst) ? "" : ", ") + column + " )");
            retVal.sbVal.append(((isFirst) ? "" : ", ") + "? )");
            retVal.isFirst = false;
            return retVal;
        }

        /**
         * Return the INSERT Statement
         */
        public String toString() {
            return sbCol.toString() + " " + sbVal.toString();
        }
    }

    /**
     * Prepare the SQL Statement that is generated and assign the values to prepared statement. setValues should be
     * called to set the Real Values for place holders
     *
     * @param conn Connection
     * @return Prepared SQL Statement
     * @throws SQLException
     */
    public PreparedStatement prepareAndSetValues(Connection conn) throws SQLException {
        String stmt = toString();
        if (forUpdate && !Schema.isHsqlConnection(conn)) {
            stmt += " FOR UPDATE";
        }
        PreparedStatement pStmt = conn.prepareStatement(stmt);
        int i = 1;
        for (Object value : this.values) {
            pStmt.setObject(i, value);
            i++;
        }
        log.trace(XLog.Info.get().createPrefix() + " Preparing : " + stmt);
        log.trace(XLog.Info.get().createPrefix() + " Values : " + values);
        return pStmt;
    }

    /**
     * Assign the values to Prepared Statement. setValues should be called to set the Real Values for place holders
     *
     * @param pStmt Prepared Statement
     * @return PreparedStatement with values set
     * @throws SQLException
     */
    public PreparedStatement prepare(PreparedStatement pStmt) throws SQLException {
        int i = 1;
        pStmt.clearParameters();
        for (Object value : this.values) {
            pStmt.setObject(i, value);
            i++;
        }
        return pStmt;
    }

    /**
     * Prepare the SQL Statement. Doesn't set the values.
     *
     * @param conn Connection
     * @return PreparedStatement
     * @throws SQLException
     */
    public PreparedStatement prepare(Connection conn) throws SQLException {
        String stmt = toString();
        if (forUpdate && !Schema.isHsqlConnection(conn)) {
            stmt += " FOR UPDATE";
        }
        return conn.prepareStatement(stmt);
    }

    /**
     * Preparing Multiple statements for batch execution.
     *
     * @param conn Connection
     * @param values A list of maps that contains the actual values
     * @return Prepared Statement
     * @throws SQLException
     */
    public PreparedStatement prepareForBatch(Connection conn, List<? extends Map<Object, Object>> values,
                                             PreparedStatement pStmt) throws SQLException {
        String stmt = toString();
        if (forUpdate && !Schema.isHsqlConnection(conn)) {
            stmt += " FOR UPDATE";
        }
        // PreparedStatement pStmt = conn.prepareStatement(stmt);
        for (Map<Object, Object> map : values) {
            getNewStatementWithValues(map).prepare(pStmt);
            pStmt.addBatch();
        }
        return pStmt;
    }

    /**
     * Replace the place holders with actual values in the sql statement
     *
     * @param oldVal Place holder
     * @param newVal Actual Value
     * @return SQL Statement with oldVal(place holder) replaced with newVal(actual value)
     */
    public SqlStatement setValue(Object oldVal, Object newVal) {
        ArrayList<Object> temp = new ArrayList<Object>(values);
        if (values.contains(oldVal)) {
            int i = temp.indexOf(oldVal);
            temp.set(i, newVal);
        }
        SqlStatement retVal = create(temp);
        return retVal;
    }

    /**
     * Replace the keys(newValues) which are place holders in the sql statements with the corresponding new values. And
     * Gives back a new SQL Statement so that the actual statement can be re-used
     *
     * @param newValues
     * @return A New SQL Statement object with actual values set in its member
     */
    public SqlStatement getNewStatementWithValues(Map<Object, Object> newValues) {
        ArrayList<Object> temp = new ArrayList<Object>();
        for (Object value : values) {
            if (newValues.containsKey(value)) {
                temp.add(newValues.get(value));
            }
            else {
                temp.add(value);
            }
        }
        SqlStatement retVal = create(temp);
        return retVal;
    }

    /**
     * Create the Appropriate SQL Statement with the given values
     *
     * @param temp
     * @return
     */
    private SqlStatement create(ArrayList<Object> temp) {
        SqlStatement retVal = null;
        if (this instanceof Select) {
            retVal = new Select((Select) this);
        }
        else {
            if (this instanceof Insert) {
                retVal = new Insert((Insert) this);
            }
            else {
                if (this instanceof Update) {
                    retVal = new Update((Update) this);
                }
                else {
                    if (this instanceof Delete) {
                        retVal = new Delete((Delete) this);
                    }
                }
            }
        }
        retVal.values.clear();
        retVal.values.addAll(temp);
        return retVal;
    }

    /**
     * Create the <code>ResultSetReader</code> object that has the methods to access the data from the result set
     *
     * @param rSet Result Set
     * @return ResultSet Reader
     */
    public static ResultSetReader parse(ResultSet rSet) {
        return new ResultSetReader(rSet);
    }

    /**
     * Return a new Insert Statement
     *
     * @param table
     * @return Insert statement
     */
    public static Insert insertInto(Table table) {
        return new Insert(table);
    }

    /**
     * Return a new Update Statement
     *
     * @param table
     * @return Update statement
     */
    public static Update update(Table table) {
        return new Update(table);
    }

    /**
     * Return a new Delete Statement
     *
     * @param table
     * @return Delete Statement
     */
    public static Delete deleteFrom(Table table) {
        return new Delete(table);
    }

    /**
     * Return a Select All Statement
     *
     * @param tables
     * @return Select * statement
     */
    public static Select selectAllFrom(Table... tables) {
        return new Select(false, tables);
    }

    /**
     * Return a Select Statement
     *
     * @param columns columns to select
     * @return select statement
     */
    public static Select selectColumns(Column... columns) {
        return new Select(columns);
    }

    /**
     * Select count(*) Statement generator.
     *
     * @param tables
     * @return "select count(*) from tables" statement
     */
    public static Select getCount(Table... tables) {
        return new Select(true, tables);
    }
}
