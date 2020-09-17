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

package org.apache.oozie.command;

import org.apache.commons.collections.CollectionUtils;
import org.apache.oozie.BinaryBlob;
import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.SLAEventBean;
import org.apache.oozie.StringBlob;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.client.rest.JsonSLAEvent;
import org.apache.oozie.service.SchemaCheckerService;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.SLARegistrationBean;
import org.apache.oozie.sla.SLASummaryBean;
import org.apache.oozie.util.Pair;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.db.CompositeIndex;
import org.apache.openjpa.persistence.jdbc.Index;

import javax.persistence.Column;
import javax.persistence.DiscriminatorColumn;
import javax.persistence.DiscriminatorType;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SchemaCheckXCommand extends XCommand<Void> {
    private XLog LOG = XLog.getLog(SchemaCheckXCommand.class);

    private String dbType;
    private String url;
    private String user;
    private String pass;
    private boolean ignoreExtras;

    public SchemaCheckXCommand(String dbType, String url, String user, String pass, boolean ignoreExtras) {
        super("schema-check", "schema-check", 0);
        this.dbType = dbType;
        this.url = url;
        this.user = user;
        this.pass = pass;
        this.ignoreExtras = ignoreExtras;
    }

    @Override
    protected Void execute() throws CommandException {
        Connection conn = null;
        LOG.info("About to check database schema");
        Date startTime = new Date();
        boolean problem = false;
        try {
            conn = DriverManager.getConnection(url, user, pass);
            String catalog = conn.getCatalog();
            DatabaseMetaData metaData = conn.getMetaData();

            Map<String, Class<? extends JsonBean>> tableClasses = new HashMap<String, Class<? extends JsonBean>>();
            tableClasses.put(getTableName(BundleActionBean.class), BundleActionBean.class);
            tableClasses.put(getTableName(BundleJobBean.class), BundleJobBean.class);
            tableClasses.put(getTableName(CoordinatorActionBean.class), CoordinatorActionBean.class);
            tableClasses.put(getTableName(CoordinatorJobBean.class), CoordinatorJobBean.class);
            tableClasses.put(getTableName(JsonSLAEvent.class), JsonSLAEvent.class);
            tableClasses.put(getTableName(SLARegistrationBean.class), SLARegistrationBean.class);
            tableClasses.put(getTableName(SLASummaryBean.class), SLASummaryBean.class);
            tableClasses.put(getTableName(WorkflowActionBean.class), WorkflowActionBean.class);
            tableClasses.put(getTableName(WorkflowJobBean.class), WorkflowJobBean.class);

            boolean tableProblem = checkTables(metaData, catalog, tableClasses.keySet());
            problem = problem | tableProblem;
            if (!tableProblem) {
                for (Map.Entry<String, Class<? extends JsonBean>> table : tableClasses.entrySet()) {
                        TableInfo ti = new TableInfo(table.getValue(), dbType);
                        boolean columnProblem = checkColumns(metaData, catalog, table.getKey(), ti.columnTypes);
                        problem = problem | columnProblem;
                        if (!columnProblem) {
                            boolean primaryKeyProblem = checkPrimaryKey(metaData, catalog, table.getKey(), ti.primaryKeyColumn);
                            problem = problem | primaryKeyProblem;
                            boolean indexProblem = checkIndexes(metaData, catalog, table.getKey(), ti.indexedColumns);
                            problem = problem | indexProblem;
                        }
                    }
            }
            if (problem) {
                LOG.error("Database schema is BAD! Check previous error log messages for details");
            } else {
                LOG.info("Database schema is GOOD");
            }
        } catch (SQLException sqle) {
            LOG.error("An Exception occurred while talking to the database: " + sqle.getMessage(), sqle);
            problem = true;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception e) {
                    LOG.error("An Exception occurred while disconnecting from the database: " + e.getMessage(), e);
                }
            }
            Services.get().get(SchemaCheckerService.class).updateInstrumentation(problem, startTime);
        }
        return null;
    }

    private boolean checkTables(DatabaseMetaData metaData, String catalog, final Collection<String> expectedTablesRaw)
            throws SQLException {
        boolean problem = false;
        Set<String> expectedTables = new HashSet<String>(expectedTablesRaw);
        expectedTables.add(caseTableName("oozie_sys"));
        expectedTables.add(caseTableName("openjpa_sequence_table"));
        expectedTables.add(caseTableName("validate_conn"));
        // Oracle returns > 1000 tables if we don't have the schema "OOZIE"; MySQL and Postgres don't want this
        String schema = null;
        if (dbType.equals("oracle")) {
            schema = "OOZIE";
        }
        ResultSet rs = metaData.getTables(catalog, schema, null, new String[]{"TABLE"});
        Set<String> foundTables = new HashSet<String>();
        while (rs.next()) {
            String tabName = rs.getString("TABLE_NAME");
            if (tabName != null) {
                foundTables.add(tabName);
            }
        }
        Collection missingTables = CollectionUtils.subtract(expectedTables, foundTables);
        if (!missingTables.isEmpty()) {
            LOG.error("Found [{0}] missing tables: {1}", missingTables.size(), Arrays.toString(missingTables.toArray()));
            problem = true;
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("No missing tables found: {0}", Arrays.toString(expectedTables.toArray()));
        }
        if (!ignoreExtras) {
            Collection extraTables = CollectionUtils.subtract(foundTables, expectedTables);
            if (!extraTables.isEmpty()) {
                LOG.error("Found [{0}] extra tables: {1}", extraTables.size(), Arrays.toString(extraTables.toArray()));
                problem = true;
            } else {
                LOG.debug("No extra tables found");
            }
        }
        return problem;
    }

    private boolean checkColumns(DatabaseMetaData metaData, String catalog, String table,
                                 Map<String, Integer> expectedColumnTypes) throws SQLException {
        boolean problem = false;
        Map<String, Pair<Integer, String>> foundColumns = new HashMap<String, Pair<Integer, String>>();
        ResultSet rs = metaData.getColumns(catalog, null, table, null);
        while (rs.next()) {
            String colName = rs.getString("COLUMN_NAME");
            Integer dataType = rs.getInt("DATA_TYPE");
            String colDef = rs.getString("COLUMN_DEF");
            if (colName != null) {
                foundColumns.put(colName, new Pair<Integer, String>(dataType, colDef));
            }
        }
        Collection missingColumns = CollectionUtils.subtract(expectedColumnTypes.keySet(), foundColumns.keySet());
        if (!missingColumns.isEmpty()) {
            LOG.error("Found [{0}] missing columns in table [{1}]: {2}",
                    missingColumns.size(), table, Arrays.toString(missingColumns.toArray()));
            problem = true;
        } else {
            for (Map.Entry<String, Integer> ent : expectedColumnTypes.entrySet()) {
                if (!foundColumns.get(ent.getKey()).getFirst().equals(ent.getValue())) {
                    LOG.error("Expected column [{0}] in table [{1}] to have type [{2}], but found type [{3}]",
                            ent.getKey(), table, getSQLTypeFromInt(ent.getValue()),
                            getSQLTypeFromInt(foundColumns.get(ent.getKey()).getFirst()));
                    problem = true;
                } else if (foundColumns.get(ent.getKey()).getSecond() != null) {
                    LOG.error("Expected column [{0}] in table [{1}] to have default value [NULL], but found default vale [{2}]",
                            ent.getKey(), table, foundColumns.get(ent.getKey()).getSecond());
                    problem = true;
                } else {
                    LOG.debug("Found column [{0}] in table [{1}] with type [{2}] and default value [NULL]",
                            ent.getKey(), table, getSQLTypeFromInt(ent.getValue()));
                }
            }
        }
        if (!ignoreExtras) {
            Collection extraColumns = CollectionUtils.subtract(foundColumns.keySet(), expectedColumnTypes.keySet());
            if (!extraColumns.isEmpty()) {
                LOG.error("Found [{0}] extra columns in table [{1}]: {2}",
                        extraColumns.size(), table, Arrays.toString(extraColumns.toArray()));
                problem = true;
            } else {
                LOG.debug("No extra columns found in table [{0}]", table);
            }
        }
        return problem;
    }

    private boolean checkPrimaryKey(DatabaseMetaData metaData, String catalog, String table, String expectedPrimaryKeyColumn)
            throws SQLException {
        boolean problem = false;
        ResultSet rs = metaData.getPrimaryKeys(catalog, null, table);
        if (!rs.next()) {
            LOG.error("Expected column [{0}] to be the primary key in table [{1}], but none were found",
                    expectedPrimaryKeyColumn, table);
            problem = true;
        } else {
            String foundPrimaryKeyColumn = rs.getString("COLUMN_NAME");
            if (!foundPrimaryKeyColumn.equals(expectedPrimaryKeyColumn)) {
                LOG.error("Expected column [{0}] to be the primary key in table [{1}], but found column [{2}] instead",
                        expectedPrimaryKeyColumn, table, foundPrimaryKeyColumn);
                problem = true;
            } else {
                LOG.debug("Found column [{0}] to be the primary key in table [{1}]", expectedPrimaryKeyColumn, table);
            }
        }
        return problem;
    }

    private boolean checkIndexes(DatabaseMetaData metaData, String catalog, String table, Set<String> expectedIndexedColumns)
            throws SQLException {
        boolean problem = false;
        Set<String> foundIndexedColumns = new HashSet<String>();
        ResultSet rs = metaData.getIndexInfo(catalog, null, table, false, true);
        while (rs.next()) {
            String colName = rs.getString("COLUMN_NAME");
            String indexName = rs.getString("INDEX_NAME");
            final boolean isExtraIndexedColumn = !CompositeIndex.find(indexName) && colName != null;
            if (isExtraIndexedColumn) {
                foundIndexedColumns.add(colName);
            }
        }
        Collection missingIndexColumns = CollectionUtils.subtract(expectedIndexedColumns, foundIndexedColumns);
        if (!missingIndexColumns.isEmpty()) {
            LOG.error("Found [{0}] missing indexes for columns in table [{1}]: {2}",
                    missingIndexColumns.size(), table, Arrays.toString(missingIndexColumns.toArray()));
            problem = true;
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("No missing indexes found in table [{0}]: {1}",
                        table, Arrays.toString(expectedIndexedColumns.toArray()));
            }
        }
        if (!ignoreExtras) {
            Collection extraIndexColumns = CollectionUtils.subtract(foundIndexedColumns, expectedIndexedColumns);
            if (!extraIndexColumns.isEmpty()) {
                LOG.error("Found [{0}] extra indexes for columns in table [{1}]: {2}",
                        extraIndexColumns.size(), table, Arrays.toString(extraIndexColumns.toArray()));
                problem = true;
            } else {
                LOG.debug("No extra indexes found in table [{0}]", table);
            }
        }
        return problem;
    }

    private String getTableName(Class<? extends JsonBean> clazz) {
        Table tabAnn = clazz.getAnnotation(Table.class);
        if (tabAnn != null) {
            return caseTableName(tabAnn.name());
        }
        return null;
    }

    private String caseTableName(String name) {
        // MySQL and Oracle wants table names in all caps
        if (dbType.equals("mysql") || dbType.equals("oracle")) {
            return name.toUpperCase();
        }
        // Postgres wants table names in all lowers
        if (dbType.equals("postgresql")) {
            return name.toLowerCase();
        }
        return name;
    }

    private String getSQLTypeFromInt(int t) {
        switch (t) {
            case Types.BIT:
                return "BIT";
            case Types.TINYINT:
                return "TINYINT";
            case Types.SMALLINT:
                return "SMALLINT";
            case Types.INTEGER:
                return "INTEGER";
            case Types.BIGINT:
                return "BIGINT";
            case Types.FLOAT:
                return "FLOAT";
            case Types.REAL:
                return "REAL";
            case Types.DOUBLE:
                return "DOUBLE";
            case Types.NUMERIC:
                return "NUMERIC";
            case Types.DECIMAL:
                return "DECIMAL";
            case Types.CHAR:
                return "CHAR";
            case Types.VARCHAR:
                return "VARCHAR";
            case Types.LONGVARCHAR:
                return "LONGVARCHAR";
            case Types.DATE:
                return "DATE";
            case Types.TIME:
                return "TIME";
            case Types.TIMESTAMP:
                return "TIMESTAMP";
            case Types.BINARY:
                return "BINARY";
            case Types.VARBINARY:
                return "VARBINARY";
            case Types.LONGVARBINARY:
                return "LONGVARBINARY";
            case Types.NULL:
                return "NULL";
            case Types.OTHER:
                return "OTHER";
            case Types.JAVA_OBJECT:
                return "JAVA_OBJECT";
            case Types.DISTINCT:
                return "DISTINCT";
            case Types.STRUCT:
                return "STRUCT";
            case Types.ARRAY:
                return "ARRAY";
            case Types.BLOB:
                return "BLOB";
            case Types.CLOB:
                return "CLOB";
            case Types.REF:
                return "REF";
            case Types.DATALINK:
                return "DATALINK";
            case Types.BOOLEAN:
                return "BOOLEAN";
            case Types.ROWID:
                return "ROWID";
            case Types.NCHAR:
                return "NCHAR";
            case Types.NVARCHAR:
                return "NVARCHAR";
            case Types.LONGNVARCHAR:
                return "LONGNVARCHAR";
            case Types.NCLOB:
                return "NCLOB";
            case Types.SQLXML:
                return "SQLXML";
            default:
                return "unknown";
        }
    }

    private static class TableInfo {
        String primaryKeyColumn;
        Map<String, Integer> columnTypes;
        Set<String> indexedColumns;

        public TableInfo(Class<? extends JsonBean> clazz, String dbType) {
            columnTypes = new HashMap<String, Integer>();
            indexedColumns = new HashSet<String>();
            populate(clazz, dbType);
            // The "SLA_EVENTS" table is made up of two classes (JsonSLAEvent and SLAEventBean), and the reflection doesn't pick up
            // from both automatically, so we have to manually do this
            if (clazz.equals(JsonSLAEvent.class)) {
                populate(SLAEventBean.class, dbType);
            }
        }

        private void populate(Class<? extends JsonBean> clazz, String dbType) {
            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                Column colAnn = field.getAnnotation(Column.class);
                if (colAnn != null) {
                    String name = caseColumnName(colAnn.name(), dbType);
                    boolean isLob = (field.getAnnotation(Lob.class) != null);
                    Integer type = getSQLType(field.getType(), isLob, dbType);
                    columnTypes.put(name, type);
                    boolean isIndex = (field.getAnnotation(Index.class) != null);
                    if (isIndex) {
                        indexedColumns.add(name);
                    }
                    boolean isPrimaryKey = (field.getAnnotation(Id.class) != null);
                    if (isPrimaryKey) {
                        indexedColumns.add(name);
                        primaryKeyColumn = name;
                    }
                } else {
                    // Some Id fields don't have an @Column annotation
                    Id idAnn = field.getAnnotation(Id.class);
                    if (idAnn != null) {
                        String name = caseColumnName(field.getName(), dbType);
                        boolean isLob = (field.getAnnotation(Lob.class) != null);
                        Integer type = getSQLType(field.getType(), isLob, dbType);
                        columnTypes.put(name, type);
                        indexedColumns.add(name);
                        primaryKeyColumn = name;
                    }
                }
            }
            DiscriminatorColumn discAnn = clazz.getAnnotation(DiscriminatorColumn.class);
            if (discAnn != null) {
                String name = caseColumnName(discAnn.name(), dbType);
                Integer type = getSQLType(discAnn.discriminatorType());
                columnTypes.put(name, type);
                indexedColumns.add(name);
            }
        }

        private static Integer getSQLType(Class<?> clazz, boolean isLob, String dbType) {
            if (clazz.equals(String.class)) {
                if (dbType.equals("mysql") && isLob) {
                    return Types.LONGVARCHAR;
                }
                if (dbType.equals("oracle") && isLob) {
                    return Types.CLOB;
                }
                return Types.VARCHAR;
            }
            if (clazz.equals(StringBlob.class) || clazz.equals(BinaryBlob.class)) {
                if (dbType.equals("mysql")) {
                    return Types.LONGVARBINARY;
                }
                if (dbType.equals("oracle")) {
                    return Types.BLOB;
                }
                return Types.BINARY;
            }
            if (clazz.equals(Timestamp.class)) {
                return Types.TIMESTAMP;
            }
            if (clazz.equals(int.class)) {
                if (dbType.equals("oracle")) {
                    return Types.DECIMAL;
                }
                return Types.INTEGER;
            }
            if (clazz.equals(long.class)) {
                if (dbType.equals("oracle")) {
                    return Types.DECIMAL;
                }
                return Types.BIGINT;
            }
            if (clazz.equals(byte.class)) {
                if (dbType.equals("mysql")) {
                    return Types.TINYINT;
                }
                if (dbType.equals("oracle")) {
                    return Types.DECIMAL;
                }
                return Types.SMALLINT;
            }
            return null;
        }

        private static Integer getSQLType(DiscriminatorType discType) {
            switch (discType) {
                case STRING:
                    return Types.VARCHAR;
                case CHAR:
                    return Types.CHAR;
                case INTEGER:
                    return Types.INTEGER;
            }
            return null;
        }

        private static String caseColumnName(String name, String dbType) {
            // Oracle wants column names in all caps
            if (dbType.equals("oracle")) {
                return name.toUpperCase();
            }
            // Postgres and MySQL want column names in all lowers
            if (dbType.equals("postgresql") || dbType.equals("mysql")) {
                return name.toLowerCase();
            }
            return name;
        }
    }

    @Override
    protected void loadState() throws CommandException {
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }

    @Override
    protected boolean isLockRequired() {
        return false;
    }

    @Override
    public String getEntityKey() {
        return null;
    }
}
