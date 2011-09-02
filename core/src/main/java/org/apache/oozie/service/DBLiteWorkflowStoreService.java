/**
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
package org.apache.oozie.service;

import java.util.HashMap;
import java.util.Map;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.Instrumentable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.store.DBWorkflowStore;
import org.apache.oozie.store.OozieSchema;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.store.OozieSchema.OozieIndex;
import org.apache.oozie.store.OozieSchema.OozieTable;
import org.apache.oozie.workflow.WorkflowLib;
import org.apache.oozie.workflow.lite.DBLiteWorkflowLib;
import org.apache.oozie.util.db.Schema;
import org.apache.oozie.util.db.Schema.DBType;
import org.apache.oozie.util.db.Schema.Table;
import org.apache.oozie.util.XLog;
import org.apache.oozie.ErrorCode;

public class DBLiteWorkflowStoreService extends LiteWorkflowStoreService implements Instrumentable {
    private String schemaName;
    private boolean selectForUpdate;
    private XLog log;
    private int statusMetricsCollectionInterval;
    private int statusWindow;

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "DBLiteWorkflowStoreService.";
    public static final String CONF_METRICS_INTERVAL_MINS = CONF_PREFIX + "status.metrics.collection.interval";
    public static final String CONF_METRICS_INTERVAL_WINDOW = CONF_PREFIX + "status.metrics.window";

    public static final String CONF_SCHEMA_NAME = "oozie.db.schema.name";
    public static final String CONF_CREATE_SCHEMA = "oozie.db.schema.create";

    private static final String INSTRUMENTATION_GROUP = "jobstatus";
    private static final String INSTRUMENTATION_GROUP_WINDOW = "windowjobstatus";

    private Map<String, Integer> statusCounts = new HashMap<String, Integer>();
    private Map<String, Integer> statusWindowCounts = new HashMap<String, Integer>();


    /**
     * Gets the number of workflows for each status and populates the hash.
     */
    class JobStatusCountCallable implements Runnable {
        @Override
        public void run() {
            WorkflowStore store = null;
            try {
                store = Services.get().get(WorkflowStoreService.class).create();
                WorkflowJob.Status[] wfStatusArr = WorkflowJob.Status.values();
                for (int i = 0; i < wfStatusArr.length; i++) {
                    statusCounts.put(wfStatusArr[i].name(), store.getWorkflowCountWithStatus(wfStatusArr[i].name()));
                    statusWindowCounts.put(wfStatusArr[i].name(), store
                            .getWorkflowCountWithStatusInLastNSeconds(wfStatusArr[i].name(), statusWindow));
                }
            }
            catch (StoreException e) {
                log.warn("Exception while accessing the store", e);
            }
            finally {
                try {
                    if (store != null) {
                        store.close();
                    }
                }
                catch (StoreException ex) {
                    log.warn("Exception while attempting to close store", ex);
                }
            }
        }
    }

    public void init(Services services) throws ServiceException {
        Configuration conf = services.getConf();
        schemaName = conf.get(CONF_SCHEMA_NAME, "oozie");
        statusWindow = conf.getInt(CONF_METRICS_INTERVAL_WINDOW, 3600);
        statusMetricsCollectionInterval = conf.getInt(CONF_METRICS_INTERVAL_MINS, 5);
        boolean createSchema = conf.getBoolean(CONF_CREATE_SCHEMA, true);
        OozieSchema.setOozieDbName(schemaName);
        String validation_query = OozieSchema.getValidationQuery(schemaName);
        String jdbcUri = conf.get(DataSourceService.CONF_URL, "jdbc:hsqldb:mem:testdb");
        log = XLog.getLog(getClass());
        Connection conn = null;
        try {
            conn = Services.get().get(DataSourceService.class).getRawConnection();
            DBType dbType;
            if (Schema.isHsqlConnection(conn)) {
                dbType = DBType.HSQL;
            }
            else {
                dbType = DBType.MySQL;
            }
            boolean schemaExists = schemaExists(conn, validation_query);

            if (!createSchema && !schemaExists) {
                throw new ServiceException(ErrorCode.E0141,
                        XLog.format("Oozie Schema [{0}] does not exist at [{1}]", schemaName, jdbcUri));
            }
            if (createSchema && schemaExists) {
                log.warn(XLog.OPS, "Oozie Schema [{0}] already exists at [{1}], ignoring create", schemaName, jdbcUri);
            }

            if (createSchema && !schemaExists) {
                setupOozieSchema(conn, dbType);
                log.info(XLog.OPS, "Oozie Schema [{0}] created at [{1}]", schemaName, jdbcUri);
            }

            //switching off select for update for all SQL DBs, using memory locking instead, to avoid long running TRXs
            //checkAndSetSelectForUpdateSupport(conn, validation_query);
            //if (!selectForUpdate) {
            //    log.warn(XLog.OPS, "Database does not support select for update, JDBC URI [{0}]", jdbcUri);
            //}
            selectForUpdate = false;
            
            WorkflowJob.Status[] wfStatusArr = WorkflowJob.Status.values();
            for (int i = 0; i < wfStatusArr.length; i++) {
                statusCounts.put(wfStatusArr[i].name(), 0);
                statusWindowCounts.put(wfStatusArr[i].name(), 0);
            }
            Runnable jobStatusCountCallable = new JobStatusCountCallable();
            services.get(SchedulerService.class).schedule(jobStatusCountCallable, 1,
                    statusMetricsCollectionInterval, SchedulerService.Unit.MIN);
        }
        catch (SQLException e) {
            throw new ServiceException(ErrorCode.E0140, e.getMessage(), e);
        }
        finally {
            if (conn != null) {
                try {
                    conn.close();
                }
                catch (SQLException ex1) {
                    log.warn(XLog.OPS, "JDBC error on close() for [{0}], {1}", jdbcUri, ex1);
                }
            }
        }
    }

    public void destroy() {
    }

    /**
     * Return the workflow lib without DB connection. Will be used for parsing
     * purpose.
     * 
     * @return Workflow Library
     */
    public WorkflowLib getWorkflowLibWithNoDB() {
        return getWorkflowLib(null);
    }

    /**
     * Indicate if the database supports SELECT FOR UPDATE.
     * 
     * @return if the database supports SELECT FOR UPDATE.
     */
    public boolean getSelectForUpdate() {
        return selectForUpdate;
    }

    private WorkflowLib getWorkflowLib(Connection conn) {
        javax.xml.validation.Schema schema = Services.get().get(WorkflowSchemaService.class).getSchema();
        return new DBLiteWorkflowLib(schema, LiteDecisionHandler.class, LiteActionHandler.class, conn);
    }

    public WorkflowStore create() throws StoreException {
        try {
            Connection conn = Services.get().get(DataSourceService.class).getConnection();
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            return new DBWorkflowStore(conn, getWorkflowLib(conn), selectForUpdate);
        }
        catch (SQLException ex) {
            throw new StoreException(ErrorCode.E0600, ex.getMessage(), ex);
        }
    }

    /**
     * Set up the oozie schema, create the tables and indexes. Also insert the
     * Version number to version table.
     * 
     * @param conn DB connection
     * @param dbType DB Type
     * @throws ServiceException On failure to create the schema
     */
    private void setupOozieSchema(Connection conn, DBType dbType) throws ServiceException {
        String errorMessage = "";
        try {
            errorMessage = "Failed to create Schema.\n{0}";
            createSchema(conn, dbType);
            errorMessage = "Failed to create Tables.\n{0}";
            createTables(conn, dbType);
            errorMessage = "Failed to create Indexes.\n{0}";
            createIndexes(conn, dbType);
            errorMessage = "Failed to insert version data";
            doUpdate(conn, OozieSchema.generateInsertVersionScript(schemaName));
            errorMessage = "";
        }
        catch (SQLException e) {
            throw new ServiceException(ErrorCode.E0141, XLog
                    .format(errorMessage, e.getMessage()), e);
        }
    }

    /**
     * Check for the existence of schema by querying the given sql.
     * 
     * @param conn Connection
     * @param sql Sql Statement to check the connection health
     * @return true if schema is present
     */
    private boolean schemaExists(Connection conn, String sql) {
        try {
            conn.createStatement().executeQuery(sql);
            return true;
        }
        catch (SQLException e) {
            return false;
        }
    }

    /**
     * Check for the support for "select for update" by the DB.
     *
     * @param conn Connection
     * @param validation_query query to executed. adds for update to
     *        validation_query and will do the check
     */
    private void checkAndSetSelectForUpdateSupport(Connection conn, String validation_query) {
        try {
            if (validation_query != null) {
                Statement statment = conn.createStatement();
                ResultSet resultSet = statment.executeQuery(validation_query + " for update");
                resultSet.close();
                statment.close();
                selectForUpdate = true;
            }
        }
        catch (SQLException ex) {
            selectForUpdate = false;
        }
    }

    private void createSchema(Connection conn, DBType dbType) throws SQLException {
        doUpdate(conn, "CREATE " + (dbType.equals(DBType.MySQL) ? "DATABASE " : "SCHEMA ") + schemaName
                + (dbType.equals(DBType.HSQL) ? " AUTHORIZATION DBA" : ""));
        log.debug("Created schema [{0}]!!", schemaName);
    }

    private void createTables(Connection conn, DBType dbType) throws SQLException {
        for (Table table : OozieTable.values()) {
            doUpdate(conn, OozieSchema.generateCreateTableScript(table, dbType));
            log.debug("Created Table [{0}]", table);
        }
    }

    private void createIndexes(Connection conn, DBType dbType) throws SQLException {
        for (OozieIndex index : OozieIndex.values()) {
            doUpdate(conn, Schema.generateCreateIndexScript(index, dbType));
            log.debug("Created Index [{0}]", index);
        }
    }

    private void doUpdate(Connection conn, String expression) throws SQLException {
        Statement st = conn.createStatement();
        st.executeUpdate(expression);
        st.close();
    }

    @Override
    public void instrument(Instrumentation instr) {
        final WorkflowJob.Status[] wfStatusArr = WorkflowJob.Status.values();
        for (int i = 0; i < wfStatusArr.length; i++) {
            final String statusName = wfStatusArr[i].name();
            instr.addVariable(INSTRUMENTATION_GROUP, statusName, new Instrumentation.Variable<Long>() {
                public Long getValue() {
                    return statusCounts.get(statusName).longValue();
                }
            });
            instr.addVariable(INSTRUMENTATION_GROUP_WINDOW, statusName, new Instrumentation.Variable<Long>() {
                public Long getValue() {
                    return statusWindowCounts.get(statusName).longValue();
                }
            });
        }
    }
}