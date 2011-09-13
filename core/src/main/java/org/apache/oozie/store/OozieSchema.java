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
package org.apache.oozie.store;

import java.sql.Blob;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.oozie.util.db.Schema;
import org.apache.oozie.util.db.Schema.Column;
import org.apache.oozie.util.db.Schema.DBType;
import org.apache.oozie.util.db.Schema.Index;
import org.apache.oozie.util.db.Schema.Table;

public class OozieSchema {

    private static String oozieDbName;

    private static final String OOZIE_VERSION = "0.1";

    public static final Map<Table, List<Column>> TABLE_COLUMNS = new HashMap<Table, List<Column>>();

    static {
        for (Column column : OozieColumn.values()) {
            List<Column> tColumns = TABLE_COLUMNS.get(column.table());
            if (tColumns == null) {
                tColumns = new ArrayList<Column>();
                TABLE_COLUMNS.put(column.table(), tColumns);
            }
            tColumns.add(column);
        }
    }

    public static void setOozieDbName(String dbName) {
        oozieDbName = dbName;
    }

    public static enum OozieTable implements Table {
        WORKFLOWS,
        ACTIONS,
        WF_PROCESS_INSTANCE,
        VERSION;

        @Override
        public String toString() {
            return oozieDbName + "." + name().toUpperCase();
        }
    }

    public static enum OozieIndex implements Index {
        IDX_WF_APPNAME(OozieColumn.WF_appName),
        IDX_WF_USER(OozieColumn.WF_userName),
        IDX_WF_GROUP(OozieColumn.WF_groupName),
        IDX_WF_STATUS(OozieColumn.WF_status),
        IDX_WF_EXTERNAL_ID(OozieColumn.WF_externalId),

        IDX_ACTIONS_BEGINTIME(OozieColumn.ACTIONS_pendingAge),
        IDX_ACTIONS_WFID(OozieColumn.ACTIONS_wfId);

        final Column column;

        OozieIndex(Column column) {
            this.column = column;
        }

        public Column column() {
            return column;
        }
    }

    public static enum OozieColumn implements Column {
        // Process Instance Table
        PI_wfId(OozieTable.WF_PROCESS_INSTANCE, String.class, true, 100),
        PI_state(OozieTable.WF_PROCESS_INSTANCE, Blob.class, false),

        // WorkflowJob Table
        WF_id(OozieTable.WORKFLOWS, String.class, true, 100),
        WF_externalId(OozieTable.WORKFLOWS, String.class, false, 100),
        WF_appName(OozieTable.WORKFLOWS, String.class, false, 100),
        WF_appPath(OozieTable.WORKFLOWS, String.class, false, 255),
        WF_conf(OozieTable.WORKFLOWS, String.class, false),
        WF_protoActionConf(OozieTable.WORKFLOWS, String.class, false),
        WF_logToken(OozieTable.WORKFLOWS, String.class, false, 100),
        WF_status(OozieTable.WORKFLOWS, String.class, false, 100),
        WF_run(OozieTable.WORKFLOWS, Long.class, false),
        WF_lastModTime(OozieTable.WORKFLOWS, Timestamp.class, false),
        WF_createdTime(OozieTable.WORKFLOWS, Timestamp.class, false),
        WF_startTime(OozieTable.WORKFLOWS, Timestamp.class, false),
        WF_endTime(OozieTable.WORKFLOWS, Timestamp.class, false),
        WF_userName(OozieTable.WORKFLOWS, String.class, false, 100),
        WF_groupName(OozieTable.WORKFLOWS, String.class, false, 100),
        WF_authToken(OozieTable.WORKFLOWS, String.class, false),

        // Actions Table
        ACTIONS_id(OozieTable.ACTIONS, String.class, true, 100),
        ACTIONS_name(OozieTable.ACTIONS, String.class, false, 100),
        ACTIONS_type(OozieTable.ACTIONS, String.class, false, 100),
        ACTIONS_wfId(OozieTable.ACTIONS, String.class, false, 100),
        ACTIONS_conf(OozieTable.ACTIONS, String.class, false),
        ACTIONS_status(OozieTable.ACTIONS, String.class, false, 100),
        ACTIONS_externalStatus(OozieTable.ACTIONS, String.class, false, 100),
        ACTIONS_errorCode(OozieTable.ACTIONS, String.class, false, 100),
        ACTIONS_errorMessage(OozieTable.ACTIONS, String.class, false),
        ACTIONS_transition(OozieTable.ACTIONS, String.class, false, 100),
        ACTIONS_retries(OozieTable.ACTIONS, Long.class, false),
        ACTIONS_startTime(OozieTable.ACTIONS, Timestamp.class, false),
        ACTIONS_endTime(OozieTable.ACTIONS, Timestamp.class, false),
        ACTIONS_lastCheckTime(OozieTable.ACTIONS, Timestamp.class, false),
        ACTIONS_data(OozieTable.ACTIONS, String.class, false),
        ACTIONS_externalId(OozieTable.ACTIONS, String.class, false, 100),
        ACTIONS_trackerUri(OozieTable.ACTIONS, String.class, false, 100),
        ACTIONS_consoleUrl(OozieTable.ACTIONS, String.class, false, 100),
        ACTIONS_executionPath(OozieTable.ACTIONS, String.class, false, 255),
        ACTIONS_pending(OozieTable.ACTIONS, Boolean.class, false),
        ACTIONS_pendingAge(OozieTable.ACTIONS, Timestamp.class, false),
        ACTIONS_signalValue(OozieTable.ACTIONS, String.class, false, 100),
        ACTIONS_logToken(OozieTable.ACTIONS, String.class, false, 100),

        // Version Table
        VER_versionNumber(OozieTable.VERSION, String.class, false, 255);

        final Table table;
        final Class<?> type;
        int length = -1;
        final boolean isPrimaryKey;

        OozieColumn(Table table, Class<?> type, boolean isPrimaryKey) {
            this(table, type, isPrimaryKey, -1);
        }

        OozieColumn(Table table, Class<?> type, boolean isPrimaryKey, int length) {
            this.table = table;
            this.type = type;
            this.isPrimaryKey = isPrimaryKey;
            this.length = length;
        }

        private String getName() {
            String tName = table.name();
            return tName + "." + columnName();
        }

        public String columnName() {
            return name().split("_")[1].toLowerCase();
        }

        @Override
        public String toString() {
            return getName();
        }

        public Table table() {
            return table;
        }

        public Class<?> getType() {
            return type;
        }

        public int getLength() {
            return length;
        }

        public String asLabel() {
            return name().toUpperCase();
        }

        public boolean isPrimaryKey() {
            return isPrimaryKey;
        }
    }

    /**
     * Generates the create table SQL Statement
     *
     * @param table
     * @param dbType
     * @return SQL Statement to create the table
     */
    public static String generateCreateTableScript(Table table, DBType dbType) {
        return Schema.generateCreateTableScript(table, dbType, TABLE_COLUMNS.get(table));
    }

    /**
     * Gets the query that will be used to validate the connection
     *
     * @param dbName
     * @return
     */
    public static String getValidationQuery(String dbName) {
        return "select count(" + OozieColumn.VER_versionNumber.columnName() + ") from " + dbName + "."
                + OozieTable.VERSION.name().toUpperCase();
    }

    /**
     * Generates the Insert statement to insert the OOZIE_VERSION to table
     *
     * @param dbName
     * @return
     */
    public static String generateInsertVersionScript(String dbName) {
        return "INSERT INTO " + dbName + "." + OozieTable.VERSION.name().toUpperCase() + "("
                + OozieColumn.VER_versionNumber.columnName() + ") VALUES(" + OOZIE_VERSION + ")";
    }

    /**
     * Gets the Oozie Schema Version
     *
     * @return
     */
    public static String getOozieVersion() {
        return OOZIE_VERSION;
    }
}
