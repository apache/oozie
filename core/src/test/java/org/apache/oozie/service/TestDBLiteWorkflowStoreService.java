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

import org.apache.oozie.util.db.Schema.DBType;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.service.StoreService;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.DBLiteWorkflowStoreService;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.store.OozieSchema;
import org.apache.oozie.util.db.Schema;
import org.apache.oozie.ErrorCode;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class TestDBLiteWorkflowStoreService extends XTestCase {

    DBType dbType = DBType.ORACLE;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        Services services = new Services();
        services.init();
        Connection conn = services.get(StoreService.class).getRawConnection();
        if (Schema.isHsqlConnection(conn)) {
            dbType = DBType.HSQL;
        }
        else {
            if (Schema.isMySqlConnection(conn)) {
                dbType = DBType.MySQL;
            }
        }
        services.destroy();
    }

    /* not used
        @SuppressWarnings({"ConstantConditions"})
        public void testCreateDBFalseWithNoDB() throws Exception {
            Services services = null;
            try {
                setSystemProperty(DBLiteWorkflowStoreService.CONF_CREATE_SCHEMA, "false");
                services = new Services();
                if (dbType.equals(DBType.ORACLE)) {
                    dropTables(services.getConf());
                } else {
                    cleanUpDB(services.getConf());
                }
                services.init();
                fail();
            }
            catch (ServiceException ex) {
                assertEquals(ErrorCode.E0141, ex.getErrorCode());
            }
            finally {
                services.destroy();
            }
        }
    */
    @SuppressWarnings({"ConstantConditions"})
    public void testCreateDBTrueWithNoDB() throws Exception {
        Services services = null;
        try {
            setSystemProperty(DBLiteWorkflowStoreService.CONF_CREATE_SCHEMA, "true");
            services = new Services();
            services.init();
        }
        finally {
            services.destroy();
        }
    }

    /*
    @SuppressWarnings({"ConstantConditions"})
    public void testCreateDBFalseWithDB() throws Exception {
        Services services = null;
        try {
            setSystemProperty(Services.CONF_SERVICE_CLASSES,
                    "org.apache.oozie.service.StoreService, org.apache.oozie.service.SchedulerService");
            services = new Services();
            if (dbType.equals(DBType.ORACLE)) {
                dropTables(services.getConf());
            } else {
                cleanUpDB(services.getConf());
            }
            services.init();
            Configuration conf = services.getConf();
            String dbName = conf.get(DBLiteWorkflowStoreService.CONF_SCHEMA_NAME);
            Connection conn = services.get(StoreService.class).getRawConnection();
            Statement st = conn.createStatement();
            if (!dbType.equals(DBType.ORACLE)) {
                st.executeUpdate(generateCreateSchema(dbType, dbName));
            }
            for (Schema.Table table : OozieSchema.OozieTable.values()) {
                st.executeUpdate(OozieSchema.generateCreateTableScript(table, dbType));
            }
            st.executeUpdate(OozieSchema.generateInsertVersionScript(dbName));
            st.close();
            conn.close();
            conf.set(DBLiteWorkflowStoreService.CONF_CREATE_SCHEMA, "false");
            DBLiteWorkflowStoreService wfss = new DBLiteWorkflowStoreService();
            wfss.init(services);
        }
        finally {
            services.destroy();
        }
    }

    @SuppressWarnings({"ConstantConditions"})
    public void testCreateDBTrueWithDB() throws Exception {
        Services services = null;
        try {
            setSystemProperty(Services.CONF_SERVICE_CLASSES,
                    "org.apache.oozie.service.StoreService, org.apache.oozie.service.SchedulerService");
            services = new Services();
            if (dbType.equals(DBType.ORACLE)) {
                dropTables(services.getConf());
            } else {
                cleanUpDB(services.getConf());
            }
            services.init();
            Configuration conf = services.getConf();
            String dbName = conf.get(DBLiteWorkflowStoreService.CONF_SCHEMA_NAME);
            Connection conn = services.get(StoreService.class).getRawConnection();
            Statement st = conn.createStatement();
            if (!dbType.equals(DBType.ORACLE)) {
                st.executeUpdate(generateCreateSchema(dbType, dbName));
            }
            for (Schema.Table table : OozieSchema.OozieTable.values()) {
                st.executeUpdate(OozieSchema.generateCreateTableScript(table, dbType));
            }
            st.executeUpdate(OozieSchema.generateInsertVersionScript(dbName));
            st.close();
            conn.close();
            conf.set(DBLiteWorkflowStoreService.CONF_CREATE_SCHEMA, "true");
            DBLiteWorkflowStoreService wfss = new DBLiteWorkflowStoreService();
            wfss.init(services);
        }
        finally {
            services.destroy();
        }
    }
*/
    private String generateCreateSchema(DBType dbType, String dbName) throws SQLException {
        return "CREATE " + ((dbType.equals(DBType.MySQL) || dbType.equals(DBType.ORACLE)) ? "DATABASE " : "SCHEMA ") + dbName
                + (dbType.equals(DBType.HSQL) ? " AUTHORIZATION DBA" : "");
    }
}
