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

package org.apache.oozie.service;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.command.SchemaCheckXCommand;
import org.apache.oozie.util.Instrumentable;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XLog;

import java.util.Date;

public class SchemaCheckerService implements Service, Instrumentable {
    private XLog LOG = XLog.getLog(SchemaCheckerService.class);

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "SchemaCheckerService.";
    public static final String CONF_INTERVAL = CONF_PREFIX + "check.interval";
    public static final String CONF_IGNORE_EXTRAS = CONF_PREFIX + "ignore.extras";

    private String status = "N/A (not yet run)";
    private String lastCheck = "N/A";

    @Override
    public void init(Services services) throws ServiceException {
        String url = ConfigurationService.get(JPAService.CONF_URL);
        String dbType = url.substring("jdbc:".length());
        dbType = dbType.substring(0, dbType.indexOf(":"));

        int interval = ConfigurationService.getInt(CONF_INTERVAL);
        if (dbType.equals("derby") || dbType.equals("hsqldb") || dbType.equals("sqlserver") || interval <= 0) {
            LOG.debug("SchemaCheckerService is disabled: not supported for {0}", dbType);
            status = "DISABLED (" + dbType + " not supported)";
        } else {
            String driver = ConfigurationService.get(JPAService.CONF_DRIVER);
            String user = ConfigurationService.get(JPAService.CONF_USERNAME);
            String pass = ConfigurationService.getPassword(JPAService.CONF_PASSWORD, "");
            boolean ignoreExtras = ConfigurationService.getBoolean(CONF_IGNORE_EXTRAS);

            try {
                Class.forName(driver).newInstance();
            } catch (Exception ex) {
                throw new ServiceException(ErrorCode.E0100, getClass().getName(), ex);
            }
            Runnable schemaCheckerRunnable = new SchemaCheckerRunnable(dbType, url, user, pass, ignoreExtras);
            services.get(SchedulerService.class).schedule(schemaCheckerRunnable, 0, interval, SchedulerService.Unit.HOUR);
        }
    }

    @Override
    public void destroy() {
    }

    @Override
    public Class<? extends Service> getInterface() {
        return SchemaCheckerService.class;
    }

    @Override
    public void instrument(Instrumentation instr) {
        instr.addVariable("schema-checker", "status", new Instrumentation.Variable<String>() {
            @Override
            public String getValue() {
                return status;
            }
        });
        instr.addVariable("schema-checker", "last-check", new Instrumentation.Variable<String>() {
            @Override
            public String getValue() {
                return lastCheck;
            }
        });
    }

    public void updateInstrumentation(boolean problem, Date time) {
        if (problem) {
            status = "BAD (check log for details)";
        } else {
            status = "GOOD";
        }
        lastCheck = time.toString();
    }

    private class SchemaCheckerRunnable implements Runnable {
        private String dbType;
        private String url;
        private String user;
        private String pass;
        private boolean ignoreExtras;

        public SchemaCheckerRunnable(String dbType, String url, String user, String pass, boolean ignoreExtras) {
            this.dbType = dbType;
            this.url = url;
            this.user = user;
            this.pass = pass;
            this.ignoreExtras = ignoreExtras;
        }

        @Override
        public void run() {// Only queue the schema check command if this is the leader
            if (Services.get().get(JobsConcurrencyService.class).isLeader()) {
                Services.get().get(CallableQueueService.class).queue(
                        new SchemaCheckXCommand(dbType, url, user, pass, ignoreExtras));
            } else {
                status = "DISABLED (not leader in HA)";
                lastCheck = "N/A";
            }
        }
    }
}
