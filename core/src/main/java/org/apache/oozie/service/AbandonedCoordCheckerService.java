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

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.action.email.EmailActionExecutor;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.coord.CoordKillXCommand;
import org.apache.oozie.command.wf.JobXCommand;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.XLog;

import com.google.common.annotations.VisibleForTesting;

/**
 * The Abandoned Coord Checker Service check finds out the abandoned coord jobs in system and kills it. A job is
 * considered to be abandoned/faulty if total number of actions in failed/timedout/suspended >= limit and there are no
 * succeeded action and job start time < job.older.than. Email will not be sent if
 * oozie.service.AbandonedCoordCheckerService.email.address is not configured.
 */
public class AbandonedCoordCheckerService implements Service {

    private static final String CONF_PREFIX = Service.CONF_PREFIX + "AbandonedCoordCheckerService.";
    public static final String TO_ADDRESS = CONF_PREFIX + "email.address";
    private static final String CONTENT_TYPE = "text/html";
    private static final String SUBJECT = "Abandoned Coordinators report";
    public static final String CONF_CHECK_INTERVAL = CONF_PREFIX + "check.interval";
    public static final String CONF_CHECK_DELAY = CONF_PREFIX + "check.delay";
    public static final String CONF_FAILURE_LEN = CONF_PREFIX + "failure.limit";
    public static final String CONF_JOB_OLDER_THAN = CONF_PREFIX + "job.older.than";

    public static final String CONF_JOB_KILL = CONF_PREFIX + "kill.jobs";
    public static final String OOZIE_BASE_URL = "oozie.base.url";
    private static String[] to;
    private static String serverURL;

    public static class AbandonedCoordCheckerRunnable implements Runnable {
        private  StringBuilder msg;
        final int failureLimit;
        XLog LOG = XLog.getLog(getClass());
        private boolean shouldKill = false;

        public AbandonedCoordCheckerRunnable(int failureLimit) {
            this(failureLimit, false);
        }

        public AbandonedCoordCheckerRunnable(int failureLimit, boolean shouldKill) {
            this.failureLimit = failureLimit;
            this.shouldKill = shouldKill;
        }

        public void run() {
            if (!Services.get().get(JobsConcurrencyService.class).isLeader()) {
                LOG.info("Server is not primary server. Skipping run");
                return;
            }
            msg = new StringBuilder();
            XLog.Info.get().clear();
            msg.append("<!DOCTYPE html><html><head><style>table,th,td{border:1px solid black;border-collapse:collapse;}</style>"
                    + "</head><body><table>");
            addTableHeader();
            try {
                checkCoordJobs();
                msg.append("</table></body></html>");
                sendMail(msg.toString());
            }
            catch (Exception e) {
                LOG.error("Error running AbandonedCoordChecker", e);
            }
        }

        /**
         * Check coordinator
         *
         * @throws CommandException
         */
        private void checkCoordJobs() throws CommandException {

            List<CoordinatorJobBean> jobs;
            try {
                Timestamp createdTS = new Timestamp(
                        System.currentTimeMillis()
                                - (ConfigurationService.getInt(CONF_JOB_OLDER_THAN) * 60 * 1000));

                jobs = CoordJobQueryExecutor.getInstance().getList(CoordJobQuery.GET_COORD_FOR_ABANDONEDCHECK,
                        failureLimit, createdTS);

                for (CoordinatorJobBean job : jobs) {
                    String killStatus = "Coord kill is disabled";
                    LOG.info("Abandoned Coord found : " + job.getId());
                    if (shouldKill) {
                        try {
                            new CoordKillXCommand(job.getId()).call();
                            LOG.info("Killed abandoned coord :  " + job.getId());
                            killStatus = "Successful";
                        }
                        catch (Exception e) {
                            LOG.error("Can't kill abandoned coord :  " + job.getId(), e);
                            killStatus = " Failed : " + e.getMessage();
                        }
                    }
                    addCoordToMessage(job, killStatus);
                }
            }
            catch (JPAExecutorException je) {
                throw new CommandException(je);
            }
        }

        public void addCoordToMessage(CoordinatorJobBean job, String killStatus) {
            msg.append("<tr>");
            msg.append("<td><a href=\"").append(JobXCommand.getJobConsoleUrl(job.getId())).append("\">")
                    .append(job.getId()).append("</a></td>");
            msg.append("<td>").append(job.getAppName()).append("</td>");
            msg.append("<td>").append(job.getUser()).append("</td>");
            msg.append("<td>").append(job.getGroup()).append("</td>");
            msg.append("<td>").append(killStatus).append("</td>");
            msg.append("</tr>");
        }

        public void addTableHeader() {
            msg.append("<tr>");
            msg.append("<td>").append("Coordinator id").append("</td>");
            msg.append("<td>").append("Coordinator name").append("</td>");
            msg.append("<td>").append("User name").append("</td>");
            msg.append("<td>").append("Group").append("</td>");
            msg.append("<td>").append("Kill Status").append("</td>");
            msg.append("</tr>");
        }

        @VisibleForTesting
        public String getMessage() {
            return msg.toString();
        }

        public void sendMail(String body) throws Exception {
            if (to == null || to.length == 0 || (to.length == 1 && StringUtils.isEmpty(to[0]))) {
                LOG.info(TO_ADDRESS + " is not configured. Not sending email");
                return;
            }
            EmailActionExecutor email = new EmailActionExecutor();
            String subject = SUBJECT + " for " + serverURL + " at " + DateUtils.formatDateOozieTZ(new Date());
            email.email(to, new String[0], subject, body, CONTENT_TYPE);
        }
    }

    @Override
    public void init(Services services) {
        to = ConfigurationService.getStrings(TO_ADDRESS);
        int failureLen = ConfigurationService.getInt(CONF_FAILURE_LEN);
        boolean shouldKill = ConfigurationService.getBoolean(CONF_JOB_KILL);
        serverURL = ConfigurationService.get(OOZIE_BASE_URL);

        int delay = ConfigurationService.getInt(CONF_CHECK_DELAY);

        Runnable actionCheckRunnable = new AbandonedCoordCheckerRunnable(failureLen, shouldKill);
        services.get(SchedulerService.class).schedule(actionCheckRunnable, delay,
                ConfigurationService.getInt(CONF_CHECK_INTERVAL), SchedulerService.Unit.MIN);

    }

    @Override
    public void destroy() {
    }

    @Override
    public Class<? extends Service> getInterface() {
        return AbandonedCoordCheckerService.class;
    }
}
