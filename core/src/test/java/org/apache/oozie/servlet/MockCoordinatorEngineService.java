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

package org.apache.oozie.servlet;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.BaseEngineException;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorActionInfo;
import org.apache.oozie.CoordinatorEngine;
import org.apache.oozie.CoordinatorEngineException;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.CoordinatorJobInfo;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.CoordinatorJob.Execution;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.service.CoordinatorEngineService;
import org.apache.oozie.util.DateUtils;

public class MockCoordinatorEngineService extends CoordinatorEngineService {
    public static final String JOB_ID = "coord-job-C-";
    public static final String ACTION_ID = "coord-action-C@-";
    public static final String COORD_APP = "<coordinator-app></coordinator-app>";
    public static final String CONFIGURATION = "<configuration></configuration>";
    public static final String GROUP = "group";
    public static final String USER = "user";

    public static final String LOG = "log";

    public static final String JOB_ID_END = "-C";
    public static String did = null;
    public static Integer offset = null;
    public static Integer length = null;
    public static String order = null;
    public static String filter = null;
    public static List<CoordinatorJob> coordJobs;
    public static List<Boolean> started;
    public static final int INIT_COORD_COUNT = 4;


    static {
        reset();
    }

    public static void reset() {
        did = null;
        offset = null;
        length = null;
        order = null;
        filter = null;
        coordJobs = new ArrayList<CoordinatorJob>();
        started = new ArrayList<Boolean>();
        for (int i = 0; i < INIT_COORD_COUNT; i++) {
            coordJobs.add(createDummyCoordinatorJob(i));
            started.add(false);
        }
    }

    @Override
    public CoordinatorEngine getCoordinatorEngine(String user) {
        return new MockCoordinatorEngine(user);
    }

    @Override
    public CoordinatorEngine getSystemCoordinatorEngine() {
        return new MockCoordinatorEngine();
    }

    private static class MockCoordinatorEngine extends CoordinatorEngine {

        public MockCoordinatorEngine() {
        }

        public MockCoordinatorEngine(String user) {
            super(user);
        }

        @Override
        public String submitJob(Configuration conf, boolean startJob) throws CoordinatorEngineException {
            did = "submit";
            int idx = coordJobs.size();
            coordJobs.add(createDummyCoordinatorJob(idx, conf));
            started.add(startJob);
            return JOB_ID + idx;
        }

        @Override
        public String dryRunSubmit(Configuration conf) throws CoordinatorEngineException {
            did = RestConstants.JOB_ACTION_DRYRUN;
            int idx = coordJobs.size();
            coordJobs.add(createDummyCoordinatorJob(idx, conf));
            started.add(false);
            return JOB_ID + idx;
        }

        @Override
        public void resume(String jobId) throws CoordinatorEngineException {
            did = RestConstants.JOB_ACTION_RESUME;
            int idx = validateCoordinatorIdx(jobId);
            started.set(idx, true);
        }

        @Override
        public void suspend(String jobId) throws CoordinatorEngineException {
            did = RestConstants.JOB_ACTION_SUSPEND;
            int idx = validateCoordinatorIdx(jobId);
            started.set(idx, false);
        }

        @Override
        public void kill(String jobId) throws CoordinatorEngineException {
            did = RestConstants.JOB_ACTION_KILL;
            int idx = validateCoordinatorIdx(jobId);
            started.set(idx, false);
        }

        @Override
        public CoordinatorActionInfo killActions(String jobId, String rangeType, String scope)
                throws CoordinatorEngineException {
            did = RestConstants.JOB_ACTION_KILL;
            int idx = validateCoordinatorIdx(jobId);
            started.set(idx, false);

            List<CoordinatorAction> actions = coordJobs.get(idx).getActions();
            List<CoordinatorActionBean> actionBeans = new ArrayList<CoordinatorActionBean>();
            for (CoordinatorAction action : actions) {
                actionBeans.add((CoordinatorActionBean) action);
            }
            return new CoordinatorActionInfo(actionBeans);

        }

        @Override
        public void change(String jobId, String changeValue) throws CoordinatorEngineException {
            did = RestConstants.JOB_ACTION_CHANGE;
            int idx = validateCoordinatorIdx(jobId);
            started.set(idx, true);
        }

        @Override
        public void reRun(String jobId, Configuration conf) throws BaseEngineException {
            throw new BaseEngineException(new XException(ErrorCode.E0301, "invalid use of rerun"));
        }

        @Override
        public CoordinatorActionInfo ignore(String jobId, String type, String scope) throws CoordinatorEngineException {
            did = RestConstants.JOB_ACTION_IGNORE;
            int idx = validateCoordinatorIdx(jobId);
            started.set(idx, true);
            return null;
        }
        @Override
        public CoordinatorActionInfo reRun(String jobId, String rerunType, String scope, boolean refresh,
                boolean noCleanup, boolean failed, Configuration conf) throws BaseEngineException {
            did = RestConstants.JOB_COORD_ACTION_RERUN;
            int idx = validateCoordinatorIdx(jobId);
            started.set(idx, true);
            List<CoordinatorAction> actions = coordJobs.get(idx).getActions();
            List<CoordinatorActionBean> actionBeans = new ArrayList<CoordinatorActionBean>();
            for (CoordinatorAction action : actions) {
                actionBeans.add((CoordinatorActionBean) action);
            }
            return new CoordinatorActionInfo(actionBeans);
        }

        @Override
        public CoordinatorJobBean getCoordJob(String jobId) throws BaseEngineException {
            did = RestConstants.JOB_SHOW_INFO;
            int idx = validateCoordinatorIdx(jobId);
            return (CoordinatorJobBean) coordJobs.get(idx);
        }

        @Override
        public CoordinatorJobBean getCoordJob(String jobId, String filter, int start, int length, boolean desc)
                throws BaseEngineException {
            did = RestConstants.JOB_SHOW_INFO;
            MockCoordinatorEngineService.offset = start;
            MockCoordinatorEngineService.length = length;
            MockCoordinatorEngineService.order = desc ? "desc" : "asc";
            MockCoordinatorEngineService.filter = filter;
            int idx = validateCoordinatorIdx(jobId);
            return (CoordinatorJobBean) coordJobs.get(idx);
        }

        @Override
        public String getJobStatus(String jobId) throws CoordinatorEngineException {
            did = RestConstants.JOB_SHOW_STATUS;
            int idx = validateCoordinatorIdx(jobId);
            return coordJobs.get(idx).getStatus().toString();
        }

        @Override
        public String getDefinition(String jobId) throws BaseEngineException {
            did = RestConstants.JOB_SHOW_DEFINITION;
            validateCoordinatorIdx(jobId);
            return COORD_APP;
        }

        @Override
        public void streamLog(String jobId, Writer writer, Map<String, String[]> params) throws IOException, BaseEngineException {
            did = RestConstants.JOB_SHOW_LOG;
            validateCoordinatorIdx(jobId);
            writer.write(LOG);
        }

        @Override
        public void streamLog(String jobId, String logRetrievalScope, String logRetrievalType, Writer writer,
                Map<String, String[]> params) throws IOException, BaseEngineException {
            did = RestConstants.JOB_SHOW_LOG;
            validateCoordinatorIdx(jobId);
            writer.write(LOG);
        }

        @Override
        public String updateJob(Configuration conf, String jobId, boolean dryrun, boolean showDiff)
                throws CoordinatorEngineException {
            if (dryrun) {
                did = RestConstants.JOB_COORD_UPDATE + "&" + RestConstants.JOB_ACTION_DRYRUN;
            }
            else {
                did = RestConstants.JOB_COORD_UPDATE;
            }
            validateCoordinatorIdx(jobId);
            return "";
        }

        @Override
        public CoordinatorJobInfo suspendJobs(String filter, int start, int length)
                throws CoordinatorEngineException {
            did = RestConstants.JOBS;
            return new CoordinatorJobInfo(new ArrayList<CoordinatorJobBean>(), 0, 0, 0);
        }

        @Override
        public CoordinatorJobInfo resumeJobs(String filter, int start, int length)
                throws CoordinatorEngineException {
            did = RestConstants.JOBS;
            return new CoordinatorJobInfo(new ArrayList<CoordinatorJobBean>(), 0, 0, 0);
        }

        @Override
        public CoordinatorJobInfo killJobs(String filter, int start, int length)
                throws CoordinatorEngineException {
            did = RestConstants.JOBS;
            return new CoordinatorJobInfo(new ArrayList<CoordinatorJobBean>(), 0, 0, 0);
        }

        public void disableSLAAlert(String id, String actions, String dates, String childIds)
                throws BaseEngineException {
            did = RestConstants.SLA_DISABLE_ALERT;
        }

        @Override
        public void changeSLA(String id, String actions, String dates, String childIds, String newParams)
                throws BaseEngineException {
            did = RestConstants.SLA_CHANGE;
        }

        @Override
        public void enableSLAAlert(String id, String actions, String dates, String childIds) throws BaseEngineException {
            did = RestConstants.SLA_ENABLE_ALERT;
        }

        private int validateCoordinatorIdx(String jobId) throws CoordinatorEngineException {
            int idx = -1;
            try {
                if (jobId.endsWith(JOB_ID_END)) {
                    jobId = jobId.replace(JOB_ID, "");
                    jobId = jobId.replace(JOB_ID_END, "");
                }
                else {
                    jobId = jobId.replace(JOB_ID, "");
                }
                idx = Integer.parseInt(jobId);
            }
            catch (Exception e) {
                throw new CoordinatorEngineException(ErrorCode.ETEST, jobId);
            }

            if (idx >= coordJobs.size()) {
                throw new CoordinatorEngineException(ErrorCode.ETEST, jobId);
            }

            return idx;
        }
    }

    private static CoordinatorJob createDummyCoordinatorJob(int idx) {
        CoordinatorJobBean coordJob = new CoordinatorJobBean();
        coordJob.setId(JOB_ID + idx);
        coordJob.setAppName("testApp");
        coordJob.setAppPath("testAppPath");
        coordJob.setStatus(CoordinatorJob.Status.RUNNING);
        coordJob.setCreatedTime(new Date());
        coordJob.setLastModifiedTime(new Date());
        coordJob.setUser(USER);
        coordJob.setGroup(GROUP);
        coordJob.setConf(CONFIGURATION);
        coordJob.setLastActionNumber(0);
        coordJob.setFrequency("1");
        coordJob.setExecutionOrder(Execution.FIFO);
        coordJob.setConcurrency(1);
        try {
            coordJob.setEndTime(DateUtils.parseDateOozieTZ("2009-02-03T23:59Z"));
            coordJob.setStartTime(DateUtils.parseDateOozieTZ("2009-02-01T23:59Z"));
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        List<CoordinatorActionBean> actions = new ArrayList<CoordinatorActionBean>();
        for (int i = 0; i < idx; i++) {
            actions.add(createDummyAction(i, JOB_ID + idx));
        }

        coordJob.setActions(actions);
        return coordJob;
    }

    private static CoordinatorJob createDummyCoordinatorJob(int idx, Configuration conf) {
        CoordinatorJobBean coordJob = new CoordinatorJobBean();
        coordJob.setId(JOB_ID + idx);
        coordJob.setAppName("testApp");
        coordJob.setAppPath("testAppPath");
        coordJob.setStatus(CoordinatorJob.Status.RUNNING);
        coordJob.setCreatedTime(new Date());
        coordJob.setLastModifiedTime(new Date());
        coordJob.setUser(USER);
        coordJob.setGroup(GROUP);
        coordJob.setConf(conf.toString());
        coordJob.setLastActionNumber(0);
        coordJob.setFrequency("1");
        coordJob.setExecutionOrder(Execution.FIFO);
        coordJob.setConcurrency(1);
        try {
            coordJob.setEndTime(DateUtils.parseDateOozieTZ("2009-02-03T23:59Z"));
            coordJob.setStartTime(DateUtils.parseDateOozieTZ("2009-02-01T23:59Z"));
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        List<CoordinatorActionBean> actions = new ArrayList<CoordinatorActionBean>();
        for (int i = 0; i < idx; i++) {
            actions.add(createDummyAction(i, JOB_ID + idx));
        }

        coordJob.setActions(actions);
        return coordJob;
    }

    private static CoordinatorActionBean createDummyAction(int idx, String jobId) {
        CoordinatorActionBean action = new CoordinatorActionBean();
        action.setId(ACTION_ID + idx);
        action.setJobId(jobId);
        action.setActionNumber(idx);
        action.setNominalTime(new Date());
        action.setLastModifiedTime(new Date());
        action.setStatus(CoordinatorAction.Status.SUBMITTED);
        action.setActionXml(COORD_APP);
        action.setCreatedConf(CONFIGURATION);
        return action;
    }

}
