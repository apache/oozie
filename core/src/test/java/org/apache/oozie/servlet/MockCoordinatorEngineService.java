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

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.BaseEngineException;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorActionInfo;
import org.apache.oozie.CoordinatorEngine;
import org.apache.oozie.CoordinatorEngineException;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.CoordinatorJob.Execution;
import org.apache.oozie.client.rest.JsonCoordinatorAction;
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

    public static String did = null;
    public static List<CoordinatorJob> coordJobs;
    public static List<Boolean> started;
    public static final int INIT_COORD_COUNT = 4;

    static {
        reset();
    }

    public static void reset() {
        did = null;
        coordJobs = new ArrayList<CoordinatorJob>();
        started = new ArrayList<Boolean>();
        for (int i = 0; i < INIT_COORD_COUNT; i++) {
            coordJobs.add(createDummyCoordinatorJob(i));
            started.add(false);
        }
    }

    @Override
    public CoordinatorEngine getCoordinatorEngine(String user, String authToken) {
        return new MockCoordinatorEngine(user, authToken);
    }

    @Override
    public CoordinatorEngine getSystemCoordinatorEngine() {
        return new MockCoordinatorEngine();
    }

    private static class MockCoordinatorEngine extends CoordinatorEngine {

        public MockCoordinatorEngine() {
        }

        public MockCoordinatorEngine(String user, String authToken) {
            super(user, authToken);
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
        public String dryrunSubmit(Configuration conf, boolean startJob) throws CoordinatorEngineException {
            did = RestConstants.JOB_ACTION_DRYRUN;
            int idx = coordJobs.size();
            coordJobs.add(createDummyCoordinatorJob(idx, conf));
            started.add(startJob);
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
        public void change(String jobId, String changeValue) throws CoordinatorEngineException {
            did = RestConstants.JOB_ACTION_CHANGE;
            int idx = validateCoordinatorIdx(jobId);
            started.set(idx, false);
        }

        @Override
        public void reRun(String jobId, Configuration conf) throws BaseEngineException {
            throw new BaseEngineException(new XException(ErrorCode.E0301));
        }

        @Override
        public CoordinatorActionInfo reRun(String jobId, String rerunType, String scope, boolean refresh,
                boolean noCleanup) throws BaseEngineException {
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
        public CoordinatorJobBean getCoordJob(String jobId, String filter, int start, int length) throws BaseEngineException {
            did = RestConstants.JOB_SHOW_INFO;
            int idx = validateCoordinatorIdx(jobId);
            return (CoordinatorJobBean) coordJobs.get(idx);
        }

        @Override
        public String getDefinition(String jobId) throws BaseEngineException {
            did = RestConstants.JOB_SHOW_DEFINITION;
            validateCoordinatorIdx(jobId);
            return COORD_APP;
        }

        @Override
        public void streamLog(String jobId, Writer writer) throws IOException, BaseEngineException {
            did = RestConstants.JOB_SHOW_LOG;
            validateCoordinatorIdx(jobId);
            writer.write(LOG);
        }

        @Override
        public void streamLog(String jobId, String logRetrievalScope, String logRetrievalType, Writer writer)
                throws IOException, BaseEngineException {
            did = RestConstants.JOB_SHOW_LOG;
            validateCoordinatorIdx(jobId);
            writer.write(LOG);
        }

        private int validateCoordinatorIdx(String jobId) throws CoordinatorEngineException {
            int idx = -1;
            try {
                idx = Integer.parseInt(jobId.replace(JOB_ID, ""));
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
        coordJob.setAuthToken("notoken");
        coordJob.setConf(CONFIGURATION);
        coordJob.setLastActionNumber(0);
        coordJob.setFrequency(1);
        coordJob.setExecution(Execution.FIFO);
        coordJob.setConcurrency(1);
        try {
            coordJob.setEndTime(DateUtils.parseDateUTC("2009-02-03T23:59Z"));
            coordJob.setStartTime(DateUtils.parseDateUTC("2009-02-01T23:59Z"));
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        List<JsonCoordinatorAction> actions = new ArrayList<JsonCoordinatorAction>();
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
        coordJob.setAuthToken("notoken");
        coordJob.setConf(conf.toString());
        coordJob.setLastActionNumber(0);
        coordJob.setFrequency(1);
        coordJob.setExecution(Execution.FIFO);
        coordJob.setConcurrency(1);
        try {
            coordJob.setEndTime(DateUtils.parseDateUTC("2009-02-03T23:59Z"));
            coordJob.setStartTime(DateUtils.parseDateUTC("2009-02-01T23:59Z"));
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        List<JsonCoordinatorAction> actions = new ArrayList<JsonCoordinatorAction>();
        for (int i = 0; i < idx; i++) {
            actions.add(createDummyAction(i, JOB_ID + idx));
        }

        coordJob.setActions(actions);
        return coordJob;
    }

    private static JsonCoordinatorAction createDummyAction(int idx, String jobId) {
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
