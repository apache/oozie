/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.coord.CoordActionInfoCommand;
import org.apache.oozie.command.coord.CoordActionInfoXCommand;
import org.apache.oozie.command.coord.CoordChangeCommand;
import org.apache.oozie.command.coord.CoordChangeXCommand;
import org.apache.oozie.command.coord.CoordJobCommand;
import org.apache.oozie.command.coord.CoordJobXCommand;
import org.apache.oozie.command.coord.CoordJobsCommand;
import org.apache.oozie.command.coord.CoordJobsXCommand;
import org.apache.oozie.command.coord.CoordKillCommand;
import org.apache.oozie.command.coord.CoordKillXCommand;
import org.apache.oozie.command.coord.CoordRerunCommand;
import org.apache.oozie.command.coord.CoordRerunXCommand;
import org.apache.oozie.command.coord.CoordResumeCommand;
import org.apache.oozie.command.coord.CoordResumeXCommand;
import org.apache.oozie.command.coord.CoordSubmitCommand;
import org.apache.oozie.command.coord.CoordSubmitXCommand;
import org.apache.oozie.command.coord.CoordSuspendCommand;
import org.apache.oozie.command.coord.CoordSuspendXCommand;
import org.apache.oozie.service.DagXLogInfoService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.XLogService;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XLogStreamer;

public class CoordinatorEngine extends BaseEngine {
    private static boolean useXCommand = true;
    private static XLog LOG = XLog.getLog(CoordinatorEngine.class);

    /**
     * Create a system Coordinator engine, with no user and no group.
     */
    public CoordinatorEngine() {
        if (Services.get().getConf().getBoolean(USE_XCOMMAND, true) == false) {
            useXCommand = false;
            LOG.debug("Oozie CoordinatorEngine is not using XCommands.");
        }
        else {
            LOG.debug("Oozie CoordinatorEngine is using XCommands.");
        }
    }

    /**
     * Create a Coordinator engine to perform operations on behave of a user.
     *
     * @param user user name.
     * @param authToken the authentication token.
     */
    public CoordinatorEngine(String user, String authToken) {
        this();
        this.user = ParamChecker.notEmpty(user, "user");
        this.authToken = ParamChecker.notEmpty(authToken, "authToken");
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.BaseEngine#getDefinition(java.lang.String)
     */
    @Override
    public String getDefinition(String jobId) throws BaseEngineException {
        CoordinatorJobBean job = getCoordJobWithNoActionInfo(jobId);
        return job.getOrigJobXml();
    }

    /**
     * @param jobId
     * @return CoordinatorJobBean
     * @throws BaseEngineException
     */
    private CoordinatorJobBean getCoordJobWithNoActionInfo(String jobId) throws BaseEngineException {
        try {
            if (useXCommand) {
                return new CoordJobXCommand(jobId).call();
            }
            else {
                return new CoordJobCommand(jobId).call();
            }
        }
        catch (CommandException ex) {
            throw new BaseEngineException(ex);
        }
    }

    /**
     * @param actionId
     * @return CoordinatorActionBean
     * @throws BaseEngineException
     */
    public CoordinatorActionBean getCoordAction(String actionId) throws BaseEngineException {
        try {
            if (useXCommand) {
                return new CoordActionInfoXCommand(actionId).call();
            }
            else {
                return new CoordActionInfoCommand(actionId).call();
            }
        }
        catch (CommandException ex) {
            throw new BaseEngineException(ex);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.BaseEngine#getCoordJob(java.lang.String)
     */
    @Override
    public CoordinatorJobBean getCoordJob(String jobId) throws BaseEngineException {
        try {
            if (useXCommand) {
                return new CoordJobXCommand(jobId).call();
            }
            else {
                return new CoordJobCommand(jobId).call();
            }
        }
        catch (CommandException ex) {
            throw new BaseEngineException(ex);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.BaseEngine#getCoordJob(java.lang.String, int, int)
     */
    @Override
    public CoordinatorJobBean getCoordJob(String jobId, int start, int length) throws BaseEngineException {
        try {
            if (useXCommand) {
                return new CoordJobXCommand(jobId, start, length).call();
            }
            else {
                return new CoordJobCommand(jobId, start, length).call();
            }
        }
        catch (CommandException ex) {
            throw new BaseEngineException(ex);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.BaseEngine#getJobIdForExternalId(java.lang.String)
     */
    @Override
    public String getJobIdForExternalId(String externalId) throws CoordinatorEngineException {
        return null;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.BaseEngine#kill(java.lang.String)
     */
    @Override
    public void kill(String jobId) throws CoordinatorEngineException {
        try {
            if (useXCommand) {
                new CoordKillXCommand(jobId).call();
            }
            else {
                new CoordKillCommand(jobId).call();
            }
            LOG.info("User " + user + " killed the Coordinator job " + jobId);
        }
        catch (CommandException e) {
            throw new CoordinatorEngineException(e);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#change(java.lang.String, java.lang.String)
     */
    @Override
    public void change(String jobId, String changeValue) throws CoordinatorEngineException {
        try {
            if (useXCommand) {
                new CoordChangeXCommand(jobId, changeValue).call();
            }
            else {
                new CoordChangeCommand(jobId, changeValue).call();
            }
            LOG.info("User " + user + " changed the Coordinator job " + jobId + " to " + changeValue);
        }
        catch (CommandException e) {
            throw new CoordinatorEngineException(e);
        }
    }

    @Override
    @Deprecated
    public void reRun(String jobId, Configuration conf) throws BaseEngineException {
        throw new BaseEngineException(new XException(ErrorCode.E0301));
    }

    /**
     * Rerun coordinator actions for given rerunType
     *
     * @param jobId
     * @param rerunType
     * @param scope
     * @param refresh
     * @param noCleanup
     * @throws BaseEngineException
     */
    public CoordinatorActionInfo reRun(String jobId, String rerunType, String scope, boolean refresh, boolean noCleanup)
            throws BaseEngineException {
        try {
            if  (useXCommand) {
                return new CoordRerunXCommand(jobId, rerunType, scope, refresh, noCleanup).call();
            }
            else {
                return new CoordRerunCommand(jobId, rerunType, scope, refresh, noCleanup).call();
            }
        }
        catch (CommandException ex) {
            throw new BaseEngineException(ex);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.BaseEngine#resume(java.lang.String)
     */
    @Override
    public void resume(String jobId) throws CoordinatorEngineException {
        try {
            if (useXCommand) {
                new CoordResumeXCommand(jobId).call();
            }
            else {
                new CoordResumeCommand(jobId).call();
            }
        }
        catch (CommandException e) {
            throw new CoordinatorEngineException(e);
        }
    }

    @Override
    @Deprecated
    public void start(String jobId) throws BaseEngineException {
        throw new BaseEngineException(new XException(ErrorCode.E0301));
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.BaseEngine#streamLog(java.lang.String,
     * java.io.Writer)
     */
    @Override
    public void streamLog(String jobId, Writer writer) throws IOException, BaseEngineException {
        XLogStreamer.Filter filter = new XLogStreamer.Filter();
        filter.setParameter(DagXLogInfoService.JOB, jobId);

        CoordinatorJobBean job = getCoordJobWithNoActionInfo(jobId);
        Services.get().get(XLogService.class).streamLog(filter, job.getCreatedTime(), new Date(), writer);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.oozie.BaseEngine#submitJob(org.apache.hadoop.conf.Configuration
     * , boolean)
     */
    @Override
    public String submitJob(Configuration conf, boolean startJob) throws CoordinatorEngineException {
        try {
            String jobId;
            if (useXCommand) {
                CoordSubmitXCommand submit = new CoordSubmitXCommand(conf, getAuthToken());
                jobId = submit.call();
            }
            else {
                CoordSubmitCommand submit = new CoordSubmitCommand(conf, getAuthToken());
                jobId = submit.call();
            }
            return jobId;
        }
        catch (CommandException ex) {
            throw new CoordinatorEngineException(ex);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.oozie.BaseEngine#dryrunSubmit(org.apache.hadoop.conf.Configuration
     * , boolean)
     */
    @Override
    public String dryrunSubmit(Configuration conf, boolean startJob) throws CoordinatorEngineException {
        try {
            String jobId;
            if (useXCommand) {
                CoordSubmitXCommand submit = new CoordSubmitXCommand(true, conf, getAuthToken());
                jobId = submit.call();
            }
            else {
                CoordSubmitCommand submit = new CoordSubmitCommand(true, conf, getAuthToken());
                jobId = submit.call();                
            }
            return jobId;
        }
        catch (CommandException ex) {
            throw new CoordinatorEngineException(ex);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.BaseEngine#suspend(java.lang.String)
     */
    @Override
    public void suspend(String jobId) throws CoordinatorEngineException {
        try {
            if (useXCommand) {
                new CoordSuspendXCommand(jobId).call();
            }
            else {
                new CoordSuspendCommand(jobId).call();
            }
        }
        catch (CommandException e) {
            throw new CoordinatorEngineException(e);
        }

    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.BaseEngine#getJob(java.lang.String)
     */
    @Override
    public WorkflowJob getJob(String jobId) throws BaseEngineException {
        throw new BaseEngineException(new XException(ErrorCode.E0301));
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.BaseEngine#getJob(java.lang.String, int, int)
     */
    @Override
    public WorkflowJob getJob(String jobId, int start, int length) throws BaseEngineException {
        throw new BaseEngineException(new XException(ErrorCode.E0301));
    }

    private static final Set<String> FILTER_NAMES = new HashSet<String>();

    static {
        FILTER_NAMES.add(OozieClient.FILTER_USER);
        FILTER_NAMES.add(OozieClient.FILTER_NAME);
        FILTER_NAMES.add(OozieClient.FILTER_GROUP);
        FILTER_NAMES.add(OozieClient.FILTER_STATUS);
    }

    /**
     * @param filterStr
     * @param start
     * @param len
     * @return CoordinatorJobInfo
     * @throws CoordinatorEngineException
     */
    public CoordinatorJobInfo getCoordJobs(String filterStr, int start, int len) throws CoordinatorEngineException {
        Map<String, List<String>> filter = parseFilter(filterStr);

        try {
            if (useXCommand) {
                return new CoordJobsXCommand(filter, start, len).call();
            }
            else {
                return new CoordJobsCommand(filter, start, len).call();
            }
        }
        catch (CommandException ex) {
            throw new CoordinatorEngineException(ex);
        }
    }

    /**
     * @param filter
     * @return Map<String, List<String>>
     * @throws CoordinatorEngineException
     */
    private Map<String, List<String>> parseFilter(String filter) throws CoordinatorEngineException {
        Map<String, List<String>> map = new HashMap<String, List<String>>();
        if (filter != null) {
            StringTokenizer st = new StringTokenizer(filter, ";");
            while (st.hasMoreTokens()) {
                String token = st.nextToken();
                if (token.contains("=")) {
                    String[] pair = token.split("=");
                    if (pair.length != 2) {
                        throw new CoordinatorEngineException(ErrorCode.E0420, filter,
                                                             "elements must be name=value pairs");
                    }
                    if (!FILTER_NAMES.contains(pair[0])) {
                        throw new CoordinatorEngineException(ErrorCode.E0420, filter, XLog.format("invalid name [{0}]",
                                                                                                  pair[0]));
                    }
                    if (pair[0].equals("status")) {
                        try {
                            CoordinatorJob.Status.valueOf(pair[1]);
                        }
                        catch (IllegalArgumentException ex) {
                            throw new CoordinatorEngineException(ErrorCode.E0420, filter, XLog.format(
                                    "invalid status [{0}]", pair[1]));
                        }
                    }
                    List<String> list = map.get(pair[0]);
                    if (list == null) {
                        list = new ArrayList<String>();
                        map.put(pair[0], list);
                    }
                    list.add(pair[1]);
                }
                else {
                    throw new CoordinatorEngineException(ErrorCode.E0420, filter, "elements must be name=value pairs");
                }
            }
        }
        return map;
    }
}
