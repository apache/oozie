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
package org.apache.oozie;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.coord.CoordActionInfoXCommand;
import org.apache.oozie.util.CoordActionsInDateRange;
import org.apache.oozie.command.coord.CoordActionsKillXCommand;
import org.apache.oozie.command.coord.CoordChangeXCommand;
import org.apache.oozie.command.coord.CoordJobXCommand;
import org.apache.oozie.command.coord.CoordJobsXCommand;
import org.apache.oozie.command.coord.CoordKillXCommand;
import org.apache.oozie.command.coord.CoordRerunXCommand;
import org.apache.oozie.command.coord.CoordResumeXCommand;
import org.apache.oozie.command.coord.CoordSubmitXCommand;
import org.apache.oozie.command.coord.CoordSuspendXCommand;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor.WorkflowJobQuery;
import org.apache.oozie.service.DagXLogInfoService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.XLogStreamingService;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XLogStreamer;

import com.google.common.annotations.VisibleForTesting;

public class CoordinatorEngine extends BaseEngine {
    private static XLog LOG = XLog.getLog(CoordinatorEngine.class);
    public final static String COORD_ACTIONS_LOG_MAX_COUNT = "oozie.coord.actions.log.max.count";
    private final static int COORD_ACTIONS_LOG_MAX_COUNT_DEFAULT = 50;
    private int maxNumActionsForLog;
    public final static String POSITIVE_FILTER = "positive";
    public final static String NEGATIVE_FILTER = "negative";

    /**
     * Create a system Coordinator engine, with no user and no group.
     */
    public CoordinatorEngine() {
        if (Services.get().getConf().getBoolean(USE_XCOMMAND, true) == false) {
            LOG.debug("Oozie CoordinatorEngine is not using XCommands.");
        }
        else {
            LOG.debug("Oozie CoordinatorEngine is using XCommands.");
        }
        maxNumActionsForLog = Services.get().getConf()
                .getInt(COORD_ACTIONS_LOG_MAX_COUNT, COORD_ACTIONS_LOG_MAX_COUNT_DEFAULT);
    }

    /**
     * Create a Coordinator engine to perform operations on behave of a user.
     *
     * @param user user name.
     */
    public CoordinatorEngine(String user) {
        this();
        this.user = ParamChecker.notEmpty(user, "user");
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
            return new CoordJobXCommand(jobId).call();
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
            return new CoordActionInfoXCommand(actionId).call();
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
            return new CoordJobXCommand(jobId).call();
        }
        catch (CommandException ex) {
            throw new BaseEngineException(ex);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.BaseEngine#getCoordJob(java.lang.String, java.lang.String, int, int)
     */
    @Override
    public CoordinatorJobBean getCoordJob(String jobId, String filter, int start, int length, boolean desc)
            throws BaseEngineException {
        Map<String, List<String>> filterMap = parseStatusFilter(filter);
        try {
            return new CoordJobXCommand(jobId, filterMap, start, length, desc)
                    .call();
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
            new CoordKillXCommand(jobId).call();
            LOG.info("User " + user + " killed the Coordinator job " + jobId);
        }
        catch (CommandException e) {
            throw new CoordinatorEngineException(e);
        }
    }

    public CoordinatorActionInfo killActions(String jobId, String rangeType, String scope) throws CoordinatorEngineException {
        try {
            return new CoordActionsKillXCommand(jobId, rangeType, scope).call();
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
            new CoordChangeXCommand(jobId, changeValue).call();
            LOG.info("User " + user + " changed the Coordinator job " + jobId + " to " + changeValue);
        }
        catch (CommandException e) {
            throw new CoordinatorEngineException(e);
        }
    }

    @Override
    @Deprecated
    public void reRun(String jobId, Configuration conf) throws BaseEngineException {
        throw new BaseEngineException(new XException(ErrorCode.E0301, "invalid use of rerun"));
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
            return new CoordRerunXCommand(jobId, rerunType, scope, refresh,
                    noCleanup).call();
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
            new CoordResumeXCommand(jobId).call();
        }
        catch (CommandException e) {
            throw new CoordinatorEngineException(e);
        }
    }

    @Override
    @Deprecated
    public void start(String jobId) throws BaseEngineException {
        throw new BaseEngineException(new XException(ErrorCode.E0301, "invalid use of start"));
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.BaseEngine#streamLog(java.lang.String,
     * java.io.Writer)
     */
    @Override
    public void streamLog(String jobId, Writer writer, Map<String, String[]> params) throws IOException, BaseEngineException {
        XLogStreamer.Filter filter = new XLogStreamer.Filter();
        filter.setParameter(DagXLogInfoService.JOB, jobId);

        CoordinatorJobBean job = getCoordJobWithNoActionInfo(jobId);
        Services.get().get(XLogStreamingService.class).streamLog(filter, job.getCreatedTime(), new Date(), writer, params);
    }

    /**
     * Add list of actions to the filter based on conditions
     *
     * @param jobId Job Id
     * @param logRetrievalScope Value for the retrieval type
     * @param logRetrievalType Based on which filter criteria the log is retrieved
     * @param writer writer to stream the log to
     * @param params additional parameters from the request
     * @throws IOException
     * @throws BaseEngineException
     * @throws CommandException
     */
    public void streamLog(String jobId, String logRetrievalScope, String logRetrievalType, Writer writer,
            Map<String, String[]> params) throws IOException, BaseEngineException, CommandException {

        Date startTime = null;
        Date endTime = null;
        XLogStreamer.Filter filter = new XLogStreamer.Filter();
        filter.setParameter(DagXLogInfoService.JOB, jobId);
        if (logRetrievalScope != null && logRetrievalType != null) {
            // if coordinator action logs are to be retrieved based on action id range
            if (logRetrievalType.equals(RestConstants.JOB_LOG_ACTION)) {
                // Use set implementation that maintains order or elements to achieve reproducibility:
                Set<String> actionSet = new LinkedHashSet<String>();
                String[] list = logRetrievalScope.split(",");
                for (String s : list) {
                    s = s.trim();
                    if (s.contains("-")) {
                        String[] range = s.split("-");
                        if (range.length != 2) {
                            throw new CommandException(ErrorCode.E0302, "format is wrong for action's range '" + s
                                    + "'");
                        }
                        int start;
                        int end;
                        try {
                            start = Integer.parseInt(range[0].trim());
                        } catch (NumberFormatException ne) {
                            throw new CommandException(ErrorCode.E0302, "could not parse " + range[0].trim() + "into an integer",
                                    ne);
                        }
                        try {
                            end = Integer.parseInt(range[1].trim());
                        } catch (NumberFormatException ne) {
                            throw new CommandException(ErrorCode.E0302, "could not parse " + range[1].trim() + "into an integer",
                                    ne);
                        }
                        if (start > end) {
                            throw new CommandException(ErrorCode.E0302, "format is wrong for action's range '" + s + "'");
                        }
                        for (int i = start; i <= end; i++) {
                            actionSet.add(jobId + "@" + i);
                        }
                    }
                    else {
                        try {
                            Integer.parseInt(s);
                        }
                        catch (NumberFormatException ne) {
                            throw new CommandException(ErrorCode.E0302, "format is wrong for action id'" + s
                                    + "'. Integer only.");
                        }
                        actionSet.add(jobId + "@" + s);
                    }
                }

                if (actionSet.size() >= maxNumActionsForLog) {
                    throw new CommandException(ErrorCode.E0302,
                            "Retrieving log of too many coordinator actions. Max count is "
                                    + maxNumActionsForLog + " actions");
                }
                Iterator<String> actionsIterator = actionSet.iterator();
                StringBuilder orSeparatedActions = new StringBuilder("");
                boolean orRequired = false;
                while (actionsIterator.hasNext()) {
                    if (orRequired) {
                        orSeparatedActions.append("|");
                    }
                    orSeparatedActions.append(actionsIterator.next().toString());
                    orRequired = true;
                }
                if (actionSet.size() > 1 && orRequired) {
                    orSeparatedActions.insert(0, "(");
                    orSeparatedActions.append(")");
                }

                filter.setParameter(DagXLogInfoService.ACTION, orSeparatedActions.toString());
                if (actionSet != null && actionSet.size() == 1) {
                    CoordinatorActionBean actionBean = getCoordAction(actionSet.iterator().next());
                    startTime = actionBean.getCreatedTime();
                    endTime = actionBean.getStatus().equals(CoordinatorAction.Status.RUNNING) ? new Date() : actionBean
                            .getLastModifiedTime();
                }
                else if (actionSet != null && actionSet.size() > 0) {
                    List<String> tempList = new ArrayList<String>(actionSet);
                    Collections.sort(tempList, new Comparator<String>() {
                        public int compare(String a, String b) {
                            return Integer.valueOf(a.substring(a.lastIndexOf("@") + 1)).compareTo(
                                    Integer.valueOf(b.substring(b.lastIndexOf("@") + 1)));
                        }
                    });
                    startTime = getCoordAction(tempList.get(0)).getCreatedTime();
                    endTime = CoordActionsInDateRange.getCoordActionsLastModifiedDate(jobId, tempList.get(0),
                            tempList.get(tempList.size() - 1));
                }
            }
            // if coordinator action logs are to be retrieved based on date range
            // this block gets the corresponding list of coordinator actions to be used by the log filter
            if (logRetrievalType.equalsIgnoreCase(RestConstants.JOB_LOG_DATE)) {
                List<String> coordActionIdList = null;
                try {
                    coordActionIdList = CoordActionsInDateRange.getCoordActionIdsFromDates(jobId, logRetrievalScope);
                }
                catch (XException xe) {
                    throw new CommandException(ErrorCode.E0302, "Error in date range for coordinator actions", xe);
                }
                if(coordActionIdList.size() >= maxNumActionsForLog) {
                    throw new CommandException(ErrorCode.E0302,
                            "Retrieving log of too many coordinator actions. Max count is "
                                    + maxNumActionsForLog + " actions");
                }
                StringBuilder orSeparatedActions = new StringBuilder("");
                boolean orRequired = false;
                for (String coordActionId : coordActionIdList) {
                    if (orRequired) {
                        orSeparatedActions.append("|");
                    }
                    orSeparatedActions.append(coordActionId);
                    orRequired = true;
                }
                if (coordActionIdList.size() > 1 && orRequired) {
                    orSeparatedActions.insert(0, "(");
                    orSeparatedActions.append(")");
                }
                filter.setParameter(DagXLogInfoService.ACTION, orSeparatedActions.toString());

                if (coordActionIdList != null && coordActionIdList.size() == 1) {
                    CoordinatorActionBean actionBean = getCoordAction(coordActionIdList.get(0));
                    startTime = actionBean.getCreatedTime();
                    endTime = actionBean.getStatus().equals(CoordinatorAction.Status.RUNNING) ? new Date() : actionBean
                            .getLastModifiedTime();
                }
                else if (coordActionIdList != null && coordActionIdList.size() > 0) {
                    Collections.sort(coordActionIdList, new Comparator<String>() {
                        public int compare(String a, String b) {
                            return Integer.valueOf(a.substring(a.lastIndexOf("@") + 1)).compareTo(
                                    Integer.valueOf(b.substring(b.lastIndexOf("@") + 1)));
                        }
                    });
                    startTime = getCoordAction(coordActionIdList.get(0)).getCreatedTime();
                    endTime = CoordActionsInDateRange.getCoordActionsLastModifiedDate(jobId, coordActionIdList.get(0),
                            coordActionIdList.get(coordActionIdList.size() - 1));
                }
            }
        }
        if (startTime == null || endTime == null) {
            CoordinatorJobBean job = getCoordJobWithNoActionInfo(jobId);
            if (startTime == null) {
                startTime = job.getCreatedTime();
            }
            if (endTime == null) {
                endTime = new Date();
            }
        }
        //job.getActions()
        Services.get().get(XLogStreamingService.class).streamLog(filter, startTime, endTime, writer, params);
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
            CoordSubmitXCommand submit = new CoordSubmitXCommand(conf);
            return submit.call();
        }
        catch (CommandException ex) {
            throw new CoordinatorEngineException(ex);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.oozie.BaseEngine#dryRunSubmit(org.apache.hadoop.conf.Configuration)
     */
    @Override
    public String dryRunSubmit(Configuration conf) throws CoordinatorEngineException {
        try {
            CoordSubmitXCommand submit = new CoordSubmitXCommand(true, conf);
            return submit.call();
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
            new CoordSuspendXCommand(jobId).call();
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
        throw new BaseEngineException(new XException(ErrorCode.E0301, "cannot get a workflow job from CoordinatorEngine"));
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.BaseEngine#getJob(java.lang.String, int, int)
     */
    @Override
    public WorkflowJob getJob(String jobId, int start, int length) throws BaseEngineException {
        throw new BaseEngineException(new XException(ErrorCode.E0301, "cannot get a workflow job from CoordinatorEngine"));
    }

    private static final Set<String> FILTER_NAMES = new HashSet<String>();

    static {
        FILTER_NAMES.add(OozieClient.FILTER_USER);
        FILTER_NAMES.add(OozieClient.FILTER_NAME);
        FILTER_NAMES.add(OozieClient.FILTER_GROUP);
        FILTER_NAMES.add(OozieClient.FILTER_STATUS);
        FILTER_NAMES.add(OozieClient.FILTER_ID);
        FILTER_NAMES.add(OozieClient.FILTER_FREQUENCY);
        FILTER_NAMES.add(OozieClient.FILTER_UNIT);
    }

    /**
     * @param filter
     * @param start
     * @param len
     * @return CoordinatorJobInfo
     * @throws CoordinatorEngineException
     */
    public CoordinatorJobInfo getCoordJobs(String filter, int start, int len) throws CoordinatorEngineException {
        Map<String, List<String>> filterList = parseFilter(filter);

        try {
            return new CoordJobsXCommand(filterList, start, len).call();
        }
        catch (CommandException ex) {
            throw new CoordinatorEngineException(ex);
        }
    }

    // Parses the filter string (e.g status=RUNNING;status=WAITING) and returns a list of status values
    private Map<String, List<String>> parseStatusFilter(String filter) throws CoordinatorEngineException {
        Map<String, List<String>> filterMap = new HashMap<String, List<String>>();
        if (filter != null) {
            //split name;value pairs
            StringTokenizer st = new StringTokenizer(filter, ";");
            while (st.hasMoreTokens()) {
                String token = st.nextToken();
                if (token.contains("=")) {
                    boolean negative = false;
                    String[] pair = null;
                    if(token.contains("!=")) {
                        negative = true;
                        pair = token.split("!=");
                    }else {
                        pair = token.split("=");
                    }
                    if (pair.length != 2) {
                        throw new CoordinatorEngineException(ErrorCode.E0421, token,
                                "elements must be name=value or name!=value pairs");
                    }
                    if (pair[0].equalsIgnoreCase("status")) {
                        String statusValue = pair[1];
                        try {
                            CoordinatorAction.Status.valueOf(statusValue);
                        } catch (IllegalArgumentException ex) {
                            StringBuilder validStatusList = new StringBuilder();
                            for (CoordinatorAction.Status status : CoordinatorAction.Status.values()){
                                validStatusList.append(status.toString()+" ");
                            }
                            // Check for incorrect status value
                            throw new CoordinatorEngineException(ErrorCode.E0421, filter, XLog.format(
                                "invalid status value [{0}]." + " Valid status values are: [{1}]", statusValue, validStatusList));
                        }
                        String filterType = negative ? NEGATIVE_FILTER : POSITIVE_FILTER;
                        String oppositeFilterType = negative ? POSITIVE_FILTER : NEGATIVE_FILTER;
                        List<String> filterList = filterMap.get(filterType);
                        if (filterList == null) {
                            filterList = new ArrayList<String>();
                            filterMap.put(filterType, filterList);
                        }
                        List<String> oFilterList = filterMap.get(oppositeFilterType);
                        if (oFilterList != null && oFilterList.contains(statusValue)) {
                            throw new CoordinatorEngineException(ErrorCode.E0421, filter, XLog.format(
                                    "the status [{0}] specified in both positive and negative filters", statusValue));
                        }
                        filterList.add(statusValue);
                    } else {
                        // Check for incorrect filter option
                        throw new CoordinatorEngineException(ErrorCode.E0421, filter, XLog.format(
                                "invalid filter [{0}]." + " The only valid filter is \"status\"", pair[0]));
                    }
                } else {
                    throw new CoordinatorEngineException(ErrorCode.E0421, token,
                             "elements must be name=value or name!=value pairs");
                }
            }
        }
        return filterMap;
    }

    /**
     * @param filter
     * @return Map<String, List<String>>
     * @throws CoordinatorEngineException
     */
    @VisibleForTesting
    Map<String, List<String>> parseFilter(String filter) throws CoordinatorEngineException {
        Map<String, List<String>> map = new HashMap<String, List<String>>();
        boolean isTimeUnitSpecified = false;
        String timeUnit = "MINUTE";
        boolean isFrequencySpecified = false;
        String frequency = "";
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
                    if (!FILTER_NAMES.contains(pair[0].toLowerCase())) {
                        throw new CoordinatorEngineException(ErrorCode.E0420, filter, XLog.format("invalid name [{0}]",
                                pair[0]));
                    }
                    if (pair[0].equalsIgnoreCase("frequency")) {
                        isFrequencySpecified = true;
                        try {
                            frequency = (int) Float.parseFloat(pair[1]) + "";
                            continue;
                        }
                        catch (NumberFormatException NANException) {
                            throw new CoordinatorEngineException(ErrorCode.E0420, filter, XLog.format(
                                    "invalid value [{0}] for frequency. A numerical value is expected", pair[1]));
                        }
                    }
                    if (pair[0].equalsIgnoreCase("unit")) {
                        isTimeUnitSpecified = true;
                        timeUnit = pair[1];
                        if (!timeUnit.equalsIgnoreCase("months") && !timeUnit.equalsIgnoreCase("days")
                                && !timeUnit.equalsIgnoreCase("hours") && !timeUnit.equalsIgnoreCase("minutes")) {
                            throw new CoordinatorEngineException(ErrorCode.E0420, filter, XLog.format(
                                    "invalid value [{0}] for time unit. "
                                            + "Valid value is one of months, days, hours or minutes", pair[1]));
                        }
                        continue;
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
                } else {
                    throw new CoordinatorEngineException(ErrorCode.E0420, filter, "elements must be name=value pairs");
                }
            }
            // Unit is specified and frequency is not specified
            if (!isFrequencySpecified && isTimeUnitSpecified) {
                throw new CoordinatorEngineException(ErrorCode.E0420, filter, "time unit should be added only when "
                        + "frequency is specified. Either specify frequency also or else remove the time unit");
            } else if (isFrequencySpecified) {
                // Frequency value is specified
                if (isTimeUnitSpecified) {
                    if (timeUnit.equalsIgnoreCase("months")) {
                        timeUnit = "MONTH";
                    } else if (timeUnit.equalsIgnoreCase("days")) {
                        timeUnit = "DAY";
                    } else if (timeUnit.equalsIgnoreCase("hours")) {
                        // When job details are persisted to database, frequency in hours are converted to minutes.
                        // This conversion is to conform with that.
                        frequency = Integer.parseInt(frequency) * 60 + "";
                        timeUnit = "MINUTE";
                    } else if (timeUnit.equalsIgnoreCase("minutes")) {
                        timeUnit = "MINUTE";
                    }
                }
                // Adding the frequency and time unit filters to the filter map
                List<String> list = new ArrayList<String>();
                list.add(timeUnit);
                map.put("unit", list);
                list = new ArrayList<String>();
                list.add(frequency);
                map.put("frequency", list);
            }
        }
        return map;
    }

    public List<WorkflowJobBean> getReruns(String coordActionId) throws CoordinatorEngineException {
        List<WorkflowJobBean> wfBeans;
        try {
            wfBeans = WorkflowJobQueryExecutor.getInstance().getList(WorkflowJobQuery.GET_WORKFLOWS_PARENT_COORD_RERUN,
                    coordActionId);
        }
        catch (JPAExecutorException e) {
            throw new CoordinatorEngineException(e);
        }
        return wfBeans;
    }
}
