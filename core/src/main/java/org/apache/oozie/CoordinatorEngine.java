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
import java.sql.Timestamp;
import java.text.ParseException;
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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.OperationType;
import org.apache.oozie.command.coord.BulkCoordXCommand;
import org.apache.oozie.command.coord.CoordActionInfoXCommand;
import org.apache.oozie.command.coord.CoordActionsIgnoreXCommand;
import org.apache.oozie.command.coord.CoordActionsKillXCommand;
import org.apache.oozie.command.coord.CoordChangeXCommand;
import org.apache.oozie.command.coord.CoordActionMissingDependenciesXCommand;
import org.apache.oozie.command.coord.CoordJobXCommand;
import org.apache.oozie.command.coord.CoordJobsXCommand;
import org.apache.oozie.command.coord.CoordKillXCommand;
import org.apache.oozie.command.coord.CoordRerunXCommand;
import org.apache.oozie.command.coord.CoordResumeXCommand;
import org.apache.oozie.command.coord.CoordSLAAlertsDisableXCommand;
import org.apache.oozie.command.coord.CoordSLAAlertsEnableXCommand;
import org.apache.oozie.command.coord.CoordSLAChangeXCommand;
import org.apache.oozie.command.coord.CoordSubmitXCommand;
import org.apache.oozie.command.coord.CoordSuspendXCommand;
import org.apache.oozie.command.coord.CoordUpdateXCommand;
import org.apache.oozie.command.coord.CoordWfActionInfoXCommand;
import org.apache.oozie.dependency.ActionDependency;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor.WorkflowJobQuery;
import org.apache.oozie.service.DagXLogInfoService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.XLogStreamingService;
import org.apache.oozie.util.CoordActionsInDateRange;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.JobUtils;
import org.apache.oozie.util.Pair;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XLogFilter;
import org.apache.oozie.util.XLogStreamer;
import org.apache.oozie.util.XLogUserFilterParam;

import com.google.common.annotations.VisibleForTesting;

public class CoordinatorEngine extends BaseEngine {
    private static final XLog LOG = XLog.getLog(CoordinatorEngine.class);
    public final static String COORD_ACTIONS_LOG_MAX_COUNT = "oozie.coord.actions.log.max.count";
    private final static int COORD_ACTIONS_LOG_MAX_COUNT_DEFAULT = 50;
    private final int maxNumActionsForLog;

    public enum FILTER_COMPARATORS {
        //This ordering is important, dont change this
        GREATER_EQUAL(">="), GREATER(">"), LESSTHAN_EQUAL("<="), LESSTHAN("<"), NOT_EQUALS("!="), EQUALS("=");

        private final String sign;

        FILTER_COMPARATORS(String sign) {
            this.sign = sign;
        }

        public String getSign() {
            return sign;
        }
    }

    public static final String[] VALID_JOB_FILTERS = {OozieClient.FILTER_STATUS, OozieClient.FILTER_NOMINAL_TIME};

    /**
     * Create a system Coordinator engine, with no user and no group.
     */
    public CoordinatorEngine() {
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

    @Override
    public String getDefinition(String jobId) throws BaseEngineException {
        CoordinatorJobBean job = getCoordJobWithNoActionInfo(jobId);
        return job.getOrigJobXml();
    }

    /**
     * @param jobId the job ID
     * @return CoordinatorJobBean
     * @throws BaseEngineException if the bean could not be retrieved
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
     * @param actionId the ID of the action
     * @return CoordinatorActionBean
     * @throws BaseEngineException if the bean could not be retrieved
     */
    public CoordinatorActionBean getCoordAction(String actionId) throws BaseEngineException {
        try {
            return new CoordActionInfoXCommand(actionId).call();
        }
        catch (CommandException ex) {
            throw new BaseEngineException(ex);
        }
    }

    @Override
    public CoordinatorJobBean getCoordJob(String jobId) throws BaseEngineException {
        try {
            return new CoordJobXCommand(jobId).call();
        }
        catch (CommandException ex) {
            throw new BaseEngineException(ex);
        }
    }

    @Override
    public CoordinatorJobBean getCoordJob(String jobId, String filter, int offset, int length, boolean desc)
            throws BaseEngineException {
        Map<Pair<String, FILTER_COMPARATORS>, List<Object>> filterMap = parseJobFilter(filter);
        try {
            return new CoordJobXCommand(jobId, filterMap, offset, length, desc).call();
        }
        catch (CommandException ex) {
            throw new BaseEngineException(ex);
        }
    }

    @Override
    public String getJobIdForExternalId(String externalId) throws CoordinatorEngineException {
        return null;
    }

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

    @Override
    public void change(String jobId, String changeValue) throws CoordinatorEngineException {
        try {
            new CoordChangeXCommand(jobId, changeValue).call();
            LOG.info("User " + user + " changed the Coordinator job [" + jobId + "] to " + changeValue);
        }
        catch (CommandException e) {
            throw new CoordinatorEngineException(e);
        }
    }

    public CoordinatorActionInfo ignore(String jobId, String type, String scope) throws CoordinatorEngineException {
        try {
            LOG.info("User " + user + " ignore a Coordinator Action (s) [" + scope + "] of the Coordinator Job ["
                    + jobId + "]");
            return new CoordActionsIgnoreXCommand(jobId, type, scope).call();
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
     * @param jobId the job ID
     * @param rerunType rerun type {@link RestConstants#JOB_COORD_SCOPE_DATE} or {@link RestConstants#JOB_COORD_SCOPE_ACTION}
     * @param scope the rerun scope for given rerunType separated by ","
     * @param refresh true if user wants to refresh input/output dataset urls
     * @param noCleanup false if user wants to cleanup output events for given rerun actions
     * @param failed true if user wants to rerun only failed nodes
     * @param conf configuration values for actions
     * @return  the action info
     * @throws BaseEngineException thrown if the actions could not be rerun
     */
    public CoordinatorActionInfo reRun(String jobId, String rerunType, String scope, boolean refresh, boolean noCleanup,
                                       boolean failed, Configuration conf)
            throws BaseEngineException {
        try {
            return new CoordRerunXCommand(jobId, rerunType, scope, refresh,
                    noCleanup, failed, conf).call();
        }
        catch (CommandException ex) {
            throw new BaseEngineException(ex);
        }
    }

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


    @Override
    protected void streamJobLog(XLogStreamer logStreamer, String jobId, Writer writer)
            throws IOException, BaseEngineException {
        logStreamer.getXLogFilter().setParameter(DagXLogInfoService.JOB, jobId);
        Date lastTime = null;
        CoordinatorJobBean job = getCoordJobWithNoActionInfo(jobId);
        if (job.isTerminalStatus()) {
            lastTime = job.getLastModifiedTime();
        }
        if (lastTime == null) {
            lastTime = new Date();
        }
        Services.get().get(XLogStreamingService.class).streamLog(logStreamer, job.getCreatedTime(), lastTime, writer);
    }

    /**
     * Add list of actions to the filter based on conditions
     *
     * @param jobId Job Id
     * @param logRetrievalScope Value for the retrieval type
     * @param logRetrievalType Based on which filter criteria the log is retrieved
     * @param writer writer to stream the log to
     * @param requestParameters additional parameters from the request
     * @throws IOException in case of IO error
     * @throws BaseEngineException if there is an error during streaming
     * @throws CommandException if a parameter could not be parsed
     */
    public void streamLog(String jobId, String logRetrievalScope, String logRetrievalType, Writer writer,
            Map<String, String[]> requestParameters) throws IOException, BaseEngineException, CommandException {

        Date startTime = null;
        Date endTime = null;
        XLogFilter filter = new XLogFilter(new XLogUserFilterParam(requestParameters));

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
                    filter.setActionList(true);
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
                    filter.setActionList(true);
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
                    filter.setActionList(true);
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
                    filter.setActionList(true);
                }
            }
        }
        if (startTime == null || endTime == null) {
            CoordinatorJobBean job = getCoordJobWithNoActionInfo(jobId);
            if (startTime == null) {
                startTime = job.getCreatedTime();
            }
            if (endTime == null) {
                if (job.isTerminalStatus()) {
                    endTime = job.getLastModifiedTime();
                }
                if (endTime == null) {
                    endTime = new Date();
                }
            }
        }
        Services.get().get(XLogStreamingService.class).streamLog(new XLogStreamer(filter, requestParameters), startTime,
                endTime, writer);
    }

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

    @Override
    public void suspend(String jobId) throws CoordinatorEngineException {
        try {
            new CoordSuspendXCommand(jobId).call();
        }
        catch (CommandException e) {
            throw new CoordinatorEngineException(e);
        }

    }

    @Override
    public WorkflowJob getJob(String jobId) throws BaseEngineException {
        throw new BaseEngineException(new XException(ErrorCode.E0301, "cannot get a workflow job from CoordinatorEngine"));
    }

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
        FILTER_NAMES.add(OozieClient.FILTER_SORT_BY);
        FILTER_NAMES.add(OozieClient.FILTER_CREATED_TIME_START);
        FILTER_NAMES.add(OozieClient.FILTER_CREATED_TIME_END);
        FILTER_NAMES.add(OozieClient.FILTER_TEXT);
    }

    /**
     * @param filter he filter to parse. Elements must be semicolon-separated name=value pairs.
     *               Supported names are in{@link CoordinatorEngine#FILTER_NAMES}.
     * @param start start from this job in the coordinator
     * @param len maximum number of results
     * @return CoordinatorJobInfo
     * @throws CoordinatorEngineException if the job info could no be retrieved
     */
    public CoordinatorJobInfo getCoordJobs(String filter, int start, int len) throws CoordinatorEngineException {
        Map<String, List<String>> filterList = parseJobsFilter(filter);

        try {
            return new CoordJobsXCommand(filterList, start, len).call();
        }
        catch (CommandException ex) {
            throw new CoordinatorEngineException(ex);
        }
    }

    // Parses the filter string (e.g status=RUNNING;status=WAITING) and returns a list of status values
    public Map<Pair<String, FILTER_COMPARATORS>, List<Object>> parseJobFilter(String filter) throws
        CoordinatorEngineException {
        Map<Pair<String, FILTER_COMPARATORS>, List<Object>> filterMap = new HashMap<Pair<String,
            FILTER_COMPARATORS>, List<Object>>();
        if (filter != null) {
            //split name value pairs
            StringTokenizer st = new StringTokenizer(filter, ";");
            while (st.hasMoreTokens()) {
                String token = st.nextToken().trim();
                Pair<String, FILTER_COMPARATORS> pair = null;
                for (FILTER_COMPARATORS comp : FILTER_COMPARATORS.values()) {
                    if (token.contains(comp.getSign())) {
                        int index = token.indexOf(comp.getSign());
                        String key = token.substring(0, index);
                        String valueStr = token.substring(index + comp.getSign().length());
                        Object value;

                        if (key.equalsIgnoreCase(OozieClient.FILTER_STATUS)) {
                            value = valueStr.toUpperCase();
                            try {
                                CoordinatorAction.Status.valueOf((String) value);
                            } catch (IllegalArgumentException ex) {
                                // Check for incorrect status value
                                throw new CoordinatorEngineException(ErrorCode.E0421, filter,
                                    XLog.format("invalid status value [{0}]." + " Valid status values are: [{1}]",
                                        valueStr, StringUtils.join(CoordinatorAction.Status.values(), ", ")));
                            }

                            if (!(comp == FILTER_COMPARATORS.EQUALS || comp == FILTER_COMPARATORS.NOT_EQUALS)) {
                                throw new CoordinatorEngineException(ErrorCode.E0421, filter,
                                    XLog.format("invalid comparator [{0}] for status." + " Valid are = and !=",
                                        comp.getSign()));
                            }

                            pair = Pair.of(OozieClient.FILTER_STATUS, comp);
                        } else if (key.equalsIgnoreCase(OozieClient.FILTER_NOMINAL_TIME)) {
                            try {
                                value = new Timestamp(DateUtils.parseDateUTC(valueStr).getTime());
                            } catch (ParseException e) {
                                throw new CoordinatorEngineException(ErrorCode.E0421, filter,
                                    XLog.format("invalid nominal time [{0}]." + " Valid format: " +
                                            "[{1}]", valueStr, DateUtils.ISO8601_UTC_MASK));
                            }
                            pair = Pair.of(OozieClient.FILTER_NOMINAL_TIME, comp);
                        } else {
                            // Check for incorrect filter option
                            throw new CoordinatorEngineException(ErrorCode.E0421, filter,
                                XLog.format("invalid filter [{0}]." + " Valid filters [{1}]", key, StringUtils.join
                                    (VALID_JOB_FILTERS, ", ")));
                        }
                        if (!filterMap.containsKey(pair)) {
                            filterMap.put(pair, new ArrayList<Object>());
                        }
                        filterMap.get(pair).add(value);
                        break;
                    }
                }

                if (pair == null) {
                    //token doesn't contain comparator
                    throw new CoordinatorEngineException(ErrorCode.E0421, filter,
                        "filter should be of format <key><comparator><value> pairs");
                }
            }
        }
        return filterMap;
    }

    /**
     * @param filter the filter to parse. Elements must be semicolon-separated name=value pairs.
     *               Supported names are in{@link CoordinatorEngine#FILTER_NAMES}.
     * @return Map<String, List<String>> map of parsed filters
     * @throws CoordinatorEngineException if the parameter could not be parsed
     */
    @VisibleForTesting
    Map<String, List<String>> parseJobsFilter(String filter) throws CoordinatorEngineException {
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
                                "elements must be semicolon-separated name=value pairs");
                    }
                    pair[0] = pair[0].toLowerCase();
                    if (!FILTER_NAMES.contains(pair[0])) {
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
                    throw new CoordinatorEngineException(ErrorCode.E0420, filter,
                            "elements must be semicolon-separated name=value pairs");
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

    /**
     * Update coord job definition.
     *
     * @param conf the conf
     * @param jobId the job id
     * @param dryrun the dryrun
     * @param showDiff the show diff
     * @return the string
     * @throws CoordinatorEngineException the coordinator engine exception
     */
    public String updateJob(Configuration conf, String jobId, boolean dryrun, boolean showDiff)
            throws CoordinatorEngineException {
        try {
            CoordUpdateXCommand update = new CoordUpdateXCommand(dryrun, conf, jobId, showDiff);
            return update.call();
        }
        catch (CommandException ex) {
            throw new CoordinatorEngineException(ex);
        }
    }

    /**
     * Return the status for a Job ID
     *
     * @param jobId job Id.
     * @return the job's status
     * @throws CoordinatorEngineException thrown if the job's status could not be obtained
     */
    @Override
    public String getJobStatus(String jobId) throws CoordinatorEngineException {
        try {
            CoordinatorJobBean coordJob = CoordJobQueryExecutor.getInstance().get(
                    CoordJobQueryExecutor.CoordJobQuery.GET_COORD_JOB_STATUS, jobId);
            return coordJob.getStatusStr();
        }
        catch (JPAExecutorException e) {
            throw new CoordinatorEngineException(e);
        }
    }

    /**
     * Return the status for an Action ID
     *
     * @param actionId action Id.
     * @return the action's status
     * @throws CoordinatorEngineException thrown if the action's status could not be obtained
     */
    public String getActionStatus(String actionId) throws CoordinatorEngineException {
        try {
            CoordinatorActionBean coordAction = CoordActionQueryExecutor.getInstance().get(
                    CoordActionQueryExecutor.CoordActionQuery.GET_COORD_ACTION_STATUS, actionId);
            return coordAction.getStatusStr();
        }
        catch (JPAExecutorException e) {
            throw new CoordinatorEngineException(e);
        }
    }

    @Override
    public void disableSLAAlert(String id, String actions, String dates, String childIds) throws BaseEngineException {
        try {
            new CoordSLAAlertsDisableXCommand(id, actions, dates).call();

        }
        catch (CommandException e) {
            throw new CoordinatorEngineException(e);
        }
    }

    @Override
    public void changeSLA(String id, String actions, String dates, String childIds, String newParams)
            throws BaseEngineException {
        Map<String, String> slaNewParams = null;

        try {

            if (newParams != null) {
                slaNewParams = JobUtils.parseChangeValue(newParams);
            }

            new CoordSLAChangeXCommand(id, actions, dates, slaNewParams).call();

        }
        catch (CommandException e) {
            throw new CoordinatorEngineException(e);
        }
    }

    @Override
    public void enableSLAAlert(String id, String actions, String dates, String childIds) throws BaseEngineException {
        try {
            new CoordSLAAlertsEnableXCommand(id, actions, dates).call();

        }
        catch (CommandException e) {
            throw new CoordinatorEngineException(e);
        }
    }

    /**
     * return a list of killed Coordinator job
     *
     * @param filter the filter string for which the coordinator jobs are killed
     * @param start the starting index for coordinator jobs
     * @param length maximum number of jobs to be killed
     * @return coordinatorJobInfo the list of jobs being killed
     * @throws CoordinatorEngineException thrown if one or more of the jobs cannot be killed
     */
    public CoordinatorJobInfo killJobs(String filter, int start, int length) throws CoordinatorEngineException {
        try {
            Map<String, List<String>> filterMap = parseJobsFilter(filter);
            CoordinatorJobInfo coordinatorJobInfo =
                    new BulkCoordXCommand(filterMap, start, length, OperationType.Kill).call();
            if (coordinatorJobInfo == null) {
                return new CoordinatorJobInfo(new ArrayList<CoordinatorJobBean>(), 0, 0, 0);
            }
            return coordinatorJobInfo;
        }
        catch (CommandException ex) {
            throw new CoordinatorEngineException(ex);
        }
    }

    /**
     * return the jobs that've been suspended
     * @param filter Filter for jobs that will be suspended, can be name, user, group, status, id or combination of any
     * @param start Offset for the jobs that will be suspended
     * @param length maximum number of jobs that will be suspended
     * @return coordinatorJobInfo
     * @throws CoordinatorEngineException if the jobs could not be suspended
     */
    public CoordinatorJobInfo suspendJobs(String filter, int start, int length) throws CoordinatorEngineException {
        try {
            Map<String, List<String>> filterMap = parseJobsFilter(filter);
            CoordinatorJobInfo coordinatorJobInfo =
                    new BulkCoordXCommand(filterMap, start, length, OperationType.Suspend).call();
            if (coordinatorJobInfo == null) {
                return new CoordinatorJobInfo(new ArrayList<CoordinatorJobBean>(), 0, 0, 0);
            }
            return coordinatorJobInfo;
        }
        catch (CommandException ex) {
            throw new CoordinatorEngineException(ex);
        }
    }

    /**
     * return the jobs that've been resumed
     * @param filter Filter for jobs that will be resumed, can be name, user, group, status, id or combination of any
     * @param start Offset for the jobs that will be resumed
     * @param length maximum number of jobs that will be resumed
     * @return coordinatorJobInfo returns resumed jobs
     * @throws CoordinatorEngineException if the jobs could not be resumed
     */
    public CoordinatorJobInfo resumeJobs(String filter, int start, int length) throws CoordinatorEngineException {
        try {
            Map<String, List<String>> filterMap = parseJobsFilter(filter);
            CoordinatorJobInfo coordinatorJobInfo =
                    new BulkCoordXCommand(filterMap, start, length, OperationType.Resume).call();
            if (coordinatorJobInfo == null) {
                return new CoordinatorJobInfo(new ArrayList<CoordinatorJobBean>(), 0, 0, 0);
            }
            return coordinatorJobInfo;
        }
        catch (CommandException ex) {
            throw new CoordinatorEngineException(ex);
        }
    }
    /**
     * Get coord action missing dependencies
     * @param id jobID
     * @param actions action list
     * @param dates nominal time list
     * @return CoordActionMissingDependenciesXCommand pair of coord action bean and
     * list of missing input dependencies.
     * @throws CommandException if the actions could not be retrieved
     */
    public List<Pair<CoordinatorActionBean, Map<String, ActionDependency>>> getCoordActionMissingDependencies(String id,
            String actions, String dates) throws CommandException {
        return new CoordActionMissingDependenciesXCommand(id, actions, dates).call();
    }

    /**
     * get wf actions by action name in a coordinator job
     * @param jobId coordinator job id
     * @param wfActionName workflow action name
     * @param offset offset in the coordinator job
     * @param len maximum number of results
     * @return CoordWfActionInfoXCommand list of CoordinatorWfActionBean in a coordinator
     * @throws CoordinatorEngineException if the actions could not be retrieved
     */
     public List<CoordinatorWfActionBean> getWfActionByJobIdAndName(String jobId, String wfActionName, int offset, int len)
             throws CoordinatorEngineException {
        try {
            return new CoordWfActionInfoXCommand(jobId, wfActionName, offset, len).call();
        }
        catch (CommandException ex) {
            throw new CoordinatorEngineException(ex);
        }
     }
}
