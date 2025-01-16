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

package org.apache.oozie.coord;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorEngine;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.coord.input.logic.CoordInputLogicEvaluator;
import org.apache.oozie.coord.input.logic.InputLogicParser;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetActionForNominalTimeJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.XLogService;
import org.apache.oozie.sla.SLAOperations;
import org.apache.oozie.util.CoordActionsInDateRange;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.Pair;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom2.Element;
import org.jdom2.JDOMException;

import com.google.common.annotations.VisibleForTesting;


public class CoordUtils {
    private static final XLog LOG = XLog.getLog(CoordUtils.class);
    public static final String HADOOP_USER = "user.name";

    public static String getDoneFlag(Element doneFlagElement) {
        if (doneFlagElement != null) {
            return doneFlagElement.getTextTrim();
        }
        else {
            return CoordELConstants.DEFAULT_DONE_FLAG;
        }
    }

    public static Configuration getHadoopConf(Configuration jobConf) {
        Configuration conf = new Configuration();
        Objects.requireNonNull(jobConf, "Configuration to be used for hadoop setup  cannot be null");
        String user = ParamChecker.notEmpty(jobConf.get(OozieClient.USER_NAME), OozieClient.USER_NAME);
        conf.set(HADOOP_USER, user);
        return conf;
    }

    /**
     * Get the list of actions for a given coordinator job
     * @param rangeType the rerun type (date, action)
     * @param jobId the coordinator job id
     * @param scope the date scope or action id scope
     * @param active set to true if non-terminated
     * @return the list of Coordinator actions
     * @throws CommandException thrown if failed to get coordinator actions by given date range
     */
    public static List<CoordinatorActionBean> getCoordActions(String rangeType, String jobId, String scope,
                                                              boolean active) throws CommandException {
        List<CoordinatorActionBean> coordActions = null;
        if (rangeType.equals(RestConstants.JOB_COORD_SCOPE_DATE)) {
            coordActions = CoordUtils.getCoordActionsFromDates(jobId, scope, active);
        }
        else if (rangeType.equals(RestConstants.JOB_COORD_SCOPE_ACTION)) {
            coordActions = CoordUtils.getCoordActionsFromIds(jobId, scope);
        }
        return coordActions;
    }

    public static List<String> getActionListForScopeAndDate(String id, String scope, String dates) throws CommandException {
        List<String> actionIds = new ArrayList<String>();

        List<String> parsed = new ArrayList<String>();
        if (scope == null && dates == null) {
            parsed.add(id);
            return parsed;
        }

        if (dates != null) {
            List<CoordinatorActionBean> actionSet = CoordUtils.getCoordActionsFromDates(id, dates, true);
            for (CoordinatorActionBean action : actionSet) {
                actionIds.add(action.getId());
            }
            parsed.addAll(actionIds);
        }
        if (scope != null) {
            parsed.addAll(CoordUtils.getActionsIds(id, scope));
        }
        return parsed;
    }


    /**
     * Get the list of actions for given date ranges
     *
     * @param jobId coordinator job id
     * @param scope a comma-separated list of date ranges. Each date range element is specified with two dates separated by '::'
     * @param active set to true if non-terminated
     * @return the list of Coordinator actions for the date range
     * @throws CommandException thrown if failed to get coordinator actions by given date range
     */
    @VisibleForTesting
    public static List<CoordinatorActionBean> getCoordActionsFromDates(String jobId, String scope, boolean active)
            throws CommandException {
        JPAService jpaService = Services.get().get(JPAService.class);
        ParamChecker.notEmpty(jobId, "jobId");
        ParamChecker.notEmpty(scope, "scope");

        Set<CoordinatorActionBean> actionSet = new LinkedHashSet<CoordinatorActionBean>();
        String[] list = scope.split(",");
        for (String s : list) {
            s = s.trim();
            // A date range is specified with two dates separated by '::'
            if (s.contains("::")) {
            List<CoordinatorActionBean> listOfActions;
            try {
                // Get list of actions within the range of date
                listOfActions = CoordActionsInDateRange.getCoordActionsFromDateRange(jobId, s, active);
            }
            catch (XException e) {
                throw new CommandException(e);
            }
            actionSet.addAll(listOfActions);
            }
            else {
                try {
                    // Get action for the nominal time
                    Date date = DateUtils.parseDateOozieTZ(s.trim());
                    CoordinatorActionBean coordAction = jpaService
                            .execute(new CoordJobGetActionForNominalTimeJPAExecutor(jobId, date));

                    if (coordAction != null) {
                        actionSet.add(coordAction);
                    }
                    else {
                        throw new RuntimeException("This should never happen, Coordinator Action shouldn't be null");
                    }
                }
                catch (ParseException e) {
                    throw new CommandException(ErrorCode.E0302, s.trim(), e);
                }
                catch (JPAExecutorException e) {
                    if (e.getErrorCode() == ErrorCode.E0605) {
                        XLog.getLog(CoordUtils.class).info("No action for nominal time:" + s + ". Skipping over");
                    }
                    throw new CommandException(e);
                }

            }
        }

        List<CoordinatorActionBean> coordActions = new ArrayList<CoordinatorActionBean>();
        for (CoordinatorActionBean coordAction : actionSet) {
            coordActions.add(coordAction);
        }
        return coordActions;
    }

    public static Set<String> getActionsIds(String jobId, String scope) throws CommandException {
        ParamChecker.notEmpty(jobId, "jobId");
        ParamChecker.notEmpty(scope, "scope");

        final Set<String> actions = new LinkedHashSet<>();
        for (final Range<Integer> range : parseScopeToRanges(scope)) {
            // Add the actionIds
            for (int i = range.getMinimum(); i <= range.getMaximum(); i++) {
                final String jobIdToAdd = jobId + "@" + i;
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Adding {0} to actionSet.", jobIdToAdd);
                }
                actions.add(jobIdToAdd);
            }
        }
        return actions;
    }

    /**
     * Parses a string value into a {@link org.apache.commons.lang3.Range} object while does sanity checking.
     *
     * @param rangeToParse string representing a minimum and maximum value separated by a dash: '1-5'
     * @return the parsed range class
     * @throws CommandException when the provided range string is empty, invalid or starting value is greater than ending value
     */
    public static Range<Integer> parseRange(final String rangeToParse) throws CommandException {
        final String range = StringUtils.stripToNull(rangeToParse);
        if (range == null) {
            throw new CommandException(ErrorCode.E0302, String.format("Range cannot be empty: %s", rangeToParse));
        }
        final String[] parts = range.split("-");
        if (parts.length != 2) {
            throw new CommandException(
                    ErrorCode.E0302,
                    String.format("format is wrong for action's range '%s', an example of correct format is 1-5", rangeToParse)
            );
        }
        try {
           final int start = Integer.parseInt(parts[0].trim());
           final int end = Integer.parseInt(parts[1].trim());

            if (start > end) {
                throw new CommandException(
                        ErrorCode.E0302,
                        String.format("format is wrong for action's range '%s', starting action number of the range " +
                                "should be less than ending action number, an example will be 1-4", rangeToParse)
                );
            }

           return Range.between(start, end);
        } catch (final NumberFormatException ne) {
            throw new CommandException(
                    ErrorCode.E0302,
                    String.format("could not parse boundaries of %s into an integer", rangeToParse),
                    ne
            );
        }
    }

    /**
     * Parses a scope definition (comma-separated list of values and ranges) into a list of
     * {@link org.apache.commons.lang3.Range}s. If a single value is defined, a [1-1] range will be created.
     *
     * @param scope comma-separated list of values and ranges
     * @return list of ranges based on the provided 'scope'
     * @throws CommandException when provided scope string is empty, invalid
     *     or the ranges' starting value is greater than ending value
     */
    public static List<Range<Integer>> parseScopeToRanges(final String scope) throws CommandException {
        if (StringUtils.stripToNull(scope) == null) {
            throw new CommandException(ErrorCode.E0302, "scope should not be empty");
        }
        final List<Range<Integer>> result = new ArrayList<>();
        final String[] list = scope.split(",");
        for (final String s : list) {
            final String range = StringUtils.stripToNull(s);
            if (range == null) {
                continue;
            }

            if (range.contains("-")) {
                // assume it is a valid range: 1-5
                result.add(parseRange(range));
            } else {
                // assume it is a plain number
                try {
                    final int elem = Integer.parseInt(range);
                    result.add(Range.between(elem, elem));
                } catch (final NumberFormatException ne) {
                    throw new CommandException(ErrorCode.E0302, String.format("could not parse %s into an integer", range), ne);
                }
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Created the following ranges from \"{0}\": {1}",
                    scope,
                    result
                            .stream()
                            .map(r -> String.format("[%s-%s]", r.getMinimum(), r.getMaximum()))
                            .collect(Collectors.joining(", "))
            );
        }
        return result;
    }

    /**
     * Calculates the number of elements described in the list of ranges.
     *
     * @param ranges list of {@link org.apache.commons.lang3.Range}s
     * @return the number of elements the provided ranges contain
     */
    public static int getElemCountOfRanges(final List<Range<Integer>> ranges) {
        return ranges.stream().mapToInt(r -> r.getMaximum() - r.getMinimum() + 1).sum();
    }

    /**
     * Get the list of actions for given id ranges
     *
     * @param jobId coordinator job id
     * @param scope a comma-separated list of action ranges. The action range is specified with two action numbers separated by '-'
     * @return the list of all Coordinator actions for action range
     * @throws CommandException thrown if failed to get coordinator actions by given id range
     */
     @VisibleForTesting
     public static List<CoordinatorActionBean> getCoordActionsFromIds(String jobId, String scope) throws CommandException {
        JPAService jpaService = Services.get().get(JPAService.class);
        Set<String> actions = getActionsIds(jobId, scope);
        // Retrieve the actions using the corresponding actionIds
        List<CoordinatorActionBean> coordActions = new ArrayList<CoordinatorActionBean>();
        for (String id : actions) {
            CoordinatorActionBean coordAction = null;
            try {
                coordAction = jpaService.execute(new CoordActionGetJPAExecutor(id));
            }
            catch (JPAExecutorException je) {
                if (je.getErrorCode().equals(ErrorCode.E0605)) { //ignore retrieval of non-existent actions in range
                    XLog.getLog(XLogService.class).warn(
                            "Coord action ID num [{0}] not yet materialized. Hence skipping over it for Kill action",
                            id.substring(id.indexOf("@") + 1));
                    continue;
                }
                else {
                    throw new CommandException(je);
                }
            }
            coordActions.add(coordAction);
        }
        return coordActions;
    }

     /**
      * Check if sla alert is disabled for action.
      * @param actionBean the action bean
      * @param coordName the coordinator name
      * @param jobConf the job configuration
      * @return true if SLA alert is disabled for action
      * @throws ParseException if date parse fails
      */
    public static boolean isSlaAlertDisabled(CoordinatorActionBean actionBean, String coordName, Configuration jobConf)
            throws ParseException {

        int disableSlaNotificationOlderThan = jobConf.getInt(OozieClient.SLA_DISABLE_ALERT_OLDER_THAN,
                ConfigurationService.getInt(OozieClient.SLA_DISABLE_ALERT_OLDER_THAN));

        if (disableSlaNotificationOlderThan > 0) {
            // Disable alert for catchup jobs
            long timeDiffinHrs = TimeUnit.MILLISECONDS.toHours(new Date().getTime()
                    - actionBean.getNominalTime().getTime());
            if (timeDiffinHrs > jobConf.getLong(OozieClient.SLA_DISABLE_ALERT_OLDER_THAN,
                    ConfigurationService.getLong(OozieClient.SLA_DISABLE_ALERT_OLDER_THAN))) {
                return true;
            }
        }

        boolean disableAlert = false;
        if (jobConf.get(OozieClient.SLA_DISABLE_ALERT_COORD) != null) {
            String coords = jobConf.get(OozieClient.SLA_DISABLE_ALERT_COORD);
            Set<String> coordsToDisableFor = new HashSet<String>(Arrays.asList(coords.split(",")));
            if (coordsToDisableFor.contains(coordName)) {
                return true;
            }
            if (coordsToDisableFor.contains(actionBean.getJobId())) {
                return true;
            }
        }

        // Check if sla alert is disabled for that action
        if (!StringUtils.isEmpty(jobConf.get(OozieClient.SLA_DISABLE_ALERT))
                && getCoordActionSLAAlertStatus(actionBean, coordName, jobConf, OozieClient.SLA_DISABLE_ALERT)) {
            return true;
        }

        // Check if sla alert is enabled for that action
        if (!StringUtils.isEmpty(jobConf.get(OozieClient.SLA_ENABLE_ALERT))
                && getCoordActionSLAAlertStatus(actionBean, coordName, jobConf, OozieClient.SLA_ENABLE_ALERT)) {
            return false;
        }

        return disableAlert;
    }

    /**
     * Get coord action SLA alert status.
     * @param actionBean
     * @param coordName
     * @param jobConf
     * @param slaAlertType
     * @return status of coord action SLA alert
     * @throws ParseException if parsing date is not possible
     */
    private static boolean getCoordActionSLAAlertStatus(CoordinatorActionBean actionBean, String coordName,
            Configuration jobConf, String slaAlertType) throws ParseException {
        String slaAlertList;

       if (!StringUtils.isEmpty(jobConf.get(slaAlertType))) {
            slaAlertList = jobConf.get(slaAlertType);
            // check if ALL or date/action-num range
            if (slaAlertList.equalsIgnoreCase(SLAOperations.ALL_VALUE)) {
                return true;
            }
            String[] values = slaAlertList.split(",");
            for (String value : values) {
                value = value.trim();
                if (value.contains("::")) {
                    String[] datesInRange = value.split("::");
                    Date start = DateUtils.parseDateOozieTZ(datesInRange[0].trim());
                    Date end = DateUtils.parseDateOozieTZ(datesInRange[1].trim());
                    // check if nominal time in this range
                    if (actionBean.getNominalTime().compareTo(start) >= 0
                            || actionBean.getNominalTime().compareTo(end) <= 0) {
                        return true;
                    }
                }
                else if (value.contains("-")) {
                    String[] actionsInRange = value.split("-");
                    int start = Integer.parseInt(actionsInRange[0].trim());
                    int end = Integer.parseInt(actionsInRange[1].trim());
                    // check if action number in this range
                    if (actionBean.getActionNumber() >= start || actionBean.getActionNumber() <= end) {
                        return true;
                    }
                }
                else {
                    int actionNumber = Integer.parseInt(value.trim());
                    if (actionBean.getActionNumber() == actionNumber) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    // Form the where clause to filter by status values
    public static Map<String, Object> getWhereClause(StringBuilder sb, Map<Pair<String, CoordinatorEngine.FILTER_COMPARATORS>,
            List<Object>> filterMap) {
        Map<String, Object> params = new HashMap<String, Object>();
        int pcnt= 1;
        for (Map.Entry<Pair<String, CoordinatorEngine.FILTER_COMPARATORS>, List<Object>> filter : filterMap.entrySet()) {
            String field = filter.getKey().getFirst();
            CoordinatorEngine.FILTER_COMPARATORS comp = filter.getKey().getSecond();
            String sqlField;
            if (field.equals(OozieClient.FILTER_STATUS)) {
                sqlField = "a.statusStr";
            } else if (field.equals(OozieClient.FILTER_NOMINAL_TIME)) {
                sqlField = "a.nominalTimestamp";
            } else {
                throw new IllegalArgumentException("Invalid filter key " + field);
            }

            sb.append(" and ").append(sqlField).append(" ");
            switch (comp) {
                case EQUALS:
                    sb.append("IN (");
                    params.putAll(appendParams(sb, filter.getValue(), pcnt));
                    sb.append(")");
                    break;

                case NOT_EQUALS:
                    sb.append("NOT IN (");
                    params.putAll(appendParams(sb, filter.getValue(), pcnt));
                    sb.append(")");
                    break;

                case GREATER:
                case GREATER_EQUAL:
                case LESSTHAN:
                case LESSTHAN_EQUAL:
                    if (filter.getValue().size() != 1) {
                        throw new IllegalArgumentException(field + comp.getSign() + " can't have more than 1 values");
                    }

                    sb.append(comp.getSign()).append(" ");
                    params.putAll(appendParams(sb, filter.getValue(), pcnt));
                    break;
            }

            pcnt += filter.getValue().size();
        }
        sb.append(" ");
        return params;
    }

    private static Map<String, Object> appendParams(StringBuilder sb, List<Object> value, int sindex) {
        Map<String, Object> params = new HashMap<String, Object>();
        boolean first = true;
        for (Object val : value) {
            String pname = "p" + sindex++;
            params.put(pname, val);
            if (!first) {
                sb.append(", ");
            }
            sb.append(':').append(pname);
            first = false;
        }
        return params;
    }

    public static boolean isInputLogicSpecified(String actionXml) throws JDOMException {
        return isInputLogicSpecified(XmlUtils.parseXml(actionXml));
    }

    public static boolean isInputLogicSpecified(Element eAction) throws JDOMException {
        return eAction.getChild(CoordInputLogicEvaluator.INPUT_LOGIC, eAction.getNamespace()) != null;
    }

    public static String getInputLogic(String actionXml) throws JDOMException {
        return getInputLogic(XmlUtils.parseXml(actionXml));
    }

    public static String getInputLogic(Element actionXml) throws JDOMException {
        return new InputLogicParser().parse(actionXml.getChild(CoordInputLogicEvaluator.INPUT_LOGIC,
                actionXml.getNamespace()));
    }

}
