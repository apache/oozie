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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.command.CommandException;
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
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;
import org.jdom.Element;

import com.google.common.annotations.VisibleForTesting;

public class CoordUtils {
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
        ParamChecker.notNull(jobConf, "Configuration to be used for hadoop setup ");
        String user = ParamChecker.notEmpty(jobConf.get(OozieClient.USER_NAME), OozieClient.USER_NAME);
        conf.set(HADOOP_USER, user);
        return conf;
    }

    /**
     * Get the list of actions for a given coordinator job
     * @param rangeType the rerun type (date, action)
     * @param jobId the coordinator job id
     * @param scope the date scope or action id scope
     * @return the list of Coordinator actions
     * @throws CommandException
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

    /**
     * Get the list of actions for given date ranges
     *
     * @param jobId coordinator job id
     * @param scope a comma-separated list of date ranges. Each date range element is specified with two dates separated by '::'
     * @return the list of Coordinator actions for the date range
     * @throws CommandException thrown if failed to get coordinator actions by given date range
     */
    @VisibleForTesting
    public static List<CoordinatorActionBean> getCoordActionsFromDates(String jobId, String scope, boolean active)
            throws CommandException {
        JPAService jpaService = Services.get().get(JPAService.class);
        ParamChecker.notEmpty(jobId, "jobId");
        ParamChecker.notEmpty(scope, "scope");

        Set<CoordinatorActionBean> actionSet = new HashSet<CoordinatorActionBean>();
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

        Set<String> actions = new HashSet<String>();
        String[] list = scope.split(",");
        for (String s : list) {
            s = s.trim();
            // An action range is specified with two actions separated by '-'
            if (s.contains("-")) {
                String[] range = s.split("-");
                // Check the format for action's range
                if (range.length != 2) {
                    throw new CommandException(ErrorCode.E0302, "format is wrong for action's range '" + s + "', an example of correct format is 1-5");
                }
                int start;
                int end;
                //Get the starting and ending action numbers
                try {
                    start = Integer.parseInt(range[0].trim());
                } catch (NumberFormatException ne) {
                    throw new CommandException(ErrorCode.E0302, "could not parse " + range[0].trim() + "into an integer", ne);
                }
                try {
                    end = Integer.parseInt(range[1].trim());
                } catch (NumberFormatException ne) {
                    throw new CommandException(ErrorCode.E0302, "could not parse " + range[1].trim() + "into an integer", ne);
                }
                if (start > end) {
                    throw new CommandException(ErrorCode.E0302, "format is wrong for action's range '" + s + "', starting action"
                            + "number of the range should be less than ending action number, an example will be 1-4");
                }
                // Add the actionIds
                for (int i = start; i <= end; i++) {
                    actions.add(jobId + "@" + i);
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
                actions.add(jobId + "@" + s);
            }
        }
        return actions;
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
      * @param actionBean
      * @param coordName
      * @param jobConf
      * @return
      * @throws ParseException
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
     * @return
     * @throws ParseException
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

}
