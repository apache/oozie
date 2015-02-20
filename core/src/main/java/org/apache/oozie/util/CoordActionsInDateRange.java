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


package org.apache.oozie.util;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetActionModifiedDateForRangeJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetActionRunningCountForRangeJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor.CoordActionQuery;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;

/**
 * This class provides the utility of listing
 * coordinator actions that were executed between a certain
 * date range. This is helpful in turn for retrieving the
 * required logs in that date range.
 */
public class CoordActionsInDateRange {

    /**
     * Get the list of Coordinator action Ids for given date ranges
     *
     * @param jobId coordinator job id
     * @param scope the date range for log. format is comma-separated list of date ranges.
     * Each date range element is specified with two dates separated by '::'
     * @return the list of coordinator action Ids for the date range
     *
     * Internally involves a database operation by invoking method 'getActionIdsFromDateRange'.
     */
    public static List<String> getCoordActionIdsFromDates(String jobId, String scope) throws XException {
        ParamChecker.notEmpty(jobId, "jobId");
        ParamChecker.notEmpty(scope, "scope");
        // Use an ordered set to achieve reproducible behavior.
        Set<String> actionSet = new LinkedHashSet<String>();
        String[] list = scope.split(",");
        for (String s : list) {
            s = s.trim();
            if (s.contains("::")) {
                List<String> listOfActions = getCoordActionIdsFromDateRange(jobId, s);
                actionSet.addAll(listOfActions);
            }
            else {
                throw new XException(ErrorCode.E0308, "'" + s + "'. Separator '::' is missing for start and end dates of range");
            }
        }
        return new ArrayList<String>(actionSet);
    }

    /**
     * Get the coordinator actions for a given date range
     * @param jobId the coordinator job id
     * @param range the date range separated by '::'
     * @return the list of Coordinator actions for the date range
     * @throws XException
     */
    public static List<CoordinatorActionBean> getCoordActionsFromDateRange(String jobId, String range, boolean active)
            throws XException {
            String[] dateRange = range.split("::");
            // This block checks for errors in the format of specifying date range
            if (dateRange.length != 2) {
                throw new XException(ErrorCode.E0308, "'" + range +
                    "'. Date value expected on both sides of the scope resolution operator '::' to signify start and end of range");

            }
            Date start;
            Date end;
            try {
            // Get the start and end dates for the range
                start = DateUtils.parseDateOozieTZ(dateRange[0].trim());
                end = DateUtils.parseDateOozieTZ(dateRange[1].trim());
            }
            catch (ParseException dx) {
                throw new XException(ErrorCode.E0308, "Error in parsing start or end date. " + dx);
            }
            if (start.after(end)) {
                throw new XException(ErrorCode.E0308, "'" + range + "'. Start date '" + start + "' is older than end date: '" + end
                        + "'");
            }
            List<CoordinatorActionBean> listOfActions = getActionsFromDateRange(jobId, start, end, active);
            return listOfActions;
    }

    /**
     * Get the coordinator actions for a given date range
     * @param jobId the coordinator job id
     * @param range the date range separated by '::'
     * @return the list of Coordinator actions for the date range
     * @throws XException
     */
    public static List<String> getCoordActionIdsFromDateRange(String jobId, String range) throws XException{
            String[] dateRange = range.split("::");
            // This block checks for errors in the format of specifying date range
            if (dateRange.length != 2) {
                throw new XException(ErrorCode.E0308, "'" + range
                  + "'. Date value expected on both sides of the scope resolution operator '::' to signify start and end of range");

            }
            Date start;
            Date end;
            try {
            // Get the start and end dates for the range
                start = DateUtils.parseDateOozieTZ(dateRange[0].trim());
                end = DateUtils.parseDateOozieTZ(dateRange[1].trim());
            }
            catch (ParseException dx) {
                throw new XException(ErrorCode.E0308, "Error in parsing start or end date. " + dx);
            }
            if (start.after(end)) {
                throw new XException(ErrorCode.E0308, "'" + range + "'. Start date '" + start + "' is older than end date: '" + end
+ "'");
            }
            List<CoordinatorActionBean> listOfActions = CoordActionQueryExecutor.getInstance().getList(
                    CoordActionQuery.GET_TERMINATED_ACTIONS_FOR_DATES, jobId, start, end);
            List<String> idsList = new ArrayList<String>();
            for ( CoordinatorActionBean bean : listOfActions){
                idsList.add(bean.getId());
            }
            return idsList;
    }

    /**
     * Get coordinator action ids between given start and end time
     *
     * @param jobId coordinator job id
     * @param start start time
     * @param end end time
     * @return a list of coordinator actions that correspond to the date range
     */
    private static List<CoordinatorActionBean> getActionsFromDateRange(String jobId, Date start, Date end,
            boolean active) throws XException {
        List<CoordinatorActionBean> list;
        if (!active) {
            list = CoordActionQueryExecutor.getInstance().getList(
                    CoordActionQuery.GET_TERMINATED_ACTIONS_FOR_DATES, jobId, start, end);
        }
        else {
            list = CoordActionQueryExecutor.getInstance().getList(
                    CoordActionQuery.GET_ACTIVE_ACTIONS_FOR_DATES, jobId, start, end);
        }
        return list;
    }

    /**
     * Gets the coordinator actions last modified date for range, if any action is running it return new date
     *
     * @param jobId the job id
     * @param startAction the start action
     * @param endAction the end action
     * @return the coordinator actions last modified date
     * @throws CommandException the command exception
     */
    public static Date getCoordActionsLastModifiedDate(String jobId, String startAction, String endAction)
            throws CommandException {
        JPAService jpaService = Services.get().get(JPAService.class);
        ParamChecker.notEmpty(jobId, "jobId");
        ParamChecker.notEmpty(startAction, "startAction");
        ParamChecker.notEmpty(endAction, "endAction");

        try {
            long count = jpaService.execute(new CoordJobGetActionRunningCountForRangeJPAExecutor(jobId, startAction,
                    endAction));
            if (count == 0) {
                return jpaService.execute(new CoordJobGetActionModifiedDateForRangeJPAExecutor(jobId, startAction, endAction));
            }
            else {
                return new Date();
            }
        }
        catch (JPAExecutorException je) {
            throw new CommandException(je);
        }
    }

}
