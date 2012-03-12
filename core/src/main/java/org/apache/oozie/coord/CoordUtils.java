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
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetActionForNominalTimeJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.CoordActionsInDateRange;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ParamChecker;
import org.jdom.Element;

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
     * Get the list of actions for given date ranges
     *
     * @param jobId coordinator job id
     * @param scope a comma-separated list of date ranges. Each date range element is specified with two dates separated by '::'
     * @return the list of Coordinator actions for the date range
     * @throws CommandException thrown if failed to get coordinator actions by given date range
     */
    public static List<CoordinatorActionBean> getCoordActionsFromDates(String jobId, String scope) throws CommandException {
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
                listOfActions = CoordActionsInDateRange.getCoordActionsFromDateRange(jobId, s);
            }
            catch (XException e) {
                throw new CommandException(e);
            }
            actionSet.addAll(listOfActions);
            }
            else {
                try {
                    // Get action for the nominal time
                    Date date = DateUtils.parseDateUTC(s.trim());
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
                    throw new CommandException(ErrorCode.E0302, e);
                }
                catch (JPAExecutorException e) {
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

    /**
     * Get the list of actions for given id ranges
     *
     * @param jobId coordinator job id
     * @param scope a comma-separated list of action ranges. The action range is specified with two action numbers separated by '-'
     * @return the list of all Coordinator actions for action range
     * @throws CommandException thrown if failed to get coordinator actions by given id range
     */
     public static List<CoordinatorActionBean> getCoordActionsFromIds(String jobId, String scope) throws CommandException {
        JPAService jpaService = Services.get().get(JPAService.class);
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
                try {
                    //Get the starting and ending action numbers
                    start = Integer.parseInt(range[0].trim());
                    end = Integer.parseInt(range[1].trim());
                    if (start > end) {
                        throw new CommandException(ErrorCode.E0302, "format is wrong for action's range '" + s
                                + "', starting action number of the range should be less than ending action number, an example will be 1-4");
                    }
                }
                catch (NumberFormatException ne) {
                    throw new CommandException(ErrorCode.E0302, ne);
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
        // Retrieve the actions using the corresponding actionIds
        List<CoordinatorActionBean> coordActions = new ArrayList<CoordinatorActionBean>();
        for (String id : actions) {
            CoordinatorActionBean coordAction;
            try {
                coordAction = jpaService.execute(new CoordActionGetJPAExecutor(id));
            }
            catch (JPAExecutorException je) {
                throw new CommandException(je);
            }
            coordActions.add(coordAction);
        }
        return coordActions;
    }

}
