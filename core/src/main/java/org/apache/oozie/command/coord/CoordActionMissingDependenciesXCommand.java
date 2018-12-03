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

package org.apache.oozie.command.coord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.XCommand;
import org.apache.oozie.coord.CoordUtils;
import org.apache.oozie.coord.input.dependency.CoordInputDependency;
import org.apache.oozie.dependency.ActionDependency;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor.CoordActionQuery;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.util.Pair;

public class CoordActionMissingDependenciesXCommand
        extends XCommand<List<Pair<CoordinatorActionBean, Map<String, ActionDependency>>>> {

    private String actions;
    private String dates;
    private String jobId;
    private List<CoordinatorActionBean> coordActions = new ArrayList<CoordinatorActionBean>();

    public CoordActionMissingDependenciesXCommand(String id, String actions, String dates) {
        super("CoordActionMissingDependenciesXCommand", "CoordActionMissingDependenciesXCommand", 1);
        this.jobId = id;
        this.actions = actions;
        this.dates = dates;

        if (id.contains("@")) {
            this.jobId = id.substring(0, id.indexOf("@"));
            this.actions = id.substring(id.indexOf("@") + 1);
        }
    }

    public CoordActionMissingDependenciesXCommand(String id) {
        this(id, null, null);
    }

    @Override
    protected boolean isLockRequired() {
        return false;
    }

    @Override
    public String getEntityKey() {
        return null;
    }

    @Override
    protected void eagerVerifyPrecondition() throws CommandException, PreconditionException {
        if (actions == null && dates == null) {
            throw new CommandException(ErrorCode.E1029, "Action(s) are missing.");
        }
    }

    @Override
    protected void loadState() throws CommandException {
        String actionId = null;

        try {
            List<String> actionIds = CoordUtils.getActionListForScopeAndDate(jobId, actions, dates);
            for (String id : actionIds) {
                actionId = id;
                coordActions.add(CoordActionQueryExecutor.getInstance()
                        .get(CoordActionQuery.GET_COORD_ACTION_FOR_INPUTCHECK, actionId));
            }
        }
        catch (JPAExecutorException e) {
            if (e.getErrorCode().equals(ErrorCode.E0605)) {
                throw new CommandException(ErrorCode.E0605, actionId);
            }
            else {
                throw new CommandException(ErrorCode.E1029, e);
            }
        }

    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {

    }

    @Override
    protected List<Pair<CoordinatorActionBean, Map<String, ActionDependency>>> execute() throws CommandException {

        final List<Pair<CoordinatorActionBean, Map<String, ActionDependency>>> inputDependenciesListPair =
                new ArrayList<Pair<CoordinatorActionBean, Map<String, ActionDependency>>>();
        try {

            for (final CoordinatorActionBean coordAction : coordActions) {
                final CoordInputDependency coordPullInputDependency = coordAction.getPullInputDependencies();
                final CoordInputDependency coordPushInputDependency = coordAction.getPushInputDependencies();
                final Map<String, ActionDependency> dependencyMap = new HashMap<>();
                dependencyMap.putAll(coordPullInputDependency.getMissingDependencies(coordAction));
                dependencyMap.putAll(coordPushInputDependency.getMissingDependencies(coordAction));

                if (!dependencyMap.isEmpty()) {
                    inputDependenciesListPair.add(new Pair<>(coordAction, dependencyMap));
                }
            }
        }
        catch (Exception e) {
            throw new CommandException(ErrorCode.E1028, e.getMessage(), e);
        }

        return inputDependenciesListPair;
    }

}
