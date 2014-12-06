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

import java.util.Date;
import java.util.List;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorActionInfo;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorAction.Status;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.IgnoreTransitionXCommand;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.coord.CoordUtils;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.BatchQueryExecutor.UpdateEntry;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor.CoordActionQuery;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;


public class CoordActionsIgnoreXCommand extends IgnoreTransitionXCommand<CoordinatorActionInfo>{

    CoordinatorJobBean coordJob;
    String jobId;
    String type;
    String scope;
    private List<CoordinatorActionBean> coordActions;

    public CoordActionsIgnoreXCommand(String coordId, String type, String scope) {
        super("coord_action_ignore", "coord_action_ignore", 1);
        this.jobId = ParamChecker.notEmpty(coordId, "coordJobId");
        this.type = ParamChecker.notEmpty(type, "type");
        this.scope = ParamChecker.notEmpty(scope, "scope");
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        // no actions to ignore for PREP job
        if (coordJob.getStatus() == CoordinatorJob.Status.PREP) {
            LOG.info("CoordActionsIgnoreXCommand is not able to run, job status=" + coordJob.getStatus() + ", jobid=" + jobId);
            throw new PreconditionException(ErrorCode.E1024, "No actions are materialized to ignore");
        }
        StringBuilder ineligibleActions = new StringBuilder();
        if (!checkAllActionsStatus(ineligibleActions)) {
            throw new CommandException(ErrorCode.E1024,
                    "part or all actions are not eligible to ignore, check state of action number(s) ["
                            + ineligibleActions.toString() + "]");
        }
    }

    @Override
    public void ignoreChildren() throws CommandException {
        for (CoordinatorActionBean action : coordActions) {
            action.setStatus(Status.IGNORED);
            action.setLastModifiedTime(new Date());
            action.setPending(0);
            updateList.add(new UpdateEntry<CoordActionQuery>(CoordActionQuery.UPDATE_COORD_ACTION_STATUS_PENDING_TIME,
                    action));
            LOG.info("Ignore coord action = [{0}], new status = [{1}]", action.getId(), action.getStatus());
        }
        ret = new CoordinatorActionInfo(coordActions);
    }

    private boolean checkAllActionsStatus(StringBuilder ineligibleActions)
            throws CommandException {
        boolean ret = true;
        if (coordActions == null || coordActions.size() == 0) {
            throw new CommandException(ErrorCode.E1024, "no actions are eligible to ignore");
        }
        for (CoordinatorActionBean action : coordActions) {
            ParamChecker.notNull(action, "Action cannot be null");
            if (!(action.getStatus() == Status.FAILED || action.getStatus() == Status.KILLED
                    || action.getStatus() == Status.TIMEDOUT)) {
                LOG.info("Cannot ignore coord action = [{0}], since its status is [{1}]", action.getId(),
                        action.getStatus());
                if (ineligibleActions.length() != 0) {
                    ineligibleActions.append(",");
                }
                ineligibleActions.append(action.getActionNumber());
                ret = false;
            }
        }
        return ret;
    }

    @Override
    public void performWrites() throws CommandException {
        try {
            BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(null, updateList, null);
        }
        catch (JPAExecutorException jex) {
            throw new CommandException(jex);
        }
    }

    @Override
    protected boolean isLockRequired() {
        return true;
    }

    @Override
    public String getEntityKey() {
        return jobId;
    }

    @Override
    protected void loadState() throws CommandException {
        try{
            coordJob = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB_STATUS_PARENTID, jobId);
            coordActions = CoordUtils.getCoordActions(type, jobId, scope, false);
        }catch (Exception ex){
            throw new CommandException(ErrorCode.E0603, ex.getMessage(), ex);
        }
        LogUtils.setLogInfo(this.coordJob);
    }
}
