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
import org.apache.oozie.XException;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.KillTransitionXCommand;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.wf.KillXCommand;
import org.apache.oozie.coord.CoordUtils;
import org.apache.oozie.executor.jpa.BatchQueryExecutor.UpdateEntry;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor.CoordActionQuery;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.InstrumentUtils;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;

/**
 * Kill coordinator actions by a range of dates (nominal time) or action number.
 * <p/>
 * The "range" can be set with {@link RestConstants.JOB_COORD_SCOPE_DATE} or
 * {@link RestConstants.JOB_COORD_SCOPE_ACTION}.
 * <p/>
 */
public class CoordActionsKillXCommand extends KillTransitionXCommand<CoordinatorActionInfo> {

    private String jobId;
    private CoordinatorJobBean coordJob;
    List<CoordinatorActionBean> coordActions;
    private String rangeType;
    private String scope;
    private JPAService jpaService = null;

    public CoordActionsKillXCommand(String id, String rangeType, String scope) {
        super("coord_action_kill", "coord_action_kill", 2);
        this.jobId = id;
        this.rangeType = ParamChecker.notEmpty(rangeType, "rangeType");
        this.scope = ParamChecker.notEmpty(scope, "scope");
    }

    @Override
    protected boolean isLockRequired() {
        return true;
    }

    @Override
    public String getEntityKey() {
        return this.jobId;
    }

    @Override
    protected void loadState() throws CommandException {
        try {
            jpaService = Services.get().get(JPAService.class);
            if (jpaService != null) {
                coordJob = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB_ACTION_KILL, jobId);
                LogUtils.setLogInfo(coordJob);
                coordActions = CoordUtils.getCoordActions(rangeType, coordJob.getId(), scope, true);
            }
            else {
                throw new CommandException(ErrorCode.E0610);
            }
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if (coordJob.getStatus() == CoordinatorJob.Status.SUCCEEDED
                || coordJob.getStatus() == CoordinatorJob.Status.FAILED
                || coordJob.getStatus() == CoordinatorJob.Status.DONEWITHERROR
                || coordJob.getStatus() == CoordinatorJob.Status.KILLED) {
            LOG.info("Coord actions not killed - job either finished SUCCEEDED, FAILED, KILLED or DONEWITHERROR, job id = "
                    + jobId + ", status = " + coordJob.getStatus());
            throw new PreconditionException(ErrorCode.E1020, jobId);
        }
    }

    @Override
    public void transitToNext() {
    }

    @Override
    public void killChildren() throws CommandException {
        InstrumentUtils.incrJobCounter(getName(), 1, getInstrumentation());
        for (CoordinatorActionBean coordAction : coordActions) {
            coordAction.setStatus(CoordinatorAction.Status.KILLED);
            coordAction.setLastModifiedTime(new Date());
            // kill Workflow job associated with this Coord action
            if (coordAction.getExternalId() != null) {
                queue(new KillXCommand(coordAction.getExternalId()));
                coordAction.incrementAndGetPending();
            }
            else {
                coordAction.setPending(0);
            }
            updateList.add(new UpdateEntry(CoordActionQuery.UPDATE_COORD_ACTION_STATUS_PENDING_TIME, coordAction));
            if (EventHandlerService.isEnabled()) {
                CoordinatorXCommand.generateEvent(coordAction, coordJob.getUser(), coordJob.getAppName(),
                        coordAction.getCreatedTime());
            }
            queue(new CoordActionNotificationXCommand(coordAction), 100);
        }
        CoordinatorActionInfo coordInfo = new CoordinatorActionInfo(coordActions);
        ret = coordInfo;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.command.KillTransitionXCommand#performWrites()
     */
    @Override
    public void performWrites() throws CommandException {
        try {
            BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(null, updateList, null);
        }
        catch (JPAExecutorException e) {
            throw new CommandException(e);
        }
    }

    @Override
    public void updateJob() throws CommandException {
        coordJob.setPending();
        updateList.add(new UpdateEntry(CoordJobQuery.UPDATE_COORD_JOB_STATUS_PENDING, coordJob));
    }

    @Override
    public void notifyParent() throws CommandException {
    }

}
