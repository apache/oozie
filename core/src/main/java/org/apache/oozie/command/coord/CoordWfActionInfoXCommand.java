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

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorWfActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.client.CoordinatorWfAction;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.CoordJobGetActionsSubsetJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionGetJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.ParamChecker;

import java.util.ArrayList;
import java.util.List;

public class CoordWfActionInfoXCommand extends CoordinatorXCommand<List<CoordinatorWfActionBean>>{
    /**
     * This class gets the wf action info in coordinator by action name and coordinator job ID.
     */
    private static final String ACTION_INFO = "action.info";
    private static final int DEFAULT_OFFSET = 1;
    private static final int DEFAULT_LEN = 50;

    private final String jobId;
    private final String actionName;
    private final int offset;
    private final int len;
    private List<CoordinatorActionBean> coordActions;
    private JPAService jpaService = null;

    public CoordWfActionInfoXCommand(String jobId, String actionName) {
        this(jobId, actionName, DEFAULT_OFFSET, DEFAULT_LEN);
    }

    public CoordWfActionInfoXCommand(String jobId, String actionName, int offset, int len) {
        super(ACTION_INFO, ACTION_INFO, 1);

        this.jobId = ParamChecker.notEmpty(jobId, "jobId");
        this.actionName = ParamChecker.notEmpty(actionName, "actionName");
        this.offset = offset;
        this.len = len;
    }

    @Override
    protected List<CoordinatorWfActionBean> execute() throws CommandException {
        List<CoordinatorWfActionBean> coordWfActions = new ArrayList<CoordinatorWfActionBean>();
        for(CoordinatorActionBean coordAction : coordActions) {
            String wfId = coordAction.getExternalId();
            String nullReason = null;
            WorkflowActionBean wfAction = null;
            if (wfId != null) {
                String wfActionId = wfId + "@" + actionName;
                try {
                    wfAction = jpaService.execute(new WorkflowActionGetJPAExecutor(wfActionId, true));
                    if (wfAction == null) {
                        nullReason = CoordinatorWfAction.NullReason.ACTION_NULL.getNullReason(actionName, wfId);
                    }
                } catch (JPAExecutorException ex) {
                    throw new CommandException(ex);
                }
            } else {
                nullReason = CoordinatorWfAction.NullReason.PARENT_NULL.getNullReason();
                LOG.warn(nullReason);
                wfAction = null;
            }
            int actionNumber = coordAction.getActionNumber();
            CoordinatorWfActionBean coordWfAction = new CoordinatorWfActionBean(actionNumber, wfAction, nullReason);

            coordWfActions.add(coordWfAction);
        }
        return coordWfActions;
    }

    @Override
    protected void loadState() throws CommandException {
        jpaService = Services.get().get(JPAService.class);
        if (jpaService != null) {
            try {
                coordActions = jpaService.execute(
                        new CoordJobGetActionsSubsetJPAExecutor(jobId, null, offset, len, false));
            } catch (JPAExecutorException ex) {
                LOG.error(ErrorCode.E0612);
                throw new CommandException(ex);
            }
        }
        else {
            throw new CommandException(ErrorCode.E0610);
        }
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }

    @Override
    protected boolean isLockRequired() {
        return false;
    }

    @Override
    public String getEntityKey() {
        return null;
    }
}
