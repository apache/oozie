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

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.CoordActionGetForTimeoutJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor.CoordActionQuery;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;

/**
 * This class updates the Time out in the action bean
 */
public class CoordActionTimeOutXCommand extends CoordinatorXCommand<Void> {
    private CoordinatorActionBean actionBean;
    private String user;
    private String appName;
    private JPAService jpaService = null;

    public CoordActionTimeOutXCommand(CoordinatorActionBean actionBean, String user, String appName) {
        super("coord_action_timeout", "coord_action_timeout", 1);
        this.actionBean = ParamChecker.notNull(actionBean, "ActionBean");
        this.user = ParamChecker.notEmpty(user, "user");
        this.appName = ParamChecker.notEmpty(appName, "appName");
    }

    @Override
    protected void setLogInfo() {
        LogUtils.setLogInfo(actionBean.getId());
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#execute()
     */
    @Override
    protected Void execute() throws CommandException {
        if (actionBean.getStatus() == CoordinatorAction.Status.WAITING) {
            actionBean.setStatus(CoordinatorAction.Status.TIMEDOUT);
            try {
                queue(new CoordActionNotificationXCommand(actionBean), 100);
                actionBean.setLastModifiedTime(new Date());
                CoordActionQueryExecutor.getInstance().executeUpdate(
                        CoordActionQuery.UPDATE_COORD_ACTION_STATUS_PENDING_TIME, actionBean);
                if (EventHandlerService.isEnabled()) {
                    generateEvent(actionBean, user, appName, null);
                }
            }
            catch (JPAExecutorException e) {
                throw new CommandException(e);
            }
        }
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    public String getEntityKey() {
        return actionBean.getJobId();
    }

    @Override
    public String getKey() {
        return getName() + "_" + actionBean.getId();
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#isLockRequired()
     */
    @Override
    protected boolean isLockRequired() {
        return true;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#loadState()
     */
    @Override
    protected void loadState() throws CommandException {
        jpaService = Services.get().get(JPAService.class);
        if (jpaService == null) {
            throw new CommandException(ErrorCode.E0610);
        }

        try {
            actionBean = jpaService.execute(new CoordActionGetForTimeoutJPAExecutor(actionBean.getId()));
        }
        catch (JPAExecutorException e) {
            throw new CommandException(e);
        }
        LogUtils.setLogInfo(actionBean);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if (actionBean.getStatus() != CoordinatorAction.Status.WAITING) {
            throw new PreconditionException(ErrorCode.E1100, "The coord action must have status "
                    + CoordinatorAction.Status.WAITING + " but has status [" + actionBean.getStatus() + "]");
        }
    }
}
