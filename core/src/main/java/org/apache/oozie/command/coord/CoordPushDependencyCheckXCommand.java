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

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.executor.jpa.CoordActionGetForCheckJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionGetForInputCheckJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionUpdatePushInputCheckJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.hcat.MetaDataClientWrapper;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XConfiguration;

public class CoordPushDependencyCheckXCommand extends CoordinatorXCommand<Void> {
    private String actionId;
    private JPAService jpaService = null;
    private CoordinatorActionBean coordAction = null;

    public CoordPushDependencyCheckXCommand(String actionId) {
        super("coord_push_dep_check", "coord_push_dep_check", 0);
        this.actionId = actionId;
    }

    @Override
    protected Void execute() throws CommandException {
        String pushDeps = coordAction.getPushMissingDependencies();
        if (pushDeps == null || pushDeps.length() == 0) {
            LOG.info("Nothing to check. Empty push missing dependency");
            return null;
        }
        LOG.info("Push missing dependencies .. "+ pushDeps);
        LinkedList<String> pushDepList = new LinkedList<String>();
        pushDepList.addAll(Arrays.asList(pushDeps.split(CoordELFunctions.DIR_SEPARATOR, -1)));
       // List<String> pushDepList = Arrays.asList(pushDeps.split(CoordELFunctions.DIR_SEPARATOR, -1));
        MetaDataClientWrapper mdClientWrap = new MetaDataClientWrapper();
        Configuration actionConf = null;
        try {
            actionConf = new XConfiguration(new StringReader(coordAction.getRunConf()));
        }
        catch (IOException e) {
            throw new CommandException(ErrorCode.E1307, e.getMessage(), e);
        }
        String user = ParamChecker.notEmpty(actionConf.get(OozieClient.USER_NAME), OozieClient.USER_NAME);
        mdClientWrap.checkList(pushDepList, user);
        if (pushDepList.size() > 0) {
            pushDeps = StringUtils.join(pushDepList, CoordELFunctions.DIR_SEPARATOR);
            coordAction.setPushMissingDependencies(pushDeps);
            // Checking for timeout
            handleTimeout();
        }
        else { // All push-based dependencies are available
            coordAction.setPushMissingDependencies("");
            if (coordAction.getMissingDependencies() == null || coordAction.getMissingDependencies().length() == 0) {
                coordAction.setStatus(CoordinatorAction.Status.READY);
                // pass jobID to the CoordActionReadyXCommand
                queue(new CoordActionReadyXCommand(coordAction.getJobId()), 100);
            }
        }
        updateCoordAction(coordAction);
        return null;
    }

    private void updateCoordAction(CoordinatorActionBean coordAction2) throws CommandException {
        coordAction.setLastModifiedTime(new Date());
        if (jpaService != null) {
            try {
                jpaService.execute(new CoordActionUpdatePushInputCheckJPAExecutor(coordAction));
            }
            catch (JPAExecutorException jex) {
                throw new CommandException(ErrorCode.E1023, jex.getMessage(), jex);
            }
        }
    }

    private void handleTimeout() {
        long waitingTime = (new Date().getTime() - Math.max(coordAction.getNominalTime().getTime(), coordAction
                .getCreatedTime().getTime()))
                / (60 * 1000);
        int timeOut = coordAction.getTimeOut();
        if ((timeOut >= 0) && (waitingTime > timeOut)) {
            queue(new CoordActionTimeOutXCommand(coordAction), 100);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    public String getEntityKey() {
        return actionId;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.command.XCommand#isLockRequired()
     */
    @Override
    protected boolean isLockRequired() {
        return true;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.command.XCommand#loadState()
     */
    @Override
    protected void loadState() throws CommandException {
        try {
            jpaService = Services.get().get(JPAService.class);

            if (jpaService != null) {
                coordAction = jpaService.execute(new CoordActionGetForInputCheckJPAExecutor(actionId));
                LogUtils.setLogInfo(coordAction, logInfo);
            }
            else {
                throw new CommandException(ErrorCode.E0610);
            }
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if (coordAction.getStatus().equals(CoordinatorAction.Status.WAITING) == false) {
            throw new PreconditionException(ErrorCode.E1100);
        }
    }

}
