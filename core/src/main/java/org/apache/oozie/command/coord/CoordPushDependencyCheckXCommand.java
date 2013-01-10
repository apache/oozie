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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.dependency.URIHandler;
import org.apache.oozie.executor.jpa.CoordActionGetForInputCheckJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionUpdatePushInputCheckJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.MetadataServiceException;
import org.apache.oozie.service.PartitionDependencyManagerService;
import org.apache.oozie.service.Service;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.URIAccessorException;
import org.apache.oozie.service.URIHandlerService;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.StatusUtils;
import org.apache.oozie.util.XConfiguration;

public class CoordPushDependencyCheckXCommand extends CoordinatorXCommand<Void> {
    private String actionId;
    private JPAService jpaService = null;
    private CoordinatorActionBean coordAction = null;
    private CoordinatorJobBean coordJob = null;

    /**
     * Property name of command re-queue interval for coordinator push check in
     * milliseconds.
     */
    public static final String CONF_COORD_PUSH_CHECK_REQUEUE_INTERVAL = Service.CONF_PREFIX
            + "coord.push.check.requeue.interval";
    /**
     * Default re-queue interval in ms. It is applied when no value defined in
     * the oozie configuration.
     */
    private final int DEFAULT_COMMAND_REQUEUE_INTERVAL = 600000;

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
        String[] pushDepList = pushDeps.split(CoordELFunctions.INSTANCE_SEPARATOR, -1);
        Configuration actionConf = null;
        try {
            actionConf = new XConfiguration(new StringReader(coordAction.getRunConf()));
        }
        catch (IOException e) {
            throw new CommandException(ErrorCode.E1307, e.getMessage(), e);
        }
        String user = ParamChecker.notEmpty(actionConf.get(OozieClient.USER_NAME), OozieClient.USER_NAME);
        List<String> missingDeps = getMissingDependencies(pushDepList, actionConf, user);
        List<String> availableDepList = new ArrayList<String>(Arrays.asList(pushDepList));
        availableDepList.removeAll(missingDeps);

        if (missingDeps.size() > 0) {
            pushDeps = StringUtils.join(missingDeps, CoordELFunctions.INSTANCE_SEPARATOR);
            coordAction.setPushMissingDependencies(pushDeps);
            // Checking for timeout
            if (!isTimeout()) {
                queue(new CoordPushDependencyCheckXCommand(coordAction.getId()),
                        getCoordPushCheckRequeueInterval());
            }
            else {
                queue(new CoordActionTimeOutXCommand(coordAction), 100);
            }
        }
        else { // All push-based dependencies are available
            coordAction.setPushMissingDependencies("");
            if (coordAction.getMissingDependencies() == null || coordAction.getMissingDependencies().length() == 0) {
                coordAction.setStatus(CoordinatorAction.Status.READY);
                // pass jobID to the CoordActionReadyXCommand
                queue(new CoordActionReadyXCommand(coordAction.getJobId()), 100);
            }
        }

        updateCoordAction(coordAction, availableDepList);
        return null;
    }

    /**
     * Return the re-queue interval for coord push dependency check
     * @return
     */
    public long getCoordPushCheckRequeueInterval() {
        long requeueInterval = Services.get().getConf().getLong(CONF_COORD_PUSH_CHECK_REQUEUE_INTERVAL,
                DEFAULT_COMMAND_REQUEUE_INTERVAL);
        return requeueInterval;
    }

    private List<String> getMissingDependencies(String[] dependencies, Configuration conf, String user)
            throws CommandException {
        List<String> missingDeps = new ArrayList<String>();
        URIHandlerService uriService = Services.get().get(URIHandlerService.class);
        for (String dependency : dependencies) {
            try {
                URI uri = new URI(dependency);
                URIHandler uriHandler = uriService.getURIHandler(uri);
                if (!uriHandler.exists(uri, conf, user)) {
                    missingDeps.add(dependency);
                }
            }
            catch (URISyntaxException e) {
                throw new CommandException(ErrorCode.E1025, e.getMessage(), e);
            }
            catch (URIAccessorException e) {
                throw new CommandException(e);
            }
        }
        return missingDeps;
    }

    private void updateCoordAction(CoordinatorActionBean coordAction2, List<String> availPartitionList) throws CommandException {
        coordAction.setLastModifiedTime(new Date());
        if (jpaService != null) {
            try {
                jpaService.execute(new CoordActionUpdatePushInputCheckJPAExecutor(coordAction));
                PartitionDependencyManagerService pdms = Services.get().get(PartitionDependencyManagerService.class);
                if (pdms.removeAvailablePartitions(
                        PartitionDependencyManagerService.createPartitionWrappers(availPartitionList), actionId)) {
                    LOG.debug("Succesfully removed partitions for actionId: [{0}] from available Map ", actionId);
                }
                else {
                    LOG.warn("Unable to remove partitions for actionId: [{0}] from available Map ", actionId);
                }
            }
            catch (JPAExecutorException jex) {
                throw new CommandException(ErrorCode.E1023, jex.getMessage(), jex);
            }
            catch (MetadataServiceException e) {
                throw new CommandException(ErrorCode.E0902, e.getMessage(), e);
            }
        }
    }

    // returns true if timeout command is queued
    private boolean isTimeout() {
        long waitingTime = (new Date().getTime() - Math.max(coordAction.getNominalTime().getTime(), coordAction
                .getCreatedTime().getTime()))
                / (60 * 1000);
        int timeOut = coordAction.getTimeOut();
        if ((timeOut >= 0) && (waitingTime > timeOut)) {
            return true;
        }
        return false;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    public String getEntityKey() {
        return coordAction.getJobId();
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getKey()
     */
    @Override
    public String getKey(){
        return getName() + "_" + actionId;
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

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#eagerLoadState()
     */
    @Override
    protected void eagerLoadState() throws CommandException {
        try {
            jpaService = Services.get().get(JPAService.class);

            if (jpaService != null) {
                coordAction = jpaService.execute(new CoordActionGetForInputCheckJPAExecutor(actionId));
                coordJob = jpaService.execute(new CoordJobGetJPAExecutor(coordAction.getJobId()));
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

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#eagerVerifyPrecondition()
     */
    @Override
    protected void eagerVerifyPrecondition() throws CommandException, PreconditionException {
        if (coordAction.getStatus() != CoordinatorActionBean.Status.WAITING) {
            throw new PreconditionException(ErrorCode.E1100, "[" + actionId
                    + "]::CoordPushDependencyCheck:: Ignoring action. Should be in WAITING state, but state="
                    + coordAction.getStatus());
        }

        // if eligible to do action input check when running with backward
        // support is true
        if (StatusUtils.getStatusForCoordActionInputCheck(coordJob)) {
            return;
        }

        if (coordJob.getStatus() != Job.Status.RUNNING && coordJob.getStatus() != Job.Status.RUNNINGWITHERROR
                && coordJob.getStatus() != Job.Status.PAUSED && coordJob.getStatus() != Job.Status.PAUSEDWITHERROR) {
            throw new PreconditionException(ErrorCode.E1100, "[" + actionId
                    + "]::CoordPushDependencyCheck:: Ignoring action."
                    + " Coordinator job is not in RUNNING/RUNNINGWITHERROR/PAUSED/PAUSEDWITHERROR state, but state="
                    + coordJob.getStatus());
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.command.XCommand#loadState()
     */
    @Override
    protected void loadState() throws CommandException {
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }

}
