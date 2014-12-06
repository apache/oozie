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
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.dependency.DependencyChecker;
import org.apache.oozie.dependency.ActionDependency;
import org.apache.oozie.dependency.URIHandler;
import org.apache.oozie.executor.jpa.CoordActionGetForInputCheckJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor.CoordActionQuery;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.CallableQueueService;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.PartitionDependencyManagerService;
import org.apache.oozie.service.RecoveryService;
import org.apache.oozie.service.Service;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.URIHandlerService;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.StatusUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.DateUtils;

public class CoordPushDependencyCheckXCommand extends CoordinatorXCommand<Void> {
    protected String actionId;
    protected JPAService jpaService = null;
    protected CoordinatorActionBean coordAction = null;
    protected CoordinatorJobBean coordJob = null;

    /**
     * Property name of command re-queue interval for coordinator push check in
     * milliseconds.
     */
    public static final String CONF_COORD_PUSH_CHECK_REQUEUE_INTERVAL = Service.CONF_PREFIX
            + "coord.push.check.requeue.interval";
    private boolean registerForNotification;
    private boolean removeAvailDependencies;

    public CoordPushDependencyCheckXCommand(String actionId) {
        this(actionId, false, true);
    }

    public CoordPushDependencyCheckXCommand(String actionId, boolean registerForNotification) {
        this(actionId, registerForNotification, !registerForNotification);
    }

    public CoordPushDependencyCheckXCommand(String actionId, boolean registerForNotification,
            boolean removeAvailDependencies) {
        super("coord_push_dep_check", "coord_push_dep_check", 0);
        this.actionId = actionId;
        this.registerForNotification = registerForNotification;
        this.removeAvailDependencies = removeAvailDependencies;
    }

    protected CoordPushDependencyCheckXCommand(String actionName, String actionId) {
        super(actionName, actionName, 0);
        this.actionId = actionId;
    }

    @Override
    protected void setLogInfo() {
        LogUtils.setLogInfo(actionId);
    }

    @Override
    protected Void execute() throws CommandException {
        String pushMissingDeps = coordAction.getPushMissingDependencies();
        if (pushMissingDeps == null || pushMissingDeps.length() == 0) {
            LOG.info("Nothing to check. Empty push missing dependency");
        }
        else {
            String[] missingDepsArray = DependencyChecker.dependenciesAsArray(pushMissingDeps);
            LOG.info("First Push missing dependency is [{0}] ", missingDepsArray[0]);
            LOG.trace("Push missing dependencies are [{0}] ", pushMissingDeps);
            if (registerForNotification) {
                LOG.debug("Register for notifications is true");
            }

            try {
                Configuration actionConf = null;
                try {
                    actionConf = new XConfiguration(new StringReader(coordAction.getRunConf()));
                }
                catch (IOException e) {
                    throw new CommandException(ErrorCode.E1307, e.getMessage(), e);
                }

                // Check all dependencies during materialization to avoid registering in the cache.
                // But check only first missing one afterwards similar to
                // CoordActionInputCheckXCommand for efficiency. listPartitions is costly.
                ActionDependency actionDep = DependencyChecker.checkForAvailability(missingDepsArray, actionConf,
                        !registerForNotification);

                boolean isChangeInDependency = true;
                boolean timeout = false;
                if (actionDep.getMissingDependencies().size() == 0) {
                    // All push-based dependencies are available
                    onAllPushDependenciesAvailable();
                }
                else {
                    if (actionDep.getMissingDependencies().size() == missingDepsArray.length) {
                        isChangeInDependency = false;
                    }
                    else {
                        String stillMissingDeps = DependencyChecker.dependenciesAsString(actionDep
                                .getMissingDependencies());
                        coordAction.setPushMissingDependencies(stillMissingDeps);
                    }
                    // Checking for timeout
                    timeout = isTimeout();
                    if (timeout) {
                        queue(new CoordActionTimeOutXCommand(coordAction, coordJob.getUser(), coordJob.getAppName()));
                    }
                    else {
                        queue(new CoordPushDependencyCheckXCommand(coordAction.getId()),
                                getCoordPushCheckRequeueInterval());
                    }
                }

                updateCoordAction(coordAction, isChangeInDependency);
                if (registerForNotification) {
                    registerForNotification(actionDep.getMissingDependencies(), actionConf);
                }
                if (removeAvailDependencies) {
                    unregisterAvailableDependencies(actionDep.getAvailableDependencies());
                }
                if (timeout) {
                    unregisterMissingDependencies(actionDep.getMissingDependencies(), actionId);
                }
            }
            catch (Exception e) {
                final CallableQueueService callableQueueService = Services.get().get(CallableQueueService.class);
                if (isTimeout()) {
                    LOG.debug("Queueing timeout command");
                    // XCommand.queue() will not work when there is a Exception
                    callableQueueService.queue(new CoordActionTimeOutXCommand(coordAction, coordJob.getUser(), coordJob.getAppName()));
                    unregisterMissingDependencies(Arrays.asList(missingDepsArray), actionId);
                }
                else if (coordAction.getMissingDependencies() != null
                        && coordAction.getMissingDependencies().length() > 0) {
                    // Queue again on exception as RecoveryService will not queue this again with
                    // the action being updated regularly by CoordActionInputCheckXCommand
                    callableQueueService.queue(new CoordPushDependencyCheckXCommand(coordAction.getId(),
                            registerForNotification, removeAvailDependencies),
                            Services.get().getConf().getInt(RecoveryService.CONF_COORD_OLDER_THAN, 600) * 1000);
                }
                throw new CommandException(ErrorCode.E1021, e.getMessage(), e);
            }
        }
        return null;
    }

    /**
     * Return the re-queue interval for coord push dependency check
     * @return
     */
    public long getCoordPushCheckRequeueInterval() {
        long requeueInterval = ConfigurationService.getLong(CONF_COORD_PUSH_CHECK_REQUEUE_INTERVAL);
        return requeueInterval;
    }

    /**
     * Returns true if timeout period has been reached
     *
     * @return true if it is time for timeout else false
     */
    protected boolean isTimeout() {
        long waitingTime = (new Date().getTime() - Math.max(coordAction.getNominalTime().getTime(), coordAction
                .getCreatedTime().getTime()))
                / (60 * 1000);
        int timeOut = coordAction.getTimeOut();
        return (timeOut >= 0) && (waitingTime > timeOut);
    }

    protected void onAllPushDependenciesAvailable() throws CommandException {
        coordAction.setPushMissingDependencies("");
        Services.get().get(PartitionDependencyManagerService.class)
                .removeCoordActionWithDependenciesAvailable(coordAction.getId());
        if (coordAction.getMissingDependencies() == null || coordAction.getMissingDependencies().length() == 0) {
            Date nominalTime = coordAction.getNominalTime();
            Date currentTime = new Date();
            // The action should become READY only if current time > nominal time;
            // CoordActionInputCheckXCommand will take care of moving it to READY when it is nominal time.
            if (nominalTime.compareTo(currentTime) > 0) {
                LOG.info("[" + actionId + "]::ActionInputCheck:: nominal Time is newer than current time. Current="
                        + DateUtils.formatDateOozieTZ(currentTime) + ", nominal=" + DateUtils.formatDateOozieTZ(nominalTime));
            }
            else {
                String actionXml = resolveCoordConfiguration();
                coordAction.setActionXml(actionXml);
                coordAction.setStatus(CoordinatorAction.Status.READY);
                // pass jobID to the CoordActionReadyXCommand
                queue(new CoordActionReadyXCommand(coordAction.getJobId()), 100);
            }
        }
        else if (isTimeout()) {
            // If it is timeout and all push dependencies are available but still some unresolved
            // missing dependencies queue CoordActionInputCheckXCommand now. Else it will have to
            // wait till RecoveryService kicks in
            queue(new CoordActionInputCheckXCommand(coordAction.getId(), coordAction.getJobId()));
        }
    }

    private String resolveCoordConfiguration() throws CommandException {
        try {
            Configuration actionConf = new XConfiguration(new StringReader(coordAction.getRunConf()));
            StringBuilder actionXml = new StringBuilder(coordAction.getActionXml());
            String newActionXml = CoordActionInputCheckXCommand.resolveCoordConfiguration(actionXml, actionConf,
                    actionId);
            actionXml.replace(0, actionXml.length(), newActionXml);
            return actionXml.toString();
        }
        catch (Exception e) {
            throw new CommandException(ErrorCode.E1021, e.getMessage(), e);
        }
    }

    protected void updateCoordAction(CoordinatorActionBean coordAction, boolean isChangeInDependency)
            throws CommandException {
        coordAction.setLastModifiedTime(new Date());
        if (jpaService != null) {
            try {
                if (isChangeInDependency) {
                    CoordActionQueryExecutor.getInstance().executeUpdate(
                            CoordActionQuery.UPDATE_COORD_ACTION_FOR_PUSH_INPUTCHECK, coordAction);
                    if (EventHandlerService.isEnabled() && coordAction.getStatus() != CoordinatorAction.Status.READY) {
                        // since event is not to be generated unless action
                        // RUNNING via StartX
                        generateEvent(coordAction, coordJob.getUser(), coordJob.getAppName(), null);
                    }
                }
                else {
                    CoordActionQueryExecutor.getInstance().executeUpdate(
                            CoordActionQuery.UPDATE_COORD_ACTION_FOR_MODIFIED_DATE, coordAction);
                }
            }
            catch (JPAExecutorException jex) {
                throw new CommandException(ErrorCode.E1021, jex.getMessage(), jex);
            }
        }
    }

    private void registerForNotification(List<String> missingDeps, Configuration actionConf) {
        URIHandlerService uriService = Services.get().get(URIHandlerService.class);
        String user = actionConf.get(OozieClient.USER_NAME, OozieClient.USER_NAME);
        for (String missingDep : missingDeps) {
            try {
                URI missingURI = new URI(missingDep);
                URIHandler handler = uriService.getURIHandler(missingURI);
                handler.registerForNotification(missingURI, actionConf, user, actionId);
                    LOG.debug("Registered uri [{0}] for notifications", missingURI);
            }
            catch (Exception e) {
                LOG.warn("Exception while registering uri [{0}] for notifications", missingDep, e);
            }
        }
    }

    private void unregisterAvailableDependencies(List<String> availableDeps) {
        URIHandlerService uriService = Services.get().get(URIHandlerService.class);
        for (String availableDep : availableDeps) {
            try {
                URI availableURI = new URI(availableDep);
                URIHandler handler = uriService.getURIHandler(availableURI);
                if (handler.unregisterFromNotification(availableURI, actionId)) {
                    LOG.debug("Successfully unregistered uri [{0}] from notifications", availableURI);
                }
                else {
                    LOG.warn("Unable to unregister uri [{0}] from notifications", availableURI);
                }
            }
            catch (Exception e) {
                LOG.warn("Exception while unregistering uri [{0}] from notifications", availableDep, e);
            }
        }
    }

    public static void unregisterMissingDependencies(List<String> missingDeps, String actionId) {
        final XLog LOG = XLog.getLog(CoordPushDependencyCheckXCommand.class);
        URIHandlerService uriService = Services.get().get(URIHandlerService.class);
        for (String missingDep : missingDeps) {
            try {
                URI missingURI = new URI(missingDep);
                URIHandler handler = uriService.getURIHandler(missingURI);
                if (handler.unregisterFromNotification(missingURI, actionId)) {
                    LOG.debug("Successfully unregistered uri [{0}] from notifications", missingURI);
                }
                else {
                    LOG.warn("Unable to unregister uri [{0}] from notifications", missingURI);
                }
            }
            catch (Exception e) {
                LOG.warn("Exception while unregistering uri [{0}] from notifications", missingDep, e);
            }
        }
    }

    @Override
    public String getEntityKey() {
        return actionId.substring(0, actionId.indexOf("@"));
    }

    @Override
    public String getKey(){
        return getName() + "_" + actionId;
    }

    @Override
    protected boolean isLockRequired() {
        return true;
    }

    @Override
    protected void loadState() throws CommandException {
        jpaService = Services.get().get(JPAService.class);
        try {
            coordAction = jpaService.execute(new CoordActionGetForInputCheckJPAExecutor(actionId));
            if (coordAction != null) {
                coordJob = jpaService.execute(new CoordJobGetJPAExecutor(coordAction.getJobId()));
                LogUtils.setLogInfo(coordAction);
            }
            else {
                throw new CommandException(ErrorCode.E0605, actionId);
            }
        }
        catch (JPAExecutorException je) {
            throw new CommandException(je);
        }
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
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

}
