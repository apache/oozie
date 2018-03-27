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
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.Job;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.coord.CoordELEvaluator;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.coord.ElException;
import org.apache.oozie.coord.input.dependency.CoordInputDependency;
import org.apache.oozie.executor.jpa.CoordActionGetForInputCheckJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor.CoordActionQuery;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.CallableQueueService;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Service;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.StatusUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;

/**
 * The command to check if an action's data input paths exist in the file system.
 */
public class CoordActionInputCheckXCommand extends CoordinatorXCommand<Void> {

    public static final String COORD_EXECUTION_NONE_TOLERANCE = "oozie.coord.execution.none.tolerance";

    private final String actionId;
    /**
     * Property name of command re-queue interval for coordinator action input check in
     * milliseconds.
     */
    public static final String CONF_COORD_INPUT_CHECK_REQUEUE_INTERVAL = Service.CONF_PREFIX
            + "coord.input.check.requeue.interval";
    public static final String CONF_COORD_INPUT_CHECK_REQUEUE_INTERVAL_ADDITIONAL_DELAY = Service.CONF_PREFIX
            + "coord.input.check.requeue.interval.additional.delay";
    private CoordinatorActionBean coordAction = null;
    private CoordinatorJobBean coordJob = null;
    private JPAService jpaService = null;
    private String jobId = null;
    public CoordActionInputCheckXCommand(String actionId, String jobId) {
        super("coord_action_input", "coord_action_input", 1);
        this.actionId = ParamChecker.notEmpty(actionId, "actionId");
        this.jobId = jobId;
    }

    @Override
    protected void setLogInfo() {
        LogUtils.setLogInfo(actionId);
    }

    @Override
    protected Void execute() throws CommandException {
        LOG.debug("[" + actionId + "]::ActionInputCheck:: Action is in WAITING state.");

        // this action should only get processed if current time > nominal time;
        // otherwise, requeue this action for delay execution;
        Date nominalTime = coordAction.getNominalTime();
        Date currentTime = new Date();
        if (nominalTime.compareTo(currentTime) > 0) {
            queue(new CoordActionInputCheckXCommand(coordAction.getId(), coordAction.getJobId()), nominalTime.getTime()
                    - currentTime.getTime());
            updateCoordAction(coordAction, false);
            LOG.info("[" + actionId
                    + "]::ActionInputCheck:: nominal Time is newer than current time, so requeue and wait. Current="
                    + DateUtils.formatDateOozieTZ(currentTime) + ", nominal=" + DateUtils.formatDateOozieTZ(nominalTime));

            return null;
        }

        StringBuilder actionXml = new StringBuilder(coordAction.getActionXml());
        boolean isChangeInDependency = false;
        try {
            Configuration actionConf = new XConfiguration(new StringReader(coordAction.getRunConf()));
            Date now = new Date();
            if (coordJob.getExecutionOrder().equals(CoordinatorJobBean.Execution.LAST_ONLY)) {
                Date nextNominalTime = CoordCommandUtils.computeNextNominalTime(coordJob, coordAction);
                if (nextNominalTime != null) {
                    // If the current time is after the next action's nominal time, then we've passed the window where this action
                    // should be started; so set it to SKIPPED
                    if (now.after(nextNominalTime)) {
                        LOG.info("LAST_ONLY execution: Preparing to skip action [{0}] because the current time [{1}] is later than "
                                + "the nominal time [{2}] of the next action]", coordAction.getId(),
                                DateUtils.formatDateOozieTZ(now), DateUtils.formatDateOozieTZ(nextNominalTime));
                        queue(new CoordActionSkipXCommand(coordAction, coordJob.getUser(), coordJob.getAppName()));
                        return null;
                    } else {
                        LOG.debug("LAST_ONLY execution: Not skipping action [{0}] because the current time [{1}] is earlier than "
                                + "the nominal time [{2}] of the next action]", coordAction.getId(),
                                DateUtils.formatDateOozieTZ(now), DateUtils.formatDateOozieTZ(nextNominalTime));
                    }
                }
            }
            else if (coordJob.getExecutionOrder().equals(CoordinatorJobBean.Execution.NONE)) {
                // If the current time is after the nominal time of this action plus some tolerance,
                // then we've passed the window where this action should be started; so set it to SKIPPED
                Calendar cal = Calendar.getInstance(DateUtils.getTimeZone(coordJob.getTimeZone()));
                cal.setTime(nominalTime);
                int tolerance = ConfigurationService.getInt(COORD_EXECUTION_NONE_TOLERANCE);
                cal.add(Calendar.MINUTE, tolerance);
                if (now.after(cal.getTime())) {
                    LOG.info("NONE execution: Preparing to skip action [{0}] because the current time [{1}] is more than [{2}]"
                            + " minutes later than the nominal time [{3}] of the current action]", coordAction.getId(),
                            DateUtils.formatDateOozieTZ(now), tolerance, DateUtils.formatDateOozieTZ(nominalTime));
                    queue(new CoordActionSkipXCommand(coordAction, coordJob.getUser(), coordJob.getAppName()));
                    return null;
                } else {
                    LOG.debug("NONE execution: Not skipping action [{0}] because the current time [{1}] is earlier than [{2}]"
                            + " minutes later than the nominal time [{3}] of the current action]", coordAction.getId(),
                            DateUtils.formatDateOozieTZ(now), tolerance, DateUtils.formatDateOozieTZ(coordAction.getNominalTime()));
                }
            }

            StringBuilder existList = new StringBuilder();
            StringBuilder nonExistList = new StringBuilder();
            CoordInputDependency coordPullInputDependency = coordAction.getPullInputDependencies();
            CoordInputDependency coordPushInputDependency = coordAction.getPushInputDependencies();


            String missingDependencies = coordPullInputDependency.getMissingDependencies();
            StringBuilder nonResolvedList = new StringBuilder();

            CoordCommandUtils.getResolvedList(missingDependencies, nonExistList, nonResolvedList);
            String firstMissingDependency = "";
            // For clarity regarding which is the missing dependency in synchronous order
            // instead of printing entire list, some of which, may be available
            if (nonExistList.length() > 0) {
                firstMissingDependency = nonExistList.toString().split(CoordELFunctions.INSTANCE_SEPARATOR)[0];
            }
            LOG.info("[" + actionId + "]::CoordActionInputCheck:: Missing deps:" + firstMissingDependency + " "
                    + nonResolvedList.toString());


            boolean status = checkResolvedInput(actionXml, existList, nonExistList, actionConf);
            boolean isPushDependenciesMet = coordPushInputDependency.isDependencyMet();
            if (status && nonResolvedList.length() > 0) {
                status = (isPushDependenciesMet) ? checkUnResolvedInput(actionXml, actionConf) : false;
            }
            coordAction.setLastModifiedTime(currentTime);
            coordAction.setActionXml(actionXml.toString());

            isChangeInDependency = isChangeInDependency(nonExistList, missingDependencies, nonResolvedList, status);

            if (status && isPushDependenciesMet) {
                moveCoordActionToReady(actionXml, actionConf, coordPullInputDependency, coordPushInputDependency);
            }
            else if (!isTimeout(currentTime)) {
                if (!status) {
                    long addtionalDelay = isChangeInDependency ? 0
                            : ConfigurationService.getInt(CONF_COORD_INPUT_CHECK_REQUEUE_INTERVAL_ADDITIONAL_DELAY)
                                    * 1000L;
                    queue(new CoordActionInputCheckXCommand(coordAction.getId(), coordAction.getJobId()),
                            addtionalDelay + getCoordInputCheckRequeueInterval());
                }
                updateCoordAction(coordAction, isChangeInDependency);
            }
            else {
                if (isPushDependenciesMet) {
                    queue(new CoordActionTimeOutXCommand(coordAction, coordJob.getUser(), coordJob.getAppName()));
                }
                else {
                    // Let CoordPushDependencyCheckXCommand queue the timeout
                    queue(new CoordPushDependencyCheckXCommand(coordAction.getId()));
                }
                updateCoordAction(coordAction, isChangeInDependency);
            }
        }
        catch (AccessControlException e) {
            LOG.error("Permission error in ActionInputCheck", e);
            if (isTimeout(currentTime)) {
                LOG.debug("Queueing timeout command");
                Services.get().get(CallableQueueService.class)
                        .queue(new CoordActionTimeOutXCommand(coordAction, coordJob.getUser(), coordJob.getAppName()));
            }
            else {
                // Requeue InputCheckCommand for permission denied error with longer interval
                Services.get()
                        .get(CallableQueueService.class)
                        .queue(new CoordActionInputCheckXCommand(coordAction.getId(), coordAction.getJobId()),
                                2 * getCoordInputCheckRequeueInterval());
            }
            updateCoordAction(coordAction, isChangeInDependency);
        }
        catch (Exception e) {
            if (isTimeout(currentTime)) {
                LOG.debug("Queueing timeout command");
                // XCommand.queue() will not work when there is a Exception
                Services.get().get(CallableQueueService.class)
                        .queue(new CoordActionTimeOutXCommand(coordAction, coordJob.getUser(), coordJob.getAppName()));
            }
            updateCoordAction(coordAction, isChangeInDependency);
            throw new CommandException(ErrorCode.E1021, e.getMessage(), e);
        }
        return null;
    }

    private boolean isChangeInDependency(StringBuilder nonExistList, String missingDependencies,
            StringBuilder nonResolvedList, boolean status) throws IOException {
        if (nonResolvedList.length() > 0 && status == false) {
            nonExistList.append(CoordCommandUtils.RESOLVED_UNRESOLVED_SEPARATOR).append(nonResolvedList);
        }
        return coordAction.getPullInputDependencies().isChangeInDependency(nonExistList, missingDependencies,
                nonResolvedList, status);
    }

    static String resolveCoordConfiguration(StringBuilder actionXml, Configuration actionConf, String actionId)
            throws Exception {
        return resolveCoordConfiguration(actionXml, actionConf, actionId, null, null);
    }

    static String resolveCoordConfiguration(StringBuilder actionXml, Configuration actionConf, String actionId,
            CoordInputDependency pullDependencies, CoordInputDependency pushDependencies) throws Exception {
        Element eAction = XmlUtils.parseXml(actionXml.toString());
        ELEvaluator eval = CoordELEvaluator.createDataEvaluator(eAction, actionConf, actionId, pullDependencies,
                pushDependencies);
        materializeDataProperties(eAction, actionConf, eval);
        return XmlUtils.prettyPrint(eAction).toString();
    }

    private boolean isTimeout(Date currentTime) {
        long waitingTime = (currentTime.getTime() - Math.max(coordAction.getNominalTime().getTime(), coordAction
                .getCreatedTime().getTime()))
                / (60 * 1000);
        int timeOut = coordAction.getTimeOut();
        return (timeOut >= 0) && (waitingTime > timeOut);
    }

    private void updateCoordAction(CoordinatorActionBean coordAction, boolean isChangeInDependency)
            throws CommandException {
        coordAction.setLastModifiedTime(new Date());
        if (jpaService != null) {
            try {
                if (isChangeInDependency) {
                    coordAction.setMissingDependencies(coordAction.getPullInputDependencies().serialize());
                    CoordActionQueryExecutor.getInstance().executeUpdate(
                            CoordActionQuery.UPDATE_COORD_ACTION_FOR_INPUTCHECK, coordAction);
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
            catch (Exception jex) {
                throw new CommandException(ErrorCode.E1021, jex.getMessage(), jex);
            }
        }
    }
    /**
     * This function reads the value of re-queue interval for coordinator input
     * check command from the Oozie configuration provided by Configuration
     * Service. If nothing defined in the configuration, it uses the code
     * specified default value.
     *
     * @return re-queue interval in ms
     */
    public long getCoordInputCheckRequeueInterval() {
        long requeueInterval = ConfigurationService.getLong(CONF_COORD_INPUT_CHECK_REQUEUE_INTERVAL);
        return requeueInterval;
    }

    /**
     * To check the list of input paths if all of them exist
     *
     * @param actionXml action xml
     * @param existList the list of existed paths
     * @param nonExistList the list of non existed paths
     * @param conf action configuration
     * @return true if all input paths are existed
     * @throws Exception thrown of unable to check input path
     */
    protected boolean checkResolvedInput(StringBuilder actionXml, StringBuilder existList, StringBuilder nonExistList,
            Configuration conf) throws Exception {
        return coordAction.getPullInputDependencies().checkPullMissingDependencies(coordAction, existList,
                nonExistList);
    }

    /**
     * Check un resolved input.
     *
     * @param coordAction the coord action
     * @param actionXml the action xml
     * @param conf the conf
     * @return true, if successful
     * @throws Exception the exception
     */
    protected boolean checkUnResolvedInput(CoordinatorActionBean coordAction, StringBuilder actionXml,
            Configuration conf) throws Exception {
        Element eAction = XmlUtils.parseXml(actionXml.toString());
        LOG.debug("[" + actionId + "]::ActionInputCheck:: Checking Latest/future");
        boolean allExist = checkUnresolvedInstances(coordAction, eAction, conf);
        if (allExist) {
            actionXml.replace(0, actionXml.length(), XmlUtils.prettyPrint(eAction).toString());
        }
        return allExist;
    }

    /**
     * Check un resolved input.
     *
     * @param actionXml the action xml
     * @param conf the conf
     * @return true, if successful
     * @throws Exception the exception
     */
    protected boolean checkUnResolvedInput(StringBuilder actionXml, Configuration conf) throws Exception {
        return checkUnResolvedInput(coordAction, actionXml, conf);
    }

    /**
     * Materialize data properties defined in <action> tag. it includes dataIn(<DS>) and dataOut(<DS>) it creates a list
     * of files that will be needed.
     *
     * @param eAction action element
     * @param conf action configuration
     * @throws Exception thrown if failed to resolve data properties
     * @update modify 'Action' element with appropriate list of files.
     */
    @SuppressWarnings("unchecked")
    static void materializeDataProperties(Element eAction, Configuration conf, ELEvaluator eval) throws Exception {
        Element configElem = eAction.getChild("action", eAction.getNamespace()).getChild("workflow",
                eAction.getNamespace()).getChild("configuration", eAction.getNamespace());
        if (configElem != null) {
            for (Element propElem : (List<Element>) configElem.getChildren("property", configElem.getNamespace())) {
                resolveTagContents("value", propElem, eval);
            }
        }
    }

    /**
     * To resolve property value which contains el functions
     *
     * @param tagName tag name
     * @param elem the child element of "property" element
     * @param eval el functions evaluator
     * @throws Exception thrown if unable to resolve tag value
     */
    private static void resolveTagContents(String tagName, Element elem, ELEvaluator eval) throws Exception {
        if (elem == null) {
            return;
        }
        Element tagElem = elem.getChild(tagName, elem.getNamespace());
        if (tagElem != null) {
            String updated = CoordELFunctions.evalAndWrap(eval, tagElem.getText());
            tagElem.removeContent();
            tagElem.addContent(updated);
        }
        else {
            XLog.getLog(CoordActionInputCheckXCommand.class).warn(" Value NOT FOUND " + tagName);
        }
    }

    /**
     * Check if any unsolved paths under data output. Resolve the unresolved data input paths.
     *
     * @param eAction action element
     * @param actionConf action configuration
     * @return true if successful to resolve input and output paths
     * @throws Exception thrown if failed to resolve data input and output paths
     */
    @SuppressWarnings("unchecked")
    private boolean checkUnresolvedInstances(CoordinatorActionBean coordAction, Element eAction,
            Configuration actionConf) throws Exception {

        boolean ret = coordAction.getPullInputDependencies().checkUnresolved(coordAction, eAction);

        // Using latest() or future() in output-event is not intuitive.
        // We need to make sure, this assumption is correct.
        Element outputList = eAction.getChild("output-events", eAction.getNamespace());
        if (outputList != null) {
            for (Element dEvent : (List<Element>) outputList.getChildren("data-out", eAction.getNamespace())) {
                if (dEvent.getChild(CoordCommandUtils.UNRESOLVED_INSTANCES_TAG, dEvent.getNamespace()) != null) {
                    throw new CommandException(ErrorCode.E1006, "coord:latest()/future()",
                            " not permitted in output-event ");
                }
            }
        }
        return ret;
    }

    /**
     * Resolves coordinator configuration and moves CoordAction to READY state
     *
     * @param actionXml
     * @param actionConf
     * @param coordPullInputDependency
     * @param coordPushInputDependency
     * @throws Exception
     */
    private void moveCoordActionToReady(StringBuilder actionXml, Configuration actionConf,
            CoordInputDependency coordPullInputDependency, CoordInputDependency coordPushInputDependency)
            throws Exception {
        String newActionXml = null;
        try {
            newActionXml = resolveCoordConfiguration(actionXml, actionConf, actionId, coordPullInputDependency,
                    coordPushInputDependency);
        }
        catch (ElException e) {
            coordAction.setStatus(CoordinatorAction.Status.FAILED);
            updateCoordAction(coordAction, true);
            throw e;
        }
        actionXml.replace(0, actionXml.length(), newActionXml);
        coordAction.setActionXml(actionXml.toString());
        coordAction.setStatus(CoordinatorAction.Status.READY);
        updateCoordAction(coordAction, true);
        new CoordActionReadyXCommand(coordAction.getJobId()).call();
    }

    /**
     * getting the error code of the coord action. (used mainly for unit testing)
     */
    protected String getCoordActionErrorCode() {
        if (coordAction != null) {
            return coordAction.getErrorCode();
        }
        return null;
    }

    /**
     * getting the error message of the coord action. (used mainly for unit testing)
     */
    protected String getCoordActionErrorMsg() {
        if (coordAction != null) {
            return coordAction.getErrorMessage();
        }
        return null;
    }

    @Override
    public String getEntityKey() {
        return this.jobId;
    }

    @Override
    protected boolean isLockRequired() {
        return true;
    }

    @Override
    protected void loadState() throws CommandException {
        if (jpaService == null) {
            jpaService = Services.get().get(JPAService.class);
        }
        try {
            coordAction = jpaService.execute(new CoordActionGetForInputCheckJPAExecutor(actionId));
            coordJob = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB_INPUT_CHECK,
                    coordAction.getJobId());
        }
        catch (JPAExecutorException je) {
            throw new CommandException(je);
        }
        LogUtils.setLogInfo(coordAction);
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if (coordAction.getStatus() != CoordinatorActionBean.Status.WAITING) {
            throw new PreconditionException(ErrorCode.E1100, "[" + actionId
                    + "]::CoordActionInputCheck:: Ignoring action. Should be in WAITING state, but state="
                    + coordAction.getStatus());
        }

        // if eligible to do action input check when running with backward support is true
        if (StatusUtils.getStatusForCoordActionInputCheck(coordJob)) {
            return;
        }

        if (coordJob.getStatus() != Job.Status.RUNNING && coordJob.getStatus() != Job.Status.RUNNINGWITHERROR
                && coordJob.getStatus() != Job.Status.PAUSED
                && coordJob.getStatus() != Job.Status.PAUSEDWITHERROR) {
            throw new PreconditionException(
                    ErrorCode.E1100, "["+ actionId + "]::CoordActionInputCheck:: Ignoring action." +
                    		" Coordinator job is not in RUNNING/RUNNINGWITHERROR/PAUSED/PAUSEDWITHERROR state, but state="
                            + coordJob.getStatus());
        }
    }

    @Override
    public String getKey(){
        return getName() + "_" + actionId;
    }

}
