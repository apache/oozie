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
import java.util.Date;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorActionInfo;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.SLAEventBean;
import org.apache.oozie.XException;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.action.hadoop.FsActionExecutor;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.SLAEvent.SlaAppType;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.RerunTransitionXCommand;
import org.apache.oozie.command.bundle.BundleStatusUpdateXCommand;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.coord.CoordUtils;
import org.apache.oozie.executor.jpa.BulkUpdateInsertJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.InstrumentUtils;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.StatusUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.util.db.SLADbOperations;
import org.jdom.Element;
import org.jdom.JDOMException;

/**
 * Rerun coordinator actions by a list of dates or ids. User can specify if refresh or noCleanup.
 * <p/>
 * The "rerunType" can be set as {@link RestConstants.JOB_COORD_RERUN_DATE} or
 * {@link RestConstants.JOB_COORD_RERUN_ACTION}.
 * <p/>
 * The "refresh" is used to indicate if user wants to refresh an action's input and output events.
 * <p/>
 * The "noCleanup" is used to indicate if user wants to cleanup output events for given rerun actions
 */
public class CoordRerunXCommand extends RerunTransitionXCommand<CoordinatorActionInfo> {

    private String rerunType;
    private String scope;
    private boolean refresh;
    private boolean noCleanup;
    private CoordinatorJobBean coordJob = null;
    private JPAService jpaService = null;
    protected boolean prevPending;

    /**
     * The constructor for class {@link CoordRerunXCommand}
     *
     * @param jobId the job id
     * @param rerunType rerun type {@link RestConstants.JOB_COORD_RERUN_DATE} or {@link RestConstants.JOB_COORD_RERUN_ACTION}
     * @param scope the rerun scope for given rerunType separated by ","
     * @param refresh true if user wants to refresh input/output dataset urls
     * @param noCleanup false if user wants to cleanup output events for given rerun actions
     */
    public CoordRerunXCommand(String jobId, String rerunType, String scope, boolean refresh, boolean noCleanup) {
        super("coord_rerun", "coord_rerun", 1);
        this.jobId = ParamChecker.notEmpty(jobId, "jobId");
        this.rerunType = ParamChecker.notEmpty(rerunType, "rerunType");
        this.scope = ParamChecker.notEmpty(scope, "scope");
        this.refresh = refresh;
        this.noCleanup = noCleanup;
    }

    /**
     * Check if all given actions are eligible to rerun.
     *
     * @param actions list of CoordinatorActionBean
     * @return true if all actions are eligible to rerun
     */
    private static boolean checkAllActionsRunnable(List<CoordinatorActionBean> coordActions) {
        ParamChecker.notNull(coordActions, "Coord actions to be rerun");
        boolean ret = false;
        for (CoordinatorActionBean coordAction : coordActions) {
            ret = true;
            if (!coordAction.isTerminalStatus()) {
                ret = false;
                break;
            }
        }
        return ret;
    }

    /**
     * Get the list of actions for a given coordinator job
     * @param rerunType the rerun type (date, action)
     * @param jobId the coordinator job id
     * @param scope the date scope or action id scope
     * @return the list of Coordinator actions
     * @throws CommandException
     */
    public static List<CoordinatorActionBean> getCoordActions(String rerunType, String jobId, String scope) throws CommandException{
        List<CoordinatorActionBean> coordActions = null;
        if (rerunType.equals(RestConstants.JOB_COORD_RERUN_DATE)) {
            coordActions = CoordUtils.getCoordActionsFromDates(jobId, scope);
        }
        else if (rerunType.equals(RestConstants.JOB_COORD_RERUN_ACTION)) {
            coordActions = CoordUtils.getCoordActionsFromIds(jobId, scope);
        }
        return coordActions;
    }

    /**
     * Cleanup output-events directories
     *
     * @param eAction coordinator action xml
     * @param user user name
     * @param group group name
     */
    @SuppressWarnings("unchecked")
    private void cleanupOutputEvents(Element eAction, String user, String group) {
        Element outputList = eAction.getChild("output-events", eAction.getNamespace());
        if (outputList != null) {
            for (Element data : (List<Element>) outputList.getChildren("data-out", eAction.getNamespace())) {
                if (data.getChild("uris", data.getNamespace()) != null) {
                    String uris = data.getChild("uris", data.getNamespace()).getTextTrim();
                    if (uris != null) {
                        String[] uriArr = uris.split(CoordELFunctions.INSTANCE_SEPARATOR);
                        FsActionExecutor fsAe = new FsActionExecutor();
                        for (String uri : uriArr) {
                            Path path = new Path(uri);
                            try {
                                fsAe.delete(user, group, path);
                                LOG.debug("Cleanup the output dir " + path);
                            }
                            catch (ActionExecutorException ae) {
                                LOG.warn("Failed to cleanup the output dir " + uri, ae);
                            }
                        }
                    }

                }
            }
        }
        else {
            LOG.info("No output-events defined in coordinator xml. Therefore nothing to cleanup");
        }
    }

    /**
     * Refresh an action's input and ouput events.
     *
     * @param coordJob coordinator job bean
     * @param coordAction coordinator action bean
     * @throws Exception thrown if failed to materialize coordinator action
     */
    private void refreshAction(CoordinatorJobBean coordJob, CoordinatorActionBean coordAction) throws Exception {
        Configuration jobConf = null;
        try {
            jobConf = new XConfiguration(new StringReader(coordJob.getConf()));
        }
        catch (IOException ioe) {
            LOG.warn("Configuration parse error. read from DB :" + coordJob.getConf(), ioe);
            throw new CommandException(ErrorCode.E1005, ioe.getMessage(), ioe);
        }
        String jobXml = coordJob.getJobXml();
        Element eJob = XmlUtils.parseXml(jobXml);
        Date actualTime = new Date();
        String actionXml = CoordCommandUtils.materializeOneInstance(jobId, dryrun, (Element) eJob.clone(), coordAction
                .getNominalTime(), actualTime, coordAction.getActionNumber(), jobConf, coordAction);
        LOG.debug("Refresh Action actionId=" + coordAction.getId() + ", actionXml="
                + XmlUtils.prettyPrint(actionXml).toString());
        coordAction.setActionXml(actionXml);
    }

    /**
     * Update an action into database table
     *
     * @param coordJob coordinator job bean
     * @param coordAction coordinator action bean
     * @param actionXml coordinator action xml
     * @throws Exception thrown failed to update coordinator action bean or unable to write sla registration event
     */
    private void updateAction(CoordinatorJobBean coordJob, CoordinatorActionBean coordAction, String actionXml)
            throws Exception {
        LOG.debug("updateAction for actionId=" + coordAction.getId());
        if (coordAction.getStatus() == CoordinatorAction.Status.TIMEDOUT) {
            LOG.debug("Updating created time for TIMEDOUT action id =" + coordAction.getId());
            coordAction.setCreatedTime(new Date());
        }
        coordAction.setStatus(CoordinatorAction.Status.WAITING);
        coordAction.setExternalId("");
        coordAction.setExternalStatus("");
        coordAction.setRerunTime(new Date());
        coordAction.setLastModifiedTime(new Date());
        updateList.add(coordAction);
        writeActionRegistration(coordAction.getActionXml(), coordAction, coordJob.getUser(), coordJob.getGroup());
    }

    /**
     * Create SLA RegistrationEvent
     *
     * @param actionXml action xml
     * @param actionBean coordinator action bean
     * @param user user name
     * @param group group name
     * @throws Exception thrown if unable to write sla registration event
     */
    private void writeActionRegistration(String actionXml, CoordinatorActionBean actionBean, String user, String group)
            throws Exception {
        Element eAction = XmlUtils.parseXml(actionXml);
        Element eSla = eAction.getChild("action", eAction.getNamespace()).getChild("info", eAction.getNamespace("sla"));
        SLAEventBean slaEvent = SLADbOperations.createSlaRegistrationEvent(eSla, actionBean.getId(),
                SlaAppType.COORDINATOR_ACTION, user, group, LOG);
        if(slaEvent != null) {
            insertList.add(slaEvent);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    public String getEntityKey() {
        return jobId;
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
            coordJob = jpaService.execute(new CoordJobGetJPAExecutor(jobId));
            prevPending = coordJob.isPending();
        }
        catch (JPAExecutorException je) {
            throw new CommandException(je);
        }
        LogUtils.setLogInfo(coordJob, logInfo);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        BundleStatusUpdateXCommand bundleStatusUpdate = new BundleStatusUpdateXCommand(coordJob, coordJob.getStatus());
        if (coordJob.getStatus() == CoordinatorJob.Status.KILLED
                || coordJob.getStatus() == CoordinatorJob.Status.FAILED) {
            LOG.info("CoordRerunXCommand is not able to run, job status=" + coordJob.getStatus() + ", jobid=" + jobId);
            // Call the parent so the pending flag is reset and state transition
            // of bundle can happen
            if (coordJob.getBundleId() != null) {
                bundleStatusUpdate.call();
            }
            throw new CommandException(ErrorCode.E1018,
                    "coordinator job is killed or failed so all actions are not eligible to rerun!");
        }

        // no actioins have been created for PREP job
        if (coordJob.getStatus() == CoordinatorJob.Status.PREP) {
            LOG.info("CoordRerunXCommand is not able to run, job status=" + coordJob.getStatus() + ", jobid=" + jobId);
            // Call the parent so the pending flag is reset and state transition
            // of bundle can happen
            if (coordJob.getBundleId() != null) {
                bundleStatusUpdate.call();
            }
            throw new CommandException(ErrorCode.E1018,
                    "coordinator job is PREP so no actions are materialized to rerun!");
        }
    }

    @Override
    protected void eagerVerifyPrecondition() throws CommandException, PreconditionException {
        verifyPrecondition();
    }

    @Override
    public void rerunChildren() throws CommandException {
        boolean isError = false;
        try {
            CoordinatorActionInfo coordInfo = null;
            InstrumentUtils.incrJobCounter(getName(), 1, getInstrumentation());
            List<CoordinatorActionBean> coordActions = getCoordActions(rerunType, jobId, scope);
            if (checkAllActionsRunnable(coordActions)) {
                for (CoordinatorActionBean coordAction : coordActions) {
                    String actionXml = coordAction.getActionXml();
                    if (!noCleanup) {
                        Element eAction = XmlUtils.parseXml(actionXml);
                        cleanupOutputEvents(eAction, coordJob.getUser(), coordJob.getGroup());
                    }
                    if (refresh) {
                        refreshAction(coordJob, coordAction);
                    }
                    updateAction(coordJob, coordAction, actionXml);

                    queue(new CoordActionNotificationXCommand(coordAction), 100);
                    queue(new CoordActionInputCheckXCommand(coordAction.getId(), coordAction.getJobId()), 100);
                }
            }
            else {
                isError = true;
                throw new CommandException(ErrorCode.E1018, "part or all actions are not eligible to rerun!");
            }
            coordInfo = new CoordinatorActionInfo(coordActions);

            ret = coordInfo;
        }
        catch (XException xex) {
            isError = true;
            throw new CommandException(xex);
        }
        catch (JDOMException jex) {
            isError = true;
            throw new CommandException(ErrorCode.E0700, jex.getMessage(), jex);
        }
        catch (Exception ex) {
            isError = true;
            throw new CommandException(ErrorCode.E1018, ex.getMessage(), ex);
        }
        finally{
            if(isError){
                transitToPrevious();
            }
        }
    }

    /*
     * (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#getJob()
     */
    @Override
    public Job getJob() {
        return coordJob;
    }

    @Override
    public void notifyParent() throws CommandException {
        //update bundle action
        if (getPrevStatus() != null && coordJob.getBundleId() != null) {
            BundleStatusUpdateXCommand bundleStatusUpdate = new BundleStatusUpdateXCommand(coordJob, getPrevStatus());
            bundleStatusUpdate.call();
        }
    }

    @Override
    public void updateJob() {
        if (getPrevStatus()!= null){
            Job.Status coordJobStatus = getPrevStatus();
            if(coordJobStatus.equals(Job.Status.PAUSED) || coordJobStatus.equals(Job.Status.PAUSEDWITHERROR)) {
                coordJob.setStatus(coordJobStatus);
            }
            if (prevPending) {
                coordJob.setPending();
            } else {
                coordJob.resetPending();
            }
        }

        updateList.add(coordJob);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.RerunTransitionXCommand#performWrites()
     */
    @Override
    public void performWrites() throws CommandException {
        try {
            jpaService.execute(new BulkUpdateInsertJPAExecutor(updateList, insertList));
        }
        catch (JPAExecutorException e) {
            throw new CommandException(e);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.RerunTransitionXCommand#getLog()
     */
    @Override
    public XLog getLog() {
        return LOG;
    }

    @Override
    public final void transitToNext() {
        prevStatus = coordJob.getStatus();
        if (prevStatus == CoordinatorJob.Status.SUCCEEDED || prevStatus == CoordinatorJob.Status.PAUSED
                || prevStatus == CoordinatorJob.Status.SUSPENDED || prevStatus == CoordinatorJob.Status.RUNNING) {
            coordJob.setStatus(Job.Status.RUNNING);
        }
        else {
            // Check for backward compatibility for Oozie versions (3.2 and before)
            // when RUNNINGWITHERROR, SUSPENDEDWITHERROR and
            // PAUSEDWITHERROR is not supported
            coordJob.setStatus(StatusUtils.getStatusIfBackwardSupportTrue(CoordinatorJob.Status.RUNNINGWITHERROR));
        }
        // used for backward support of coordinator 0.1 schema
        coordJob.setStatus(StatusUtils.getStatusForCoordRerun(coordJob, prevStatus));
        coordJob.setPending();
    }

    private final void transitToPrevious() throws CommandException {
        coordJob.setStatus(getPrevStatus());
        if (!prevPending) {
            coordJob.resetPending();
        }
        else {
            coordJob.setPending();
        }
    }
}
