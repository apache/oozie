/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.command.coord;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorActionInfo;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
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
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetActionForNominalTimeJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetActionsForDatesJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.InstrumentUtils;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;
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
     * Get the list of actions for given id ranges
     *
     * @param jobId coordinator job id
     * @param scope the id range to rerun separated by ","
     * @return the list of all actions to rerun
     * @throws CommandException thrown if failed to get coordinator actions by given id range
     */
    private List<CoordinatorActionBean> getCoordActionsFromIds(String jobId, String scope) throws CommandException {
        ParamChecker.notEmpty(jobId, "jobId");
        ParamChecker.notEmpty(scope, "scope");

        Set<String> actions = new HashSet<String>();
        String[] list = scope.split(",");
        for (String s : list) {
            s = s.trim();
            if (s.contains("-")) {
                String[] range = s.split("-");
                if (range.length != 2) {
                    throw new CommandException(ErrorCode.E0302, "format is wrong for action's range '" + s + "'");
                }
                int start;
                int end;
                try {
                    start = Integer.parseInt(range[0].trim());
                    end = Integer.parseInt(range[1].trim());
                    if (start > end) {
                        throw new CommandException(ErrorCode.E0302, "format is wrong for action's range '" + s + "'");
                    }
                }
                catch (NumberFormatException ne) {
                    throw new CommandException(ErrorCode.E0302, ne);
                }
                for (int i = start; i <= end; i++) {
                    actions.add(jobId + "@" + i);
                }
            }
            else {
                try {
                    Integer.parseInt(s);
                }
                catch (NumberFormatException ne) {
                    throw new CommandException(ErrorCode.E0302, "format is wrong for action id'" + s
                            + "'. Integer only.");
                }
                actions.add(jobId + "@" + s);
            }
        }

        List<CoordinatorActionBean> coordActions = new ArrayList<CoordinatorActionBean>();
        for (String id : actions) {
            CoordinatorActionBean coordAction;
            try {
                coordAction = jpaService.execute(new CoordActionGetJPAExecutor(id));
            }
            catch (JPAExecutorException je) {
                throw new CommandException(je);
            }
            coordActions.add(coordAction);
            LOG.debug("Rerun coordinator for actionId='" + id + "'");
        }
        return coordActions;
    }

    /**
     * Get the list of actions for given date ranges
     *
     * @param jobId coordinator job id
     * @param scope the date range to rerun separated by ","
     * @return the list of dates to rerun
     * @throws CommandException thrown if failed to get coordinator actions by given date range
     */
    private List<CoordinatorActionBean> getCoordActionsFromDates(String jobId, String scope) throws CommandException {
        ParamChecker.notEmpty(jobId, "jobId");
        ParamChecker.notEmpty(scope, "scope");

        Set<CoordinatorActionBean> actionSet = new HashSet<CoordinatorActionBean>();
        String[] list = scope.split(",");
        for (String s : list) {
            s = s.trim();
            if (s.contains("::")) {
                String[] dateRange = s.split("::");
                if (dateRange.length != 2) {
                    throw new CommandException(ErrorCode.E0302, "format is wrong for date's range '" + s + "'");
                }
                Date start;
                Date end;
                try {
                    start = DateUtils.parseDateUTC(dateRange[0].trim());
                    end = DateUtils.parseDateUTC(dateRange[1].trim());
                    if (start.after(end)) {
                        throw new CommandException(ErrorCode.E0302, "start date is older than end date: '" + s + "'");
                    }
                }
                catch (Exception e) {
                    throw new CommandException(ErrorCode.E0302, e);
                }

                List<CoordinatorActionBean> listOfActions = getActionIdsFromDateRange(jobId, start, end);
                actionSet.addAll(listOfActions);
            }
            else {
                try {
                    Date date = DateUtils.parseDateUTC(s.trim());
                    CoordinatorActionBean coordAction = jpaService
                            .execute(new CoordJobGetActionForNominalTimeJPAExecutor(jobId, date));
                    actionSet.add(coordAction);
                }
                catch (JPAExecutorException e) {
                    throw new CommandException(e);
                }
                catch (Exception e) {
                    throw new CommandException(ErrorCode.E0302, e);
                }
            }
        }

        List<CoordinatorActionBean> coordActions = new ArrayList<CoordinatorActionBean>();
        for (CoordinatorActionBean coordAction : actionSet) {
            coordActions.add(coordAction);
            LOG.debug("Rerun coordinator for actionId='" + coordAction.getId() + "'");
        }
        return coordActions;
    }

    /**
     * Get coordinator action ids between given start and end time
     *
     * @param jobId coordinator job id
     * @param start start time
     * @param end end time
     * @return a list of coordinator actions belong to the range of start and end time
     * @throws CommandException thrown if failed to get coordinator actions
     */
    private List<CoordinatorActionBean> getActionIdsFromDateRange(String jobId, Date start, Date end)
            throws CommandException {
        List<CoordinatorActionBean> list;
        try {
            list = jpaService.execute(new CoordJobGetActionsForDatesJPAExecutor(jobId, start, end));
        }
        catch (JPAExecutorException je) {
            throw new CommandException(je);
        }
        return list;
    }

    /**
     * Check if all given actions are eligible to rerun.
     *
     * @param actions list of CoordinatorActionBean
     * @return true if all actions are eligible to rerun
     */
    private boolean checkAllActionsRunnable(List<CoordinatorActionBean> coordActions) {
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
            throw new CommandException(ErrorCode.E1005, ioe);
        }
        String jobXml = coordJob.getJobXml();
        Element eJob = XmlUtils.parseXml(jobXml);
        String actionXml = CoordCommandUtils.materializeOneInstance(jobId, dryrun, (Element) eJob.clone(), coordAction
                .getNominalTime(), coordAction.getActionNumber(), jobConf, coordAction);
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
        jpaService.execute(new org.apache.oozie.executor.jpa.CoordActionUpdateJPAExecutor(coordAction));
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
        SLADbOperations.writeSlaRegistrationEvent(eSla, actionBean.getId(), SlaAppType.COORDINATOR_ACTION, user, group,
                LOG);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    protected String getEntityKey() {
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
        if (coordJob.getStatus() == CoordinatorJob.Status.KILLED
                || coordJob.getStatus() == CoordinatorJob.Status.FAILED) {
            LOG.info("CoordRerunCommand is not able to run, job status=" + coordJob.getStatus() + ", jobid=" + jobId);
            throw new CommandException(ErrorCode.E1018,
                    "coordinator job is killed or failed so all actions are not eligible to rerun!");
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
            List<CoordinatorActionBean> coordActions;
            if (rerunType.equals(RestConstants.JOB_COORD_RERUN_DATE)) {
                coordActions = getCoordActionsFromDates(jobId, scope);
            }
            else if (rerunType.equals(RestConstants.JOB_COORD_RERUN_ACTION)) {
                coordActions = getCoordActionsFromIds(jobId, scope);
            }
            else {
                isError = true;
                throw new CommandException(ErrorCode.E1018, "date or action expected.");
            }
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
                    queue(new CoordActionInputCheckXCommand(coordAction.getId()), 100);
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
            throw new CommandException(ErrorCode.E0700, jex);
        }
        catch (Exception ex) {
            isError = true;
            throw new CommandException(ErrorCode.E1018, ex);
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
    public void updateJob() throws CommandException {
        try {
            // rerun a paused coordinator job will keep job status at paused and pending at previous pending
            if (getPrevStatus()!= null && getPrevStatus().equals(Job.Status.PAUSED)) {
                coordJob.setStatus(Job.Status.PAUSED);
                if (prevPending) {
                    coordJob.setPending();
                } else {
                    coordJob.resetPending();
                }
            }

            jpaService.execute(new CoordJobUpdateJPAExecutor(coordJob));
        }
        catch (JPAExecutorException je) {
            throw new CommandException(je);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.RerunTransitionXCommand#getLog()
     */
    @Override
    public XLog getLog() {
        return LOG;
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
