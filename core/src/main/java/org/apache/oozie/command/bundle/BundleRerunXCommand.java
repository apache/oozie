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

package org.apache.oozie.command.bundle;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.XException;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.RerunTransitionXCommand;
import org.apache.oozie.command.coord.CoordRerunXCommand;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.executor.jpa.BundleActionQueryExecutor;
import org.apache.oozie.executor.jpa.BundleJobQueryExecutor;
import org.apache.oozie.executor.jpa.BundleJobQueryExecutor.BundleJobQuery;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.BundleActionQueryExecutor.BundleActionQuery;
import org.apache.oozie.executor.jpa.BatchQueryExecutor.UpdateEntry;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;

/**
 * Rerun bundle coordinator jobs by a list of coordinator names or dates. User can specify if refresh or noCleanup.
 * <p/>
 * The "refresh" is used to indicate if user wants to refresh an action's input/outpur dataset urls
 * <p/>
 * The "noCleanup" is used to indicate if user wants to cleanup output events for given rerun actions
 */
public class BundleRerunXCommand extends RerunTransitionXCommand<Void> {

    private final String coordScope;
    private final String dateScope;
    private final boolean refresh;
    private final boolean noCleanup;
    private BundleJobBean bundleJob;
    private List<BundleActionBean> bundleActions;
    protected boolean prevPending;

    /**
     * The constructor for class {@link BundleRerunXCommand}
     *
     * @param jobId the bundle job id
     * @param coordScope the rerun scope for coordinator job names separated by ","
     * @param dateScope the rerun scope for coordinator nominal times separated by ","
     * @param refresh true if user wants to refresh input/outpur dataset urls
     * @param noCleanup false if user wants to cleanup output events for given rerun actions
     */
    public BundleRerunXCommand(String jobId, String coordScope, String dateScope, boolean refresh, boolean noCleanup) {
        super("bundle_rerun", "bundle_rerun", 1);
        this.jobId = ParamChecker.notEmpty(jobId, "jobId");
        this.coordScope = coordScope;
        this.dateScope = dateScope;
        this.refresh = refresh;
        this.noCleanup = noCleanup;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#loadState()
     */
    @Override
    protected void loadState() throws CommandException {
        try {
            this.bundleJob = BundleJobQueryExecutor.getInstance().get(BundleJobQuery.GET_BUNDLE_JOB, jobId);
            this.bundleActions = BundleActionQueryExecutor.getInstance().getList(
                    BundleActionQuery.GET_BUNDLE_ACTIONS_STATUS_UNIGNORED_FOR_BUNDLE, jobId);
            LogUtils.setLogInfo(bundleJob);
            super.setJob(bundleJob);
            prevPending = bundleJob.isPending();
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }

    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.RerunTransitionXCommand#rerunChildren()
     */
    @Override
    public void rerunChildren() throws CommandException {
        boolean isUpdateActionDone = false;
        Map<String, BundleActionBean> coordNameToBAMapping = new HashMap<String, BundleActionBean>();
        if (bundleActions != null) {
            for (BundleActionBean action : bundleActions) {
                if (action.getCoordName() != null) {
                    coordNameToBAMapping.put(action.getCoordName(), action);
                }
            }
        }

        if (coordScope != null && !coordScope.isEmpty()) {
            String[] list = coordScope.split(",");
            for (String coordName : list) {
                coordName = coordName.trim();
                if (coordNameToBAMapping.keySet().contains(coordName)) {
                    String coordId = coordNameToBAMapping.get(coordName).getCoordId();
                    if (coordId == null) {
                        LOG.info("No coord id found. Therefore, nothing to queue for coord rerun for coordname: " + coordName);
                        continue;
                    }
                    CoordinatorJobBean coordJob = getCoordJob(coordId);

                    String rerunDateScope;
                    if (dateScope != null && !dateScope.isEmpty()) {
                        rerunDateScope = dateScope;
                    }
                    else {
                        String coordStart = DateUtils.formatDateOozieTZ(coordJob.getStartTime());
                        String coordEnd = DateUtils.formatDateOozieTZ(coordJob.getEndTime());
                        rerunDateScope = coordStart + "::" + coordEnd;
                    }
                    LOG.debug("Queuing rerun range [" + rerunDateScope + "] for coord id " + coordId + " of bundle "
                            + bundleJob.getId());
                    queue(new CoordRerunXCommand(coordId, RestConstants.JOB_COORD_SCOPE_DATE, rerunDateScope, refresh,
                            noCleanup));
                    updateBundleAction(coordNameToBAMapping.get(coordName));
                    isUpdateActionDone = true;
                }
                else {
                    LOG.info("Rerun for coord " + coordName + " NOT performed because it is not in bundle ", bundleJob.getId());
                }
            }
        }
        else if (dateScope != null && !dateScope.isEmpty()) {
            if (bundleActions != null) {
                for (BundleActionBean action : bundleActions) {
                    if (action.getCoordId() == null) {
                        LOG.info("No coord id found. Therefore nothing to queue for coord rerun with coord name "
                                + action.getCoordName());
                        continue;
                    }
                    LOG.debug("Queuing rerun range [" + dateScope + "] for coord id " + action.getCoordId() + " of bundle "
                            + bundleJob.getId());
                    queue(new CoordRerunXCommand(action.getCoordId(), RestConstants.JOB_COORD_SCOPE_DATE, dateScope,
                            refresh, noCleanup));
                    updateBundleAction(action);
                    isUpdateActionDone = true;
                }
            }
        }
        if (!isUpdateActionDone) {
            transitToPrevious();
        }
        LOG.info("Rerun coord jobs for the bundle=[{0}]", jobId);
    }

    private final void transitToPrevious() throws CommandException {
        bundleJob.setStatus(getPrevStatus());
        if (!prevPending) {
            bundleJob.resetPending();
        }
        else {
            bundleJob.setPending();
        }
        updateJob();
    }

    /**
     * Update bundle action
     *
     * @param action the bundle action
     * @throws CommandException thrown if failed to update bundle action
     */
    private void updateBundleAction(BundleActionBean action) {
        action.incrementAndGetPending();
        action.setLastModifiedTime(new Date());
        updateList.add(new UpdateEntry<BundleActionQuery>(BundleActionQuery.UPDATE_BUNDLE_ACTION_PENDING_MODTIME, action));
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#updateJob()
     */
    @Override
    public void updateJob() {
        // rerun a paused bundle job will keep job status at paused and pending at previous pending
        if (getPrevStatus() != null) {
            Job.Status bundleJobStatus = getPrevStatus();
            if (bundleJobStatus.equals(Job.Status.PAUSED) || bundleJobStatus.equals(Job.Status.PAUSEDWITHERROR)) {
                bundleJob.setStatus(bundleJobStatus);
                if (prevPending) {
                    bundleJob.setPending();
                }
                else {
                    bundleJob.resetPending();
                }
            }
        }
        updateList.add(new UpdateEntry<BundleJobQuery>(BundleJobQuery.UPDATE_BUNDLE_JOB_STATUS_PENDING, bundleJob));
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.RerunTransitionXCommand#performWrites()
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

    /*
     * (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#getJob()
     */
    @Override
    public Job getJob() {
        return bundleJob;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#notifyParent()
     */
    @Override
    public void notifyParent() throws CommandException {

    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.RerunTransitionXCommand#getLog()
     */
    @Override
    public XLog getLog() {
        return LOG;
    }

    private final CoordinatorJobBean getCoordJob(String coordId) throws CommandException {
        try {
            CoordinatorJobBean job = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB, coordId);
            return job;
        }
        catch (JPAExecutorException je) {
            throw new CommandException(je);
        }

    }

}
