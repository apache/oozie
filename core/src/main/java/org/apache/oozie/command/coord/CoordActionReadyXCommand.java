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

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor.CoordActionQuery;
import org.apache.oozie.executor.jpa.CoordJobGetReadyActionsJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetRunningActionsCountJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.XLog;
import org.jdom.JDOMException;

public class CoordActionReadyXCommand extends CoordinatorXCommand<Void> {
    private final String jobId;
    private final XLog log = getLog();
    private CoordinatorJobBean coordJob = null;
    private JPAService jpaService = null;

    public CoordActionReadyXCommand(String id) {
        super("coord_action_ready", "coord_action_ready", 1);
        this.jobId = id;
    }

    @Override
    protected void setLogInfo() {
        LogUtils.setLogInfo(jobId);
    }

    @Override
    /**
     * Check for READY actions and change state to SUBMITTED by a command to submit the job to WF engine.
     * This method checks all the actions associated with a jobId to figure out which actions
     * to start (based on concurrency and execution order [FIFO, LIFO, LAST_ONLY, NONE])
     *
     */
    protected Void execute() throws CommandException {
        // number of actions to start (-1 means start ALL)
        int numActionsToStart = -1;

        // get execution setting for this job (FIFO, LIFO, LAST_ONLY)
        CoordinatorJob.Execution jobExecution = coordJob.getExecutionOrder();
        // get concurrency setting for this job
        int jobConcurrency = coordJob.getConcurrency();
        // if less than 0, then UNLIMITED concurrency
        if (jobConcurrency >= 0) {
            // count number of actions that are already RUNNING or SUBMITTED
            // subtract from CONCURRENCY to calculate number of actions to start
            // in WF engine

            int numRunningJobs;
            try {
                numRunningJobs = jpaService.execute(new CoordJobGetRunningActionsCountJPAExecutor(jobId));
            }
            catch (JPAExecutorException je) {
                throw new CommandException(je);
            }

            numActionsToStart = jobConcurrency - numRunningJobs;
            if (numActionsToStart < 0) {
                numActionsToStart = 0;
            }
            log.debug("concurrency=" + jobConcurrency + ", execution=" + jobExecution + ", numRunningJobs="
                    + numRunningJobs + ", numLeftover=" + numActionsToStart);
            // no actions to start
            if (numActionsToStart == 0) {
                log.info("Not starting any additional actions because max concurrency [{0}]" +
                        " for coordinator [{1}] has been reached.", jobConcurrency, jobId);
            }
        }
        // get list of actions that are READY and fit in the concurrency and execution

        List<CoordinatorActionBean> actions;
        try {
            actions = jpaService.execute(new CoordJobGetReadyActionsJPAExecutor(jobId, jobExecution.name()));
        }
        catch (JPAExecutorException je) {
            throw new CommandException(je);
        }
        log.debug("Number of READY actions = " + actions.size());
        Date now = new Date();
        // If we're using LAST_ONLY or NONE, we should check if any of these need to be SKIPPED instead of SUBMITTED
        if (jobExecution.equals(CoordinatorJobBean.Execution.LAST_ONLY)) {
            for (Iterator<CoordinatorActionBean> it = actions.iterator(); it.hasNext(); ) {
                CoordinatorActionBean action = it.next();
                try {
                    Date nextNominalTime = CoordCommandUtils.computeNextNominalTime(coordJob, action);
                    if (nextNominalTime != null) {
                        // If the current time is after the next action's nominal time, then we've passed the window where this
                        // action should be started; so set it to SKIPPED
                        if (now.after(nextNominalTime)) {
                            LOG.info("LAST_ONLY execution: Preparing to skip action [{0}] because the current time [{1}] is later "
                                    + "than the nominal time [{2}] of the next action]", action.getId(),
                                    DateUtils.formatDateOozieTZ(now), DateUtils.formatDateOozieTZ(nextNominalTime));
                            queue(new CoordActionSkipXCommand(action, coordJob.getUser(), coordJob.getAppName()));
                            it.remove();
                        } else {
                            LOG.debug("LAST_ONLY execution: Not skipping action [{0}] because the current time [{1}] is earlier "
                                    + "than the nominal time [{2}] of the next action]", action.getId(),
                                    DateUtils.formatDateOozieTZ(now), DateUtils.formatDateOozieTZ(nextNominalTime));
                        }
                    }
                } catch (ParseException | JDOMException e) {
                    LOG.error("Failed to calculate next nominal time", e);
                }
            }
        }
        else if (jobExecution.equals(CoordinatorJobBean.Execution.NONE)) {
            for (Iterator<CoordinatorActionBean> it = actions.iterator(); it.hasNext(); ) {
                CoordinatorActionBean action = it.next();
                // If the current time is after the nominal time of this action plus some tolerance,
                // then we've passed the window where this action should be started; so set it to SKIPPED
                Calendar cal = Calendar.getInstance(DateUtils.getTimeZone(coordJob.getTimeZone()));
                cal.setTime(action.getNominalTime());
                int tolerance = ConfigurationService.getInt(CoordActionInputCheckXCommand.COORD_EXECUTION_NONE_TOLERANCE);
                cal.add(Calendar.MINUTE, tolerance);
                if (now.after(cal.getTime())) {
                    LOG.info("NONE execution: Preparing to skip action [{0}] because the current time [{1}] is more than [{2}]"
                                    + " minutes later than the nominal time [{3}] of the current action]", action.getId(),
                            DateUtils.formatDateOozieTZ(now), tolerance, DateUtils.formatDateOozieTZ(action.getNominalTime()));
                    queue(new CoordActionSkipXCommand(action, coordJob.getUser(), coordJob.getAppName()));
                    it.remove();
                } else {
                    LOG.debug("NONE execution: Not skipping action [{0}] because the current time [{1}] is earlier than [{2}]"
                                    + " minutes later than the nominal time [{3}] of the current action]", action.getId(),
                            DateUtils.formatDateOozieTZ(now), tolerance, DateUtils.formatDateOozieTZ(action.getNominalTime()));
                }
            }
        }

        int counter = 0;
        for (CoordinatorActionBean action : actions) {
            // continue if numActionsToStart is negative (no limit on number of
            // actions), or if the counter is less than numActionsToStart
            if ((numActionsToStart < 0) || (counter < numActionsToStart)) {
                log.debug("Set status to SUBMITTED for id: " + action.getId());
                // change state of action to SUBMITTED
                action.setStatus(CoordinatorAction.Status.SUBMITTED);
                try {
                    CoordActionQueryExecutor.getInstance().executeUpdate(
                            CoordActionQuery.UPDATE_COORD_ACTION_STATUS_PENDING_TIME, action);
                }
                catch (JPAExecutorException je) {
                    throw new CommandException(je);
                }
                // start action
                new CoordActionStartXCommand(action.getId(), coordJob.getUser(), coordJob.getAppName(),
                        action.getJobId()).call();
            }
            else {
                break;
            }
            counter++;

        }
        return null;
    }

    @Override
    public String getEntityKey() {
        return jobId;
    }

    @Override
    public String getKey() {
        return getName() + "_" + jobId;
    }

    @Override
    protected boolean isLockRequired() {
        return true;
    }

    @Override
    protected void loadState() throws CommandException {
        jpaService = Services.get().get(JPAService.class);
        if (jpaService == null) {
            throw new CommandException(ErrorCode.E0610);
        }
        try {
            coordJob = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB_ACTION_READY, jobId);
        }
        catch (JPAExecutorException e) {
            throw new CommandException(e);
        }
        LogUtils.setLogInfo(coordJob);
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if (coordJob.getStatus() != Job.Status.RUNNING && coordJob.getStatus() != Job.Status.RUNNINGWITHERROR
                && coordJob.getStatus() != Job.Status.SUCCEEDED && coordJob.getStatus() != Job.Status.PAUSED
                && coordJob.getStatus() != Job.Status.PAUSEDWITHERROR) {
            throw new PreconditionException(ErrorCode.E1100, "[" + jobId
                    + "]::CoordActionReady:: Ignoring job. Coordinator job is not in RUNNING state, but state="
                    + coordJob.getStatus());
        }
    }
}
