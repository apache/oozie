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

import java.sql.Timestamp;
import java.util.Date;

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;

public class CoordJobMatLookupXCommand extends CoordinatorXCommand<Void> {
    private static final int LOOKAHEAD_WINDOW = 300; // We look ahead 5 minutes for materialization;

    private static final XLog LOG = XLog.getLog(CoordJobMatLookupXCommand.class);
    private int materializationWindow;
    private String jobId;
    private CoordinatorJobBean coordJob = null;
    private JPAService jpaService = null;

    public CoordJobMatLookupXCommand(String id, int materializationWindow) {
        super("materialization_lookup", "materialization_lookup", 1);
        this.jobId = ParamChecker.notEmpty(id, "id");
        this.materializationWindow = materializationWindow;
    }

    @Override
    protected Void execute() throws CommandException {
        LOG.debug("STARTED CoordJobMatLookupXCommand jobId=" + jobId + ", materializationWindow="
                + materializationWindow);
        Timestamp startTime = coordJob.getNextMaterializedTimestamp();
        if (startTime == null) {
            startTime = coordJob.getStartTimestamp();
        }
        // calculate end time by adding materializationWindow to start time.
        // need to convert materializationWindow from secs to milliseconds
        long startTimeMilli = startTime.getTime();
        long endTimeMilli = startTimeMilli + (materializationWindow * 1000);
        Timestamp endTime = new Timestamp(endTimeMilli);
        // if MaterializationWindow end time is greater than endTime
        // for job, then set it to endTime of job
        Timestamp jobEndTime = coordJob.getEndTimestamp();
        if (endTime.compareTo(jobEndTime) > 0) {
            endTime = jobEndTime;
        }
        // update status of job from PREP or RUNNING to PREMATER in coordJob
        coordJob.setStatus(CoordinatorJob.Status.PREMATER);
        coordJob.setLastModifiedTime(new Date());
        try {
            jpaService.execute(new CoordJobUpdateJPAExecutor(coordJob));
        }
        catch (JPAExecutorException ex) {
            throw new CommandException(ex);
        }

        LOG.debug("Materializing coord job id=" + jobId + ", start=" + DateUtils.toDate(startTime) + ", end=" + DateUtils.toDate(endTime)
                + ", window=" + materializationWindow + ", status=PREMATER");
        queue(new CoordActionMaterializeXCommand(jobId, DateUtils.toDate(startTime), DateUtils.toDate(endTime)),
                100);
        LOG.debug("ENDED CoordJobMatLookupXCommand jobId=" + jobId + ", materializationWindow="
                + materializationWindow);
        return null;
    }

    @Override
    protected String getEntityKey() {
        return jobId;
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
            coordJob = jpaService.execute(new CoordJobGetJPAExecutor(jobId));
        }
        catch (JPAExecutorException ex) {
            throw new CommandException(ex);
        }
        LogUtils.setLogInfo(coordJob, logInfo);
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if (!(coordJob.getStatus() == CoordinatorJobBean.Status.PREP || coordJob.getStatus() == CoordinatorJobBean.Status.RUNNING)) {
            throw new PreconditionException(ErrorCode.E1100, "CoordJobMatLookupCommand for jobId=" + jobId + " job is not in PREP or RUNNING but in "
                    + coordJob.getStatus());
        }

        if (coordJob.getNextMaterializedTimestamp() != null
                && coordJob.getNextMaterializedTimestamp().compareTo(coordJob.getEndTimestamp()) >= 0) {
            throw new PreconditionException(ErrorCode.E1100, "CoordJobMatLookupCommand for jobId=" + jobId + " job is already materialized");
        }

        if (coordJob.getNextMaterializedTimestamp() != null
                && coordJob.getNextMaterializedTimestamp().compareTo(new Timestamp(System.currentTimeMillis())) >= 0) {
            throw new PreconditionException(ErrorCode.E1100, "CoordJobMatLookupCommand for jobId=" + jobId + " job is already materialized");
        }

        Timestamp startTime = coordJob.getNextMaterializedTimestamp();
        if (startTime == null) {
            startTime = coordJob.getStartTimestamp();

            if (startTime.after(new Timestamp(System.currentTimeMillis() + LOOKAHEAD_WINDOW * 1000))) {
                throw new PreconditionException(ErrorCode.E1100, "CoordJobMatLookupCommand for jobId=" + jobId + " job's start time is not reached yet - nothing to materialize");
            }
        }
    }
}