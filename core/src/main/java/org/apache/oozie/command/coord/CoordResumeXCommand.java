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

import java.util.List;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.wf.ResumeXCommand;
import org.apache.oozie.executor.jpa.CoordJobGetActionsJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.InstrumentUtils;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;

public class CoordResumeXCommand extends CoordinatorXCommand<Void> {
    private final String jobId;
    private final static XLog log = XLog.getLog(CoordResumeXCommand.class);
    private CoordinatorJobBean coordJob = null;
    private JPAService jpaService = null;
    private boolean exceptionOccured = false;
    CoordinatorJob.Status prevStatus;

    public CoordResumeXCommand(String id) {
        super("coord_resume", "coord_resume", 1);
        this.jobId = ParamChecker.notEmpty(id, "id");
    }

    @Override
    protected Void execute() throws CommandException {
        try {
            InstrumentUtils.incrJobCounter(getName(), 1, getInstrumentation());
            coordJob.setStatus(CoordinatorJob.Status.PREP);

            List<CoordinatorActionBean> actionList = jpaService.execute(new CoordJobGetActionsJPAExecutor(jobId));

            for (CoordinatorActionBean action : actionList) {
                // queue a ResumeCommand
                if (action.getExternalId() != null) {
                    queue(new ResumeXCommand(action.getExternalId()));
                }
            }
            jpaService.execute(new CoordJobUpdateJPAExecutor(coordJob));

            return null;
        }
        catch (XException ex) {
            exceptionOccured = true;
            throw new CommandException(ex);
        }
        finally {
            if (exceptionOccured) {
                coordJob.setStatus(CoordinatorJob.Status.FAILED);
            }
        }
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
        catch (JPAExecutorException e) {
            throw new CommandException(e);
        }
        prevStatus = coordJob.getStatus();
        LogUtils.setLogInfo(coordJob, logInfo);
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if (coordJob.getStatus() != CoordinatorJob.Status.SUSPENDED) {
            throw new PreconditionException(ErrorCode.E1100, "CoordResumeCommand not Resumed - "
                    + "job not in SUSPENDED state " + jobId);
        }
    }
}