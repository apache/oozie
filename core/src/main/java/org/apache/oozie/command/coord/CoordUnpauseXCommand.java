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

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.Job;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.UnpauseTransitionXCommand;

import org.apache.oozie.executor.jpa.CoordJobUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.LogUtils;

public class CoordUnpauseXCommand extends UnpauseTransitionXCommand {
    private final CoordinatorJobBean coordJob;
    private final JPAService jpaService = Services.get().get(JPAService.class);

    public CoordUnpauseXCommand(CoordinatorJobBean coordJob) {
        super("coord_unpause", "coord_unpause", 1);
        this.coordJob = coordJob;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    protected String getEntityKey() {
        return coordJob.getId();
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
    public void loadState() throws CommandException {
        LogUtils.setLogInfo(coordJob, logInfo);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.command.TransitionXCommand#notifyParent()
     */
    @Override
    public void notifyParent() {
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.command.TransitionXCommand#getJob()
     */
    @Override
    public Job getJob() {
        return coordJob;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.command.TransitionXCommand#updateJob()
     */
    @Override
    public void updateJob() throws CommandException {
        try {
            jpaService.execute(new CoordJobUpdateJPAExecutor(coordJob));
        }
        catch (JPAExecutorException e) {
            throw new CommandException(e);
        }
    }

    @Override
    public void unpauseChildren() throws CommandException {
        // TODO - need revisit when revisiting coord job status redesign;

    }

}
