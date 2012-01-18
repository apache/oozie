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

import java.util.List;

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.CoordActionsDeleteForPurgeJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobDeleteJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobsGetForPurgeJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;

/**
 * This class is used for coordinator purge command
 */
public class CoordPurgeXCommand extends CoordinatorXCommand<Void> {
    private JPAService jpaService = null;
    private final int olderThan;
    private final int limit;
    private List<CoordinatorJobBean> jobList = null;

    public CoordPurgeXCommand(int olderThan, int limit) {
        super("coord_purge", "coord_purge", 0);
        this.olderThan = olderThan;
        this.limit = limit;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#execute()
     */
    @Override
    protected Void execute() throws CommandException {
        LOG.debug("STARTED Coord-Purge to purge Jobs older than [{0}] days.", olderThan);

        int actionDeleted = 0;
        if (jobList != null && jobList.size() != 0) {
            for (CoordinatorJobBean coord : jobList) {
                String jobId = coord.getId();
                try {
                    jpaService.execute(new CoordJobDeleteJPAExecutor(jobId));
                    actionDeleted += jpaService.execute(new CoordActionsDeleteForPurgeJPAExecutor(jobId));
                }
                catch (JPAExecutorException e) {
                    throw new CommandException(e);
                }
            }
            LOG.debug("ENDED Coord-Purge deleted jobs :" + jobList.size() + " and actions " + actionDeleted);
        }
        else {
            LOG.debug("ENDED Coord-Purge no Coord job to be deleted");
        }
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    public String getEntityKey() {
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#isLockRequired()
     */
    @Override
    protected boolean isLockRequired() {
        return false;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#loadState()
     */
    @Override
    protected void loadState() throws CommandException {
        try {
            jpaService = Services.get().get(JPAService.class);

            if (jpaService != null) {
                this.jobList = jpaService.execute(new CoordJobsGetForPurgeJPAExecutor(olderThan, limit));
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
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }
}
