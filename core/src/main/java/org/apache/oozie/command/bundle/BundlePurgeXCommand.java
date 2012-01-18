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

import java.util.List;

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.XCommand;
import org.apache.oozie.executor.jpa.BundleActionsDeleteForPurgeJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobDeleteJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobsGetForPurgeJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XLog;

/**
 * This class is used for bundle purge command
 */
public class BundlePurgeXCommand extends XCommand<Void> {
    private JPAService jpaService = null;
    private final int olderThan;
    private final int limit;
    private List<BundleJobBean> jobList = null;

    public BundlePurgeXCommand(int olderThan, int limit) {
        super("bundle_purge", "bundle_purge", 0);
        this.olderThan = olderThan;
        this.limit = limit;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#loadState()
     */
    @Override
    protected void loadState() throws CommandException {
        try {
            jpaService = Services.get().get(JPAService.class);

            if (jpaService != null) {
                this.jobList = jpaService.execute(new BundleJobsGetForPurgeJPAExecutor(olderThan, limit));
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
     * @see org.apache.oozie.command.XCommand#execute()
     */
    @Override
    protected Void execute() throws CommandException {
        LOG.debug("STARTED Bundle-Purge to purge Jobs older than [{0}] days.", olderThan);

        int actionDeleted = 0;
        if (jobList != null && jobList.size() != 0) {
            for (BundleJobBean bundle : jobList) {
                String jobId = bundle.getId();
                try {
                    jpaService.execute(new BundleJobDeleteJPAExecutor(jobId));
                    actionDeleted += jpaService.execute(new BundleActionsDeleteForPurgeJPAExecutor(jobId));
                }
                catch (JPAExecutorException e) {
                    throw new CommandException(e);
                }
            }
            LOG.debug("ENDED Bundle-Purge deleted jobs :" + jobList.size() + " and actions " + actionDeleted);
        }
        else {
            LOG.debug("ENDED Bundle-Purge no Bundle job to be deleted");
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
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }
}
