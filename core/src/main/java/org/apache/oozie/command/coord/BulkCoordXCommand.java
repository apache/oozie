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

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.CoordinatorJobInfo;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.OperationType;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.XCommand;
import org.apache.oozie.executor.jpa.CoordJobInfoGetJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;

import java.util.List;
import java.util.Map;

public class BulkCoordXCommand extends XCommand<CoordinatorJobInfo> {
    private Map<String, List<String>> filter;
    private final int start;
    private final int len;
    private CoordinatorJobInfo coordinatorJobInfo;
    private OperationType operation;

    /**
     * The constructor for BulkCoordXCommand
     *
     * @param filter the filter string
     * @param start start location for paging
     * @param length total length to get
     */
    public BulkCoordXCommand(Map<String, List<String>> filter, int start, int length, OperationType operation) {
        super("bulkcoord" + operation, "bulkcoord" + operation, 1);
        this.filter = filter;
        this.start = start;
        this.len = length;
        this.operation = operation;
    }

    /* (non-Javadoc)
    * @see org.apache.oozie.command.XCommand#isLockRequired()
    */
    @Override
    protected boolean isLockRequired() {
        return false;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    public String getEntityKey() {
        return null;
    }

    /* (non-Javadoc)
    * @see org.apache.oozie.command.XCommand#loadState()
    */
    @Override
    protected void loadState() throws CommandException {
        loadJobs();
    }

    /* (non-Javadoc)
    * @see org.apache.oozie.command.XCommand#verifyPrecondition()
    */
    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#execute()
     */
    @Override
    protected CoordinatorJobInfo execute() throws CommandException {
        List<CoordinatorJobBean> jobs = this.coordinatorJobInfo.getCoordJobs();
        for (CoordinatorJobBean job : jobs) {
            switch (operation) {
                case Kill:
                    if (job.getStatus() != CoordinatorJob.Status.SUCCEEDED
                            && job.getStatus() != CoordinatorJob.Status.FAILED
                            && job.getStatus() != CoordinatorJob.Status.DONEWITHERROR
                            && job.getStatus() != CoordinatorJob.Status.KILLED
                            && job.getStatus() != CoordinatorJob.Status.IGNORED) {
                        new CoordKillXCommand(job.getId()).call();
                    }
                    break;
                case Suspend:
                    if (job.getStatus() != CoordinatorJob.Status.SUCCEEDED
                            && job.getStatus() != CoordinatorJob.Status.FAILED
                            && job.getStatus() != CoordinatorJob.Status.KILLED
                            && job.getStatus() != CoordinatorJob.Status.IGNORED) {
                        new CoordSuspendXCommand(job.getId()).call();
                    }
                    break;
                case Resume:
                    if (job.getStatus() == CoordinatorJob.Status.SUSPENDED ||
                            job.getStatus() == CoordinatorJob.Status.SUSPENDEDWITHERROR ||
                            job.getStatus() == Job.Status.PREPSUSPENDED) {
                        new CoordResumeXCommand(job.getId()).call();
                    }
                    break;
                default:
                    throw new CommandException(ErrorCode.E1102, operation);
            }
        }
        loadJobs();
        return this.coordinatorJobInfo;
    }

    private void loadJobs() throws CommandException {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            if (jpaService != null) {
                this.coordinatorJobInfo = jpaService.execute(new CoordJobInfoGetJPAExecutor(filter, start, len));
            }
            else {
                throw new CommandException(ErrorCode.E0610);
            }
        }
        catch (Exception ex) {
            throw new CommandException(ErrorCode.E0603, ex.getMessage(), ex);
        }
    }
}
