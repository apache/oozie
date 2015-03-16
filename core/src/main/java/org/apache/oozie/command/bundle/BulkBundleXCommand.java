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

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.BundleJobInfo;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.Job;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.OperationType;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.XCommand;
import org.apache.oozie.executor.jpa.BundleJobInfoGetJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;

import java.util.List;
import java.util.Map;

public class BulkBundleXCommand extends XCommand<BundleJobInfo> {
    private Map<String, List<String>> filter;
    private final int start;
    private final int len;
    private BundleJobInfo bundleJobInfo;
    private OperationType operation;

    /**
     * The constructor for BulkBundleXCommand
     *
     * @param filter the filter string
     * @param start start location for paging
     * @param length total length to get
     * @param operation the type of operation to perform, it can be kill, suspend or resume
     */
    public BulkBundleXCommand(Map<String, List<String>> filter, int start, int length, OperationType operation) {
        super("bulkbundle" + operation, "bulkbundle" + operation, 1);
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
        loadBundleJobs();
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
    protected BundleJobInfo execute() throws CommandException {
        List<BundleJobBean> jobs = this.bundleJobInfo.getBundleJobs();
        for (BundleJobBean job : jobs) {
            switch (operation) {
                case Kill:
                    if (job.getStatus() != Job.Status.SUCCEEDED
                            && job.getStatus() != Job.Status.FAILED
                            && job.getStatus() != Job.Status.DONEWITHERROR
                            && job.getStatus() != Job.Status.KILLED) {
                        new BundleKillXCommand(job.getId()).call();
                    }
                    break;
                case Suspend:
                    if (job.getStatus() != Job.Status.SUCCEEDED
                            && job.getStatus() != Job.Status.FAILED
                            && job.getStatus() != Job.Status.KILLED
                            && job.getStatus() != Job.Status.DONEWITHERROR) {
                        new BundleJobSuspendXCommand(job.getId()).call();
                    }
                    break;
                case Resume:
                    if (job.getStatus() == Job.Status.SUSPENDED
                            || job.getStatus() == Job.Status.SUSPENDEDWITHERROR
                            || job.getStatus() == Job.Status.PREPSUSPENDED) {
                        new BundleJobResumeXCommand(job.getId()).call();
                    }
                    break;
                default:
                    throw new CommandException(ErrorCode.E1102, operation);
            }
        }
        loadBundleJobs();
        return this.bundleJobInfo;
    }

    private void loadBundleJobs() throws CommandException {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            if (jpaService != null) {
                this.bundleJobInfo = jpaService.execute(new BundleJobInfoGetJPAExecutor(filter, start, len));
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
