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
import org.apache.oozie.client.Job;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PauseTransitionXCommand;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.BundleJobQueryExecutor;
import org.apache.oozie.executor.jpa.BundleJobQueryExecutor.BundleJobQuery;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.util.LogUtils;

public class BundlePauseXCommand extends PauseTransitionXCommand {
    private BundleJobBean bundleJob;

    public BundlePauseXCommand(BundleJobBean bundleJob) {
        super("bundle_pause", "bundle_pause", 1);
        this.bundleJob = bundleJob;
    }

    @Override
    public String getEntityKey() {
        return bundleJob.getId();
    }

    @Override
    protected boolean isLockRequired() {
        return true;
    }

    @Override
    public void loadState() throws CommandException {
        LogUtils.setLogInfo(bundleJob);
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }

    @Override
    public void notifyParent() {
    }

    @Override
    public Job getJob() {
        return bundleJob;
    }

    @Override
    public void updateJob() throws CommandException {
        try {
            BundleJobQueryExecutor.getInstance().executeUpdate(BundleJobQuery.UPDATE_BUNDLE_JOB_STATUS, bundleJob);
        }
        catch (JPAExecutorException e) {
            throw new CommandException(e);
        }
    }

    @Override
    public void pauseChildren() throws CommandException {
        // TODO - need revisit when revisiting coord job status redesign;

    }

    @Override
    public void performWrites() throws CommandException {
    }

}
