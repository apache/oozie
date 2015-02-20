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

package org.apache.oozie.command;

import java.util.Map;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.util.LogUtils;

public abstract class SLAAlertsXCommand extends XCommand<Void> {

    private String jobId;

    public SLAAlertsXCommand(String jobId, String name, String type) {
        super(name, type, 1);
        this.jobId = jobId;
    }

    @Override
    final protected boolean isLockRequired() {
        return true;
    }

    @Override
    final public String getEntityKey() {
        return getJobId();
    }

    final public String getJobId() {
        return jobId;
    }

    @Override
    protected void setLogInfo() {
        LogUtils.setLogInfo(jobId);
    }

    @Override
    protected void loadState() throws CommandException {

    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }

    @Override
    protected Void execute() throws CommandException {
        try {
            if (!executeSlaCommand()) {
                if (!isJobRequest()) {
                    throw new CommandException(ErrorCode.E1026, "No record found");
                }
            }

        }
        catch (ServiceException e) {
            throw new CommandException(e);
        }
        updateJob();
        return null;
    }

    @Override
    public String getKey() {
        return getName() + "_" + jobId;
    }

    protected void validateSLAChangeParam(Map<String, String> slaParams) throws CommandException, PreconditionException {
        for (String key : slaParams.keySet()) {
            if (key.equals(RestConstants.SLA_NOMINAL_TIME) || key.equals(RestConstants.SLA_SHOULD_START)
                    || key.equals(RestConstants.SLA_SHOULD_END) || key.equals(RestConstants.SLA_MAX_DURATION)) {
                // good.
            }
            else {
                throw new CommandException(ErrorCode.E1027, "Unsupported parameter " + key);
            }
        }
    }

    /**
     * Execute sla command.
     *
     * @return true, if successful
     * @throws ServiceException the service exception
     * @throws CommandException the command exception
     */
    protected abstract boolean executeSlaCommand() throws ServiceException, CommandException;

    /**
     * Update job.
     *
     * @throws CommandException the command exception
     */
    protected abstract void updateJob() throws CommandException;

    protected abstract boolean isJobRequest() throws CommandException;

}
