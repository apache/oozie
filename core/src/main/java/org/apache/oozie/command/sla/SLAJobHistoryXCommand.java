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

package org.apache.oozie.command.sla;

import java.util.Date;

import org.apache.oozie.XException;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.XCommand;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.SLASummaryQueryExecutor;
import org.apache.oozie.executor.jpa.SLASummaryQueryExecutor.SLASummaryQuery;
import org.apache.oozie.sla.SLASummaryBean;

public abstract class SLAJobHistoryXCommand extends XCommand<Boolean> {

    protected String jobId;

    public SLAJobHistoryXCommand(String jobId) {
        super("SLAJobHistoryXCommand", "SLAJobHistoryXCommand", 1);
        this.jobId = jobId;

    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }

    @Override
    protected boolean isLockRequired() {
        return true;
    }

    @Override
    protected boolean isReQueueRequired() {
        return false;
    }

    @Override
    public String getEntityKey() {
        return SLAJobEventXCommand.SLA_LOCK_PREFIX + jobId;
    }

    protected long getLockTimeOut() {
        return 0L;
    }

    protected Boolean execute() throws CommandException {
        if (isJobEnded()) {
            try {
                updateSLASummary();
            }
            catch (XException e) {
                throw new CommandException(e);
            }
            return true;
        }
        else {
            LOG.debug("Job [{0}] is not finished", jobId);
        }
        return false;

    }

    /**
     * Checks if is job ended.
     *
     * @return true, if is job ended
     */
    protected abstract boolean isJobEnded();

    /**
     * Update SLASummary
     *
     */
    protected abstract void updateSLASummary() throws CommandException, XException;

    /**
     * Update sla summary.
     *
     * @param id the id
     * @param startTime the start time
     * @param endTime the end time
     * @param status the status
     * @throws JPAExecutorException the JPA executor exception
     */
    protected void updateSLASummary(String id, Date startTime, Date endTime, String status) throws JPAExecutorException {
        SLASummaryBean sla = SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, id);
        if (sla.getJobStatus().equals(status) && sla.getEventProcessed() == 8) {
            LOG.debug("SLA job is already updated", sla.getId(), sla.getEventProcessed(), sla.getJobStatus());
            return;
        }
        if (sla != null) {
            sla.setActualStart(startTime);
            sla.setActualEnd(endTime);
            if (startTime != null && endTime != null) {
                sla.setActualDuration(endTime.getTime() - startTime.getTime());
            }
            sla.setLastModifiedTime(new Date());
            sla.setEventProcessed(8);
            sla.setJobStatus(status);
            SLASummaryQueryExecutor.getInstance().executeUpdate(SLASummaryQuery.UPDATE_SLA_SUMMARY_FOR_STATUS_ACTUAL_TIMES,
                    sla);
            LOG.debug(" Stored SLA SummaryBean Job [{0}] eventProc = [{1}], status = [{2}]", sla.getId(),
                    sla.getEventProcessed(), sla.getJobStatus());

        }
    }

}
