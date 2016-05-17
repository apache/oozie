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
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.event.SLAEvent.EventStatus;
import org.apache.oozie.client.event.SLAEvent.SLAStatus;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.XCommand;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.SLASummaryQueryExecutor;
import org.apache.oozie.executor.jpa.SLASummaryQueryExecutor.SLASummaryQuery;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.SLACalcStatus;
import org.apache.oozie.sla.SLASummaryBean;

public abstract class SLAJobEventXCommand extends XCommand<Void> {
    private long lockTimeOut = 0 ;
    JPAService jpaService = Services.get().get(JPAService.class);
    SLACalcStatus slaCalc;
    final static String SLA_LOCK_PREFIX = "sla_";
    private boolean isEnded = false;
    private boolean isEndMiss = false;

    public SLAJobEventXCommand(SLACalcStatus slaCalc, long lockTimeOut) {
        super("SLA.job.event", "SLA.job.event", 1);
        this.slaCalc = slaCalc;
        this.lockTimeOut = lockTimeOut;
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
        return SLA_LOCK_PREFIX + slaCalc.getId();
    }

    protected long getLockTimeOut() {
        return lockTimeOut;
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }


    @Override
    protected Void execute() throws CommandException {
        updateJobInfo();
        if (isEnded) {
            processForEnd();
        }
        else {
            processForRunning();
        }
        try {
            writeToDB();
        }
        catch (XException e) {
            throw new CommandException(e);
        }
        return null;
    }

    /**
     * Verify job.
     */
    protected abstract void updateJobInfo();

    /**
     * Should alert.
     *
     * @param slaObj the sla obj
     * @return true, if successful
     */
    private boolean shouldAlert(SLACalcStatus slaObj) {
        return !slaObj.getSLAConfigMap().containsKey(OozieClient.SLA_DISABLE_ALERT);
    }

    /**
     * Queue event.
     *
     * @param event the event
     */
    private void queueEvent(SLACalcStatus event) {
        Services.get().get(EventHandlerService.class).queueEvent(event);
    }

    /**
     * Process duration sla.
     *
     * @param expected the expected
     * @param actual the actual
     * @param slaCalc the sla calc
     */
    private void processDurationSLA(long expected, long actual, SLACalcStatus slaCalc) {
        if (expected != -1) {
            if (actual > expected) {
                slaCalc.setEventStatus(EventStatus.DURATION_MISS);
            }
            else if (actual <= expected) {
                slaCalc.setEventStatus(EventStatus.DURATION_MET);
            }
            if (shouldAlert(slaCalc)) {
                queueEvent(new SLACalcStatus(slaCalc));
            }
        }
    }


    /**
     * WriteSLA object to DB.
     *
     * @throws JPAExecutorException the JPA executor exception
     */
    private void writeToDB() throws JPAExecutorException {
        byte eventProc = slaCalc.getEventProcessed();
        // no more processing, no transfer to history set
        if (slaCalc.getEventProcessed() >= 8) {
            slaCalc.setEventProcessed(8);
        }

        SLASummaryBean slaSummaryBean = new SLASummaryBean();
        slaSummaryBean.setId(slaCalc.getId());
        slaSummaryBean.setEventProcessed(eventProc);
        slaSummaryBean.setSLAStatus(slaCalc.getSLAStatus());
        slaSummaryBean.setEventStatus(slaCalc.getEventStatus());
        slaSummaryBean.setActualEnd(slaCalc.getActualEnd());
        slaSummaryBean.setActualStart(slaCalc.getActualStart());
        slaSummaryBean.setActualDuration(slaCalc.getActualDuration());
        slaSummaryBean.setJobStatus(slaCalc.getJobStatus());
        slaSummaryBean.setLastModifiedTime(new Date());

        SLASummaryQueryExecutor.getInstance().executeUpdate(SLASummaryQuery.UPDATE_SLA_SUMMARY_FOR_STATUS_ACTUAL_TIMES,
                slaSummaryBean);

        LOG.debug(" Stored SLA SummaryBean Job [{0}] eventProc = [{1}], status = [{2}]", slaCalc.getId(),
                slaCalc.getEventProcessed(), slaCalc.getJobStatus());

    }

    /**
     * Process for end.
     */
    private void processForEnd() {
        byte eventProc = slaCalc.getEventProcessed();

        LOG.debug("Job {0} has ended. endtime = [{1}]", slaCalc.getId(), slaCalc.getActualEnd());
        if (isEndMiss()) {
            slaCalc.setSLAStatus(SLAStatus.MISS);
        }
        else {
            slaCalc.setSLAStatus(SLAStatus.MET);
        }
        if (eventProc != 8 && slaCalc.getActualStart() != null) {
            if ((eventProc & 1) == 0) {
                if (slaCalc.getExpectedStart() != null) {
                    if (slaCalc.getExpectedStart().getTime() < slaCalc.getActualStart().getTime()) {
                        slaCalc.setEventStatus(EventStatus.START_MISS);
                    }
                    else {
                        slaCalc.setEventStatus(EventStatus.START_MET);
                    }
                    if (shouldAlert(slaCalc)) {
                        queueEvent(new SLACalcStatus(slaCalc));
                    }
                }
            }
            slaCalc.setActualDuration(slaCalc.getActualEnd().getTime() - slaCalc.getActualStart().getTime());
            if (((eventProc >> 1) & 1) == 0) {
                processDurationSLA(slaCalc.getExpectedDuration(), slaCalc.getActualDuration(), slaCalc);
            }
        }
        if (eventProc != 8 && eventProc < 4) {
            if (isEndMiss()) {
                slaCalc.setEventStatus(EventStatus.END_MISS);
            }
            else {
                slaCalc.setEventStatus(EventStatus.END_MET);
            }
            if (shouldAlert(slaCalc)) {
                queueEvent(new SLACalcStatus(slaCalc));
            }
        }
        slaCalc.setEventProcessed(8);
    }

    /**
     * Process for running.
     */
    private void processForRunning() {
        byte eventProc = slaCalc.getEventProcessed();

        if (eventProc != 8 && slaCalc.getActualStart() != null) {
            slaCalc.setSLAStatus(SLAStatus.IN_PROCESS);
        }
        if (eventProc != 8 && (eventProc & 1) == 0) {
            if (slaCalc.getExpectedStart() == null) {
                eventProc++;
            }
            else if (slaCalc.getActualStart() != null) {
                if (slaCalc.getExpectedStart().getTime() < slaCalc.getActualStart().getTime()) {
                    slaCalc.setEventStatus(EventStatus.START_MISS);
                }
                else {
                    slaCalc.setEventStatus(EventStatus.START_MET);
                }
                if (shouldAlert(slaCalc)) {
                    queueEvent(new SLACalcStatus(slaCalc));
                }
                eventProc++;
            }
            else if (slaCalc.getExpectedStart() != null
                    && slaCalc.getExpectedStart().getTime() < System.currentTimeMillis()) {
                slaCalc.setEventStatus(EventStatus.START_MISS);
                if (shouldAlert(slaCalc)) {
                    queueEvent(new SLACalcStatus(slaCalc));
                }
                eventProc++;
            }

        }
        if (eventProc != 8 && ((eventProc >> 1) & 1) == 0) {
            if (slaCalc.getExpectedDuration() == -1) {
                eventProc += 2;
            }
            else if (slaCalc.getActualStart() != null && slaCalc.getExpectedDuration() != -1) {
                if (System.currentTimeMillis() - slaCalc.getActualStart().getTime() > slaCalc.getExpectedDuration()) {
                    slaCalc.setEventStatus(EventStatus.DURATION_MISS);
                    if (shouldAlert(slaCalc)) {
                        queueEvent(new SLACalcStatus(slaCalc));
                    }
                    eventProc += 2;
                }
            }
        }
        if (eventProc < 4) {
            if (slaCalc.getExpectedEnd() != null) {
                if (slaCalc.getExpectedEnd().getTime() < System.currentTimeMillis()) {
                    slaCalc.setEventStatus(EventStatus.END_MISS);
                    slaCalc.setSLAStatus(SLAStatus.MISS);
                    if (shouldAlert(slaCalc)) {
                        queueEvent(new SLACalcStatus(slaCalc));
                    }
                    eventProc += 4;
                }
            }
            else {
                eventProc += 4;
            }
        }
        slaCalc.setEventProcessed(eventProc);
    }

    public boolean isEnded() {
        return isEnded;
    }

    public void setEnded(boolean isEnded) {
        this.isEnded = isEnded;
    }

    public boolean isEndMiss() {
        return isEndMiss;
    }

    public void setEndMiss(boolean isEndMiss) {
        this.isEndMiss = isEndMiss;
    }

}
