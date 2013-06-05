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
package org.apache.oozie.sla;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.event.JobEvent;
import org.apache.oozie.client.event.SLAEvent.EventStatus;
import org.apache.oozie.client.event.SLAEvent.SLAStatus;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.sla.SLACalculationInsertUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.sla.SLASummaryGetJPAExecutor;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.service.SLAService;
import org.apache.oozie.util.XLog;

/**
 * Implementation class for SLACalculator that calculates SLA related to
 * start/end/duration of jobs using a memory-based map
 */
public class SLACalculatorMemory implements SLACalculator {
    // TODO optimization priority based insertion/processing/bumping up-down
    private static Map<String, SLACalcStatus> slaMap;
    private static Set<String> historySet;
    private static int capacity;
    private static JPAService jpa;
    private EventHandlerService eventHandler;

    @Override
    public void init(Configuration conf) throws ServiceException {
        capacity = conf.getInt(SLAService.CONF_CAPACITY, 5000);
        slaMap = new ConcurrentHashMap<String, SLACalcStatus>();
        historySet = Collections.synchronizedSet(new HashSet<String>());
        jpa = Services.get().get(JPAService.class);
        eventHandler = Services.get().get(EventHandlerService.class);
    }

    @Override
    public int size() {
        return slaMap.size();
    }

    @Override
    public SLACalcStatus get(String jobId) {
        return slaMap.get(jobId);
    }

    public Map<String, SLACalcStatus> getMap() {
        return slaMap;
    }

    @Override
    public Iterator<String> iterator() {
        return slaMap.keySet().iterator();
    }

    @Override
    public boolean isEmpty() {
        return slaMap.isEmpty();
    }

    @Override
    public void clear() {
        slaMap.clear();
        historySet.clear();
    }

    /**
     * Invoked via periodic run, update the SLA for registered jobs
     */
    @Override
    public void updateSlaStatus(String jobId) throws JPAExecutorException, ServiceException {
        SLACalcStatus slaCalc = slaMap.get(jobId);
        List<JsonBean> updateList = new ArrayList<JsonBean>();
        synchronized (slaCalc) {
            boolean change = false;
            byte eventProc = slaCalc.getEventProcessed();
            SLARegistrationBean reg = slaCalc.getSLARegistrationBean();
            // calculation w.r.t current time and status
            if ((eventProc & 1) == 0) { // first bit (start-processed) unset
                if (reg.getExpectedStart() != null) {
                    if (reg.getExpectedStart().getTime() < Calendar.getInstance(TimeZone.getTimeZone("UTC"))
                            .getTimeInMillis()) {
                        slaCalc.setEventStatus(EventStatus.START_MISS);
                        change = true;
                        eventHandler.queueEvent(new SLACalcStatus(slaCalc));
                        eventProc++;
                    }
                }
                else {
                    eventProc++; //disable further processing for optional start sla condition
                }
            }
            byte eventProcCopy = eventProc;
            if (((eventProcCopy >> 1) & 1) == 0) { // check if second bit (duration-processed) is unset
                if (slaCalc.getActualStart() != null) {
                    if (reg.getExpectedDuration() < Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTimeInMillis()
                            - slaCalc.getActualStart().getTime()) {
                        slaCalc.setEventStatus(EventStatus.DURATION_MISS);
                        change = true;
                        eventHandler.queueEvent(new SLACalcStatus(slaCalc));
                        eventProc += 2;
                    }
                }
            }
            if (eventProc < 5) {
                if (reg.getExpectedEnd().getTime() < Calendar.getInstance(TimeZone.getTimeZone("UTC"))
                        .getTimeInMillis()) {
                    slaCalc.setEventStatus(EventStatus.END_MISS);
                    slaCalc.setSLAStatus(SLAStatus.MISS);
                    slaCalc.setSlaProcessed(1);
                    change = true;
                    eventHandler.queueEvent(new SLACalcStatus(slaCalc));
                    eventProc += 4;
                }
            }
            if (change) {
                slaCalc.setEventProcessed(eventProc);
                slaCalc.setLastModifiedTime(new Date());
                updateList.add(new SLASummaryBean(slaCalc));
                jpa.execute(new SLACalculationInsertUpdateJPAExecutor(null, updateList));
                if (slaCalc.getEventProcessed() == 7 && slaCalc.getSlaProcessed() != 2) {
                    historySet.add(jobId);
                    slaMap.remove(jobId);
                    XLog.getLog(SLAService.class).trace("Removed Job [{0}] from map after End-processed", jobId);
                }
            }
        }
    }

    /**
     * Periodically run by the SLAService worker threads to update SLA status by
     * iterating through all the jobs in the map
     */
    @Override
    public void updateAllSlaStatus() {
        Iterator<String> iterator = slaMap.keySet().iterator();
        while (iterator.hasNext()) {
            String jobId = iterator.next();
            try {
                updateSlaStatus(jobId);
            }
            catch (Exception e) {
                XLog.getLog(SLAService.class).error("Exception in SLA processing for job [{0}]", jobId, e);
            }
        }
    }

    /**
     * Register a new job into the map for SLA tracking
     */
    @Override
    public boolean addRegistration(String jobId, SLARegistrationBean reg) throws JPAExecutorException {
        List<JsonBean> insertList = new ArrayList<JsonBean>();
        try {
            if (slaMap.size() < capacity) {
                SLACalcStatus slaCalc = new SLACalcStatus(reg);
                slaCalc.setSLAStatus(SLAStatus.NOT_STARTED);
                slaMap.put(jobId, slaCalc);
                insertList.add(reg);
                insertList.add(new SLASummaryBean(slaCalc));
                jpa.execute(new SLACalculationInsertUpdateJPAExecutor(insertList, null));
                XLog.getLog(SLAService.class).trace("SLA Registration Event - Job:" + jobId);
                return true;
            }
            else {
                XLog.getLog(SLAService.class).error(
                        "SLACalculator memory capacity reached. Cannot add new SLA Registration entry for job [{0}]",
                        reg.getId());
            }
        }
        catch (JPAExecutorException jpa) {
            throw jpa;
        }
        return false;
    }

    /**
     * Triggered after receiving Job status change event, update SLA status
     * accordingly
     */
    @Override
    public boolean addJobStatus(String jobId, String jobStatus, JobEvent.EventStatus jobEventStatus, Date startTime,
            Date endTime) throws JPAExecutorException, ServiceException {
        SLACalcStatus slaCalc = slaMap.get(jobId);
        List<JsonBean> updateList = new ArrayList<JsonBean>();
        SLASummaryBean slaInfo = null;
        boolean hasSla = false;
        if (slaCalc != null) {
            synchronized (slaCalc) {
                byte eventProc = slaCalc.getEventProcessed();
                slaCalc.setJobStatus(jobStatus);
                switch (jobEventStatus) {
                    case STARTED:
                        slaInfo = processJobStartSLA(slaCalc, startTime);
                        eventProc += 1;
                        break;
                    case SUCCESS:
                        slaInfo = processJobEndSuccessSLA(slaCalc, startTime, endTime);
                        slaInfo.setSlaProcessed(2);
                        eventProc += 6;
                        break;
                    case FAILURE:
                        slaInfo = processJobEndFailureSLA(slaCalc, startTime, endTime);
                        slaInfo.setSlaProcessed(2);
                        eventProc += 6;
                        break;
                    default:
                        XLog.getLog(SLAService.class).debug("Unknown Job Status [{0}]", jobEventStatus);
                        return false;
                }
                slaCalc.setEventProcessed(eventProc);
                slaInfo.setLastModifiedTime(new Date());
                if (slaInfo.getSlaProcessed() == 2) {
                    slaMap.remove(jobId);
                }
                hasSla = true;
            }
        }
        else if (historySet.contains(jobId)) {
            slaInfo = jpa.execute(new SLASummaryGetJPAExecutor(jobId));
            slaInfo.setJobStatus(jobStatus);
            slaInfo.setActualStart(startTime);
            slaInfo.setActualEnd(endTime);
            if (endTime != null) {
                slaInfo.setActualDuration(endTime.getTime() - startTime.getTime());
            }
            slaInfo.setSlaProcessed(2);
            historySet.remove(jobId);
            hasSla = true;
        }
        if (hasSla) {
            slaInfo.setLastModifiedTime(new Date());
            updateList.add(slaInfo);
            if (jpa != null) {
                jpa.execute(new SLACalculationInsertUpdateJPAExecutor(null, updateList));
            }
            XLog.getLog(SLAService.class)
                    .trace("SLA Status Event - Job:" + jobId + " Status:" + slaCalc.getSLAStatus());
        }

        return hasSla;
    }

    /**
     * Process SLA for jobs that started running. Also update actual-start time
     *
     * @param slaCalc
     * @param actualStart
     * @return SLASummaryBean
     */
    public SLASummaryBean processJobStartSLA(SLACalcStatus slaCalc, Date actualStart) {
        slaCalc.setActualStart(actualStart);
        slaCalc.setSLAStatus(SLAStatus.IN_PROCESS);
        SLARegistrationBean reg = slaCalc.getSLARegistrationBean();
        Date expecStart = reg.getExpectedStart();
        byte eventProc = slaCalc.getEventProcessed();
        if (((eventProc & 1) == 0) && expecStart != null) {
            if (actualStart.getTime() > expecStart.getTime()) {
                slaCalc.setEventStatus(EventStatus.START_MISS);
            }
            else {
                slaCalc.setEventStatus(EventStatus.START_MET);
            }
            eventHandler.queueEvent(new SLACalcStatus(slaCalc));
        }
        return new SLASummaryBean(slaCalc);
    }

    /**
     * Process SLA for jobs that ended successfully. Also update actual-start
     * and end time
     *
     * @param slaCalc
     * @param actualStart
     * @param actualEnd
     * @return SLASummaryBean
     */
    public SLASummaryBean processJobEndSuccessSLA(SLACalcStatus slaCalc, Date actualStart, Date actualEnd) {
        SLARegistrationBean reg = slaCalc.getSLARegistrationBean();
        slaCalc.setActualStart(actualStart);
        slaCalc.setActualEnd(actualEnd);
        long expectedDuration = reg.getExpectedDuration();
        long actualDuration = actualEnd.getTime() - actualStart.getTime();
        slaCalc.setActualDuration(actualDuration);
        if (actualDuration > expectedDuration) {
            slaCalc.setEventStatus(EventStatus.DURATION_MISS);
        }
        else {
            slaCalc.setEventStatus(EventStatus.DURATION_MET);
        }
        eventHandler.queueEvent(new SLACalcStatus(slaCalc));

        Date expectedEnd = reg.getExpectedEnd();
        if (actualEnd.getTime() > expectedEnd.getTime()) {
            slaCalc.setEventStatus(EventStatus.END_MISS);
            slaCalc.setSLAStatus(SLAStatus.MISS);
        }
        else {
            slaCalc.setEventStatus(EventStatus.END_MET);
            slaCalc.setSLAStatus(SLAStatus.MET);
        }
        eventHandler.queueEvent(new SLACalcStatus(slaCalc));
        return new SLASummaryBean(slaCalc);
    }

    /**
     * Process SLA for jobs that ended in failure. Also update actual-start and
     * end time
     *
     * @param slaCalc
     * @param actualStart
     * @param actualEnd
     * @return SLASummaryBean
     */
    public SLASummaryBean processJobEndFailureSLA(SLACalcStatus slaCalc, Date actualStart, Date actualEnd) {
        SLASummaryBean summ;
        slaCalc.setActualStart(actualStart);
        slaCalc.setActualEnd(actualEnd);
        SLARegistrationBean reg = slaCalc.getSLARegistrationBean();
        long expectedDuration = reg.getExpectedDuration();
        long actualDuration = actualEnd.getTime() - actualStart.getTime();
        slaCalc.setActualDuration(actualDuration);
        if (actualDuration > expectedDuration) {
            slaCalc.setEventStatus(EventStatus.DURATION_MISS);
            eventHandler.queueEvent(new SLACalcStatus(slaCalc));
        }
        else {
            slaCalc.setEventStatus(EventStatus.DURATION_MET);
        }
        slaCalc.setEventStatus(EventStatus.END_MISS);
        slaCalc.setSLAStatus(SLAStatus.MISS);
        eventHandler.queueEvent(new SLACalcStatus(slaCalc));
        summ = new SLASummaryBean(slaCalc);
        return summ;
    }

}
