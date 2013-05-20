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

import java.util.Date;
import org.apache.oozie.AppType;
import org.apache.oozie.client.event.SLAEvent;

/**
 * Class used by SLAService to store SLA objects and perform calculations and
 * sla decisions
 */
public class SLACalcStatus extends SLAEvent {

    private SLARegistrationBean regBean;
    private SLACalculatorBean calcBean;
    private String jobStatus;
    private SLAStatus slaStatus;
    private EventStatus eventStatus;
    private Date actualStart;
    private Date actualEnd;
    private long actualDuration;

    public SLACalcStatus(SLARegistrationBean reg, String jobStatus, EventStatus eventStatus) {
        setSLARegistrationBean(reg);
        calcBean = new SLACalculatorBean();
        setJobId(reg.getJobId());
        setJobStatus(jobStatus);
        setEventStatus(eventStatus);
    }

    public SLACalcStatus(SLARegistrationBean reg, SLACalculatorBean calcBean) {
        setSLARegistrationBean(reg);
        setSLACalculatorBean(calcBean);
    }

    public SLACalcStatus(SLARegistrationBean reg) {
        this(reg, null, null);
    }

    public SLACalcStatus(SLASummaryBean summary) {
        SLARegistrationBean reg = new SLARegistrationBean();
        reg.setUser(summary.getUser());
        reg.setAppName(summary.getAppName());
        reg.setParentId(summary.getParentId());
        reg.setNominalTime(summary.getNominalTime());
        reg.setExpectedStart(summary.getExpectedStart());
        reg.setExpectedEnd(summary.getExpectedEnd());
        reg.setExpectedDuration(summary.getExpectedDuration());
        setActualStart(summary.getActualStart());
        setActualEnd(summary.getActualEnd());
        setActualDuration(summary.getActualDuration());
        setSLAStatus(summary.getSLAStatus());
        setSLARegistrationBean(reg);
        calcBean = new SLACalculatorBean();
        setJobId(reg.getJobId());
        setJobStatus(summary.getJobStatus());
        setEventStatus(summary.getEventStatus());
    }

    /**
     * copy constructor
     * @return
     */
    public SLACalcStatus(SLACalcStatus a) {
        this.setSLARegistrationBean(a.getSLARegistrationBean());
        this.setSLACalculatorBean(a.getSLACalculatorBean());
        this.setJobStatus(a.getJobStatus());
        this.setSLAStatus(a.getSLAStatus());
        this.setEventStatus(a.getEventStatus());
        this.setActualStart(a.getActualStart());
        this.setActualEnd(a.getActualEnd());
        this.setActualDuration(a.getActualDuration());
    }

    public SLACalcStatus() {
        setMsgType(MessageType.SLA);
    }

    public SLARegistrationBean getSLARegistrationBean() {
        return regBean;
    }

    public void setSLARegistrationBean(SLARegistrationBean slaBean) {
        this.regBean = slaBean;
    }

    public SLACalculatorBean getSLACalculatorBean() {
        return calcBean;
    }

    public void setSLACalculatorBean(SLACalculatorBean calcBean) {
        this.calcBean = calcBean;
    }

    @Override
    public String getJobId() {
        return regBean.getJobId();
    }

    public void setJobId(String id) {
        regBean.setJobId(id);
        calcBean.setJobId(id);
    }

    @Override
    public Date getActualStart() {
        return actualStart;
    }

    public void setActualStart(Date actualStart) {
        this.actualStart = actualStart;
    }

    @Override
    public Date getActualEnd() {
        return actualEnd;
    }

    public void setActualEnd(Date actualEnd) {
        this.actualEnd = actualEnd;
    }

    @Override
    public long getActualDuration() {
        return actualDuration;
    }

    public void setActualDuration(long actualDuration) {
        this.actualDuration = actualDuration;
    }

    @Override
    public String getJobStatus() {
        return jobStatus;
    }

    public void setJobStatus(String status) {
        this.jobStatus = status;
    }

    @Override
    public SLAStatus getSLAStatus() {
        return slaStatus;
    }

    public void setSLAStatus(SLAStatus slaStatus) {
        this.slaStatus = slaStatus;
    }

    @Override
    public EventStatus getEventStatus() {
        return eventStatus;
    }

    public void setEventStatus(EventStatus es) {
        this.eventStatus = es;
    }

    public boolean isStartProcessed() {
        return calcBean.isStartProcessed();
    }

    public void setStartProcessed(boolean startProcessed) {
        calcBean.setStartProcessed(startProcessed);
    }

    public boolean isEndProcessed() {
        return calcBean.isEndProcessed();
    }

    public void setEndProcessed(boolean endProcessed) {
        calcBean.setEndProcessed(endProcessed);
    }

    public boolean isDurationProcessed() {
        return calcBean.isDurationProcessed();
    }

    public void setDurationProcessed(boolean durationProcessed) {
        calcBean.setDurationProcessed(durationProcessed);
    }

    @Override
    public String getParentId() {
        return regBean.getParentId();
    }

    @Override
    public AppType getAppType() {
        return regBean.getAppType();
    }

    @Override
    public String getAppName() {
        return regBean.getAppName();
    }

    @Override
    public Date getNominalTime() {
        return regBean.getNominalTime();
    }

    @Override
    public Date getExpectedStart() {
        return regBean.getExpectedStart();
    }

    @Override
    public Date getExpectedEnd() {
        return regBean.getExpectedEnd();
    }

    @Override
    public long getExpectedDuration() {
        return regBean.getExpectedDuration();
    }

    @Override
    public String getNotificationMsg() {
        return regBean.getNotificationMsg();
    }

    @Override
    public String getAlertEvents() {
        return regBean.getAlertEvents();
    }

    @Override
    public String getAlertContact() {
        return regBean.getAlertContact();
    }

    @Override
    public String getUpstreamApps() {
        return regBean.getUpstreamApps();
    }

    @Override
    public String getJobData() {
        return regBean.getJobData();
    }

    @Override
    public String getUser() {
        return regBean.getUser();
    }

    @Override
    public String getSlaConfig() {
        return regBean.getSlaConfig();
    }

    @Override
    public MessageType getMsgType() {
        return regBean.getMsgType();
    }

}
