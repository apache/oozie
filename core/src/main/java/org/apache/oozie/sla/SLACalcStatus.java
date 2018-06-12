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
import java.util.Map;

import org.apache.oozie.AppType;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.event.SLAEvent;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.XLog;

/**
 * Class used by SLAService to store SLA objects and perform calculations and
 * sla decisions
 */
public class SLACalcStatus extends SLAEvent {

    public static String SLA_ENTITYKEY_PREFIX = "sla-";
    private SLARegistrationBean regBean;
    private SLASummaryBean summary;
    private String jobStatus;
    private SLAStatus slaStatus;
    private EventStatus eventStatus;
    private Date actualStart;
    private Date actualEnd;
    private long actualDuration = -1;
    private Date lastModifiedTime;
    private byte eventProcessed;
    private String jobId;
    private int retryCount = 0;

    private XLog LOG;

    public SLACalcStatus(SLARegistrationBean reg) {
        this();
        setSLARegistrationBean(reg);
        LOG = LogUtils.setLogPrefix(LOG, this);
    }

    public SLACalcStatus(SLASummaryBean summary, SLARegistrationBean regBean) {
        this(summary);
        updateSLARegistrationBean(regBean);
        LOG = LogUtils.setLogPrefix(LOG, this);
    }

    public SLACalcStatus(SLASummaryBean summary) {
        this();
        setActualStart(summary.getActualStart());
        setActualEnd(summary.getActualEnd());
        setActualDuration(summary.getActualDuration());
        setSLAStatus(summary.getSLAStatus());
        setJobStatus(summary.getJobStatus());
        setEventStatus(summary.getEventStatus());
        setLastModifiedTime(summary.getLastModifiedTime());
        setEventProcessed(summary.getEventProcessed());
        setId(summary.getId());
        this.summary = summary;
    }

    /**
     * copy constructor
     */
    public SLACalcStatus(SLACalcStatus a) {
        this();
        setSLARegistrationBean(a.getSLARegistrationBean());
        setJobStatus(a.getJobStatus());
        setSLAStatus(a.getSLAStatus());
        setEventStatus(a.getEventStatus());
        setActualStart(a.getActualStart());
        setActualEnd(a.getActualEnd());
        setActualDuration(a.getActualDuration());
        setEventProcessed(a.getEventProcessed());
        LOG = LogUtils.setLogPrefix(LOG, this);
    }

    public SLACalcStatus() {
        setMsgType(MessageType.SLA);
        setLastModifiedTime(new Date());
        LOG = XLog.getLog(getClass());
    }

    public SLARegistrationBean getSLARegistrationBean() {
        return regBean;
    }

    public SLASummaryBean getSLASummaryBean() {
        return summary;
    }

    public void setSLARegistrationBean(SLARegistrationBean slaBean) {
        if (slaBean != null) {
            this.jobId = slaBean.getId();
        }
        this.regBean = slaBean;
    }

    @Override
    public String getId() {
        return jobId;
    }

    public void setId(String id) {
        this.jobId = id;
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

    public void setLastModifiedTime(Date lastModifiedTime) {
        this.lastModifiedTime = lastModifiedTime;
    }

    /**
     * Get which type of sla event has been processed needed when calculator
     * periodically loops to update all jobs' sla
     *
     * @return byte 1st bit set (from LSB) = start processed
     * 2nd bit set = duration processed
     * 3rd bit set = end processed
     * only 4th bit set = everything processed
     */
    public byte getEventProcessed() {
        return eventProcessed;
    }

    public void setEventProcessed(int eventProcessed) {
        this.eventProcessed = (byte) eventProcessed;
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
    public String getSLAConfig() {
        return regBean.getSlaConfig();
    }

    public Map<String, String> getSLAConfigMap() {
        return regBean.getSLAConfigMap();
    }

    @Override
    public MessageType getMsgType() {
        return regBean.getMsgType();
    }

    @Override
    public Date getLastModifiedTime() {
        return lastModifiedTime;
    }

    public String getEntityKey() {
        return SLA_ENTITYKEY_PREFIX + this.getId();
    }

    public void updateSLARegistrationBean(SLARegistrationBean slaBean) {
        SLARegistrationBean reg = new SLARegistrationBean();
        reg.setNotificationMsg(slaBean.getNotificationMsg());
        reg.setUpstreamApps(slaBean.getUpstreamApps());
        reg.setAlertContact(slaBean.getAlertContact());
        reg.setAlertEvents(slaBean.getAlertEvents());
        reg.setJobData(slaBean.getJobData());
        if (slaBean.getSLAConfigMap().containsKey(OozieClient.SLA_DISABLE_ALERT)) {
            reg.addToSLAConfigMap(OozieClient.SLA_DISABLE_ALERT,
                    slaBean.getSLAConfigMap().get(OozieClient.SLA_DISABLE_ALERT));
        }
        reg.setId(summary.getId());
        reg.setAppType(summary.getAppType());
        reg.setUser(summary.getUser());
        reg.setAppName(summary.getAppName());
        reg.setParentId(summary.getParentId());
        reg.setNominalTime(summary.getNominalTime());
        reg.setExpectedStart(summary.getExpectedStart());
        reg.setExpectedEnd(summary.getExpectedEnd());
        reg.setExpectedDuration(summary.getExpectedDuration());
        setSLARegistrationBean(reg);
    }

    int getRetryCount() {
        return retryCount;
    }

    void incrementRetryCount() {
        this.retryCount++;
    }

    void resetRetryCount() {
        this.retryCount = 0;
    }
}
