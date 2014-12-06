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
import org.apache.oozie.lock.LockToken;
import org.apache.oozie.service.JobsConcurrencyService;
import org.apache.oozie.service.MemoryLocksService;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.service.SLAService;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.XLog;

/**
 * Class used by SLAService to store SLA objects and perform calculations and
 * sla decisions
 */
public class SLACalcStatus extends SLAEvent {

    public static String SLA_ENTITYKEY_PREFIX = "sla-";
    private SLARegistrationBean regBean;
    private String jobStatus;
    private SLAStatus slaStatus;
    private EventStatus eventStatus;
    private Date actualStart;
    private Date actualEnd;
    private long actualDuration = -1;
    private Date lastModifiedTime;
    private byte eventProcessed;
    private LockToken lock;

    private XLog LOG;

    public SLACalcStatus(SLARegistrationBean reg) {
        this();
        setSLARegistrationBean(reg);
        LOG = LogUtils.setLogPrefix(LOG, this);
    }

    public SLACalcStatus(SLASummaryBean summary, SLARegistrationBean regBean) {
        this();
        SLARegistrationBean reg = new SLARegistrationBean();
        reg.setNotificationMsg(regBean.getNotificationMsg());
        reg.setUpstreamApps(regBean.getUpstreamApps());
        reg.setAlertContact(regBean.getAlertContact());
        reg.setAlertEvents(regBean.getAlertEvents());
        reg.setJobData(regBean.getJobData());
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
        setActualStart(summary.getActualStart());
        setActualEnd(summary.getActualEnd());
        setActualDuration(summary.getActualDuration());
        setSLAStatus(summary.getSLAStatus());
        setJobStatus(summary.getJobStatus());
        setEventStatus(summary.getEventStatus());
        setLastModifiedTime(summary.getLastModifiedTime());
        setEventProcessed(summary.getEventProcessed());
        LOG = LogUtils.setLogPrefix(LOG, this);
    }

    /**
     * copy constructor
     * @return SLACalcStatus
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

    public void setSLARegistrationBean(SLARegistrationBean slaBean) {
        this.regBean = slaBean;
    }

    @Override
    public String getId() {
        return regBean.getId();
    }

    public void setId(String id) {
        regBean.setId(id);
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
    public String getSlaConfig() {
        return regBean.getSlaConfig();
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
    /**
     * Obtain an exclusive lock on the {link #getEntityKey}.
     * <p/>
     * A timeout of {link #getLockTimeOut} is used when trying to obtain the lock.
     *
     * @throws InterruptedException thrown if an interruption happened while trying to obtain the lock
     */
    public void acquireLock() throws InterruptedException {
        // only get ZK lock when multiple servers running
        if (Services.get().get(JobsConcurrencyService.class).isHighlyAvailableMode()) {
            lock = Services.get().get(MemoryLocksService.class).getWriteLock(getEntityKey(), getLockTimeOut());
            if (lock == null) {
            LOG.debug("Could not aquire lock for [{0}]", getEntityKey());
            }
            else {
                LOG.debug("Acquired lock for [{0}]", getEntityKey());
            }
        }
        else {
            lock = new DummyToken();
        }
    }

    private static class DummyToken implements LockToken {
        @Override
        public void release() {
        }
    }

    public boolean isLocked() {
        boolean locked = false;
        if(lock != null) {
            locked = true;
        }
        return locked;
    }

    public void releaseLock(){
        if (lock != null) {
            lock.release();
            lock = null;
            LOG.debug("Released lock for [{0}]", getEntityKey());
        }
    }

    public long getLockTimeOut() {
        return Services.get().getConf().getLong(SLAService.CONF_SLA_CALC_LOCK_TIMEOUT, 5 * 1000);
    }

}
