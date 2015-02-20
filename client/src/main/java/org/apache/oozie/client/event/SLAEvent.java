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

package org.apache.oozie.client.event;

import java.util.Date;
import org.apache.oozie.AppType;

/**
 * A sub-class of the Event interface, related to
 * notification events after SLA mets/misses
 */
public abstract class SLAEvent extends Event {

    public static enum EventStatus {
        START_MET, START_MISS, DURATION_MET, DURATION_MISS, END_MET, END_MISS
    }

    public static enum SLAStatus {
        NOT_STARTED, IN_PROCESS, MET, MISS
    }

    /**
     * Get the SLA status
     *
     * @return SLAEvent.SLAStatus
     */
    public abstract SLAStatus getSLAStatus();

    /**
     * Get the SLA event status
     *
     * @return SLAEvent.EventStatus
     */
    public abstract EventStatus getEventStatus();

    @Override
    public MessageType getMsgType() {
        return msgType;
    }

    /**
     * Get the job-id
     *
     * @return String job-id
     */
    public abstract String getId();

    /**
     * Get the id of the parent job
     *
     * @return String parent-id
     */
    public abstract String getParentId();

    /**
     * Get the AppType of the event
     *
     * @return AppType
     */
    public abstract AppType getAppType();

    /**
     * Get the app-name of the job generating this event
     *
     * @return String app-name
     */
    public abstract String getAppName();

    /**
     * Get the job's nominal time
     *
     * @return Date nominal time
     */
    public abstract Date getNominalTime();

    /**
     * Get the expected start-time for this job
     *
     * @return Date expected start-time
     */
    public abstract Date getExpectedStart(); // nominal time + should-start

    /**
     * Get the expected end-time for this job
     *
     * @return Date expected end-time
     */
    public abstract Date getExpectedEnd(); // nominal time + should-end

    /**
     * Get the expected duration for this job
     *
     * @return Date expected duration
     */
    public abstract long getExpectedDuration();

    /**
     * Get the sla notification-message
     *
     * @return String notification-message
     */
    public abstract String getNotificationMsg();

    /**
     * Get the SLA alert-events
     *
     * @return String alert-events
     */
    public abstract String getAlertEvents();

    /**
     * Get the SLA alert-contact
     *
     * @return String alert-contact
     */
    public abstract String getAlertContact();

    /**
     * Get the dependent upstream apps
     *
     * @return String upstream-apps
     */
    public abstract String getUpstreamApps();

    /**
     * Get job related data or configuration
     *
     * @return String job-data
     */
    public abstract String getJobData();

    /**
     * Get the user for this job sla
     *
     * @return String user
     */
    public abstract String getUser();

    /**
     * Get the miscellaneous key-value configs
     *
     * @return String slaConfig
     */
    public abstract String getSLAConfig();

    /**
     * Get the actual start time of job for SLA
     *
     * @return Date actual-start
     */
    public abstract Date getActualStart();

    /**
     * Get the actual end time of job for SLA
     *
     * @return Date actual-end
     */
    public abstract Date getActualEnd();

    /**
     * Get the actual duration of job for SLA
     *
     * @return long duration
     */
    public abstract long getActualDuration();

    /**
     * Get the job status
     *
     * @return String job-status
     */
    public abstract String getJobStatus();

    /**
     * Get the last modified time
     *
     * @return Date last modified time
     */
    public abstract Date getLastModifiedTime();

    @Override
    public String toString() {
        return "ID: " + getId() + ", MsgType:" + getMsgType() + ", SLAStatus: " + getSLAStatus() + ", EventStatus: "
                + getEventStatus() + " AppType " + getAppType();
    }

}
