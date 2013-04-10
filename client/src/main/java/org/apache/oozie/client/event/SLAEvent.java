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

import org.apache.oozie.client.event.Event;
import org.apache.oozie.client.event.JobEvent.EventStatus;

/**
 * A sub-class of the Event interface, related to
 * notification events after SLA mets/misses
 */
public interface SLAEvent extends Event {

    public static enum EventType {
        START_MET,
        START_MISS,
        END_MET,
        END_MISS,
        DURATION_MET,
        DURATION_MISS
    }

    /**
     * Get the SLA event-type
     * @return SLAEvent.EventType
     */
    public EventType getEventType();

    /**
     * Get the job id
     * @return String id
     */
    public String getId();

    /**
     * Get the parent job id
     * @return String id
     */
    public String getParentId();

    /**
     * Get the actual job status
     * @return EventStatus status
     */
    public EventStatus getJobStatus();

    /**
     * Get the job-id
     * @return String job-id
     */
    public String getJobId();

    /**
     * Get the sla event sequence-id
     * @return String sequence-id
     */
    public String getSequenceId();

    /**
     * Get the expected start-time for this job
     * @return Date expected start-time
     */
    public Date getExpectedStart();

    /**
     * Get the actual start-time for this job
     * @return Date actual start-time
     */
    public Date getActualStart();

    /**
     * Get the expected end-time for this job
     * @return Date expected end-time
     */
    public Date getExpectedEnd();

    /**
     * Get the actual end-time for this job
     * @return Date actual end-time
     */
    public Date getActualEnd();

    /**
     * Get the expected duration for this job
     * @return Date expected duration
     */
    public long getExpectedDuration();

    /**
     * Get the actual duration for this job
     * @return Date actual duration
     */
    public long getActualDuration();

    /**
     * Get the sla notification-message
     * @return String notification-message
     */
    public String getNotificationMsg();

    /**
     * Get the SLA alert-contact
     * @return String alert-contact
     */
    public String getAlertContact();

    /**
     * Get the SLA dev-contact
     * @return String dev-contact
     */
    public String getDevContact();

    /**
     * Get the SLA qa-contact
     * @return String qa-contact
     */
    public String getQaContact();

    /**
     * Get the SLA alert-contact
     * @return String alert-contact
     */
    public String getSeContact();

    /**
     * Get the SLA alert-frequency
     * @return String alert-frequency
     */
    public String getAlertFrequency();

    /**
     * Get the SLA alert-percentage
     * @return String alert-percentage
     */
    public String getAlertPercentage();

    /**
     * Get the dependent upstream apps
     * @return String upstream-apps
     */
    public String getUpstreamApps();

    /**
     * Get job related data or configuration
     * @return String job-data
     */
    public String getJobData();

    /**
     * Get last modified time of this sla event
     * @return Date last-modified-time
     */
    public Date getLastModified();

    /**
     * Get the user for this job sla
     * @return String user
     */
    public String getUser();

    /**
     * Get the nominal time for this job under sla
     * @return Date nominalTime
     */
    public Date getNominalTime();

}
