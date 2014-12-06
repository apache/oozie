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

package org.apache.oozie.client;

import java.util.Date;

@Deprecated
/**
 * Bean that represents a SLA event
 * @deprecated This interface has been deprecated by the abstract class
 * org.apache.oozie.client.event.SLAEvent designed for the new SLA Calculator system OOZIE-1244
 */
public interface SLAEvent {

    /**
     * Defines the possible status of an SLA events or Job status for SLA events.
     */
    public static enum Status {
        CREATED, STARTED, SUCCEEDED, KILLED, FAILED
    }

    /**
     * Defines the possible status of an SLA events.
     */
    public static enum SlaAppType {
        COORDINATOR_ACTION, COORDINATOR_JOB, WORKFLOW_JOB, WORKFLOW_ACTION
    }

    public long getEvent_id();

    public String getSlaId();

    public SlaAppType getAppType();

    public String getAppName();

    public String getUser();

    public String getGroupName();

    public String getParentClientId();

    public String getParentSlaId();

    public Date getExpectedStart();

    public Date getExpectedEnd();

    public Date getStatusTimestamp();

    public String getNotificationMsg();

    public String getAlertContact();

    public String getDevContact();

    public String getQaContact();

    public String getSeContact();

    public String getAlertFrequency();

    public String getAlertPercentage();

    public String getUpstreamApps();

    public Status getJobStatus();

    public String getJobData();

}
