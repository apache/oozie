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

/**
 * This interface defines an Event that can be generated via
 * Job status changes or SLA related events
 */
public interface Event {

    /**
     * Events will be messages, broadly of type - Job related or SLA related
     *
     */
    public static enum MessageType {
        JOB,
        SLA
    }

    /**
     * Events carry the associated app-type or job-type to enable toggling on/off
     * events generated only for specific app-types or filtering on receiving side
     */
    public static enum AppType {
        WORKFLOW_JOB,
        WORKFLOW_ACTION,
        COORDINATOR_JOB,
        COORDINATOR_ACTION,
        BUNDLE_JOB
    }

    /**
     * Get the AppType of the event
     * @return AppType
     */
    public AppType getAppType();

    /**
     * Get the MessageType of the event
     * @return MessageType
     */
    public MessageType getMsgType();

    /**
     * Get the app-name of the job generating this event
     * @return String app-name
     */
    public String getAppName();

}
