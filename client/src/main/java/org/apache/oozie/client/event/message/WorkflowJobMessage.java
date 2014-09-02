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

package org.apache.oozie.client.event.message;

import java.util.Date;

import org.apache.oozie.AppType;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.event.JobEvent.EventStatus;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * Class holding attributes related to a workflow job message
 *
 */
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class WorkflowJobMessage extends JobMessage {

    @JsonProperty
    private WorkflowJob.Status status;
    @JsonProperty
    private String errorCode;
    @JsonProperty
    private String errorMessage;

    /**
     * Default constructor
     */
    public WorkflowJobMessage() {
        // Default constructor for jackson
    }

    /**
     * Constructor for a workflow job message
     * @param eventStatus event status
     * @param workflowJobId the workflow job id
     * @param coordinatorActionId the parent coordinator action id
     * @param startTime start time of workflow
     * @param endTime end time of workflow
     * @param status status of workflow
     * @param user the user
     * @param appName appName of workflow
     * @param errorCode errorCode of the failed wf actions
     * @param errorMessage errorMessage of the failed wf action
     */
    public WorkflowJobMessage(EventStatus eventStatus, String workflowJobId,
            String coordinatorActionId, Date startTime, Date endTime, WorkflowJob.Status status, String user,
            String appName, String errorCode, String errorMessage) {
        super(eventStatus, AppType.WORKFLOW_JOB, workflowJobId, coordinatorActionId, startTime,
                endTime, user, appName);
        this.status = status;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    /**
     * Set the workflow job status
     * @param status
     */
    public void setStatus(WorkflowJob.Status status) {
        this.status = status;
    }

    /**
     * Get the workflow job status
     * @return the workflow status
     */
    public WorkflowJob.Status getStatus() {
        return status;
    }

    /**
     * Set the workflow error code
     * @param errorCode
     */
    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    /**
     * Get the workflow error code
     * @return the error code
     */
    public String getErrorCode() {
        return errorCode;
    }

    /**
     * Set the workflow error message
     * @param errorMessage
     */
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    /**
     * Get the error message
     * @return the error message
     */
    public String getErrorMessage() {
        return errorMessage;
    }
}
