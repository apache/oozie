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
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.event.JobEvent.EventStatus;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * Class holding attributes related to Coordinator action message
 *
 */
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class CoordinatorActionMessage extends JobMessage {

    @JsonProperty
    private CoordinatorAction.Status status;
    @JsonProperty
    private Date nominalTime;
    @JsonProperty
    private String missingDependency;
    @JsonProperty
    private String errorCode;
    @JsonProperty
    private String errorMessage;

    /**
     * Default constructor
     */
    public CoordinatorActionMessage() {
        // Default constructor for jackson
    }

    /**
     * Constructs the coordinator action message
     * @param eventStatus the event status
     * @param coordinatorActionId the coord action id
     * @param coordinatorJobId the parent job id
     * @param startTime the created time of coord action
     * @param endTime the end time of coord action
     * @param nominalTime the nominal time of coord action
     * @param status the status of coord action
     * @param user the user of coordinator
     * @param appName the app name of coordinator
     * @param missingDependency the action's first missing dependency
     * @param errorCode the action's error code
     * @param errorMessage the action's error message
     */
    public CoordinatorActionMessage(EventStatus eventStatus, String coordinatorActionId,
            String coordinatorJobId, Date startTime, Date endTime, Date nominalTime, CoordinatorAction.Status status,
            String user, String appName, String missingDependency, String errorCode, String errorMessage) {
        super(eventStatus, AppType.COORDINATOR_ACTION, coordinatorActionId, coordinatorJobId, startTime,
                endTime, user, appName);
        this.status = status;
        this.nominalTime = nominalTime;
        this.missingDependency = missingDependency;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;

    }

    /**
     * Set the status of coordinator action
     * @param status
     */
    public void setStatus(CoordinatorAction.Status status) {
        this.status = status;
    }

    /**
     * Get the status of coord action
     * @return the CoordinatorAction status
     */
    public CoordinatorAction.Status getStatus() {
        return status;
    }

    /**
     * Set the nominal time
     * @param nominalTime
     */
    public void setNominalTime(Date nominalTime) {
        this.nominalTime = nominalTime;
    }

    /**
     * Get the nominal time
     * @return the nominal time
     */
    public Date getNominalTime() {
        return nominalTime;
    }

    /**
     * Set the error code
     * @param errorCode
     */
    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    /**
     * Get the error code
     * @return the errorCode
     */
    public String getErrorCode() {
        return errorCode;
    }

    /**
     * Set the error message
     * @param errorMessage
     */
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    /**
     * Get the error message
     * @return the errorMessage
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * Set the missing dependency
     * @param missingDependency
     */
    public void setMissingDependency(String missingDependency) {
        this.missingDependency = missingDependency;
    }

    /**
     * Get the missing dependency
     * @return the missing Dependency
     */
    public String getMissingDependency() {
        return missingDependency;
    }

}
