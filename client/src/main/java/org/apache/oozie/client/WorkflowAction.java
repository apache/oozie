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

/**
 * Bean that represents a workflow action in a workflow job.
 */
public interface WorkflowAction {

    /**
     * Defines the possible stati of a action.
     */
    public static enum Status {
        PREP,
        RUNNING,
        OK,
        ERROR,
        USER_RETRY,
        START_RETRY,
        START_MANUAL,
        DONE,
        END_RETRY,
        END_MANUAL,
        KILLED,
        FAILED, }

    /**
     * Return the action action ID.
     *
     * @return the action action ID.
     */
    String getId();

    /**
     * Return the action name.
     *
     * @return the action name.
     */
    String getName();

    /**
     * Return the Credential.
     *
     * @return the Credential.
     */
    String getCred();
    
    /**
     * Return the action type.
     *
     * @return the action type.
     */
    String getType();


    /**
     * Return the action configuration.
     *
     * @return the action configuration.
     */
    String getConf();

    /**
     * Return the current status of the action action.
     *
     * @return the current status of the action action.
     */
    Status getStatus();

    /**
     * Return the number of retries of the action.
     *
     * @return the number of retries of the action.
     */
    int getRetries();
    
    /**
     * Return the number of user retry of the action.
     *
     * @return the number of user retry of the action.
     */
    int getUserRetryCount();
    
    /**
     * Return the max number of user retry of the action.
     *
     * @return the max number of user retry of the action.
     */
    int getUserRetryMax();
    
    /**
     * Return the interval of user retry of the action, in minutes.
     *
     * @return the interval of user retry of the action, in minutes.
     */
    int getUserRetryInterval();

    /**
     * Return the start time of the action action.
     *
     * @return the start time of the action action.
     */
    Date getStartTime();

    /**
     * Return the end time of the action action.
     *
     * @return the end time of the action action.
     */
    Date getEndTime();

    /**
     * Return the transition a action took.
     *
     * @return the transition a action took.
     */
    String getTransition();

    /**
     * Return the action data.
     *
     * @return the action data.
     */
    String getData();

    /**
     * Return the action statistics.
     *
     * @return the action statistics.
     */
    String getStats();

    /**
     * Return the external child IDs of the action.
     *
     * @return the external child IDs of the action.
     */
    String getExternalChildIDs();

    /**
     * Return the external ID of the action.
     *
     * @return the external ID of the action.
     */
    String getExternalId();

    /**
     * Return the external status of the action.
     *
     * @return the external status of the action.
     */
    String getExternalStatus();

    /**
     * Return the URL to programmatically track the status of the action.
     *
     * @return the URL to programmatically track the status of the action.
     */
    String getTrackerUri();

    /**
     * Return the URL to the web console of the system executing the action.
     *
     * @return the URL to the web console of the system executing the action.
     */
    String getConsoleUrl();

    /**
     * Return the error code of the action, if it ended in ERROR.
     *
     * @return the error code of the action.
     */
    String getErrorCode();

    /**
     * Return the error message of the action, if it ended in ERROR.
     *
     * @return the error message of the action.
     */
    String getErrorMessage();
}
