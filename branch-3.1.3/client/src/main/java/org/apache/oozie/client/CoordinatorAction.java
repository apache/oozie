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
 * Bean that represents an Oozie application instance.
 */

public interface CoordinatorAction {
    /**
     * Defines the possible stati of an application instance.
     */
    public static enum Status {
        WAITING,
        READY,
        SUBMITTED,
        RUNNING,
        SUSPENDED,
        TIMEDOUT,
        SUCCEEDED,
        KILLED,
        FAILED,
        DISCARDED
    }

    /**
     * Return the coordinator job ID.
     *
     * @return the coordinator job ID.
     */
    String getJobId();

    /**
     * Return the application instance ID.
     *
     * @return the application instance ID.
     */
    String getId();

    /**
     * Return the nominal time for the application instance
     *
     * @return the nominal time for the application instance
     */
    Date getNominalTime();

    /**
     * Return the creation time for the application instance
     *
     * @return the creation time for the application instance
     */
    Date getCreatedTime();

    /**
     * Return the application instance ?? created configuration.
     *
     * @return the application instance configuration.
     */
    String getCreatedConf();


    /**
     * Return the last modified time
     *
     * @return the last modified time
     */
    Date getLastModifiedTime();

    /**
     * Return the action number
     *
     * @return the action number
     */
    int getActionNumber();

    /**
     * Return the run-time configuration
     *
     * @return the run-time configuration
     */
    String getRunConf();

    /**
     * Return the current status of the application instance.
     *
     * @return the current status of the application instance.
     */
    Status getStatus();

    /**
     * Return the missing dependencies for the particular action
     *
     * @return the missing dependencies for the particular action
     */
    String getMissingDependencies();


    /**
     * Return the external status of the application instance.
     *
     * @return the external status of the application instance.
     */
    String getExternalStatus();

    /**
     * Return the URL to programmatically track the status of the application instance.
     *
     * @return the URL to programmatically track the status of the application instance.
     */
    String getTrackerUri();

    /**
     * Return the URL to the web console of the system executing the application instance.
     *
     * @return the URL to the web console of the system executing the application instance.
     */
    String getConsoleUrl();

    /**
     * Return the error code of the application instance, if it ended in ERROR.
     *
     * @return the error code of the application instance.
     */
    String getErrorCode();

    /**
     * Return the error message of the application instance, if it ended in ERROR.
     *
     * @return the error message of the application instance.
     */
    String getErrorMessage();

    void setErrorCode(String errorCode);

    void setErrorMessage(String errorMessage);

    String getExternalId();

}
