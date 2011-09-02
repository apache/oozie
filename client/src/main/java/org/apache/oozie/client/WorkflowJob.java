/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.client;

import java.util.Date;
import java.util.List;

/**
 * Bean that represents a workflow job.
 */
public interface WorkflowJob {

    /**
     * Defines the possible stati of a workflow.
     */
    public static enum Status {
        PREP, RUNNING, SUCCEEDED, KILLED, FAILED, SUSPENDED
    }

    //add NAME

    /**
     * Return the path to the workflow application for the workflow job.
     *
     * @return the path to the workflow application for the workflow job.
     */
    String getAppPath();

    /**
     * Return the name of the workflow application (from the workflow definition).
     *
     * @return the name of the workflow application.
     */
    String getAppName();

    /**
     * Return the workflow job ID.
     *
     * @return the workflow job ID.
     */
    String getId();

    /**
     * Return the job configuration.
     *
     * @return the job configuration.
     */
    String getConf();

    /**
     * Return the workflow job status.
     *
     * @return the workflow job status.
     */
    Status getStatus();

    /**
     * Return the workflow job last modified time.
     *
     * @return the workflow job last modified time.
     */
    Date getLastModifiedTime();

    /**
     * Return the workflow job creation time.
     *
     * @return the workflow job creation time.
     */
    Date getCreatedTime();

    /**
     * Return the workflow job start time.
     *
     * @return the workflow job start time.
     */
    Date getStartTime();

    /**
     * Return the workflow job end time.
     *
     * @return the workflow job end time.
     */
    Date getEndTime();

    /**
     * Return the workflow job user owner.
     *
     * @return the workflow job user owner.
     */
    String getUser();

    /**
     * Return the workflow job group.
     *
     * @return the workflow job group.
     */
    String getGroup();

    /**
     * Return the workflow job run number. <p/> Except for reruns, this property is always 1.
     *
     * @return the workflow job run number.
     */
    int getRun();

    /**
     * Return the workflow job console URL.
     *
     * @return the workflow job console URL.
     */
    String getConsoleUrl();
    
    /**
     * Return the coordinator action ID.
     *
     * @return the coordinator action ID.
     */
    String getParentId();

    /**
     * Return the workflow nodes that already executed and are executing.
     *
     * @return the workflow nodes that already executed and are executing.
     */
    List<WorkflowAction> getActions();

}
