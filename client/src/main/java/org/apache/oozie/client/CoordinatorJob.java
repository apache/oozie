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
import java.util.List;

/**
 * Bean that represents an Oozie application.
 */
public interface CoordinatorJob extends Job {

    /**
     * Defines the possible execution order of an Oozie application.
     */
    public static enum Execution {
        FIFO, LIFO, LAST_ONLY, NONE
    }

    /**
     * Defines the possible frequency unit of an Oozie application.
     */
    public static enum Timeunit {
        MINUTE, HOUR, DAY, WEEK, MONTH, END_OF_DAY, END_OF_MONTH, CRON, NONE
    }

    /**
     * Return the path to the Oozie application.
     *
     * @return the path to the Oozie application.
     */
    String getAppPath();

    /**
     * Return the name of the Oozie application (from the application definition).
     *
     * @return the name of the Oozie application.
     */
    String getAppName();

    /**
     * Return the application ID.
     *
     * @return the application ID.
     */
    String getId();

    /**
     * Return the application configuration.
     *
     * @return the application configuration.
     */
    String getConf();

    /**
     * Return the application status.
     *
     * @return the application status.
     */
    Status getStatus();

    /**
     * Return the frequency for the coord job in unit of minute
     *
     * @return the frequency for the coord job in unit of minute
     */
    String getFrequency();

    /**
     * Return the timeUnit for the coord job, it could be, Timeunit enum, e.g. MINUTE, HOUR, DAY, WEEK or MONTH
     *
     * @return the time unit for the coord job
     */
    Timeunit getTimeUnit();

    /**
     * Return the time zone information for the coord job
     *
     * @return the time zone information for the coord job
     */
    String getTimeZone();

    /**
     * Return the concurrency for the coord job
     *
     * @return the concurrency for the coord job
     */
    int getConcurrency();

    /**
     * Return the execution order policy for the coord job
     *
     * @return the execution order policy for the coord job
     */
    Execution getExecutionOrder();

    /**
     * Return the time out value for the coord job
     *
     * @return the time out value for the coord job
     */
    int getTimeout();

    /**
     * Return the date for the last action of the coord job
     *
     * @return the date for the last action of the coord job
     */
    Date getLastActionTime();

    /**
     * Return the application next materialized time.
     *
     * @return the application next materialized time.
     */
    Date getNextMaterializedTime();

    /**
     * Return the application start time.
     *
     * @return the application start time.
     */
    Date getStartTime();

    /**
     * Return the application end time.
     *
     * @return the application end time.
     */
    Date getEndTime();

    /**
     * Return the application user owner.
     *
     * @return the application user owner.
     */
    String getUser();

    /**
     * Return the application group.
     * <p/>
     * Use the {@link #getAcl()} method instead.
     *
     * @return the application group.
     */
    @Deprecated
    String getGroup();

    /**
     * Return the workflow job group.
     *
     * @return the workflow job group.
     */
    String getAcl();

    /**
     * Return the BundleId.
     *
     * @return the BundleId.
     */
    String getBundleId();

    /**
     * Return the application console URL.
     *
     * @return the application console URL.
     */
    String getConsoleUrl();

    /**
     * Return list of coordinator actions.
     *
     * @return the list of coordinator actions.
     */
    List<CoordinatorAction> getActions();
}
