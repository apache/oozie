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
 * Bean that represents an Oozie bundle.
 */
public interface BundleJob extends Job {

    /**
     * Defines the possible frequency unit of all Oozie applications in Bundle.
     */
    public static enum Timeunit {
        MINUTE, HOUR, DAY, WEEK, MONTH, END_OF_DAY, END_OF_MONTH, NONE
    }

    /**
     * Return the timeUnit for the Bundle job, it could be, Timeunit enum, e.g. MINUTE, HOUR, DAY, WEEK or MONTH
     *
     * @return the time unit for the Bundle job
     */
    Timeunit getTimeUnit();

    /**
     * Return the time out value for all the coord jobs within Bundle
     *
     * @return the time out value for the coord jobs within Bundle
     */
    int getTimeout();

    /**
     * Return the list of CoordinatorJob.
     *
     * @return the list of CoordinatorJob.
     */
    List<CoordinatorJob> getCoordinators();
    
    /**
     * Return the JOB Kickoff time.
     *
     * @return the JOB Kickoff time.
     */
    Date getKickoffTime();

    /**
     * Get createdTime
     *
     * @return createdTime
     */
    public Date getCreatedTime();

}
