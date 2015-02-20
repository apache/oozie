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

package org.apache.oozie.sla;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.event.JobEvent.EventStatus;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.util.Pair;

public interface SLACalculator {

    void init(Configuration conf) throws ServiceException;

    int size();

    Iterator<String> iterator();

    boolean isEmpty();

    boolean addRegistration(String jobId, SLARegistrationBean reg) throws JPAExecutorException;

    boolean updateRegistration(String jobId, SLARegistrationBean reg) throws JPAExecutorException;

    void removeRegistration(String jobId);

    boolean addJobStatus(String jobId, String jobStatus, EventStatus jobEventStatus, Date startTime, Date endTime)
            throws JPAExecutorException, ServiceException;

    void updateAllSlaStatus();

    void clear();

    SLACalcStatus get(String jobId) throws JPAExecutorException;

    /**
     * Enable jobs sla alert.
     *
     * @param jobId the job ids
     * @return true, if successful
     * @throws JPAExecutorException the JPA executor exception
     * @throws ServiceException the service exception
     */
    boolean enableAlert(List<String> jobId) throws JPAExecutorException, ServiceException;

    /**
     * Enable sla alert for child jobs.
     * @param jobId the parent job ids
     * @return
     * @throws JPAExecutorException
     * @throws ServiceException
     */
    boolean enableChildJobAlert(List<String> parentJobIds) throws JPAExecutorException, ServiceException;

    /**
     * Disable jobs Sla alert.
     *
     * @param jobId the job ids
     * @return true, if successful
     * @throws JPAExecutorException the JPA executor exception
     * @throws ServiceException the service exception
     */
    boolean disableAlert(List<String> jobId) throws JPAExecutorException, ServiceException;


    /**
     * Disable Sla alert for child jobs.
     * @param jobId the parent job ids
     * @return
     * @throws JPAExecutorException
     * @throws ServiceException
     */
    boolean disableChildJobAlert(List<String> parentJobIds) throws JPAExecutorException, ServiceException;

    /**
     * Change jobs Sla definitions
     * It takes list of pairs of jobid and key/value pairs of el evaluated sla definition.
     * Support definition are sla-should-start, sla-should-end, sla-nominal-time and sla-max-duration.
     *
     * @param jobIdsSLAPair the job ids sla pair
     * @return true, if successful
     * @throws JPAExecutorException the JPA executor exception
     * @throws ServiceException the service exception
     */
    public boolean changeDefinition(List<Pair<String, Map<String,String>>> jobIdsSLAPair ) throws JPAExecutorException,
            ServiceException;
}
