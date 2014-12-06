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

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.event.JobEvent.EventStatus;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.ServiceException;

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

}
