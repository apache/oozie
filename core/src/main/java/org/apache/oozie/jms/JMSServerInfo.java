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
package org.apache.oozie.jms;

import org.apache.oozie.client.rest.JMSConnectionInfoBean;
import org.apache.oozie.executor.jpa.JPAExecutorException;

/**
 * Get the JMS ConnectionInfoBean
 */
public interface JMSServerInfo {

    /**
     * Retrive the conn info bean using conn properties and job id
     * @param connectionProperties the jms producer conn properties
     * @param jobId the job id
     * @return
     * @throws Exception
     */
    public JMSConnectionInfoBean getJMSConnectionInfoBean(String connectionProperties, String jobId) throws Exception;

}
