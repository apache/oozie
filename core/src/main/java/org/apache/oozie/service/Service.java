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

package org.apache.oozie.service;

/**
 * A service is component managed by the {@link Services} singleton.
 */
public interface Service {
    public static final String DEFAULT_LOCK_TIMEOUT = "oozie.service.default.lock.timeout";

    /**
     * Prefix for all services configuration properties.
     */
    public static final String CONF_PREFIX = "oozie.service.";

    /**
     * Initialize the service. <p/> Invoked by the {@link Service} singleton at start up time.
     *
     * @param services services singleton initializing the service.
     * @throws ServiceException thrown if the service could not initialize.
     */
    public void init(Services services) throws ServiceException;

    /**
     * Destroy the service. <p/> Invoked by the {@link Service} singleton at shutdown time.
     */
    public void destroy();

    /**
     * Return the public interface of the service. <p/> Services are retrieved by its public interface. Specializations
     * of services must return the public interface.
     *
     * @return the interface of the service.
     */
    public Class<? extends Service> getInterface();

    /**
     * Lock timeout value if service is only allowed to have one single running instance.
     */
    public static long lockTimeout = ConfigurationService.getLong(DEFAULT_LOCK_TIMEOUT);

}
