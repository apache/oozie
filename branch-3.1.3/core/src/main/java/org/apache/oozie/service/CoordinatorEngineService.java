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

import org.apache.oozie.CoordinatorEngine;
import org.apache.oozie.service.Service;
import org.apache.oozie.service.Services;

/**
 * Service that return a coordinator engine for a user.
 */
public class CoordinatorEngineService implements Service {

    /**
     * Initialize the service.
     *
     * @param services services instance.
     */
    public void init(Services services) {
    }

    /**
     * Destroy the service.
     */
    public void destroy() {
    }

    /**
     * Return the public interface of the Coordinator engine service.
     *
     * @return {@link CoordinatorEngineService}.
     */
    public Class<? extends Service> getInterface() {
        return CoordinatorEngineService.class;
    }

    /**
     * Return a Coordinator engine.
     *
     * @param user user for the coordinator engine.
     * @param authToken the authentication token.
     * @return the coordinator engine for the specified user.
     */
    public CoordinatorEngine getCoordinatorEngine(String user, String authToken) {
        return new CoordinatorEngine(user, authToken);
    }

    /**
     * Return a Coordinator engine for a system user (no user, no group).
     *
     * @return a system Coordinator engine.
     */
    public CoordinatorEngine getSystemCoordinatorEngine() {
        return new CoordinatorEngine();
    }

}
