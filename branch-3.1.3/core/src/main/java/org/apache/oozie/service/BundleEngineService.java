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

import org.apache.oozie.BundleEngine;
import org.apache.oozie.service.Service;
import org.apache.oozie.service.Services;

/**
 * Service that return a bundle engine for a user.
 */
public class BundleEngineService implements Service {

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
     * Return the public interface of the Bundle engine service.
     *
     * @return {@link BundleEngineService}.
     */
    public Class<? extends Service> getInterface() {
        return BundleEngineService.class;
    }

    /**
     * Return a Bundle engine.
     *
     * @param user user for the bundle engine.
     * @param authToken the authentication token.
     * @return the bundle engine for the specified user.
     */
    public BundleEngine getBundleEngine(String user, String authToken) {
        return new BundleEngine(user, authToken);
    }

    /**
     * Return a Bundle engine for a system user (no user, no group).
     *
     * @return a system Bundle engine.
     */
    public BundleEngine getSystemBundleEngine() {
        return new BundleEngine();
    }

}
