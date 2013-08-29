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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.oozie.util.ConfigUtils;
import org.apache.oozie.util.Instrumentable;
import org.apache.oozie.util.Instrumentation;

/**
 * This Service helps coordinate other Services to prevent duplicate processing of Jobs if there are multiple Oozie Servers.  This
 * implementation assumes that there are NO other Oozie Servers (i.e. not HA).
 * The {@link ZKJobsConcurrencyService} provides a more meaningful implementation.
 */
public class JobsConcurrencyService implements Service, Instrumentable {

    private static final Map<String, String> urls;
    static {
        urls = new HashMap<String, String>();
        urls.put(System.getProperty("oozie.http.hostname"), ConfigUtils.getOozieEffectiveUrl());
    }

    /**
     * Initialize the jobs concurrency service
     *
     * @param services services instance.
     */
    @Override
    public void init(Services services) throws ServiceException {
    }

    /**
     * Destroy the jobs concurrency service.
     */
    @Override
    public void destroy() {
    }

    /**
     * Return the public interface for the jobs concurrency services
     *
     * @return {@link JobsConcurrencyService}.
     */
    @Override
    public Class<? extends Service> getInterface() {
        return JobsConcurrencyService.class;
    }

    /**
     * Instruments the jobs concurrency service.
     *
     * @param instr instance to instrument the jobs concurrency service to.
     */
    @Override
    public void instrument(Instrumentation instr) {
        // nothing to instrument
    }

    /**
     * Check to see if this server is the first server.  This implementation always returns true.
     *
     * @return true
     */
    public boolean isFirstServer() {
        return true;
    }

    /**
     * Check to see if jobId should be processed by this server.  This implementation always returns true.
     *
     * @param jobId The jobId to check
     * @return true
     */
    public boolean isJobIdForThisServer(String jobId) {
        return true;
    }

    /**
     * Filter out any job ids that should not be processed by this server.  This implementation always returns an unmodified list.
     *
     * @param ids The list of job ids to check
     * @return ids
     */
    public List<String> getJobIdsForThisServer(List<String> ids) {
        return ids;
    }

    /**
     * Return a map of server id to Oozie server URL.  This implementation always returns a map with a single entry where the key is
     * the hostname and the value is the URL (of this Oozie server).
     *
     * @return A map of Oozie server ids and URLs
     * @throws Exception
     */
    public Map<String, String> getServerUrls() {
        return urls;
    }
}
