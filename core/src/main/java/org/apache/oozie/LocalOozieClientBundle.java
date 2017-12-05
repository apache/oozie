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

package org.apache.oozie;

import org.apache.oozie.client.BulkResponse;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.OozieClientException;

import java.util.List;

/**
 * Client API to submit and manage Oozie Bundle jobs against an Oozie instance.
 * <p>
 * This class is thread safe.
 * <p>
 * Syntax for filter for the {@link #getJobsInfo(String)} {@link #getJobsInfo(String, int, int)} methods:
 * <code>[NAME=VALUE][;NAME=VALUE]*</code>.
 * <p>
 * Valid filter names are:
 * <ul>
 * <li>name: the bundle application name from the bundle definition.</li>
 * <li>user: the user that submitted the job.</li>
 * <li>group: the group for the job.</li>
 * <li>status: the status of the job.</li>
 * </ul>
 * <p>
 * The query will do an AND among all the filter names. The query will do an OR among all the filter values for the same
 * name. Multiple values must be specified as different name value pairs.
 */
public class LocalOozieClientBundle extends BaseLocalOozieClient {

    private final BundleEngine bundleEngine;

    public LocalOozieClientBundle(BundleEngine bundleEngine) {
        super(bundleEngine);
        this.bundleEngine = bundleEngine;
    }

    @Override
    public BundleJob getBundleJobInfo(String jobId) throws OozieClientException {
        try {
            return bundleEngine.getBundleJob(jobId);
        } catch (BundleEngineException e) {
            throw new OozieClientException(e.getErrorCode().toString(), e);
        }
    }

    @Override
    public Void reRunBundle(String jobId, String coordScope, String dateScope, boolean refresh, boolean noCleanup)
            throws OozieClientException {
        try {
            bundleEngine.reRun(jobId, coordScope, dateScope, refresh, noCleanup);
        } catch (BaseEngineException e) {
            throw new OozieClientException(e.getErrorCode().toString(), e);
        }
        return null;
    }

    @Override
    public List<BundleJob> getBundleJobsInfo(String filter, int start, int len) throws OozieClientException {
        try {
            return (List) bundleEngine.getBundleJobs(filter, start, len).getBundleJobs();
        } catch (BundleEngineException e) {
            throw new OozieClientException(e.getErrorCode().toString(), e);
        }
    }

    @Override
    public List<BulkResponse> getBulkInfo(String filter, int start, int len) throws OozieClientException {
        try {
            return (List) bundleEngine.getBulkJobs(filter, start, len).getResponses();
        } catch (BundleEngineException e) {
            throw new OozieClientException(e.getErrorCode().toString(), e);
        }
    }
}