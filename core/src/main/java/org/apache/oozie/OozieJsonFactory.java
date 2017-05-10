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

import org.apache.oozie.client.rest.JsonTags;
import org.json.simple.JSONObject;

public final class OozieJsonFactory {

    private OozieJsonFactory() {
    }

    public static JSONObject getWFJSONObject(WorkflowsInfo jobs, String timeZoneId) {
        JSONObject json = new JSONObject();
        json.put(JsonTags.WORKFLOWS_JOBS, WorkflowJobBean.toJSONArray(jobs.getWorkflows(), timeZoneId));
        json.put(JsonTags.WORKFLOWS_TOTAL, jobs.getTotal());
        json.put(JsonTags.WORKFLOWS_OFFSET, jobs.getStart());
        json.put(JsonTags.WORKFLOWS_LEN, jobs.getLen());
        return json;
    }

    public static JSONObject getCoordJSONObject(CoordinatorJobInfo jobs, String timeZoneId) {
        JSONObject json = new JSONObject();
        json.put(JsonTags.COORDINATOR_JOBS, CoordinatorJobBean.toJSONArray(jobs.getCoordJobs(), timeZoneId));
        json.put(JsonTags.COORD_JOB_TOTAL, jobs.getTotal());
        json.put(JsonTags.COORD_JOB_OFFSET, jobs.getStart());
        json.put(JsonTags.COORD_JOB_LEN, jobs.getLen());
        return json;
    }

    public static JSONObject getBundleJSONObject(BundleJobInfo jobs, String timeZoneId) {
        JSONObject json = new JSONObject();
        json.put(JsonTags.BUNDLE_JOBS, BundleJobBean.toJSONArray(jobs.getBundleJobs(), timeZoneId));
        json.put(JsonTags.BUNDLE_JOB_TOTAL, jobs.getTotal());
        json.put(JsonTags.BUNDLE_JOB_OFFSET, jobs.getStart());
        json.put(JsonTags.BUNDLE_JOB_LEN, jobs.getLen());
        return json;
    }
}