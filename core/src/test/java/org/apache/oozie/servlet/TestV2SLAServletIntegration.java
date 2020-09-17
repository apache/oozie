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

package org.apache.oozie.servlet;

import org.apache.oozie.client.rest.RestConstants;

import javax.servlet.http.HttpServletResponse;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class TestV2SLAServletIntegration extends DagServletTestCase {

    private static final boolean IS_SECURITY_ENABLED = false;

    public void testEmptyQueryParams() throws Exception {
        Map<String, String> queryParams = new HashMap<>();
        callHttpEndpointAndAssertResponse(queryParams, HttpServletResponse.SC_BAD_REQUEST);
    }

    public void testFilterNameTypo() throws Exception {
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put(RestConstants.JOBS_FILTER_PARAM + "typo", "app_name=testapp-1");
        callHttpEndpointAndAssertResponse(queryParams, HttpServletResponse.SC_BAD_REQUEST);
    }

    public void testValidRequest() throws Exception {
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put(RestConstants.JOBS_FILTER_PARAM, "app_name=testapp-1");
        callHttpEndpointAndAssertResponse(queryParams, HttpServletResponse.SC_OK);
    }

    private void callHttpEndpointAndAssertResponse(Map<String, String> queryParams, int expectedResponseCode)  throws Exception {
        runTest("/v2/sla", V2SLAServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                URL url = createURL("", queryParams);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals("HTTP response code", expectedResponseCode, conn.getResponseCode());
                return null;
            }
        });
    }

}