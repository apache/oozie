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
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class MyJsonRestServlet extends JsonRestServlet {
    static ResourceInfo[] EMPTY = new ResourceInfo[0];

    static ResourceInfo[] NO_RESOURCE_NO_PARAMS = {
            new ResourceInfo("", Arrays.asList("GET"), Collections.EMPTY_LIST)};

    static ResourceInfo[] PARAMS_REQUIRED = {
            new ResourceInfo("", Arrays.asList("GET"), Arrays.asList(
                    new ParameterInfo("required", Boolean.class, true, Arrays.asList("GET")),
                    new ParameterInfo("optional", Boolean.class, false, Arrays.asList("GET"))))};

    static ResourceInfo[] PARAM_TYPES = {
            new ResourceInfo("", Arrays.asList("GET"), Arrays.asList(
                    new ParameterInfo("boolean", Boolean.class, false, Arrays.asList("GET")),
                    new ParameterInfo("integer", Integer.class, false, Arrays.asList("GET")),
                    new ParameterInfo("string", String.class, false, Arrays.asList("GET"))))};

    static ResourceInfo[] RESOURCE_GET_POST_PARAM_GET = {
            new ResourceInfo("", Arrays.asList("GET", "POST"), Arrays.asList(
                    new ParameterInfo("param", Boolean.class, true, Arrays.asList("GET"))))};

    static ResourceInfo[] FIXED_RESOURCE = {
            new ResourceInfo("resource", Arrays.asList("GET"), Collections.EMPTY_LIST)};

    static ResourceInfo[] WILDCARD_RESOURCE = {
            new ResourceInfo("*", Arrays.asList("GET", "PUT"),
            Arrays.asList(new ParameterInfo("action", String.class, false, Arrays.asList("PUT"))))};

    static ResourceInfo[] MULTIPLE_RESOURCES = {
            new ResourceInfo("resource1", Arrays.asList("GET"), Collections.EMPTY_LIST),
            new ResourceInfo("resource2", Arrays.asList("POST"), Collections.EMPTY_LIST)};

    static ResourceInfo[] MULTIPLE_RESOURCES_NO_RESOURCE = {
            new ResourceInfo("resource1", Arrays.asList("GET"), Collections.EMPTY_LIST),
            new ResourceInfo("resource2", Arrays.asList("GET"), Collections.EMPTY_LIST),
            new ResourceInfo("", Arrays.asList("POST"), Collections.EMPTY_LIST),
    };

    static ResourceInfo[] MULTIPLE_RESOURCES_WILDCARD = {
            new ResourceInfo("resource1", Arrays.asList("GET"), Collections.EMPTY_LIST),
            new ResourceInfo("resource2", Arrays.asList("GET"), Collections.EMPTY_LIST),
            new ResourceInfo("*", Arrays.asList("POST"), Collections.EMPTY_LIST),
    };

    static ResourceInfo[] CONTENT_TYPE_JSON_CRON_TEST = {
            new ResourceInfo("", Arrays.asList("GET"),
                             Arrays.asList(new ParameterInfo("json", String.class, true, Arrays.asList("GET"))))};

    static ResourceInfo[] ACTIVE = NO_RESOURCE_NO_PARAMS;

    public MyJsonRestServlet() {
        super("my", ACTIVE);
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        if (ACTIVE != CONTENT_TYPE_JSON_CRON_TEST) {
            response.setStatus(HttpServletResponse.SC_OK);
        }
        else {
            try {
                stopCron();
                validateContentType(request, "application/xml");
            }
            finally {
                startCron();
            }
            if (request.getParameter("json").equals("object")) {
                JSONObject json = new JSONObject();
                json.put("a", "object");
                sendJsonResponse(response, HttpServletResponse.SC_OK, json);
            }
            else {
                if (request.getParameter("json").equals("array")) {
                    JSONArray json = new JSONArray();
                    json.add("array");
                    sendJsonResponse(response, HttpServletResponse.SC_OK, json);
                }
            }
        }
    }

    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setStatus(HttpServletResponse.SC_OK);
    }

    @Override
    protected void doPut(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

      String jobId = getResourceName(request);
      String action = request.getParameter(RestConstants.ACTION_PARAM);

      if (action == null || action.isEmpty()) {
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      }

      if (jobId.isEmpty()) {
        throw new IllegalArgumentException("Job Id cannot be empty " + jobId);
      }
    }
}
