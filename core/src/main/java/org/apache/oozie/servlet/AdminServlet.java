/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.servlet;

import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.service.InstrumentationService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.AuthorizationService;
import org.apache.oozie.service.AuthorizationException;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.BuildInfo;
import org.apache.oozie.ErrorCode;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class AdminServlet extends JsonRestServlet {
    private static final String INSTRUMENTATION_NAME = "admin";

    private static final ResourceInfo RESOURCES_INFO[] = new ResourceInfo[6];

    static {
        RESOURCES_INFO[0] = new ResourceInfo(RestConstants.ADMIN_STATUS_RESOURCE, Arrays.asList("PUT", "GET"),
                                             Arrays.asList(new ParameterInfo(RestConstants.ADMIN_SAFE_MODE_PARAM,
                                                                             Boolean.class, true,
                                                                             Arrays.asList("PUT"))));
        RESOURCES_INFO[1] = new ResourceInfo(RestConstants.ADMIN_OS_ENV_RESOURCE, Arrays.asList("GET"),
                                             Collections.EMPTY_LIST);
        RESOURCES_INFO[2] = new ResourceInfo(RestConstants.ADMIN_JAVA_SYS_PROPS_RESOURCE, Arrays.asList("GET"),
                                             Collections.EMPTY_LIST);
        RESOURCES_INFO[3] = new ResourceInfo(RestConstants.ADMIN_CONFIG_RESOURCE, Arrays.asList("GET"),
                                             Collections.EMPTY_LIST);
        RESOURCES_INFO[4] = new ResourceInfo(RestConstants.ADMIN_INSTRUMENTATION_RESOURCE, Arrays.asList("GET"),
                                             Collections.EMPTY_LIST);
        RESOURCES_INFO[5] = new ResourceInfo(RestConstants.ADMIN_BUILD_VERSION_RESOURCE, Arrays.asList("GET"),
                                             Collections.EMPTY_LIST);
    }

    public AdminServlet() {
        super(INSTRUMENTATION_NAME, RESOURCES_INFO);
        setAllowSafeModeChanges(true);
    }

    /**
     * Change safemode state.
     */
    protected void doPut(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String resourceName = getResourceName(request);
        request.setAttribute(AUDIT_OPERATION, resourceName);
        request.setAttribute(AUDIT_PARAM, request.getParameter(RestConstants.ADMIN_SAFE_MODE_PARAM));

        try {
            AuthorizationService auth = Services.get().get(AuthorizationService.class);
            auth.authorizeForAdmin(getUser(request), true);
        }
        catch (AuthorizationException ex) {
            throw new XServletException(HttpServletResponse.SC_UNAUTHORIZED, ex);
        }

        if (resourceName.equals(RestConstants.ADMIN_STATUS_RESOURCE)) {
            boolean safeMode = Boolean.parseBoolean(request.getParameter(RestConstants.ADMIN_SAFE_MODE_PARAM));
            Services.get().setSafeMode(safeMode);
            response.setStatus(HttpServletResponse.SC_OK);
        }
        else {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0301, resourceName);
        }
    }

    /**
     * Return safemode state, instrumentation, configuration, osEnv or javaSysProps
     */
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String resource = getResourceName(request);
        Instrumentation instr = Services.get().get(InstrumentationService.class).get();

            if (resource.equals(RestConstants.ADMIN_STATUS_RESOURCE)) {
                JSONObject json = new JSONObject();
                json.put(JsonTags.SYSTEM_SAFE_MODE, Services.get().isSafeMode());
                sendJsonResponse(response, HttpServletResponse.SC_OK, json);
            }
            else if (resource.equals(RestConstants.ADMIN_OS_ENV_RESOURCE)) {
                JSONObject json = new JSONObject();
                json.putAll(instr.getOSEnv());
                sendJsonResponse(response, HttpServletResponse.SC_OK, json);
            }
            else if (resource.equals(RestConstants.ADMIN_JAVA_SYS_PROPS_RESOURCE)) {
                JSONObject json = new JSONObject();
                json.putAll(instr.getJavaSystemProperties());
                sendJsonResponse(response, HttpServletResponse.SC_OK, json);
            }
            else if (resource.equals(RestConstants.ADMIN_CONFIG_RESOURCE)) {
                JSONObject json = new JSONObject();
                json.putAll(instr.getConfiguration());
                sendJsonResponse(response, HttpServletResponse.SC_OK, json);
            }
            else if (resource.equals(RestConstants.ADMIN_INSTRUMENTATION_RESOURCE)) {
                sendJsonResponse(response, HttpServletResponse.SC_OK, instrToJson(instr));
            }
            else if (resource.equals(RestConstants.ADMIN_BUILD_VERSION_RESOURCE)) {
                JSONObject json = new JSONObject();
                json.put(JsonTags.BUILD_VERSION, BuildInfo.getBuildInfo().getProperty(BuildInfo.BUILD_VERSION));
                sendJsonResponse(response, HttpServletResponse.SC_OK, json);
            }
    }

    @SuppressWarnings("unchecked")
    private <T> JSONArray instrElementsToJson(Map<String, Map<String, Instrumentation.Element<T>>> instrElements) {
        JSONArray array = new JSONArray();
        for (Map.Entry<String, Map<String, Instrumentation.Element<T>>> group : instrElements.entrySet()) {
            JSONObject json = new JSONObject();
            String groupName = group.getKey();
            json.put(JsonTags.INSTR_GROUP, groupName);
            JSONArray dataArray = new JSONArray();
            for (Map.Entry<String, Instrumentation.Element<T>> elementEntry : group.getValue().entrySet()) {
                String samplerName = elementEntry.getKey();
                JSONObject dataJson = new JSONObject();
                dataJson.put(JsonTags.INSTR_NAME, samplerName);
                Object value = elementEntry.getValue().getValue();
                if (value instanceof Instrumentation.Timer) {
                    Instrumentation.Timer timer = (Instrumentation.Timer) value;
                    dataJson.put(JsonTags.INSTR_TIMER_TICKS, timer.getTicks());
                    dataJson.put(JsonTags.INSTR_TIMER_OWN_TIME_AVG, timer.getOwnAvg());
                    dataJson.put(JsonTags.INSTR_TIMER_TOTAL_TIME_AVG, timer.getTotalAvg());
                    dataJson.put(JsonTags.INSTR_TIMER_OWN_STD_DEV, timer.getOwnStdDev());
                    dataJson.put(JsonTags.INSTR_TIMER_TOTAL_STD_DEV, timer.getTotalStdDev());
                    dataJson.put(JsonTags.INSTR_TIMER_OWN_MIN_TIME, timer.getOwnMin());
                    dataJson.put(JsonTags.INSTR_TIMER_OWN_MAX_TIME, timer.getOwnMax());
                    dataJson.put(JsonTags.INSTR_TIMER_TOTAL_MIN_TIME, timer.getTotalMin());
                    dataJson.put(JsonTags.INSTR_TIMER_TOTAL_MAX_TIME, timer.getTotalMax());
                }
                else {
                    dataJson.put(JsonTags.INSTR_VARIABLE_VALUE, value);
                }
                dataArray.add(dataJson);
            }
            json.put(JsonTags.INSTR_DATA, dataArray);
            array.add(json);
        }
        return array;
    }

    @SuppressWarnings("unchecked")
    private JSONObject instrToJson(Instrumentation instr) {
        JSONObject json = new JSONObject();
        json.put(JsonTags.INSTR_VARIABLES, instrElementsToJson(instr.getVariables()));
        json.put(JsonTags.INSTR_SAMPLERS, instrElementsToJson(instr.getSamplers()));
        json.put(JsonTags.INSTR_COUNTERS, instrElementsToJson(instr.getCounters()));
        json.put(JsonTags.INSTR_TIMERS, instrElementsToJson(instr.getTimers()));
        return json;
    }

}
