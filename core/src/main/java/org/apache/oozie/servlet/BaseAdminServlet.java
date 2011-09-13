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

import java.io.IOException;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.oozie.BuildInfo;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.service.AuthorizationException;
import org.apache.oozie.service.AuthorizationService;
import org.apache.oozie.service.InstrumentationService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.Instrumentation;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public abstract class BaseAdminServlet extends JsonRestServlet {

    private static final long serialVersionUID = 1L;
    protected String modeTag;

    public BaseAdminServlet(String instrumentationName, ResourceInfo[] RESOURCES_INFO) {
        super(instrumentationName, RESOURCES_INFO);
        setAllowSafeModeChanges(true);
    }

    /**
     * Change safemode state.
     */
    @Override
    protected void doPut(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String resourceName = getResourceName(request);
        request.setAttribute(AUDIT_OPERATION, resourceName);
        request.setAttribute(AUDIT_PARAM, request.getParameter(modeTag));

        try {
            AuthorizationService auth = Services.get().get(AuthorizationService.class);
            auth.authorizeForAdmin(getUser(request), true);
        }
        catch (AuthorizationException ex) {
            throw new XServletException(HttpServletResponse.SC_UNAUTHORIZED, ex);
        }

        setOozieMode(request, response, resourceName);
        /*if (resourceName.equals(RestConstants.ADMIN_STATUS_RESOURCE)) {
            boolean safeMode = Boolean.parseBoolean(request.getParameter(RestConstants.ADMIN_SAFE_MODE_PARAM));
            Services.get().setSafeMode(safeMode);
            response.setStatus(HttpServletResponse.SC_OK);
        }
        else {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0301, resourceName);
        }*/
    }

    /**
     * Return safemode state, instrumentation, configuration, osEnv or
     * javaSysProps
     */
    @Override
    @SuppressWarnings("unchecked")
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String resource = getResourceName(request);
        Instrumentation instr = Services.get().get(InstrumentationService.class).get();

        if (resource.equals(RestConstants.ADMIN_STATUS_RESOURCE)) {
            JSONObject json = new JSONObject();
            populateOozieMode(json);
            // json.put(JsonTags.SYSTEM_SAFE_MODE, getOozeMode());
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
        else if (resource.equals(RestConstants.ADMIN_QUEUE_DUMP_RESOURCE)) {
            JSONObject json = new JSONObject();
            getQueueDump(json);
            sendJsonResponse(response, HttpServletResponse.SC_OK, json);
        }
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException,
            IOException {
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

    protected abstract void populateOozieMode(JSONObject json);

    protected abstract void setOozieMode(HttpServletRequest request, HttpServletResponse response, String resourceName)
            throws XServletException;

    protected abstract void getQueueDump(JSONObject json) throws XServletException;

}
