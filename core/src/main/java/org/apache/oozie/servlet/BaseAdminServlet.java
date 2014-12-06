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
import java.io.Reader;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Pattern;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.fs.Path;
import org.apache.oozie.BuildInfo;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.service.AuthorizationException;
import org.apache.oozie.service.AuthorizationService;
import org.apache.oozie.service.InstrumentationService;
import org.apache.oozie.service.JobsConcurrencyService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.ShareLibService;
import org.apache.oozie.util.AuthUrlClient;
import org.apache.oozie.util.ConfigUtils;
import org.apache.oozie.util.Instrumentation;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

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

        authorizeRequest(request);

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
     * Get JMS connection Info
     * @param request
     * @param response
     * @throws XServletException
     * @throws IOException
     */
    abstract JsonBean getJMSConnectionInfo(HttpServletRequest request, HttpServletResponse response)
            throws XServletException, IOException;


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
            sendInstrumentationResponse(response, instr);
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
        else if (resource.equals(RestConstants.ADMIN_TIME_ZONES_RESOURCE)) {
            JSONObject json = new JSONObject();
            json.put(JsonTags.AVAILABLE_TIME_ZONES, availableTimeZonesToJsonArray());
            sendJsonResponse(response, HttpServletResponse.SC_OK, json);
        }
        else if (resource.equals(RestConstants.ADMIN_JMS_INFO)) {
            String timeZoneId = request.getParameter(RestConstants.TIME_ZONE_PARAM) == null ? "GMT" : request
                    .getParameter(RestConstants.TIME_ZONE_PARAM);
            JsonBean jmsBean = getJMSConnectionInfo(request, response);
            sendJsonResponse(response, HttpServletResponse.SC_OK, jmsBean, timeZoneId);
        }
        else if (resource.equals(RestConstants.ADMIN_AVAILABLE_OOZIE_SERVERS_RESOURCE)) {
            JSONObject json = new JSONObject();
            json.putAll(getOozieURLs());
            sendJsonResponse(response, HttpServletResponse.SC_OK, json);
        }
        else if (resource.equals(RestConstants.ADMIN_UPDATE_SHARELIB)) {
            authorizeRequest(request);
            updateShareLib(request, response);
        }
        else if (resource.equals(RestConstants.ADMIN_LIST_SHARELIB)) {
            String sharelibKey = request.getParameter(RestConstants.SHARE_LIB_REQUEST_KEY);
            sendJsonResponse(response, HttpServletResponse.SC_OK, getShareLib(sharelibKey));
        }
        else if (resource.equals(RestConstants.ADMIN_METRICS_RESOURCE)) {
            sendMetricsResponse(response);
        }
    }

    /**
     * Gets the list of share lib.
     *
     * @param sharelibKey the sharelib key
     * @return the list of supported share lib
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    private JSONObject getShareLib(String sharelibKey) throws IOException {
        JSONObject json = new JSONObject();

        ShareLibService shareLibService = Services.get().get(ShareLibService.class);

        // for testcases.
        if (shareLibService == null) {
            return json;
        }
        JSONArray shareLibList = new JSONArray();

        Map<String, List<Path>> shareLibLauncherMap = shareLibService.getShareLib();
        if (sharelibKey != null && !sharelibKey.isEmpty()) {
            Pattern pattern = Pattern.compile(sharelibKey);
            for (String key : shareLibLauncherMap.keySet()) {
                if (pattern.matcher(key).matches() == true) {
                    JSONObject object = new JSONObject();
                    JSONArray fileList = new JSONArray();
                    List<Path> pathList = shareLibLauncherMap.get(key);

                    for (Path file : pathList) {
                        fileList.add(file.toString());
                    }
                    object.put(JsonTags.SHARELIB_LIB_NAME, key);
                    object.put(JsonTags.SHARELIB_LIB_FILES, fileList);
                    shareLibList.add(object);

                }
            }
        }
        else {
            for (String key : shareLibLauncherMap.keySet()) {
                JSONObject object = new JSONObject();
                object.put(JsonTags.SHARELIB_LIB_NAME, key);
                shareLibList.add(object);
            }

        }
        json.put(JsonTags.SHARELIB_LIB, shareLibList);

        return json;
    }

    /**
     * Update share lib. support HA
     *
     * @param request the request
     * @param response the response
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @SuppressWarnings("unchecked")
    public void updateShareLib(HttpServletRequest request, HttpServletResponse response) throws IOException {
        JSONArray jsonArray = new JSONArray();
        JobsConcurrencyService jc = Services.get().get(JobsConcurrencyService.class);
        if (jc.isAllServerRequest(request.getParameterMap())) {
            Map<String, String> servers = jc.getOtherServerUrls();
            for (String otherUrl : servers.values()) {
                // It's important that we specify ALL_SERVERS_PARAM=false, so that other oozie server should not call other oozie
                //servers to update sharelib (and creating an infinite recursion)
                String serverUrl = otherUrl + "/v2/admin/" + RestConstants.ADMIN_UPDATE_SHARELIB + "?"
                        + RestConstants.ALL_SERVER_REQUEST + "=false";
                try {
                    Reader reader = AuthUrlClient.callServer(serverUrl);
                    JSONObject json = (JSONObject) JSONValue.parse(reader);
                    jsonArray.add(json);
                }
                catch (Exception e) {
                    JSONObject errorJson = new JSONObject();
                    errorJson.put(JsonTags.SHARELIB_UPDATE_HOST, otherUrl);
                    errorJson.put(JsonTags.SHARELIB_UPDATE_STATUS, e.getMessage());
                    JSONObject newJson = new JSONObject();
                    newJson.put(JsonTags.SHARELIB_LIB_UPDATE, errorJson);
                    jsonArray.add(newJson);
                }
            }
            //For current server
            JSONObject newJson = new JSONObject();
            newJson.put(JsonTags.SHARELIB_LIB_UPDATE, updateLocalShareLib(request));
            jsonArray.add(newJson);
            sendJsonResponse(response, HttpServletResponse.SC_OK, jsonArray);
        }
        else {
            JSONObject newJson = new JSONObject();
            newJson.put(JsonTags.SHARELIB_LIB_UPDATE, updateLocalShareLib(request));
            sendJsonResponse(response, HttpServletResponse.SC_OK, newJson);
        }
    }

    @SuppressWarnings("unchecked")
    private JSONObject updateLocalShareLib(HttpServletRequest request) {
        ShareLibService shareLibService = Services.get().get(ShareLibService.class);
        JSONObject json = new JSONObject();
        json.put(JsonTags.SHARELIB_UPDATE_HOST, ConfigUtils.getOozieEffectiveUrl());
        try {
            json.putAll(shareLibService.updateShareLib());
            json.put(JsonTags.SHARELIB_UPDATE_STATUS, "Successful");
        }
        catch (Exception e) {
            json.put(JsonTags.SHARELIB_UPDATE_STATUS, e.getClass().getName() + ": " + e.getMessage());
        }
        return json;
    }

    /**
     * Authorize request.
     *
     * @param request the HttpServletRequest
     * @throws XServletException the x servlet exception
     */
    private void authorizeRequest(HttpServletRequest request) throws XServletException {
        try {
            AuthorizationService auth = Services.get().get(AuthorizationService.class);
            auth.authorizeForAdmin(getUser(request), true);
        }
        catch (AuthorizationException ex) {
            throw new XServletException(HttpServletResponse.SC_UNAUTHORIZED, ex);
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

    private static final JSONArray GMTOffsetTimeZones = new JSONArray();
    static {
        prepareGMTOffsetTimeZones();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void prepareGMTOffsetTimeZones() {
        for (String tzId : new String[]{"GMT-12:00", "GMT-11:00", "GMT-10:00", "GMT-09:00", "GMT-08:00", "GMT-07:00", "GMT-06:00",
                                        "GMT-05:00", "GMT-04:00", "GMT-03:00", "GMT-02:00", "GMT-01:00", "GMT+01:00", "GMT+02:00",
                                        "GMT+03:00", "GMT+04:00", "GMT+05:00", "GMT+06:00", "GMT+07:00", "GMT+08:00", "GMT+09:00",
                                        "GMT+10:00", "GMT+11:00", "GMT+12:00"}) {
            TimeZone tz = TimeZone.getTimeZone(tzId);
            JSONObject json = new JSONObject();
            json.put(JsonTags.TIME_ZOME_DISPLAY_NAME, tz.getDisplayName(false, TimeZone.SHORT) + " (" + tzId + ")");
            json.put(JsonTags.TIME_ZONE_ID, tzId);
            GMTOffsetTimeZones.add(json);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private JSONArray availableTimeZonesToJsonArray() {
        JSONArray array = new JSONArray();
        for (String tzId : TimeZone.getAvailableIDs()) {
            // skip id's that are like "Etc/GMT+01:00" because their display names are like "GMT-01:00", which is confusing
            if (!tzId.startsWith("Etc/GMT")) {
                JSONObject json = new JSONObject();
                TimeZone tZone = TimeZone.getTimeZone(tzId);
                json.put(JsonTags.TIME_ZOME_DISPLAY_NAME, tZone.getDisplayName(false, TimeZone.SHORT) + " (" + tzId + ")");
                json.put(JsonTags.TIME_ZONE_ID, tzId);
                array.add(json);
            }
        }

        // The combo box this populates cannot be edited, so the user can't type in GMT offsets (like in the CLI), so we'll add
        // in some hourly offsets here (though the user will not be able to use other offsets without editing the cookie manually
        // and they are not in order)
        array.addAll(GMTOffsetTimeZones);

        return array;
    }

    protected void sendInstrumentationResponse(HttpServletResponse response, Instrumentation instr)
            throws IOException, XServletException {
        sendJsonResponse(response, HttpServletResponse.SC_OK, instrToJson(instr));
    }

    protected abstract Map<String, String> getOozieURLs() throws XServletException;

    protected abstract void sendMetricsResponse(HttpServletResponse response) throws IOException, XServletException;
}
