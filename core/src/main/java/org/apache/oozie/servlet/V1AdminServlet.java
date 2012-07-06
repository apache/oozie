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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.OozieClient.SYSTEM_MODE;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.service.CallableQueueService;
import org.apache.oozie.service.Services;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class V1AdminServlet extends BaseAdminServlet {

    private static final long serialVersionUID = 1L;
    private static final String INSTRUMENTATION_NAME = "v1admin";
    private static final ResourceInfo RESOURCES_INFO[] = new ResourceInfo[8];

    static {
        RESOURCES_INFO[0] = new ResourceInfo(RestConstants.ADMIN_STATUS_RESOURCE, Arrays.asList("PUT", "GET"),
                                             Arrays.asList(new ParameterInfo(RestConstants.ADMIN_SYSTEM_MODE_PARAM, String.class, true,
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
        RESOURCES_INFO[6] = new ResourceInfo(RestConstants.ADMIN_QUEUE_DUMP_RESOURCE, Arrays.asList("GET"),
                Collections.EMPTY_LIST);
        RESOURCES_INFO[7] = new ResourceInfo((RestConstants.ADMIN_TIME_ZONES_RESOURCE), Arrays.asList("GET"),
                Collections.EMPTY_LIST);
    }

    public V1AdminServlet() {
        super(INSTRUMENTATION_NAME, RESOURCES_INFO);
        modeTag = RestConstants.ADMIN_SYSTEM_MODE_PARAM;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.oozie.servlet.BaseAdminServlet#populateOozieMode(org.json.
     * simple.JSONObject)
     */
    @SuppressWarnings("unchecked")
    @Override
    protected void populateOozieMode(JSONObject json) {
        json.put(JsonTags.OOZIE_SYSTEM_MODE, Services.get().getSystemMode().toString());
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.oozie.servlet.BaseAdminServlet#setOozieMode(javax.servlet.
     * http.HttpServletRequest, javax.servlet.http.HttpServletResponse,
     * java.lang.String)
     */
    @Override
    protected void setOozieMode(HttpServletRequest request,
                                HttpServletResponse response, String resourceName)
            throws XServletException {
        if (resourceName.equals(RestConstants.ADMIN_STATUS_RESOURCE)) {
            SYSTEM_MODE sysMode = SYSTEM_MODE.valueOf(request.getParameter(modeTag));
            Services.get().setSystemMode(sysMode);
            response.setStatus(HttpServletResponse.SC_OK);
        }
        else {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST,
                                        ErrorCode.E0301, resourceName);
        }
    }

    /**
     * Get a json array of queue dump and a json array of unique map dump
     *
     * @param JSONObject the result json object that contains a JSONArray for the callable dump
     *
     * @see
     * org.apache.oozie.servlet.BaseAdminServlet#getQueueDump(org.json.simple
     * .JSONObject)
     */
    @SuppressWarnings("unchecked")
    @Override
    protected void getQueueDump(JSONObject json) throws XServletException {
        List<String> queueDumpList = Services.get().get(CallableQueueService.class).getQueueDump();
        JSONArray queueDumpArray = new JSONArray();
        for (String str: queueDumpList) {
            JSONObject jObject = new JSONObject();
            jObject.put(JsonTags.CALLABLE_DUMP, str);
            queueDumpArray.add(jObject);
        }
        json.put(JsonTags.QUEUE_DUMP, queueDumpArray);

        List<String> uniqueDumpList = Services.get().get(CallableQueueService.class).getUniqueDump();
        JSONArray uniqueDumpArray = new JSONArray();
        for (String str: uniqueDumpList) {
            JSONObject jObject = new JSONObject();
            jObject.put(JsonTags.UNIQUE_ENTRY_DUMP, str);
            uniqueDumpArray.add(jObject);
        }
        json.put(JsonTags.UNIQUE_MAP_DUMP, uniqueDumpArray);
    }

}
