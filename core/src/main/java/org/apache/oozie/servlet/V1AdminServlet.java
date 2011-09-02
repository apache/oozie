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

import java.util.Arrays;
import java.util.Collections;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.OozieClient.SYSTEM_MODE;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.service.Services;
import org.apache.oozie.servlet.JsonRestServlet.ParameterInfo;
import org.apache.oozie.servlet.JsonRestServlet.ResourceInfo;
import org.json.simple.JSONObject;

public class V1AdminServlet extends BaseAdminServlet {

    private static final String INSTRUMENTATION_NAME = "v1admin";
    private static final ResourceInfo RESOURCES_INFO[] = new ResourceInfo[6];

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
    }

    public V1AdminServlet() {
        super(INSTRUMENTATION_NAME, RESOURCES_INFO);
        modeTag = RestConstants.ADMIN_SYSTEM_MODE_PARAM;
    }

    protected void populateOozieMode(JSONObject json) {
        System.out.println(" getOozeMode " + Services.get().getSystemMode());
        json.put(JsonTags.OOZIE_SYSTEM_MODE, Services.get().getSystemMode().toString());
    }

    @Override
    protected void setOozieMode(HttpServletRequest request,
                                HttpServletResponse response, String resourceName)
            throws XServletException {
        if (resourceName.equals(RestConstants.ADMIN_STATUS_RESOURCE)) {
            SYSTEM_MODE sysMode = SYSTEM_MODE.valueOf(request.getParameter(modeTag));
            System.out.println(modeTag + " CCCC " + sysMode);
            Services.get().setSystemMode(sysMode);
            //Services.get().setSafeMode(safeMode);
            response.setStatus(HttpServletResponse.SC_OK);
        }
        else {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST,
                                        ErrorCode.E0301, resourceName);
        }
    }

}
