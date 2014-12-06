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
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.oozie.DagEngine;
import org.apache.oozie.DagEngineException;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.service.CallbackService;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.DagEngineService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.PropertiesUtils;
import org.apache.oozie.util.XLog;

public class CallbackServlet extends JsonRestServlet {
    private static final String INSTRUMENTATION_NAME = "callback";

    private static final ResourceInfo RESOURCE_INFO =
            new ResourceInfo("", Arrays.asList("POST", "GET"), Collections.EMPTY_LIST);

    public final static String CONF_MAX_DATA_LEN = "oozie.servlet.CallbackServlet.max.data.len";

    private static int maxDataLen;

    private XLog log = null;

    public CallbackServlet() {
        super(INSTRUMENTATION_NAME, RESOURCE_INFO);
    }

    @Override
    public void init() {
        maxDataLen = ConfigurationService.getInt(CONF_MAX_DATA_LEN);
    }

    /**
     * GET callback
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String queryString = request.getQueryString();
        CallbackService callbackService = Services.get().get(CallbackService.class);

        if (!callbackService.isValid(queryString)) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0402, queryString);
        }

        String actionId = callbackService.getActionId(queryString);
        if (actionId == null) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0402, queryString);
        }
        log = XLog.getLog(getClass());
        setLogInfo(actionId);
        log.debug("Received a CallbackServlet.doGet() with query string " + queryString);

        DagEngine dagEngine = Services.get().get(DagEngineService.class).getSystemDagEngine();
        try {
            log.info(XLog.STD, "callback for action [{0}]", actionId);
            dagEngine.processCallback(actionId, callbackService.getExternalStatus(queryString), null);
        }
        catch (DagEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }

    /**
     * POST callback
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException,
            IOException {
        String queryString = request.getQueryString();
        CallbackService callbackService = Services.get().get(CallbackService.class);

        if (!callbackService.isValid(queryString)) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0402, queryString);
        }

        String actionId = callbackService.getActionId(queryString);
        if (actionId == null) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0402, queryString);
        }
        log = XLog.getLog(getClass());
        setLogInfo(actionId);
        log.debug("Received a CallbackServlet.doPost() with query string " + queryString);

        validateContentType(request, RestConstants.TEXT_CONTENT_TYPE);
        try {
            log.info(XLog.STD, "callback for action [{0}]", actionId);
            String data = IOUtils.getReaderAsString(request.getReader(), maxDataLen);
            Properties props = PropertiesUtils.stringToProperties(data);
            DagEngine dagEngine = Services.get().get(DagEngineService.class).getSystemDagEngine();
            dagEngine.processCallback(actionId, callbackService.getExternalStatus(queryString), props);
        }
        catch (IOException ex) {
            if (ex.getMessage().startsWith("stream exceeds limit")) {
                // TODO, WE MUST SET THE ACTION TO ERROR
                throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0403, "data too long");
            }
            else {
                throw ex;
            }
        }
        catch (DagEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }
}
