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
import org.apache.oozie.service.Services;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.PropertiesUtils;
import org.apache.oozie.util.XLog;
import org.apache.oozie.DagEngine;
import org.apache.oozie.DagEngineException;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.service.DagEngineService;
import org.apache.oozie.service.CallbackService;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class CallbackServlet extends JsonRestServlet {
    private static final String INSTRUMENTATION_NAME = "callback";

    private static final ResourceInfo RESOURCE_INFO =
            new ResourceInfo("", Arrays.asList("POST", "GET"), Collections.EMPTY_LIST);

    public final static String CONF_MAX_DATA_LEN = "oozie.servlet.CallbackServlet.max.data.len";

    private static int maxDataLen;

    public CallbackServlet() {
        super(INSTRUMENTATION_NAME, RESOURCE_INFO);
    }

    public void init() {
        maxDataLen = Services.get().getConf().getInt(CONF_MAX_DATA_LEN, 2 * 1024);
    }

    /**
     * GET callback
     */
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String queryString = request.getQueryString();
        XLog.getLog(getClass()).debug("Received a CallbackServlet.doGet() with query string " + queryString);
        CallbackService callbackService = Services.get().get(CallbackService.class);
        if (!callbackService.isValid(queryString)) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0402, queryString);
        }
        DagEngine dagEngine = Services.get().get(DagEngineService.class).getSystemDagEngine();
        try {
            XLog.getLog(getClass()).info(XLog.STD, "callback for action [{0}]",
                                         callbackService.getActionId(queryString));
            dagEngine.processCallback(callbackService.getActionId(queryString),
                                      callbackService.getExternalStatus(queryString), null);
        }
        catch (DagEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }

    /**
     * POST callback
     */
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String queryString = request.getQueryString();
        XLog.getLog(getClass()).debug("Received a CallbackServlet.doPost() with query string " + queryString);
        CallbackService callbackService = Services.get().get(CallbackService.class);
        if (!callbackService.isValid(queryString)) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0402, queryString);
        }
        validateContentType(request, RestConstants.TEXT_CONTENT_TYPE);
        try {
            XLog.getLog(getClass()).info(XLog.STD, "callback for action [{0}]",
                                         callbackService.getActionId(queryString));
            String data = IOUtils.getReaderAsString(request.getReader(), maxDataLen);
            Properties props = PropertiesUtils.stringToProperties(data);
            DagEngine dagEngine = Services.get().get(DagEngineService.class).getSystemDagEngine();
            dagEngine.processCallback(callbackService.getActionId(queryString),
                                      callbackService.getExternalStatus(queryString), props);
        }
        catch (IOException ex) {
            if (ex.getMessage().startsWith("stream exceeds limit")) {
                //TODO, WE MUST SET THE ACTION TO ERROR
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