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

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.servlet.JsonRestServlet;
import org.json.simple.JSONArray;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class VersionServlet extends JsonRestServlet {
    private static final String INSTRUMENTATION_NAME = "version";

    private static final ResourceInfo RESOURCE_INFO =
            new ResourceInfo("", Arrays.asList("GET"), Collections.EMPTY_LIST);

    // private static JSONArray versions = new JSONArray();

    public VersionServlet() {
        super(INSTRUMENTATION_NAME, RESOURCE_INFO);
        // versions.add(OozieClient.WS_PROTOCOL_VERSION_0);
        // versions.add(OozieClient.WS_PROTOCOL_VERSION);
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        JSONArray versions = new JSONArray();
        versions.add(OozieClient.WS_PROTOCOL_VERSION_0);
        versions.add(OozieClient.WS_PROTOCOL_VERSION_1);
        versions.add(OozieClient.WS_PROTOCOL_VERSION);
        sendJsonResponse(response, HttpServletResponse.SC_OK, versions);
    }
}
