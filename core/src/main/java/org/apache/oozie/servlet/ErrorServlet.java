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

import org.apache.oozie.client.rest.JsonTags;
import org.json.simple.JSONObject;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import static org.apache.oozie.servlet.JsonRestServlet.JSON_UTF8;

/**
 * Servlet to show error response in JSON
 */
public class ErrorServlet extends HttpServlet{
    private static final long serialVersionUID = 1L;

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        handleError(request, response);
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        handleError(request, response);
    }

    @Override
    protected void doPut(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        handleError(request, response);
    }

    /**
     * Writes error message as JSON to response
     * @param request the request to get the error message from
     * @param response the response to write the error to
     * @throws IOException IOException
     */
    private void handleError(HttpServletRequest request, HttpServletResponse response) throws IOException {
        Object errorMsg = request.getAttribute("javax.servlet.error.message");
        Object statusCode = request.getAttribute("javax.servlet.error.status_code");
        JSONObject json = new JSONObject();
        json.put(JsonTags.HTTP_STATUS_CODE, statusCode);
        json.put(JsonTags.WORKFLOW_ACTION_ERROR_MESSAGE, errorMsg);
        response.setContentType(JSON_UTF8);
        json.writeJSONString(response.getWriter());
    }
}