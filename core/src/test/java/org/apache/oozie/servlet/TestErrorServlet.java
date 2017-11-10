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
import org.json.JSONObject;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestErrorServlet {

    @Test
    public void testServlet() throws Exception {
        String testErrorMsg = "test-error";
        int testErrorCode = 400;
        StringWriter stringWriter = new StringWriter();

        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);

        when(request.getAttribute("javax.servlet.error.message")).thenReturn(testErrorMsg);
        when(request.getAttribute("javax.servlet.error.status_code")).thenReturn(testErrorCode);
        when(response.getWriter()).thenReturn(new PrintWriter(stringWriter));

        new ErrorServlet().doGet(request, response);
        JSONObject json = new JSONObject(stringWriter.toString().trim());
        assertEquals("Error message is different.", testErrorMsg, json.getString(JsonTags.WORKFLOW_ACTION_ERROR_MESSAGE));
        assertEquals("Http code is different", testErrorCode, json.getInt(JsonTags.HTTP_STATUS_CODE));
    }
}