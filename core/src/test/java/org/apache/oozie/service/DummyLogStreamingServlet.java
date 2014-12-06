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

package org.apache.oozie.service;

import java.io.IOException;
import java.io.Writer;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Used by {@link TestZKXLogStreamingService#testStreamingWithMultipleOozieServers() } to stream logs from another Oozie "server".
 * Due to the way the servlet gets loaded, it has to be its own class instead of an inner class.
 */
public class DummyLogStreamingServlet extends HttpServlet {

    public static String lastQueryString = null;
    public static String logs = null;

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        lastQueryString = request.getQueryString();
        response.setStatus(HttpServletResponse.SC_OK);
        Writer writer = response.getWriter();
        writer.append(logs);
        writer.close();
    }
}
