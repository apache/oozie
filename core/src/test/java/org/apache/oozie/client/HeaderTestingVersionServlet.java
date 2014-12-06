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

package org.apache.oozie.client;

import org.apache.oozie.servlet.VersionServlet;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.Enumeration;

// doing this because jetty 5.* does not allow to simply set a filter
public class HeaderTestingVersionServlet extends HttpServlet {
    public final static Map<String, String> OOZIE_HEADERS = new HashMap<String, String>();

    private HttpServlet servlet;

    public HeaderTestingVersionServlet() {
        servlet = new VersionServlet();
    }

    public void init(ServletConfig config) throws ServletException {
        servlet.init(config);
    }

    public void destroy() {
        servlet.destroy();
    }

    @SuppressWarnings("unchecked")
    public void service(ServletRequest request, ServletResponse response) throws ServletException, IOException {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        Enumeration<String> names = (Enumeration<String>) httpRequest.getHeaderNames();
        while (names.hasMoreElements()) {
            String name = names.nextElement();
            String value = httpRequest.getHeader(name);
            OOZIE_HEADERS.put(name, value);
        }
        servlet.service(request, response);
    }
}
