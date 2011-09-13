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

import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.servlet.CallbackServlet;

import javax.servlet.http.HttpServletResponse;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

public class TestCallbackServlet extends DagServletTestCase {

    static {
        new CallbackServlet();
    }

    @SuppressWarnings("unchecked")
    public void testCallbackGet() throws Exception {
        runTest("/callback", CallbackServlet.class, true, new Callable<Void>() {
            public Void call() throws Exception {
                URL url = createURL("", Collections.EMPTY_MAP);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());

                Map<String, String> params = new HashMap<String, String>();
                params.put("id", "error");
                params.put("status", "error");
                url = createURL("", params);
                conn = (HttpURLConnection) url.openConnection();
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());

                params = new HashMap<String, String>();
                params.put("id", "ok");
                params.put("status", "ok");
                url = createURL("", params);
                conn = (HttpURLConnection) url.openConnection();
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());

                return null;
            }
        });
    }

    @SuppressWarnings("unchecked")
    public void testCallbackPost() throws Exception {
        runTest("/callback", CallbackServlet.class, true, new Callable<Void>() {
            public Void call() throws Exception {
                MockDagEngineService.reset();
                URL url = createURL("", Collections.EMPTY_MAP);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());

                MockDagEngineService.reset();
                Map<String, String> params = new HashMap<String, String>();
                params.put("id", "error");
                params.put("status", "error");
                url = createURL("", params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setDoOutput(true);
                conn.setRequestMethod("POST");
                conn.setRequestProperty("content-type", RestConstants.TEXT_CONTENT_TYPE);
                Properties props = new Properties();
                props.setProperty("a", "A");
                props.store(conn.getOutputStream(), "UTF-8");
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());

                MockDagEngineService.reset();
                params = new HashMap<String, String>();
                params.put("id", "ok");
                params.put("status", "ok");
                url = createURL("", params);
                conn = (HttpURLConnection) url.openConnection();
                conn.setDoOutput(true);
                conn.setRequestMethod("POST");
                conn.setRequestProperty("content-type", RestConstants.TEXT_CONTENT_TYPE);
                props = new Properties();
                props.setProperty("a", "A");
                props.store(conn.getOutputStream(), "UTF-8");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertEquals(props, MockDagEngineService.properties);
                return null;
            }
        });
    }

}
