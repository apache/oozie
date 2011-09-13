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
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.servlet.VersionServlet;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;

import javax.servlet.http.HttpServletResponse;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class TestVersionServlet extends DagServletTestCase {

    static {
        new VersionServlet();
    }

    public void testVersion() throws Exception {
        runTest("/version", VersionServlet.class, true, new Callable<Void>() {
            public Void call() throws Exception {
                Map<String, String> params = new HashMap<String, String>();
                URL url = createURL("", params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
                JSONArray array = (JSONArray) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                assertEquals(2, array.size());
                assertEquals(OozieClient.WS_PROTOCOL_VERSION, array.get(1));
                return null;
            }
        });
    }

}
