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

import org.apache.commons.io.IOUtils;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.EmbeddedServletContainer;
import org.apache.oozie.test.XTestCase;
import org.json.JSONException;
import org.json.JSONObject;

import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.Callable;

public class TestJsonRestServlet extends XTestCase {

    static {
        new MyJsonRestServlet();
    }

    EmbeddedServletContainer container;

    private int invoke(String method, String resource, String queryString) throws Exception {
        return invoke(method, resource, queryString, "dummy").getResponseCode();
    }

    private HttpURLConnection invoke(String method, String resource, String queryString, String contentType) throws Exception {
        String s = container.getServletURL("/dummy");
        if (resource != null) {
            s += resource;
        }
        if (queryString != null) {
            s += "?" + queryString;
        }
        HttpURLConnection conn = (HttpURLConnection) new URL(s).openConnection();
        conn.setRequestProperty("content-type", contentType);
        conn.setRequestMethod(method);
        conn.connect();
        return conn;
    }

    private String invokeAndGetResponse(String method, String resource, String queryString, String contentType)
            throws Exception {
        String s = container.getServletURL("/dummy");
        if (resource != null) {
            s += resource;
        }
        if (queryString != null) {
            s += "?" + queryString;
        }
        HttpURLConnection conn = (HttpURLConnection) new URL(s).openConnection();
        conn.setRequestProperty("content-type", contentType);
        conn.setRequestMethod(method);
        conn.connect();
        StringBuilder sb = new StringBuilder();
        BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String line = reader.readLine();
        while (line != null) {
            sb.append(line);
            line = reader.readLine();
        }
        return sb.toString();
    }

    private void runTest(JsonRestServlet.ResourceInfo[] resourceInfo, Callable<Void> assertions) throws Exception {

        container = new EmbeddedServletContainer("test");

        Services services = new Services();
        try {
            services.init();
            MyJsonRestServlet.ACTIVE = resourceInfo;
            container.addServletEndpoint("/dummy/*", MyJsonRestServlet.class);
            container.start();
            assertions.call();
        }
        finally {
            container.stop();
            services.destroy();
        }
    }

    public void testEmptyResources() {
        try {
            MyJsonRestServlet.ACTIVE = MyJsonRestServlet.EMPTY;
            new MyJsonRestServlet();
            fail();
        }
        catch (IllegalArgumentException ex) {
            //nop
        }
    }

    public void testNoResourceNoParams() throws Exception {
        runTest(MyJsonRestServlet.NO_RESOURCE_NO_PARAMS, new Callable<Void>() {
            public Void call() throws Exception {
                assertEquals(HttpServletResponse.SC_OK, invoke("GET", null, null));
                assertEquals(HttpServletResponse.SC_OK, invoke("GET", "", null));
                assertEquals(HttpServletResponse.SC_OK, invoke("GET", "", "a=A"));
                assertEquals(HttpServletResponse.SC_OK, invoke("GET", "/", null));
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, invoke("GET", "/hello", null));
                return null;
            }
        });
    }

    public void testParamsRequired() throws Exception {
        runTest(MyJsonRestServlet.PARAMS_REQUIRED, new Callable<Void>() {
            public Void call() throws Exception {
                assertEquals(HttpServletResponse.SC_OK, invoke("GET", "", "required=true"));
                assertEquals(HttpServletResponse.SC_OK, invoke("GET", "", "required=true&optional=true"));
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, invoke("GET", "", "optional=true"));
                return null;
            }
        });
    }

    public void testParamTypes() throws Exception {
        runTest(MyJsonRestServlet.PARAM_TYPES, new Callable<Void>() {
            public Void call() throws Exception {
                assertEquals(HttpServletResponse.SC_OK, invoke("GET", "", "boolean=true"));
                assertEquals(HttpServletResponse.SC_OK, invoke("GET", "", "boolean=false"));
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, invoke("GET", "", "boolean=x"));
                assertEquals(HttpServletResponse.SC_OK, invoke("GET", "", "integer=1"));
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, invoke("GET", "", "integer=x"));
                assertEquals(HttpServletResponse.SC_OK, invoke("GET", "", "string=a"));
                return null;
            }
        });
    }

    public void testResourceGetPostParamGet() throws Exception {
        runTest(MyJsonRestServlet.RESOURCE_GET_POST_PARAM_GET, new Callable<Void>() {
            public Void call() throws Exception {
                assertEquals(HttpServletResponse.SC_OK, invoke("GET", "", "param=true"));
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, invoke("POST", "", "param=true"));
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, invoke("GET", "", ""));
                assertEquals(HttpServletResponse.SC_OK, invoke("POST", "", ""));
                return null;
            }
        });
    }

    public void testFixedResource() throws Exception {
        runTest(MyJsonRestServlet.FIXED_RESOURCE, new Callable<Void>() {
            public Void call() throws Exception {
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, invoke("GET", "", ""));
                assertEquals(HttpServletResponse.SC_OK, invoke("GET", "/resource", ""));
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, invoke("POST", "/resource", ""));
                return null;
            }
        });
    }

    public void testWildCardResource() throws Exception {
        runTest(MyJsonRestServlet.WILDCARD_RESOURCE, new Callable<Void>() {
            public Void call() throws Exception {
                assertEquals(HttpServletResponse.SC_OK, invoke("GET", "/any", ""));
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, invoke("POST", "/any", ""));
                return null;
            }
        });
    }

    public void testInvalidResource() throws Exception {
        runTest(MyJsonRestServlet.WILDCARD_RESOURCE, new Callable<Void>() {
            public Void call() throws Exception {
                HttpURLConnection conn = invoke("GET", "/any/any", "dummy", "dummy");
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
                assertEquals("E0301: Invalid resource [any/any]", conn.getResponseMessage());
                checkErrorResponse(conn, HttpServletResponse.SC_BAD_REQUEST, "E0301: Invalid resource [any/any]");
                return null;
            }
        });
    }

    public void testNoResourceWithParam() throws Exception {
        runTest(MyJsonRestServlet.WILDCARD_RESOURCE, new Callable<Void>() {
            public Void call() throws Exception {
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, invoke("PUT", "/", "action=kill"));
                return null;
            }
        });
    }

    public void testMultipleResources() throws Exception {
        runTest(MyJsonRestServlet.MULTIPLE_RESOURCES, new Callable<Void>() {
            public Void call() throws Exception {
                assertEquals(HttpServletResponse.SC_OK, invoke("GET", "/resource1", ""));
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, invoke("POST", "/resource1", ""));
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, invoke("GET", "/resource2", ""));
                assertEquals(HttpServletResponse.SC_OK, invoke("POST", "/resource2", ""));
                return null;
            }
        });
    }

    public void testMultipleResourcesNoResource() throws Exception {
        runTest(MyJsonRestServlet.MULTIPLE_RESOURCES_NO_RESOURCE, new Callable<Void>() {
            public Void call() throws Exception {
                assertEquals(HttpServletResponse.SC_OK, invoke("GET", "/resource1", ""));
                assertEquals(HttpServletResponse.SC_OK, invoke("GET", "/resource2", ""));
                assertEquals(HttpServletResponse.SC_OK, invoke("POST", "", ""));
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, invoke("POST", "/resource1", ""));
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, invoke("POST", "/resource2", ""));
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, invoke("GET", "", ""));
                return null;
            }
        });
    }

    public void testMultipleResourcesWildCard() throws Exception {
        runTest(MyJsonRestServlet.MULTIPLE_RESOURCES_WILDCARD, new Callable<Void>() {
            public Void call() throws Exception {
                assertEquals(HttpServletResponse.SC_OK, invoke("GET", "/resource1", ""));
                assertEquals(HttpServletResponse.SC_OK, invoke("GET", "/resource2", ""));
                assertEquals(HttpServletResponse.SC_OK, invoke("POST", "/any", ""));
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, invoke("POST", "/resource1", ""));
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, invoke("POST", "/resource2", ""));
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, invoke("GET", "/any", ""));
                return null;
            }
        });
    }

    public void testContentTypeJsonCron() throws Exception {
        runTest(MyJsonRestServlet.CONTENT_TYPE_JSON_CRON_TEST, new Callable<Void>() {
            public Void call() throws Exception {
                assertEquals(HttpServletResponse.SC_OK, invoke("GET", "", "json=object", "application/xml").getResponseCode());
                assertEquals(HttpServletResponse.SC_OK, invoke("GET", "", "json=object", "application/xml; param=x")
                        .getResponseCode());
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, invoke("GET", "", "json=object", "").getResponseCode());
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, invoke("GET", "", "json=object", "application/json")
                        .getResponseCode());
                String response = invokeAndGetResponse("GET", "", "json=object", "application/xml");
                assertTrue(response.contains("object"));
                response = invokeAndGetResponse("GET", "", "json=array", "application/xml");
                assertTrue(response.contains("array"));
                return null;
            }
        });
    }

    private void checkErrorResponse(HttpURLConnection conn, int responseCode, String responseMessage) throws JSONException,
            IOException {
        JSONObject json = new JSONObject(IOUtils.toString(conn.getErrorStream()).trim());
        assertEquals("Error message is different.", responseMessage,
                json.getString(JsonTags.WORKFLOW_ACTION_ERROR_MESSAGE));
        assertEquals("Error code is different", responseCode,
                json.getInt(JsonTags.HTTP_STATUS_CODE));
    }

}
