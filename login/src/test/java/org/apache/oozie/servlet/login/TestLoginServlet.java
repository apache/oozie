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
package org.apache.oozie.servlet.login;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletResponse;
import junit.framework.TestCase;
import org.apache.oozie.test.EmbeddedServletContainer;

public class TestLoginServlet extends TestCase {

    protected EmbeddedServletContainer container;
    protected static String loginPageTemplate;

    static {
        try {
            StringBuilder sb = new StringBuilder();
            InputStream is = new FileInputStream(new File("src/main/resources/login-page-template.html"));
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String line = br.readLine();
            while (line != null) {
                sb.append(line).append("\n");
                line = br.readLine();
            }
            br.close();
            loginPageTemplate = sb.toString();
        } catch (IOException ex) {
            ex.printStackTrace();
            fail("Unable to read login-page-template.html");
        }
    }

    protected Class getServletClass() {
        return LoginServlet.class;
    }

    protected Map<String, String> getInitParameters() {
        return null;
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        container = new EmbeddedServletContainer("oozie-login");
        container.addServletEndpoint("/", getServletClass(), getInitParameters());
        container.start();
    }

    @Override
    protected void tearDown() throws Exception {
        if (container != null) {
            container.stop();
        }
        super.tearDown();
    }

    public void testGetMissingBackurl() throws Exception {
        URL url = new URL(container.getServletURL("/"));
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
        assertEquals("missing or invalid 'backurl' parameter", conn.getResponseMessage());
    }

    public void testGetSuccess() throws Exception {
        URL url = new URL(container.getServletURL("/") + "?backurl=http://foo:11000/oozie");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
        String html = getHTML(conn);
        assertEquals(MessageFormat.format(loginPageTemplate, "", "", "http://foo:11000/oozie"), html);

        // With optional username parameter
        url = new URL(container.getServletURL("/") + "?backurl=http://foo:11000/oozie&username=foo");
        conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
        html = getHTML(conn);
        assertEquals(MessageFormat.format(loginPageTemplate, "", "foo", "http://foo:11000/oozie"), html);
    }

    public void testPostMissingBackurl() throws Exception {
        // Missing all
        URL url = new URL(container.getServletURL("/"));
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
        assertEquals("missing or invalid 'backurl' parameter", conn.getResponseMessage());

        // Missing only backurl
        url = new URL(container.getServletURL("/") + "?username=foo&password=bar");
        conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
        assertEquals("missing or invalid 'backurl' parameter", conn.getResponseMessage());
    }

    public void testPostMissingUsernamePassword() throws Exception {
        // Missing password
        URL url = new URL(container.getServletURL("/") + "?backurl=http://foo:11000/oozie&username=foo");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
        String html = getHTML(conn);
        assertEquals(MessageFormat.format(loginPageTemplate,
                "<font color=\"red\">Error: Invalid Username or Password</font><br>", "foo", "http://foo:11000/oozie"), html);

        // Missing username
        url = new URL(container.getServletURL("/") + "?backurl=http://foo:11000/oozie&password=bar");
        conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
        html = getHTML(conn);
        assertEquals(MessageFormat.format(loginPageTemplate,
                "<font color=\"red\">Error: Invalid Username or Password</font><br>", "", "http://foo:11000/oozie"), html);

        // Missing both
        url = new URL(container.getServletURL("/") + "?backurl=http://foo:11000/oozie");
        conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
        html = getHTML(conn);
        assertEquals(MessageFormat.format(loginPageTemplate,
                "<font color=\"red\">Error: Invalid Username or Password</font><br>", "", "http://foo:11000/oozie"), html);
    }

    public void testPostInvalidUsernamePassword() throws Exception {
        URL url = new URL(container.getServletURL("/") + "?backurl=http://foo:11000/oozie&username=foo&password=bar");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
        String html = getHTML(conn);
        assertEquals(MessageFormat.format(loginPageTemplate,
                "<font color=\"red\">Error: Invalid Username or Password</font><br>", "foo", "http://foo:11000/oozie"), html);
    }

    public void testPostSuccess() throws Exception {
        // Now that its actually going to work successfully, the backurl needs to go somewhere real; about:blank provides a
        // convinient location that doesn't require internet access or another servlet running locally
        URL url = new URL(container.getServletURL("/") + "?backurl=about:blank&username=foo&password=foo");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        assertEquals(HttpServletResponse.SC_FOUND, conn.getResponseCode());
        String cookies = getCookies(conn);
        String username = getUsernameFromCookies(cookies);
        assertEquals("foo", username);
    }

    protected String getHTML(HttpURLConnection conn) throws Exception {
        BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String line;
        StringBuilder htmlBuilder = new StringBuilder();
        while ((line = br.readLine()) != null) {
            htmlBuilder.append(line);
            htmlBuilder.append("\n");
        }
        br.close();
        return htmlBuilder.toString();
    }

    protected String getCookies(HttpURLConnection conn) throws Exception {
        Map<String, List<String>> headers = conn.getHeaderFields();
        for (String key : headers.keySet()) {
            if (key != null && key.equals("Set-Cookie")) {
                List<String> cookies = headers.get(key);
                return cookies.get(0);
            }
        }
        return null;
    }

    protected String getUsernameFromCookies(String cookies) throws UnsupportedEncodingException {
        String[] cookiesSplit = cookies.split(";");
        for (String split : cookiesSplit) {
            if (split.startsWith("oozie.web.login.auth=")) {
                String value = split.substring("oozie.web.login.auth=".length());
                if (value.startsWith("\"") && value.endsWith("\"")) {
                    value = value.substring(1, value.length() - 1);
                }
                return URLDecoder.decode(value, "UTF-8");
            }
        }
        return null;
    }
}
