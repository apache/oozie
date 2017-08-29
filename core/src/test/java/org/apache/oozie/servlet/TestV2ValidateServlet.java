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

import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.RestConstants;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class TestV2ValidateServlet extends DagServletTestCase {

    static {
        new V2ValidateServlet();
    }

    private static final boolean IS_SECURITY_ENABLED = false;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    public void testValidateWF() throws Exception {
        runTest("/v2/validate", V2ValidateServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {

                Map<String, String> params = new HashMap<String, String>();
                params.put("file", "workflow.xml");
                params.put("user", getTestUser());
                URL url = createURL("", params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                        "<workflow-app xmlns=\"uri:oozie:workflow:0.3\" name=\"test\">\n" +
                        "    <start to=\"shell-1\"/>\n" +
                        "    <action name=\"shell-1\">\n" +
                        "        <shell xmlns=\"uri:oozie:shell-action:0.3\">\n" +
                        "            <job-tracker>${jobTracker}</job-tracker>\n" +
                        "            <name-node>${nameNode}</name-node>\n" +
                        "            <exec>script-outstream.sh</exec>\n" +
                        "            <argument></argument>\n" +
                        "            <file>script-outstream.sh</file>\n" +
                        "            <capture-output/>\n" +
                        "        </shell>\n" +
                        "        <ok to=\"end\"/>\n" +
                        "        <error to=\"fail\"/>\n" +
                        "    </action>\n" +
                        "    <kill name=\"fail\">\n" +
                        "        <message>failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>\n" +
                        "    </kill>\n" +
                        "    <end name=\"end\"/>\n" +
                        "</workflow-app>";
                writeXML(conn.getOutputStream(), xml);

                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                JSONObject obj = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                assertEquals("Valid workflow-app", obj.get(JsonTags.VALIDATE));

                return null;
            }
        });
    }

    public void testValidateWFonHDFS() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                "<workflow-app xmlns=\"uri:oozie:workflow:0.3\" name=\"test\">\n" +
                "    <start to=\"shell-1\"/>\n" +
                "    <action name=\"shell-1\">\n" +
                "        <shell xmlns=\"uri:oozie:shell-action:0.3\">\n" +
                "            <job-tracker>${jobTracker}</job-tracker>\n" +
                "            <name-node>${nameNode}</name-node>\n" +
                "            <exec>script-outstream.sh</exec>\n" +
                "            <argument></argument>\n" +
                "            <file>script-outstream.sh</file>\n" +
                "            <capture-output/>\n" +
                "        </shell>\n" +
                "        <ok to=\"end\"/>\n" +
                "        <error to=\"fail\"/>\n" +
                "    </action>\n" +
                "    <kill name=\"fail\">\n" +
                "        <message>failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>\n" +
                "    </kill>\n" +
                "    <end name=\"end\"/>\n" +
                "</workflow-app>";
        final Path path = new Path(getFsTestCaseDir(), "workflow.xml");
        OutputStreamWriter writer = new OutputStreamWriter(getFileSystem().create(path));
        writer.write(xml.toCharArray());
        writer.flush();
        writer.close();

        runTest("/v2/validate", V2ValidateServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {

                Map<String, String> params = new HashMap<String, String>();
                params.put("file", path.toString());
                params.put("user", getTestUser());
                URL url = createURL("", params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);

                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                JSONObject obj = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                assertEquals("Valid workflow-app", obj.get(JsonTags.VALIDATE));

                return null;
            }
        });
    }

    public void testValidateWFNegative() throws Exception {
        runTest("/v2/validate", V2ValidateServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {

                Map<String, String> params = new HashMap<String, String>();
                params.put("file", "workflow.xml");
                params.put("user", getTestUser());
                URL url = createURL("", params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                String xml = "<workflow-app xmlns=\"uri:oozie:workflow:0.3\" name=\"test\">\n" +
                        "    <start to=\"shell-1\"/>\n" +
                        "    <action name=\"shell-1\">\n" +
                        "        <shell xmlns=\"uri:oozie:shell-action:0.3\">\n" +
                        "            <name-node2>${nameNode}</name-node2>\n" +
                        "            <exec>script-outstream.sh</exec>\n" +
                        "            <argument></argument>\n" +
                        "            <file>script-outstream.sh</file>\n" +
                        "            <capture-output/>\n" +
                        "        </shell>\n" +
                        "        <ok to=\"end\"/>\n" +
                        "        <error to=\"fail\"/>\n" +
                        "    </action>\n" +
                        "    <kill name=\"fail\">\n" +
                        "        <message>failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>\n" +
                        "    </kill>\n" +
                        "    <end name=\"end\"/>\n" +
                        "</workflow-app>";
                writeXML(conn.getOutputStream(), xml);

                assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
                String error = conn.getHeaderField(RestConstants.OOZIE_ERROR_CODE);
                String message = conn.getHeaderField(RestConstants.OOZIE_ERROR_MESSAGE);
                assertEquals("E0701", error);
                assertEquals(true, message.contains("Invalid content was found starting with element 'name-node2'"));
                return null;
            }
        });
    }

    public void testValidateWFNegative2() throws Exception {
        runTest("/v2/validate", V2ValidateServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {

                Map<String, String> params = new HashMap<String, String>();
                params.put("file", "workflow.xml");
                params.put("user", getTestUser());
                URL url = createURL("", params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                String xml = "<workflow-app xmlns=\"uri:oozie:workflow:0.3\" name=\"test\">\n" +
                        "    <start to=\"shell-1\"/>\n" +
                        "    <action name=\"shell-1\">\n" +
                        "        <shell xmlns=\"uri:oozie:shell-action:0.3\">\n" +
                        "            <name-node>${nameNode}</name-node>\n" +
                        "            <exec>script-outstream.sh</exec>\n" +
                        "            <argument></argument>\n" +
                        "            <file>script-outstream.sh</file>\n" +
                        "            <capture-output/>\n" +
                        "        </shell>\n" +
                        "        <ok to=\"end\"/>\n" +
                        "        <error to=\"fail\"/>\n" +
                        "    </action>\n" +
                        "    <kill-invalid name=\"fail\">\n" +
                        "        <message>failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>\n" +
                        "    </kill-invalid>\n" +
                        "    <end name=\"end\"/>\n" +
                        "</workflow-app>";
                writeXML(conn.getOutputStream(), xml);

                assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
                String error = conn.getHeaderField(RestConstants.OOZIE_ERROR_CODE);
                String message = conn.getHeaderField(RestConstants.OOZIE_ERROR_MESSAGE);
                assertEquals("E0701", error);
                assertEquals(true, message.contains("Invalid content was found starting with element 'kill-invalid'"));
                return null;
            }
        });
    }

    public void testValidateWFNegative3() throws Exception {
        runTest("/v2/validate", V2ValidateServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {

                Map<String, String> params = new HashMap<String, String>();
                params.put("file", "workflow.xml");
                params.put("user", getTestUser());
                URL url = createURL("", params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                String xml = "<workflow-app-invalid xmlns=\"uri:oozie:workflow:0.3\" name=\"test\">\n" +
                        "    <start to=\"shell-1\"/>\n" +
                        "    <action name=\"shell-1\">\n" +
                        "        <shell xmlns=\"uri:oozie:shell-action:0.3\">\n" +
                        "            <name-node>${nameNode}</name-node>\n" +
                        "            <exec>script-outstream.sh</exec>\n" +
                        "            <argument></argument>\n" +
                        "            <file>script-outstream.sh</file>\n" +
                        "            <capture-output/>\n" +
                        "        </shell>\n" +
                        "        <ok to=\"end\"/>\n" +
                        "        <error to=\"fail\"/>\n" +
                        "    </action>\n" +
                        "    <kill name=\"fail\">\n" +
                        "        <message>failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>\n" +
                        "    </kill>\n" +
                        "    <end name=\"end\"/>\n" +
                        "</workflow-app-invalid>";
                writeXML(conn.getOutputStream(), xml);

                assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
                String error = conn.getHeaderField(RestConstants.OOZIE_ERROR_CODE);
                String message = conn.getHeaderField(RestConstants.OOZIE_ERROR_MESSAGE);
                assertEquals("E0701", error);
                assertEquals(true, message.contains("Cannot find the declaration of element 'workflow-app-invalid"));
                return null;
            }
        });
    }

    public void testValidateWFNegative4() throws Exception {
        runTest("/v2/validate", V2ValidateServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {

                Map<String, String> params = new HashMap<String, String>();
                params.put("file", "workflow.xml");
                params.put("user", getTestUser());
                URL url = createURL("", params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                String xml = "<workflow-app xmlns=\"uri:oozie:workflow:0.3\" name=\"test\">\n" +
                        "    <start to=\"shell-1\"/>\n" +
                        "    <start to=\"shell-1\"/>\n" +
                        "    <action name=\"shell-1\">\n" +
                        "        <shell xmlns=\"uri:oozie:shell-action:0.3\">\n" +
                        "            <name-node>${nameNode}</name-node>\n" +
                        "            <exec>script-outstream.sh</exec>\n" +
                        "            <argument></argument>\n" +
                        "            <file>script-outstream.sh</file>\n" +
                        "            <capture-output/>\n" +
                        "        </shell>\n" +
                        "        <ok to=\"end\"/>\n" +
                        "        <error to=\"fail\"/>\n" +
                        "    </action>\n" +
                        "    <kill name=\"fail\">\n" +
                        "        <message>failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>\n" +
                        "    </kill>\n" +
                        "    <end name=\"end\"/>\n" +
                        "</workflow-app>";
                writeXML(conn.getOutputStream(), xml);

                assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
                String error = conn.getHeaderField(RestConstants.OOZIE_ERROR_CODE);
                String message = conn.getHeaderField(RestConstants.OOZIE_ERROR_MESSAGE);
                assertEquals("E0701", error);
                assertEquals(true, message.contains("cvc-complex-type.2.4.a: " +
                        "Invalid content was found starting with element 'start'"));
                return null;
            }
        });
    }

    public void testValidateWFonHDFSNegative() throws Exception {
        String xml = "<workflow-app xmlns=\"uri:oozie:workflow:0.3\" name=\"test\">\n" +
                "    <start to=\"shell-1\"/>\n" +
                "    <action name=\"shell-1\">\n" +
                "        <shell xmlns=\"uri:oozie:shell-action:0.3\">\n" +
                "            <name-node2>${nameNode}</name-node2>\n" +
                "            <exec>script-outstream.sh</exec>\n" +
                "            <argument></argument>\n" +
                "            <file>script-outstream.sh</file>\n" +
                "            <capture-output/>\n" +
                "        </shell>\n" +
                "        <ok to=\"end\"/>\n" +
                "        <error to=\"fail\"/>\n" +
                "    </action>\n" +
                "    <kill name=\"fail\">\n" +
                "        <message>failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>\n" +
                "    </kill>\n" +
                "    <end name=\"end\"/>\n" +
                "</workflow-app>";
        final Path path = new Path(getFsTestCaseDir(), "workflow.xml");
        OutputStreamWriter writer = new OutputStreamWriter(getFileSystem().create(path));
        writer.write(xml.toCharArray());
        writer.flush();
        writer.close();

        runTest("/v2/validate", V2ValidateServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {

                Map<String, String> params = new HashMap<String, String>();
                params.put("file", path.toString());
                params.put("user", getTestUser());
                URL url = createURL("", params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);

                assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
                String error = conn.getHeaderField(RestConstants.OOZIE_ERROR_CODE);
                String message = conn.getHeaderField(RestConstants.OOZIE_ERROR_MESSAGE);
                assertEquals("E0701", error);
                assertEquals(true, message.contains("Invalid content was found starting with element 'name-node2'"));
                return null;
            }
        });
    }

    public void testValidateCoordinator() throws Exception {
        runTest("/v2/validate", V2ValidateServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {

                Map<String, String> params = new HashMap<String, String>();
                params.put("file", "coordinator.xml");
                params.put("user", getTestUser());
                URL url = createURL("", params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                String xml = "<coordinator-app name=\"coord-simple\" frequency=\"${coord:minutes(1)}\"\n" +
                        "                 start=\"${startTime}\" end=\"${endTime}\"\n" +
                        "                 timezone=\"Asia/Seoul\"\n" +
                        "                 xmlns=\"uri:oozie:coordinator:0.1\">\n" +
                        "    <action>\n" +
                        "        <workflow>\n" +
                        "            <app-path>${nameNode}/user/seoeun/workflow-ndap/apps/v40/shell-outstream</app-path>\n" +
                        "        </workflow>\n" +
                        "    </action>\n" +
                        "</coordinator-app>";
                writeXML(conn.getOutputStream(), xml);

                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                JSONObject obj = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                assertEquals("Valid workflow-app", obj.get(JsonTags.VALIDATE));

                return null;

            }
        });
    }

    public void testValidateCoordinatorNegative1() throws Exception {
        runTest("/v2/validate", V2ValidateServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {

                Map<String, String> params = new HashMap<String, String>();
                params.put("file", "coordinator.xml");
                params.put("user", getTestUser());
                URL url = createURL("", params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                String xml = "<coordinator-app name=\"coord-simple\" frequency=\"${coord:minutes(1)}\"\n" +
                        "                 start=\"${startTime}\" end=\"${endTime}\"\n" +
                        "                 timezone=\"Asia/Seoul\"\n" +
                        "                 xmlns=\"uri:oozie:coordinator:0.1\">\n" +
                        "    <action>\n" +
                        "        <workflow>\n" +
                        "            <app-path>${nameNode}/user/seoeun/workflow-ndap/apps/v40/shell-outstream</app-path>\n" +
                        "            <action name=\"shell-1\">\n" +
                        "                <shell xmlns=\"uri:oozie:shell-action:0.3\">\n" +
                        "                <job-tracker>${jobTracker}</job-tracker>\n" +
                        "                <name-node>${nameNode}</name-node>\n" +
                        "                <exec>script-outstream.sh</exec>\n" +
                        "                </shell>\n" +
                        "                <ok to=\"end\"/>\n" +
                        "                <error to=\"fail\"/>\n" +
                        "            </action>\n" +
                        "        </workflow>\n" +
                        "    </action>\n" +
                        "</coordinator-app>";
                writeXML(conn.getOutputStream(), xml);

                assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
                String error = conn.getHeaderField(RestConstants.OOZIE_ERROR_CODE);
                String message = conn.getHeaderField(RestConstants.OOZIE_ERROR_MESSAGE);
                assertEquals("E0701", error);
                assertEquals(true, message.contains("Invalid content was found starting with element 'action'"));
                return null;

            }
        });
    }

    public void testValidateCoordinatorNegative2() throws Exception {
        runTest("/v2/validate", V2ValidateServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {

                Map<String, String> params = new HashMap<String, String>();
                params.put("file", "coordinator.xml");
                params.put("user", getTestUser());
                URL url = createURL("", params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                String xml = "<coordinator-app-invalid name=\"coord-simple\" frequency=\"${coord:minutes(1)}\"\n" +
                        "                 start=\"${startTime}\" end=\"${endTime}\"\n" +
                        "                 timezone=\"Asia/Seoul\"\n" +
                        "                 xmlns=\"uri:oozie:coordinator:0.1\">\n" +
                        "    <action>\n" +
                        "        <workflow>\n" +
                        "            <app-path>${nameNode}/user/seoeun/workflow-ndap/apps/v40/shell-outstream</app-path>\n" +
                        "        </workflow>\n" +
                        "    </action>\n" +
                        "</coordinator-app-invalid>";
                writeXML(conn.getOutputStream(), xml);

                assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
                String error = conn.getHeaderField(RestConstants.OOZIE_ERROR_CODE);
                String message = conn.getHeaderField(RestConstants.OOZIE_ERROR_MESSAGE);
                assertEquals("E0701", error);
                assertEquals(true, message.contains("Cannot find the declaration of element 'coordinator-app-invalid'"));
                return null;

            }
        });
    }

    public void testValidateBundle() throws Exception {
        runTest("/v2/validate", V2ValidateServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {

                Map<String, String> params = new HashMap<String, String>();
                params.put("file", "bundle.xml");
                params.put("user", getTestUser());
                URL url = createURL("", params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                String xml = "<bundle-app name='test_bundle' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' "
                        + "xmlns='uri:oozie:bundle:0.1'> "
                        + "<controls> <kick-off-time>2009-02-02T00:00Z</kick-off-time> </controls> "
                        + "<coordinator name='c12'> "
                        + "<app-path>#app_path1</app-path>"
                        + "<configuration> "
                        + "<property> <name>START_TIME</name> <value>2009-02-01T00:00Z</value> </property> </configuration> "
                        + "</coordinator></bundle-app>";
                writeXML(conn.getOutputStream(), xml);

                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                JSONObject obj = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                assertEquals("Valid workflow-app", obj.get(JsonTags.VALIDATE));

                return null;

            }
        });
    }

    public void testValidateBundleNegative1() throws Exception {
        runTest("/v2/validate", V2ValidateServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {

                Map<String, String> params = new HashMap<String, String>();
                params.put("file", "bundle.xml");
                params.put("user", getTestUser());
                URL url = createURL("", params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                String xml = "<bundle-app name='test_bundle' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' "
                        + "xmlns='uri:oozie:bundle:0.1'> "
                        + "<controls> <kick-off-time>2009-02-02T00:00Z</kick-off-time> </controls> "
                        + "</bundle-app>";
                writeXML(conn.getOutputStream(), xml);

                assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
                String error = conn.getHeaderField(RestConstants.OOZIE_ERROR_CODE);
                String message = conn.getHeaderField(RestConstants.OOZIE_ERROR_MESSAGE);
                assertEquals("E0701", error);
                assertEquals(true, message.contains("cvc-complex-type.2.4.b: The content of element 'bundle-app' is not " +
                        "complete. One of '{\"uri:oozie:bundle:0.1\":coordinator}' is expected"));
                return null;

            }
        });
    }

    public void testValidateBundleNegative2() throws Exception {
        runTest("/v2/validate", V2ValidateServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {

                Map<String, String> params = new HashMap<String, String>();
                params.put("file", "bundle.xml");
                params.put("user", getTestUser());
                URL url = createURL("", params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                String xml = "<bundle-app-invalid name='test_bundle' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' "
                        + "xmlns='uri:oozie:bundle:0.1'> "
                        + "<controls> <kick-off-time>2009-02-02T00:00Z</kick-off-time> </controls> "
                        + "<coordinator name='c12'> "
                        + "<app-path>#app_path1</app-path>"
                        + "<configuration> "
                        + "<property> <name>START_TIME</name> <value>2009-02-01T00:00Z</value> </property> </configuration> "
                        + "</coordinator></bundle-app-invalid>";
                writeXML(conn.getOutputStream(), xml);

                assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
                String error = conn.getHeaderField(RestConstants.OOZIE_ERROR_CODE);
                String message = conn.getHeaderField(RestConstants.OOZIE_ERROR_MESSAGE);
                assertEquals("E0701", error);
                assertEquals(true, message.matches("^.*cvc-elt.1(.a)?: " +
                        "Cannot find the declaration of element 'bundle-app-invalid'.*$"));
                return null;

            }
        });
    }

    public void testValidateSla() throws Exception {
        runTest("/v2/validate", V2ValidateServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {

                Map<String, String> params = new HashMap<String, String>();
                params.put("file", "workflow.xml");
                params.put("user", getTestUser());
                URL url = createURL("", params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                String xml = "<workflow-app xmlns=\"uri:oozie:workflow:0.5\" xmlns:sla=\"uri:oozie:sla:0.2\" name=\"test\">\n" +
                        "    <start to=\"shell-1\"/>\n" +
                        "    <action name=\"shell-1\">\n" +
                        "        <shell xmlns=\"uri:oozie:shell-action:0.3\">\n" +
                        "            <job-tracker>${jobTracker}</job-tracker>\n" +
                        "            <name-node>${nameNode}</name-node>\n" +
                        "            <exec>script-outstream.sh</exec>\n" +
                        "            <argument></argument>\n" +
                        "            <file>script-outstream.sh</file>\n" +
                        "            <capture-output/>\n" +
                        "        </shell>\n" +
                        "        <ok to=\"end\"/>\n" +
                        "        <error to=\"fail\"/>\n" +
                        "        <sla:info>\n" +
                        "            <sla:nominal-time>${nominal_time}</sla:nominal-time>\n" +
                        "            <sla:should-start>${10 * MINUTES}</sla:should-start>\n" +
                        "            <sla:should-end>${30 * MINUTES}</sla:should-end>\n" +
                        "            <sla:max-duration>${30 * MINUTES}</sla:max-duration>\n" +
                        "            <sla:alert-events>start_miss,end_met,end_miss</sla:alert-events>\n" +
                        "            <sla:alert-contact>joe@example.com</sla:alert-contact>\n" +
                        "        </sla:info>\n" +
                        "    </action>\n" +
                        "    <kill name=\"fail\">\n" +
                        "        <message>failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>\n" +
                        "    </kill>\n" +
                        "    <end name=\"end\"/>\n" +
                        "</workflow-app>";
                writeXML(conn.getOutputStream(), xml);

                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                JSONObject obj = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                assertEquals("Valid workflow-app", obj.get(JsonTags.VALIDATE));

                return null;

            }
        });
    }

    public void testValidateSlaNegative() throws Exception {
        runTest("/v2/validate", V2ValidateServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {

                Map<String, String> params = new HashMap<String, String>();
                params.put("file", "workflow.xml");
                params.put("user", getTestUser());
                URL url = createURL("", params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setDoOutput(true);
                String xml = "<workflow-app xmlns=\"uri:oozie:workflow:0.5\" xmlns:sla=\"uri:oozie:sla:0.2\" name=\"test\">\n" +
                        "    <start to=\"shell-1\"/>\n" +
                        "    <action name=\"shell-1\">\n" +
                        "        <shell xmlns=\"uri:oozie:shell-action:0.3\">\n" +
                        "            <job-tracker>${jobTracker}</job-tracker>\n" +
                        "            <name-node>${nameNode}</name-node>\n" +
                        "            <exec>script-outstream.sh</exec>\n" +
                        "            <argument></argument>\n" +
                        "            <file>script-outstream.sh</file>\n" +
                        "            <capture-output/>\n" +
                        "        </shell>\n" +
                        "        <ok to=\"end\"/>\n" +
                        "        <error to=\"fail\"/>\n" +
                        "        <sla:info>\n" +
                        "            <sla:app-name>${nominal_time}</sla:app-name>\n" +
                        "            <sla:nominal-time>${nominal_time}</sla:nominal-time>\n" +
                        "            <sla:should-start>${10 * MINUTES}</sla:should-start>\n" +
                        "            <sla:should-end>${30 * MINUTES}</sla:should-end>\n" +
                        "            <sla:max-duration>${30 * MINUTES}</sla:max-duration>\n" +
                        "            <sla:alert-events>start_miss,end_met,end_miss</sla:alert-events>\n" +
                        "            <sla:alert-contact>joe@example.com</sla:alert-contact>\n" +
                        "        </sla:info>\n" +
                        "    </action>\n" +
                        "    <kill name=\"fail\">\n" +
                        "        <message>failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>\n" +
                        "    </kill>\n" +
                        "    <end name=\"end\"/>\n" +
                        "</workflow-app>";
                writeXML(conn.getOutputStream(), xml);

                assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
                String error = conn.getHeaderField(RestConstants.OOZIE_ERROR_CODE);
                String message = conn.getHeaderField(RestConstants.OOZIE_ERROR_MESSAGE);
                assertEquals("E0701", error);
                assertEquals(true, message.contains("Invalid content was found starting with element 'sla:app-name'"));
                return null;

            }
        });
    }


    private void writeXML(OutputStream outputStream, String xml) throws IOException {
        outputStream.write(xml.getBytes());
    }

}
