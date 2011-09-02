/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.workflow.WorkflowApp;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.lite.LiteWorkflowApp;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.ErrorCode;

import java.io.FileWriter;
import java.io.Reader;
import java.io.Writer;

public class TestLiteWorkflowAppService extends XTestCase {

    public static class TestActionExecutor extends ActionExecutor {
        protected TestActionExecutor() {
            super("test");
        }

        public void start(Context context, WorkflowAction action) throws ActionExecutorException {
        }

        public void end(Context context, WorkflowAction action) throws ActionExecutorException {
        }

        public void check(Context context, WorkflowAction action) throws ActionExecutorException {
        }

        public void kill(Context context, WorkflowAction action) throws ActionExecutorException {
        }

        public boolean isCompleted(String externalStatus) {
            return true;
        }
    }

    public void testService() throws Exception {
        Services services = new Services();
        try {
            services.init();
            assertNotNull(services.get(WorkflowAppService.class));
        }
        finally {
            services.destroy();
        }
    }

    public void testReadDefinition() throws Exception {
        Services services = new Services();
        try {
            services.init();

            Reader reader = IOUtils.getResourceAsReader("wf-schema-valid.xml", -1);
            Writer writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
            IOUtils.copyCharStream(reader, writer);

            WorkflowAppService wps = services.get(WorkflowAppService.class);
            String wfDef = wps.readDefinition("file://" + getTestCaseDir(), getTestUser(), "group",
                                              "authToken");
            assertNotNull(reader.toString(), wfDef);
        }
        finally {
            services.destroy();
        }
    }

    public void testNoAppPath() throws Exception {
        Services services = new Services();
        services.init();
        WorkflowAppService wps = services.get(WorkflowAppService.class);
        try {
            assertNotNull(wps.parseDef(new XConfiguration(), "authToken"));
            fail();
        }
        catch (Exception ex) {
            //nop
        }
        services.destroy();
    }

    public void testSchema() throws Exception {
        Services services = new Services();
        try {
            services.init();

            Reader reader = IOUtils.getResourceAsReader("wf-schema-valid.xml", -1);
            Writer writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
            IOUtils.copyCharStream(reader, writer);

            WorkflowAppService wps = services.get(WorkflowAppService.class);

            Configuration jobConf = new XConfiguration();
            jobConf.set(OozieClient.APP_PATH, "file://" + getTestCaseDir());
            jobConf.set(OozieClient.USER_NAME, getTestUser());
            jobConf.set(OozieClient.GROUP_NAME, "group");

            WorkflowApp app = wps.parseDef(jobConf, "authToken");
            assertNotNull(app);
            assertEquals("test-wf", app.getName());

            reader = IOUtils.getResourceAsReader("wf-schema-invalid.xml", -1);
            writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
            IOUtils.copyCharStream(reader, writer);

            try {
                wps.parseDef(jobConf, "authToken");
                fail();
            }
            catch (WorkflowException ex) {
                //nop
            }
        }
        finally {
            services.destroy();
        }
    }


    public void testExtSchema() throws Exception {
        setSystemProperty(SchemaService.WF_CONF_EXT_SCHEMAS, "wf-ext-schema.xsd");
        setSystemProperty("oozie.service.ActionService.executor.ext.classes", TestActionExecutor.class.getName());
        Services services = new Services();
        try {
            services.init();

            Reader reader = IOUtils.getResourceAsReader("wf-ext-schema-valid.xml", -1);
            Writer writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
            IOUtils.copyCharStream(reader, writer);

            WorkflowAppService wps = services.get(WorkflowAppService.class);

            Configuration jobConf = new XConfiguration();
            jobConf.set(OozieClient.APP_PATH, "file://" + getTestCaseDir());
            jobConf.set(OozieClient.USER_NAME, getTestUser());
            jobConf.set(OozieClient.GROUP_NAME, "group");

            LiteWorkflowApp app = (LiteWorkflowApp) wps.parseDef(jobConf, "authToken");
            assertNotNull(app);
            assertEquals("test-wf", app.getName());

            reader = IOUtils.getResourceAsReader("wf-ext-schema-invalid.xml", -1);
            writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
            IOUtils.copyCharStream(reader, writer);

            try {
                wps.parseDef(jobConf, "authToken");
                fail();
            }
            catch (WorkflowException ex) {
                //nop
            }
        }
        finally {
            services.destroy();
        }
    }

    public void testActionNameLength() throws Exception {
        setSystemProperty("oozie.service.ActionService.executor.ext.classes", TestActionExecutor.class.getName());
        Services services = new Services();
        try {
            services.init();

            Reader reader = IOUtils.getResourceAsReader("wf-schema-action-name-too-long.xml", -1);
            Writer writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
            IOUtils.copyCharStream(reader, writer);

            WorkflowAppService wps = services.get(WorkflowAppService.class);

            Configuration jobConf = new XConfiguration();
            jobConf.set(OozieClient.APP_PATH, "file://" + getTestCaseDir());
            jobConf.set(OozieClient.USER_NAME, getTestUser());
            jobConf.set(OozieClient.GROUP_NAME, "group");

            try {
                LiteWorkflowApp app = (LiteWorkflowApp) wps.parseDef(jobConf, "authToken");
                fail();
            }
            catch (WorkflowException ex) {
                assertEquals(ErrorCode.E0724, ex.getErrorCode());
                //nop
            }
        }
        finally {
            services.destroy();
        }
    }

    public void testParsing() throws Exception {
        Services services = new Services();
        try {
            services.init();
            WorkflowAppService wps = services.get(WorkflowAppService.class);

            Reader reader = IOUtils.getResourceAsReader("wf-schema-valid.xml", -1);
            Writer writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
            IOUtils.copyCharStream(reader, writer);

            Configuration jobConf = new XConfiguration();
            jobConf.set(OozieClient.APP_PATH, "file://" + getTestCaseDir());
            jobConf.set(OozieClient.USER_NAME, getTestUser());
            jobConf.set(OozieClient.GROUP_NAME, "group");

            LiteWorkflowApp app = (LiteWorkflowApp) wps.parseDef(jobConf, "authToken");
            assertNotNull(app);
            assertEquals("test-wf", app.getName());
            assertNotNull(app.getNode("::start::"));
            assertEquals("a", app.getNode("::start::").getTransitions().get(0));
            assertEquals("b", app.getNode("a").getTransitions().get(0));
            assertEquals("c", app.getNode("a").getTransitions().get(1));
            assertEquals("d", app.getNode("a").getTransitions().get(2));
            assertTrue(app.getNode("b").getConf().contains("kill"));
            assertEquals("d", app.getNode("c").getTransitions().get(0));
            assertEquals("e", app.getNode("c").getTransitions().get(1));
            assertEquals(2, app.getNode("c").getTransitions().size());

            assertEquals("e", app.getNode("d").getTransitions().get(0));
            assertEquals("b", app.getNode("d").getTransitions().get(1));
            assertTrue(app.getNode("d").getConf().startsWith("<map-reduce"));

            assertEquals("z", app.getNode("e").getTransitions().get(0));
            assertEquals("b", app.getNode("e").getTransitions().get(1));
            assertTrue(app.getNode("e").getConf().startsWith("<pig"));

            assertEquals("g", app.getNode("f").getTransitions().get(0));

            assertNotNull(app.getNode("z"));
        }
        finally {
            services.destroy();
        }
    }

    public void testCreateprotoConf() throws Exception {
        Reader reader = IOUtils.getResourceAsReader("wf-schema-valid.xml", -1);
        Writer writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
        IOUtils.copyCharStream(reader, writer);

        createTestCaseSubDir("lib");
        writer = new FileWriter(getTestCaseDir() + "/lib/maputil.jar");
        writer.write("bla bla");
        writer.close();
        writer = new FileWriter(getTestCaseDir() + "/lib/reduceutil.so");
        writer.write("bla bla");
        writer.close();
        createTestCaseSubDir("scripts");
        writer = new FileWriter(getTestCaseDir() + "/scripts/myscript.sh");
        writer.write("bla bla");
        writer.close();
        Services services = new Services();
        services.init();
        WorkflowAppService wps = services.get(WorkflowAppService.class);
        Configuration jobConf = new XConfiguration();
        jobConf.set(OozieClient.APP_PATH, "file://" + getTestCaseDir());
        jobConf.set(OozieClient.USER_NAME, getTestUser());
        jobConf.set(OozieClient.GROUP_NAME, getTestGroup());
        injectKerberosInfo(jobConf);
        Configuration protoConf = wps.createProtoActionConf(jobConf, "authToken");
        assertEquals(getTestUser(), protoConf.get(OozieClient.USER_NAME));
        assertEquals(getTestGroup(), protoConf.get(OozieClient.GROUP_NAME));
        assertEquals(getTestCaseDir() + "/lib/maputil.jar", protoConf
                .getStrings(WorkflowAppService.APP_LIB_JAR_PATH_LIST)[0]);
        assertEquals(1, protoConf.getStrings(WorkflowAppService.APP_LIB_JAR_PATH_LIST).length);
        assertEquals(getTestCaseDir() + "/lib/reduceutil.so", protoConf
                .getStrings(WorkflowAppService.APP_LIB_SO_PATH_LIST)[0]);
        assertEquals(1, protoConf.getStrings(WorkflowAppService.APP_LIB_SO_PATH_LIST).length);
    }
}
