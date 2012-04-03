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

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.workflow.WorkflowApp;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.lite.LiteWorkflowApp;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.ErrorCode;

import java.io.File;
import java.io.FileWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.framework.Assert;

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

            Configuration conf = new XConfiguration();

            WorkflowAppService wps = services.get(WorkflowAppService.class);
            String wfDef = wps.readDefinition("file://" + getTestCaseDir() + File.separator + "workflow.xml",
                                              getTestUser(), "authToken", conf);
            assertNotNull(reader.toString(), wfDef);
        }
        finally {
            services.destroy();
        }
    }

    /**
     * Making sure an exception is thrown when a WF exceeds the maximum length
     *
     * @throws Exception
     */
    public void testMaxWfDefinition() throws Exception {
        setSystemProperty(WorkflowAppService.CONFG_MAX_WF_LENGTH, "100");
        Services services = new Services();
        try {
            services.init();

            Reader reader = IOUtils.getResourceAsReader("wf-schema-valid.xml", -1);
            Writer writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
            IOUtils.copyCharStream(reader, writer);

            Configuration conf = new XConfiguration();

            WorkflowAppService wps = services.get(WorkflowAppService.class);
            wps.readDefinition("file://" + getTestCaseDir() + File.separator + "workflow.xml", getTestUser(),
                    "authToken", conf);
            fail("an exception should be thrown as the definition exceeds the given maximum");
        }
        catch (WorkflowException wfe) {
            assertEquals(wfe.getErrorCode(), ErrorCode.E0736);
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
            jobConf.set(OozieClient.APP_PATH, "file://" + getTestCaseDir() + File.separator + "workflow.xml");
            jobConf.set(OozieClient.USER_NAME, getTestUser());


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
            jobConf.set(OozieClient.APP_PATH, "file://" + getTestCaseDir() + File.separator + "workflow.xml");
            jobConf.set(OozieClient.USER_NAME, getTestUser());


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
            jobConf.set(OozieClient.APP_PATH, "file://" + getTestCaseDir() + File.separator + "workflow.xml");
            jobConf.set(OozieClient.USER_NAME, getTestUser());


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
            jobConf.set(OozieClient.APP_PATH, "file://" + getTestCaseDir() + File.separator + "workflow.xml");
            jobConf.set(OozieClient.USER_NAME, getTestUser());


            LiteWorkflowApp app = (LiteWorkflowApp) wps.parseDef(jobConf, "authToken");
            assertNotNull(app);
            assertEquals("test-wf", app.getName());
            assertNotNull(app.getNode("::start::"));
            assertEquals("a", app.getNode("::start::").getTransitions().get(0));
            assertEquals("b", app.getNode("a").getTransitions().get(0));
            assertEquals("c", app.getNode("a").getTransitions().get(1));
            assertEquals("c", app.getNode("a").getTransitions().get(2));
            assertTrue(app.getNode("b").getConf().contains("kill"));
            assertEquals("d", app.getNode("c").getTransitions().get(0));
            assertEquals("e", app.getNode("c").getTransitions().get(1));
            assertEquals(2, app.getNode("c").getTransitions().size());

            assertEquals("f", app.getNode("d").getTransitions().get(0));
            assertEquals("b", app.getNode("d").getTransitions().get(1));
            assertTrue(app.getNode("d").getConf().startsWith("<map-reduce"));

            assertEquals("f", app.getNode("e").getTransitions().get(0));
            assertEquals("b", app.getNode("e").getTransitions().get(1));
            assertTrue(app.getNode("e").getConf().startsWith("<pig"));

            assertEquals("z", app.getNode("f").getTransitions().get(0));

            assertNotNull(app.getNode("z"));
        }
        finally {
            services.destroy();
        }
    }

    public void testCreateprotoConf() throws Exception {
        Services services = new Services();
        try {
            services.init();
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
            WorkflowAppService wps = Services.get().get(WorkflowAppService.class);
            Configuration jobConf = new XConfiguration();
            jobConf.set(OozieClient.APP_PATH, "file://" + getTestCaseDir() + File.separator + "workflow.xml");
            jobConf.set(OozieClient.USER_NAME, getTestUser());

            Configuration protoConf = wps.createProtoActionConf(jobConf, "authToken", true);
            assertEquals(getTestUser(), protoConf.get(OozieClient.USER_NAME));

            assertEquals(2, protoConf.getStrings(WorkflowAppService.APP_LIB_PATH_LIST).length);
            String f1 = protoConf.getStrings(WorkflowAppService.APP_LIB_PATH_LIST)[0];
            String f2 = protoConf.getStrings(WorkflowAppService.APP_LIB_PATH_LIST)[1];
            String ref1 = getTestCaseDir() + "/lib/reduceutil.so";
            String ref2 = getTestCaseDir() + "/lib/maputil.jar";
            Assert.assertTrue(f1.equals(ref1) || f1.equals(ref2));
            Assert.assertTrue(f2.equals(ref1) || f2.equals(ref2));
            Assert.assertTrue(!f1.equals(f2));
        }
        finally {
            services.destroy();
        }
    }

    public void testCreateprotoConfWithLibPath() throws Exception {
        Services services = new Services();
        try {
            services.init();
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
            createTestCaseSubDir("libx");
            writer = new FileWriter(getTestCaseDir() + "/libx/maputilx.jar");
            writer.write("bla bla");
            writer.close();
            WorkflowAppService wps = Services.get().get(WorkflowAppService.class);
            Configuration jobConf = new XConfiguration();
            jobConf.set(OozieClient.APP_PATH, "file://" + getTestCaseDir() + "/workflow.xml");
            jobConf.set(OozieClient.LIBPATH, "file://" + getTestCaseDir() + "/libx");
            jobConf.set(OozieClient.USER_NAME, getTestUser());

            Configuration protoConf = wps.createProtoActionConf(jobConf, "authToken", true);
            assertEquals(getTestUser(), protoConf.get(OozieClient.USER_NAME));

            assertEquals(3, protoConf.getStrings(WorkflowAppService.APP_LIB_PATH_LIST).length);
            List<String> found = new ArrayList<String>();
            found.add(protoConf.getStrings(WorkflowAppService.APP_LIB_PATH_LIST)[0]);
            found.add(protoConf.getStrings(WorkflowAppService.APP_LIB_PATH_LIST)[1]);
            found.add(protoConf.getStrings(WorkflowAppService.APP_LIB_PATH_LIST)[2]);
            List<String> expected = new ArrayList<String>();
            expected.add(getTestCaseDir() + "/lib/reduceutil.so");
            expected.add(getTestCaseDir() + "/lib/maputil.jar");
            expected.add(getTestCaseDir() + "/libx/maputilx.jar");
            Collections.sort(found);
            Collections.sort(expected);
            assertEquals(expected, found);
        }
        finally {
            services.destroy();
        }
    }

    public void testCreateprotoConfWithMulipleLibPath() throws Exception {
        Services services = new Services();
        try {
            services.init();
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
            createTestCaseSubDir("libx");
            writer = new FileWriter(getTestCaseDir() + "/libx/maputil_x.jar");
            writer.write("bla bla");
            writer.close();
            createTestCaseSubDir("liby");
            writer = new FileWriter(getTestCaseDir() + "/liby/maputil_y1.jar");
            writer.write("bla bla");
            writer.close();
            writer = new FileWriter(getTestCaseDir() + "/liby/maputil_y2.jar");
            writer.write("bla bla");
            writer.close();
            createTestCaseSubDir("libz");
            writer = new FileWriter(getTestCaseDir() + "/libz/maputil_z.jar");
            writer.write("bla bla");
            writer.close();

            WorkflowAppService wps = Services.get().get(WorkflowAppService.class);
            Configuration jobConf = new XConfiguration();
            jobConf.set(OozieClient.APP_PATH, "file://" + getTestCaseDir() + "/workflow.xml");
            jobConf.setStrings(OozieClient.LIBPATH, "file://" + getTestCaseDir() + "/libx",
                    "file://" + getTestCaseDir() + "/liby", "file://" + getTestCaseDir() + "/libz");
            jobConf.set(OozieClient.USER_NAME, getTestUser());

            Configuration protoConf = wps.createProtoActionConf(jobConf, "authToken", true);
            assertEquals(getTestUser(), protoConf.get(OozieClient.USER_NAME));

            assertEquals(6, protoConf.getStrings(WorkflowAppService.APP_LIB_PATH_LIST).length);
            List<String> found = new ArrayList<String>();
            found.add(protoConf.getStrings(WorkflowAppService.APP_LIB_PATH_LIST)[0]);
            found.add(protoConf.getStrings(WorkflowAppService.APP_LIB_PATH_LIST)[1]);
            found.add(protoConf.getStrings(WorkflowAppService.APP_LIB_PATH_LIST)[2]);
            found.add(protoConf.getStrings(WorkflowAppService.APP_LIB_PATH_LIST)[3]);
            found.add(protoConf.getStrings(WorkflowAppService.APP_LIB_PATH_LIST)[4]);
            found.add(protoConf.getStrings(WorkflowAppService.APP_LIB_PATH_LIST)[5]);
            List<String> expected = new ArrayList<String>();
            expected.add(getTestCaseDir() + "/lib/reduceutil.so");
            expected.add(getTestCaseDir() + "/lib/maputil.jar");
            expected.add(getTestCaseDir() + "/libx/maputil_x.jar");
            expected.add(getTestCaseDir() + "/liby/maputil_y1.jar");
            expected.add(getTestCaseDir() + "/liby/maputil_y2.jar");
            expected.add(getTestCaseDir() + "/libz/maputil_z.jar");
            Collections.sort(found);
            Collections.sort(expected);
            assertEquals(expected, found);
        }
        finally {
            services.destroy();
        }
    }

    public void testCreateprotoConfWithSubWorkflow_Case1_ParentWorkflowContainingLibs() throws Exception {
        // When parent workflow has an non-empty lib directory,
        // APP_LIB_PATH_LIST should contain libraries from both parent and
        // subworkflow (child)
        Services services = new Services();
        try {
            services.init();
            Reader reader = IOUtils.getResourceAsReader("wf-schema-valid.xml", -1);
            Writer writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
            IOUtils.copyCharStream(reader, writer);

            createTestCaseSubDir("lib");
            writer = new FileWriter(getTestCaseDir() + "/lib/childdependency1.jar");
            writer.write("bla bla");
            writer.close();
            writer = new FileWriter(getTestCaseDir() + "/lib/childdependency2.so");
            writer.write("bla bla");
            writer.close();
            WorkflowAppService wps = Services.get().get(WorkflowAppService.class);
            Configuration jobConf = new XConfiguration();
            jobConf.set(OozieClient.APP_PATH, "file://" + getTestCaseDir() + File.separator + "workflow.xml");
            jobConf.set(OozieClient.USER_NAME, getTestUser());
            jobConf.set(WorkflowAppService.APP_LIB_PATH_LIST, "parentdependency1.jar");

            Configuration protoConf = wps.createProtoActionConf(jobConf, "authToken", true);
            assertEquals(getTestUser(), protoConf.get(OozieClient.USER_NAME));

            assertEquals(3, protoConf.getStrings(WorkflowAppService.APP_LIB_PATH_LIST).length);
            String f1 = protoConf.getStrings(WorkflowAppService.APP_LIB_PATH_LIST)[0];
            String f2 = protoConf.getStrings(WorkflowAppService.APP_LIB_PATH_LIST)[1];
            String f3 = protoConf.getStrings(WorkflowAppService.APP_LIB_PATH_LIST)[2];
            String ref1 = "parentdependency1.jar";
            String ref2 = getTestCaseDir() + "/lib/childdependency1.jar";
            String ref3 = getTestCaseDir() + "/lib/childdependency2.so";
            List<String> expected = new ArrayList<String>();
            expected.add(ref1);
            expected.add(ref2);
            expected.add(ref3);
            List<String> found = new ArrayList<String>();
            found.add(f1);
            found.add(f2);
            found.add(f3);
            Collections.sort(found);
            Collections.sort(expected);
            assertEquals(expected, found);
        }
        finally {
            services.destroy();
        }
    }

    public void testCreateprotoConfWithSubWorkflow_Case2_ParentWorkflowWithoutLibs() throws Exception {
        // When parent workflow has an empty (or missing) lib directory,
        // APP_LIB_PATH_LIST should contain libraries from only the subworkflow
        // (child)
        Services services = new Services();
        try {
            services.init();
            Reader reader = IOUtils.getResourceAsReader("wf-schema-valid.xml", -1);
            Writer writer = new FileWriter(getTestCaseDir() + "/workflow.xml");
            IOUtils.copyCharStream(reader, writer);

            createTestCaseSubDir("lib");
            writer = new FileWriter(getTestCaseDir() + "/lib/childdependency1.jar");
            writer.write("bla bla");
            writer.close();
            writer = new FileWriter(getTestCaseDir() + "/lib/childdependency2.so");
            writer.write("bla bla");
            writer.close();
            WorkflowAppService wps = Services.get().get(WorkflowAppService.class);
            Configuration jobConf = new XConfiguration();
            jobConf.set(OozieClient.APP_PATH, "file://" + getTestCaseDir() + File.separator + "workflow.xml");
            jobConf.set(OozieClient.USER_NAME, getTestUser());

            Configuration protoConf = wps.createProtoActionConf(jobConf, "authToken", true);
            assertEquals(getTestUser(), protoConf.get(OozieClient.USER_NAME));

            assertEquals(2, protoConf.getStrings(WorkflowAppService.APP_LIB_PATH_LIST).length);
            String f1 = protoConf.getStrings(WorkflowAppService.APP_LIB_PATH_LIST)[0];
            String f2 = protoConf.getStrings(WorkflowAppService.APP_LIB_PATH_LIST)[1];
            String ref1 = getTestCaseDir() + "/lib/childdependency1.jar";
            String ref2 = getTestCaseDir() + "/lib/childdependency2.so";
            List<String> expected = new ArrayList<String>();
            expected.add(ref1);
            expected.add(ref2);
            List<String> found = new ArrayList<String>();
            found.add(f1);
            found.add(f2);
            Collections.sort(found);
            Collections.sort(expected);
            assertEquals(expected, found);
        }
        finally {
            services.destroy();
        }
    }
}
