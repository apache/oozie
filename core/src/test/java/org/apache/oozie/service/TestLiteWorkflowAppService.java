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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import junit.framework.Assert;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.workflow.lite.StartNodeDef;

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
        finally {
            services.destroy();
        }
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
            assertNotNull(app.getNode(StartNodeDef.START));
            assertEquals("a", app.getNode(StartNodeDef.START).getTransitions().get(0));
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

            assertEquals("g", app.getNode("f").getTransitions().get(0));

            assertEquals("z", app.getNode("g").getTransitions().get(0));
            assertEquals("b", app.getNode("g").getTransitions().get(1));
            assertTrue(app.getNode("g").getConf().startsWith("<fs"));

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
            String ref1 = "file://" + getTestCaseDir() + "/lib/reduceutil.so";
            String ref2 = "file://" + getTestCaseDir() + "/lib/maputil.jar";
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
            expected.add("file://" + getTestCaseDir() + "/lib/reduceutil.so");
            expected.add("file://" + getTestCaseDir() + "/lib/maputil.jar");
            expected.add("file://" + getTestCaseDir() + "/libx/maputilx.jar");
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
            expected.add("file://" + getTestCaseDir() + "/lib/reduceutil.so");
            expected.add("file://" + getTestCaseDir() + "/lib/maputil.jar");
            expected.add("file://" + getTestCaseDir() + "/libx/maputil_x.jar");
            expected.add("file://" + getTestCaseDir() + "/liby/maputil_y1.jar");
            expected.add("file://" + getTestCaseDir() + "/liby/maputil_y2.jar");
            expected.add("file://" + getTestCaseDir() + "/libz/maputil_z.jar");
            Collections.sort(found);
            Collections.sort(expected);
            assertEquals(expected, found);
        }
        finally {
            services.destroy();
        }
    }

    private static String[] parentLibs1 = {"parent1.jar", "parent2.jar"};
    private static String[] childLibs1 = {"child1.jar", "child2.so"};
    private static String[] parentLibs2 = {"parent1.jar", "parent2.jar"};
    private static String[] childLibs2 = {};;
    private static String[] parentLibs3 = {};;
    private static String[] childLibs3 = {"child1.jar", "child2.so"};;
    private static String[] parentLibs4 = {};;
    private static String[] childLibs4 = {};;
    private static String[] parentLibs5 = {"parent1.jar", "parent2.jar", "same.jar"};;
    private static String[] childLibs5 = {"child1.jar", "same.jar", "child2.so"};;

    public void testCreateProtoConfWithSubWorkflowLib1() throws Exception {
        String inherit = "true";
        String inheritWF = null;

        String[] expectedLibs1 = {"parent1.jar", "parent2.jar", "child1.jar", "child2.so"};
        checkSubworkflowLibHelper(inherit, inheritWF, 1, parentLibs1, childLibs1, expectedLibs1);

        String[] expectedLibs2 = {"parent1.jar", "parent2.jar"};
        checkSubworkflowLibHelper(inherit, inheritWF, 2, parentLibs2, childLibs2, expectedLibs2);

        String[] expectedLibs3 = {"child1.jar", "child2.so"};
        checkSubworkflowLibHelper(inherit, inheritWF, 3, parentLibs3, childLibs3, expectedLibs3);

        String[] expectedLibs4 = {};
        checkSubworkflowLibHelper(inherit, inheritWF, 4, parentLibs4, childLibs4, expectedLibs4);

        String[] expectedLibs5 = {"parent1.jar", "parent2.jar", "child1.jar", "child2.so", "same.jar"};
        checkSubworkflowLibHelper(inherit, inheritWF, 5, parentLibs5, childLibs5, expectedLibs5);
    }

    public void testCreateProtoConfWithSubWorkflowLib2() throws Exception {
        String inherit = "false";
        String inheritWF = null;

        String[] expectedLibs1 = {"child1.jar", "child2.so"};
        checkSubworkflowLibHelper(inherit, inheritWF, 1, parentLibs1, childLibs1, expectedLibs1);

        String[] expectedLibs2 = {};
        checkSubworkflowLibHelper(inherit, inheritWF, 2, parentLibs2, childLibs2, expectedLibs2);

        String[] expectedLibs3 = {"child1.jar", "child2.so"};
        checkSubworkflowLibHelper(inherit, inheritWF, 3, parentLibs3, childLibs3, expectedLibs3);

        String[] expectedLibs4 = {};
        checkSubworkflowLibHelper(inherit, inheritWF, 4, parentLibs4, childLibs4, expectedLibs4);

        String[] expectedLibs5 = {"child1.jar", "child2.so", "same.jar"};
        checkSubworkflowLibHelper(inherit, inheritWF, 5, parentLibs5, childLibs5, expectedLibs5);
    }

    public void testCreateProtoConfWithSubWorkflowLib3() throws Exception {
        String inherit = "true";
        String inheritWF = "true";

        String[] expectedLibs1 = {"parent1.jar", "parent2.jar", "child1.jar", "child2.so"};
        checkSubworkflowLibHelper(inherit, inheritWF, 1, parentLibs1, childLibs1, expectedLibs1);

        String[] expectedLibs2 = {"parent1.jar", "parent2.jar"};
        checkSubworkflowLibHelper(inherit, inheritWF, 2, parentLibs2, childLibs2, expectedLibs2);

        String[] expectedLibs3 = {"child1.jar", "child2.so"};
        checkSubworkflowLibHelper(inherit, inheritWF, 3, parentLibs3, childLibs3, expectedLibs3);

        String[] expectedLibs4 = {};
        checkSubworkflowLibHelper(inherit, inheritWF, 4, parentLibs4, childLibs4, expectedLibs4);

        String[] expectedLibs5 = {"parent1.jar", "parent2.jar", "child1.jar", "child2.so", "same.jar"};
        checkSubworkflowLibHelper(inherit, inheritWF, 5, parentLibs5, childLibs5, expectedLibs5);
    }

    public void testCreateProtoConfWithSubWorkflowLib4() throws Exception {
        String inherit = "false";
        String inheritWF = "true";

        String[] expectedLibs1 = {"parent1.jar", "parent2.jar", "child1.jar", "child2.so"};
        checkSubworkflowLibHelper(inherit, inheritWF, 1, parentLibs1, childLibs1, expectedLibs1);

        String[] expectedLibs2 = {"parent1.jar", "parent2.jar"};
        checkSubworkflowLibHelper(inherit, inheritWF, 2, parentLibs2, childLibs2, expectedLibs2);

        String[] expectedLibs3 = {"child1.jar", "child2.so"};
        checkSubworkflowLibHelper(inherit, inheritWF, 3, parentLibs3, childLibs3, expectedLibs3);

        String[] expectedLibs4 = {};
        checkSubworkflowLibHelper(inherit, inheritWF, 4, parentLibs4, childLibs4, expectedLibs4);

        String[] expectedLibs5 = {"parent1.jar", "parent2.jar", "child1.jar", "child2.so", "same.jar"};
        checkSubworkflowLibHelper(inherit, inheritWF, 5, parentLibs5, childLibs5, expectedLibs5);
    }

    public void testCreateProtoConfWithSubWorkflowLib5() throws Exception {
        String inherit = "true";
        String inheritWF = "false";

        String[] expectedLibs1 = {"child1.jar", "child2.so"};
        checkSubworkflowLibHelper(inherit, inheritWF, 1, parentLibs1, childLibs1, expectedLibs1);

        String[] expectedLibs2 = {};
        checkSubworkflowLibHelper(inherit, inheritWF, 2, parentLibs2, childLibs2, expectedLibs2);

        String[] expectedLibs3 = {"child1.jar", "child2.so"};
        checkSubworkflowLibHelper(inherit, inheritWF, 3, parentLibs3, childLibs3, expectedLibs3);

        String[] expectedLibs4 = {};
        checkSubworkflowLibHelper(inherit, inheritWF, 4, parentLibs4, childLibs4, expectedLibs4);

        String[] expectedLibs5 = {"child1.jar", "child2.so", "same.jar"};
        checkSubworkflowLibHelper(inherit, inheritWF, 5, parentLibs5, childLibs5, expectedLibs5);
    }

    public void testCreateProtoConfWithSubWorkflowLib6() throws Exception {
        String inherit = "false";
        String inheritWF = "false";

        String[] expectedLibs1 = {"child1.jar", "child2.so"};
        checkSubworkflowLibHelper(inherit, inheritWF, 1, parentLibs1, childLibs1, expectedLibs1);

        String[] expectedLibs2 = {};
        checkSubworkflowLibHelper(inherit, inheritWF, 2, parentLibs2, childLibs2, expectedLibs2);

        String[] expectedLibs3 = {"child1.jar", "child2.so"};
        checkSubworkflowLibHelper(inherit, inheritWF, 3, parentLibs3, childLibs3, expectedLibs3);

        String[] expectedLibs4 = {};
        checkSubworkflowLibHelper(inherit, inheritWF, 4, parentLibs4, childLibs4, expectedLibs4);

        String[] expectedLibs5 = {"child1.jar", "child2.so", "same.jar"};
        checkSubworkflowLibHelper(inherit, inheritWF, 5, parentLibs5, childLibs5, expectedLibs5);
    }

    public void checkSubworkflowLibHelper(String inherit, String inheritWF, int unique, String[] parentLibs, String[] childLibs,
            String[] expectedLibs) throws Exception {
        Services services = new Services();
        try {
            services.getConf().set("oozie.subworkflow.classpath.inheritance", inherit);
            services.init();
            Reader reader = IOUtils.getResourceAsReader("wf-schema-valid.xml", -1);
            String childWFDir = createTestCaseSubDir("child-wf-" + unique);
            Writer writer = new FileWriter(childWFDir + File.separator + "workflow.xml");
            IOUtils.copyCharStream(reader, writer);

            WorkflowAppService wps = Services.get().get(WorkflowAppService.class);
            Configuration jobConf = new XConfiguration();
            jobConf.set(OozieClient.APP_PATH, "file://" + childWFDir + File.separator + "workflow.xml");
            jobConf.set(OozieClient.USER_NAME, getTestUser());
            if (inheritWF != null) {
                jobConf.set("oozie.wf.subworkflow.classpath.inheritance", inheritWF);
            }

            String childLibDir = createTestCaseSubDir("child-wf-" + unique + File.separator + "lib");
            for (String childLib : childLibs) {
                writer = new FileWriter(childLibDir + File.separator + childLib);
                writer.write("bla bla");
                writer.close();
            }
            String parentWFDir = createTestCaseSubDir("parent-wf-" + unique);
            String parentLibDir = createTestCaseSubDir("parent-wf-" + unique + File.separator + "lib");
            String[] parentLibsFullPaths = new String[parentLibs.length];
            for (int i = 0; i < parentLibs.length; i++) {
                parentLibsFullPaths[i] = parentLibDir + File.separator + parentLibs[i];
                writer = new FileWriter(parentLibsFullPaths[i]);
                writer.write("bla bla");
                writer.close();
            }
            // Set the parent libs
            jobConf.setStrings(WorkflowAppService.APP_LIB_PATH_LIST, parentLibsFullPaths);

            Configuration protoConf = wps.createProtoActionConf(jobConf, "authToken", true);
            assertEquals(getTestUser(), protoConf.get(OozieClient.USER_NAME));

            String[] foundLibs = protoConf.getStrings(WorkflowAppService.APP_LIB_PATH_LIST);
            if (expectedLibs.length > 0) {
                assertEquals(expectedLibs.length, foundLibs.length);
                for (int i = 0; i < foundLibs.length; i++) {
                    Path p = new Path(foundLibs[i]);
                    foundLibs[i] = p.getName();
                }
                Arrays.sort(expectedLibs);
                Arrays.sort(foundLibs);
                assertEquals(Arrays.toString(expectedLibs), Arrays.toString(foundLibs));
            }
            else {
                assertEquals(null, foundLibs);
            }
        }
        finally {
            services.destroy();
        }
    }
}
