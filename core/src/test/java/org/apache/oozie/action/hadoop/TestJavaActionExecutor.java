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

package org.apache.oozie.action.hadoop;

import java.io.IOException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.net.URI;
import java.security.PrivilegedExceptionAction;

import org.jdom2.Element;
import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.LiteWorkflowStoreService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.ShareLibService;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.service.UserGroupInformationService;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.service.WorkflowStoreService;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.workflow.WorkflowApp;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.WorkflowLib;
import org.apache.oozie.workflow.lite.EndNodeDef;
import org.apache.oozie.workflow.lite.LiteWorkflowApp;
import org.apache.oozie.workflow.lite.StartNodeDef;

public class TestJavaActionExecutor extends ActionExecutorTestCase {

    private static final String YARN_RESOURCEMANAGER_ADDRESS = "yarn.resourcemanager.address";
    private static final String MAPRED_CHILD_JAVA_OPTS = "mapred.child.java.opts";
    private static final String MAPREDUCE_MAP_JAVA_OPTS = "mapreduce.map.java.opts";
    private TestWorkflowHelper helper;
    @Override
    protected void beforeSetUp() throws Exception {
        super.beforeSetUp();
        setSystemProperty("oozie.test.hadoop.minicluster2", "true");
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        helper = new TestWorkflowHelper(getJobTrackerUri(), getNameNodeUri(), getTestCaseDir());
    }

    @Override
    protected void setSystemProps() throws Exception {
        super.setSystemProps();
        setHadoopSystemProps();
        createActionConfDirFiles();
    }

    private void createActionConfDirFiles() throws IOException {
        new File(getTestCaseConfDir(), "action-conf").mkdir();
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("test-action-config.xml");
        OutputStream os = new FileOutputStream(new File(getTestCaseConfDir() + "/action-conf", "java.xml"));
        IOUtils.copyStream(is, os);
    }

    private void setHadoopSystemProps() {
        setSystemProperty("oozie.service.ActionService.executor.classes", JavaActionExecutor.class.getName());
        setSystemProperty("oozie.service.HadoopAccessorService.action.configurations",
                          "*=hadoop-conf," + getJobTrackerUri() + "=action-conf");
        setSystemProperty(WorkflowAppService.SYSTEM_LIB_PATH, getFsTestCaseDir().toUri().getPath() + "/systemlib");
    }

    public void testSetupMethods() throws Exception {
        JavaActionExecutor ae = new JavaActionExecutor();
        assertEquals(Arrays.asList(JavaMain.class), ae.getLauncherClasses());
        Configuration conf = new XConfiguration();
        conf.set("user.name", "a");
        try {
            JavaActionExecutor.checkForDisallowedProps(conf, "x");
            fail();
        }
        catch (ActionExecutorException ex) {
        }

        conf = new XConfiguration();
        conf.set(YARN_RESOURCEMANAGER_ADDRESS, "a");
        try {
            JavaActionExecutor.checkForDisallowedProps(conf, "x");
            fail();
        }
        catch (ActionExecutorException ex) {
        }

        conf = new XConfiguration();
        conf.set("fs.default.name", "a");
        try {
            JavaActionExecutor.checkForDisallowedProps(conf, "x");
            fail();
        }
        catch (ActionExecutorException ex) {
        }

        conf = new XConfiguration();
        conf.set("a", "a");
        try {
            JavaActionExecutor.checkForDisallowedProps(conf, "x");
        }
        catch (ActionExecutorException ex) {
            fail();
        }

        Element actionXml = XmlUtils.parseXml("<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<job-xml>job.xml</job-xml>" + "<job-xml>job2.xml</job-xml>" + "<configuration>" +
                "<property><name>oozie.launcher.a</name><value>LA</value></property>" +
                "<property><name>a</name><value>AA</value></property>" +
                "<property><name>b</name><value>BB</value></property>" +
                "</configuration>" + "<main-class>MAIN-CLASS</main-class>" +
                "<java-opts>JAVA-OPTS</java-opts>" + "<arg>A1</arg>" + "<arg>A2</arg>" +
                "<file>f.jar</file>" + "<archive>a.tar</archive>" + "</java>");

        Path appPath = new Path(getFsTestCaseDir(), "wf");

        Path appJarPath = new Path("lib/a.jar");
        getFileSystem().create(new Path(appPath, appJarPath)).close();

        Path appSoPath = new Path("lib/a.so");
        getFileSystem().create(new Path(appPath, appSoPath)).close();

        Path appSo1Path = new Path("lib/a.so.1");
        String expectedSo1Path = "lib/a.so.1#a.so.1";
        getFileSystem().create(new Path(appPath, appSo1Path)).close();

        Path filePath = new Path("f.jar");
        getFileSystem().create(new Path(appPath, filePath)).close();

        Path archivePath = new Path("a.tar");
        getFileSystem().create(new Path(appPath, archivePath)).close();

        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());
        protoConf.setStrings(WorkflowAppService.APP_LIB_PATH_LIST, appJarPath.toString(), appSoPath.toString());


        WorkflowJobBean wf = createBaseWorkflow(protoConf, "action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());

        Context context = new Context(wf, action);

        conf = new XConfiguration();
        conf.set("c", "C");
        conf.set("oozie.launcher.d", "D");
        OutputStream os = getFileSystem().create(new Path(getFsTestCaseDir(), "job.xml"));
        conf.writeXml(os);
        os.close();
        os = getFileSystem().create(new Path(getFsTestCaseDir(), new Path("app", "job.xml")));
        conf.writeXml(os);
        os.close();

        conf = new XConfiguration();
        conf.set("e", "E");
        conf.set("oozie.launcher.f", "F");
        os = getFileSystem().create(new Path(getFsTestCaseDir(), "job2.xml"));
        conf.writeXml(os);
        os.close();
        os = getFileSystem().create(new Path(getFsTestCaseDir(), new Path("app", "job2.xml")));
        conf.writeXml(os);
        os.close();

        conf = ae.createBaseHadoopConf(context, actionXml);
        assertEquals(protoConf.get(WorkflowAppService.HADOOP_USER), conf.get(WorkflowAppService.HADOOP_USER));
        assertEquals(getJobTrackerUri(), conf.get(YARN_RESOURCEMANAGER_ADDRESS));
        assertEquals(getNameNodeUri(), conf.get("fs.default.name"));

        conf = ae.createBaseHadoopConf(context, actionXml);
        ae.setupLauncherConf(conf, actionXml, getFsTestCaseDir(), context);
        assertEquals("LA", conf.get("oozie.launcher.a"));
        assertEquals("LA", conf.get("a"));
        assertNull(conf.get("b"));
        assertEquals("D", conf.get("oozie.launcher.d"));
        assertEquals("D", conf.get("d"));
        assertEquals("F", conf.get("oozie.launcher.f"));
        assertEquals("F", conf.get("f"));
        assertNull(conf.get("action.foo"));
        assertEquals("action.barbar", conf.get("action.foofoo"));

        conf = ae.createBaseHadoopConf(context, actionXml);
        ae.setupActionConf(conf, context, actionXml, getFsTestCaseDir());
        assertEquals("LA", conf.get("oozie.launcher.a"));
        assertEquals("AA", conf.get("a"));
        assertEquals("BB", conf.get("b"));
        assertEquals("C", conf.get("c"));
        assertEquals("D", conf.get("oozie.launcher.d"));
        assertNull(conf.get("d"));
        assertEquals("E", conf.get("e"));
        assertEquals("F", conf.get("oozie.launcher.f"));
        assertNull(conf.get("f"));
        assertEquals("action.bar", conf.get("action.foo"));

        conf = ae.createBaseHadoopConf(context, actionXml);
        ae.setupLauncherConf(conf, actionXml, getFsTestCaseDir(), context);
        ae.addToCache(conf, appPath, appJarPath.toString(), false);
        assertTrue(conf.get("mapred.job.classpath.files").contains(appJarPath.toUri().getPath()));
        ae.addToCache(conf, appPath, appSoPath.toString(), false);
        assertTrue(conf.get("mapred.cache.files").contains(appSoPath.toUri().getPath()));
        ae.addToCache(conf, appPath, appSo1Path.toString(), false);
        assertTrue(conf.get("mapred.cache.files").contains(expectedSo1Path));
        assertFalse(getFileSystem().exists(context.getActionDir()));
        ae.prepareActionDir(getFileSystem(), context);
        assertTrue(getFileSystem().exists(context.getActionDir()));

        ae.cleanUpActionDir(getFileSystem(), context);
        assertFalse(getFileSystem().exists(context.getActionDir()));

        conf = ae.createBaseHadoopConf(context, actionXml);
        ae.setupLauncherConf(conf, actionXml, getFsTestCaseDir(), context);
        ae.setLibFilesArchives(context, actionXml, appPath, conf);

        assertTrue(conf.get("mapred.cache.files").contains(filePath.toUri().getPath()));
        assertTrue(conf.get("mapred.cache.archives").contains(archivePath.toUri().getPath()));

        conf = ae.createBaseHadoopConf(context, actionXml);
        ae.setupActionConf(conf, context, actionXml, getFsTestCaseDir());
        ae.setLibFilesArchives(context, actionXml, appPath, conf);

        assertTrue(conf.get("mapred.cache.files").contains(filePath.toUri().getPath()));
        assertTrue(conf.get("mapred.cache.archives").contains(archivePath.toUri().getPath()));

        Configuration actionConf = ae.createBaseHadoopConf(context, actionXml);
        ae.setupActionConf(actionConf, context, actionXml, getFsTestCaseDir());

        conf = ae.createLauncherConf(getFileSystem(), context, action, actionXml, actionConf);
        ae.setupLauncherConf(conf, actionXml, getFsTestCaseDir(), context);
        assertEquals("MAIN-CLASS", actionConf.get("oozie.action.java.main", "null"));
        assertEquals("org.apache.oozie.action.hadoop.JavaMain", ae.getLauncherMain(conf, actionXml));
        assertTrue(conf.get(MAPRED_CHILD_JAVA_OPTS).contains("JAVA-OPTS"));
        assertTrue(conf.get(MAPREDUCE_MAP_JAVA_OPTS).contains("JAVA-OPTS"));
        assertEquals(Arrays.asList("A1", "A2"), Arrays.asList(LauncherAMUtils.getMainArguments(conf)));

        actionXml = XmlUtils.parseXml("<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node> <configuration>" +
                "<property><name>mapred.job.queue.name</name><value>AQ</value></property>" +
                "<property><name>oozie.action.sharelib.for.java</name><value>sharelib-java</value></property>" +
                "</configuration>" + "<main-class>MAIN-CLASS</main-class>" +
                "</java>");
        actionConf = ae.createBaseHadoopConf(context, actionXml);
        ae.setupActionConf(actionConf, context, actionXml, appPath);
        conf = ae.createLauncherConf(getFileSystem(), context, action, actionXml, actionConf);
        assertEquals("AQ", conf.get("mapred.job.queue.name"));
        assertEquals("AQ", actionConf.get("mapred.job.queue.name"));
        assertEquals("sharelib-java", actionConf.get("oozie.action.sharelib.for.java"));

        actionXml = XmlUtils.parseXml("<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node> <configuration>" +
                "<property><name>oozie.launcher.mapred.job.queue.name</name><value>LQ</value></property>" +
                "</configuration>" + "<main-class>MAIN-CLASS</main-class>" +
                "</java>");
        actionConf = ae.createBaseHadoopConf(context, actionXml);
        ae.setupActionConf(actionConf, context, actionXml, appPath);
        conf = ae.createLauncherConf(getFileSystem(), context, action, actionXml, actionConf);
        assertEquals("LQ", conf.get("mapred.job.queue.name"));

        actionXml = XmlUtils.parseXml("<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node> <configuration>" +
                "<property><name>oozie.launcher.mapred.job.queue.name</name><value>LQ</value></property>" +
                "<property><name>mapred.job.queue.name</name><value>AQ</value></property>" +
                "</configuration>" + "<main-class>MAIN-CLASS</main-class>" +
                "</java>");
        actionConf = ae.createBaseHadoopConf(context, actionXml);
        ae.setupActionConf(actionConf, context, actionXml, appPath);
        conf = ae.createLauncherConf(getFileSystem(), context, action, actionXml, actionConf);
        assertEquals("LQ", conf.get("mapred.job.queue.name"));
        assertEquals("AQ", actionConf.get("mapred.job.queue.name"));
        assertEquals(true, conf.getBoolean("mapreduce.job.complete.cancel.delegation.tokens", false));
        assertEquals(false, actionConf.getBoolean("mapreduce.job.complete.cancel.delegation.tokens", true));
    }

    protected Context createContext(String actionXml, String group) throws Exception {
        JavaActionExecutor ae = new JavaActionExecutor();

        Path appJarPath = new Path("lib/test.jar");
        File jarFile = IOUtils.createJar(new File(getTestCaseDir()), "test.jar", LauncherMainTester.class);
        InputStream is = new FileInputStream(jarFile);
        OutputStream os = getFileSystem().create(new Path(getAppPath(), "lib/test.jar"));
        IOUtils.copyStream(is, os);

        Path appSoPath = new Path("lib/test.so");
        getFileSystem().create(new Path(getAppPath(), appSoPath)).close();

        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());
        protoConf.setStrings(WorkflowAppService.APP_LIB_PATH_LIST, appJarPath.toString(), appSoPath.toString());

        WorkflowJobBean wf = createBaseWorkflow(protoConf, "action");
        if(group != null) {
            wf.setGroup(group);
        }
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());
        action.setConf(actionXml);

        return new Context(wf, action);
    }

    protected String submitAction(Context context, JavaActionExecutor javaActionExecutor) throws ActionExecutorException {

        WorkflowAction action = context.getAction();
        javaActionExecutor.prepareActionDir(getFileSystem(), context);
        javaActionExecutor.submitLauncher(getFileSystem(), context, action);

        String jobId = action.getExternalId();
        String jobTracker = action.getTrackerUri();
        String consoleUrl = action.getConsoleUrl();
        assertNotNull(jobId);
        assertNotNull(jobTracker);
        assertNotNull(consoleUrl);
        return jobId;
    }

    protected String submitAction(Context context) throws ActionExecutorException {
        return submitAction(context, new JavaActionExecutor());
    }

    public void testSimpestSleSubmitOK() throws Exception {
        String actionXml = "<java>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<main-class>" + LauncherMainTester.class.getName() + "</main-class>" +
                "</java>";
        Context context = createContext(actionXml, null);
        submitAction(context);
        waitUntilYarnAppDoneAndAssertSuccess(context.getAction().getExternalId());
        ActionExecutor ae = new JavaActionExecutor();
        ae.check(context, context.getAction());
        assertEquals("SUCCEEDED", context.getAction().getExternalStatus());
        assertNull(context.getAction().getData());

        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());
    }


    public void testSimplestSubmitWithResourceManagerOK() throws Exception {
        final String actionXml = "<java>" +
                "<resource-manager>" + getJobTrackerUri() + "</resource-manager>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<main-class>" + LauncherMainTester.class.getName() + "</main-class>" +
                "</java>";
        final Context context = createContext(actionXml, null);
        submitAction(context);
        waitUntilYarnAppDoneAndAssertSuccess(context.getAction().getExternalId());
        final ActionExecutor ae = new JavaActionExecutor();
        ae.check(context, context.getAction());
        assertEquals("SUCCEEDED", context.getAction().getExternalStatus());
        assertNull(context.getAction().getData());

        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());
    }

    public void testOutputSubmitOK() throws Exception {
        String actionXml = "<java>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<main-class>" + LauncherMainTester.class.getName() + "</main-class>" +
                "<arg>out</arg>" +
                "<capture-output/>" +
                "</java>";
        Context context = createContext(actionXml, null);
        final String runningJob = submitAction(context);
        waitUntilYarnAppDoneAndAssertSuccess(runningJob);
        ActionExecutor ae = new JavaActionExecutor();
        ae.check(context, context.getAction());
        assertEquals("SUCCEEDED", context.getAction().getExternalStatus());
        assertNotNull(context.getAction().getData());
        StringReader sr = new StringReader(context.getAction().getData());
        Properties props = new Properties();
        props.load(sr);
        assertEquals("A", props.get("a"));

        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());
    }


    public void testIdSwapSubmitOK() throws Exception {
        String actionXml = "<java>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<main-class>" + LauncherMainTester.class.getName() + "</main-class>" +
                "<arg>id</arg>" +
                "<capture-output/>" +
                "</java>";
        Context context = createContext(actionXml, null);
        final String runningJob = submitAction(context);
        waitUntilYarnAppDoneAndAssertSuccess(runningJob);
        ActionExecutor ae = new JavaActionExecutor();
        try {
            ae.check(context, context.getAction());
        }
        catch (ActionExecutorException ex) {
            if (!ex.getMessage().contains("IDSWAP")) {
                fail();
            }
        }
    }

    public void testAdditionalJarSubmitOK() throws Exception {
        Path appJarPath = new Path("test-extra.jar");

        File jarFile = IOUtils.createJar(new File(getTestCaseDir()), appJarPath.getName(), LauncherMainTester2.class);
        InputStream is = new FileInputStream(jarFile);
        OutputStream os = getFileSystem().create(new Path(getAppPath(), appJarPath.toString()));
        IOUtils.copyStream(is, os);

        String actionXml = "<java>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<main-class>" + LauncherMainTester2.class.getName() + "</main-class>" +
                "<file>" + appJarPath.toString() + "</file>" +
                "</java>";

        Context context = createContext(actionXml, null);
        final String runningJobId = submitAction(context);
        ActionExecutor ae = new JavaActionExecutor();
        assertFalse(ae.isCompleted(context.getAction().getExternalStatus()));
        waitUntilYarnAppDoneAndAssertSuccess(runningJobId);
        ae.check(context, context.getAction());
        assertEquals("SUCCEEDED", context.getAction().getExternalStatus());
        assertNull(context.getAction().getData());

        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());
    }

    public void testExit0SubmitOK() throws Exception {
        String actionXml = "<java>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<main-class>" + LauncherMainTester.class.getName() + "</main-class>" +
                "<arg>exit0</arg>" +
                "</java>";

        Context context = createContext(actionXml, null);
        final String runningJob = submitAction(context);
        waitUntilYarnAppDoneAndAssertSuccess(runningJob);
        ActionExecutor ae = new JavaActionExecutor();
        ae.check(context, context.getAction());
        assertTrue(ae.isCompleted(context.getAction().getExternalStatus()));
        assertEquals("SUCCEEDED", context.getAction().getExternalStatus());
        assertNull(context.getAction().getData());

        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());
    }

    public void testExit1SubmitError() throws Exception {
        String actionXml = "<java>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<main-class>" + LauncherMainTester.class.getName() + "</main-class>" +
                "<arg>exit1</arg>" +
                "</java>";

        Context context = createContext(actionXml, null);
        final String runningJob = submitAction(context);
        waitUntilYarnAppDoneAndAssertSuccess(runningJob);
      //FIXME  assertFalse(LauncherHelper.isMainSuccessful(runningJob));
        ActionExecutor ae = new JavaActionExecutor();
        ae.check(context, context.getAction());
        assertTrue(ae.isCompleted(context.getAction().getExternalStatus()));
        assertEquals("FAILED/KILLED", context.getAction().getExternalStatus());
        assertEquals("1", context.getAction().getErrorCode());
        assertNull(context.getAction().getData());

        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.ERROR, context.getAction().getStatus());
    }

    public void testExceptionSubmitException() throws Exception {

        String actionXml = "<java>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<main-class>" + LauncherMainTester.class.getName() + "</main-class>" +
                "<arg>exception</arg>" +
                "</java>";

        Context context = createContext(actionXml, null);
        final String runningJob = submitAction(context);
        waitUntilYarnAppDoneAndAssertSuccess(runningJob);
     //FIXME   assertFalse(LauncherHelper.isMainSuccessful(runningJob));
        ActionExecutor ae = new JavaActionExecutor();
        ae.check(context, context.getAction());
        assertTrue(ae.isCompleted(context.getAction().getExternalStatus()));
        assertEquals("FAILED/KILLED", context.getAction().getExternalStatus());
        assertNull(context.getAction().getData());

        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.ERROR, context.getAction().getStatus());
    }

    public void testExceptionSubmitThrowable() throws Exception {
        String actionXml = "<java>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<main-class>" + LauncherMainTester.class.getName() + "</main-class>" +
                "<arg>throwable</arg>" +
                "</java>";

        Context context = createContext(actionXml, null);
        final String runningJob = submitAction(context);
        waitUntilYarnAppDoneAndAssertSuccess(runningJob);
      //FIXME  assertFalse(LauncherHelper.isMainSuccessful(runningJob));
        ActionExecutor ae = new JavaActionExecutor();
        ae.check(context, context.getAction());
        assertTrue(ae.isCompleted(context.getAction().getExternalStatus()));
        assertEquals("FAILED/KILLED", context.getAction().getExternalStatus());
        assertNull(context.getAction().getData());

        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.ERROR, context.getAction().getStatus());
    }

    public void testKill() throws Exception {
        String actionXml = "<java>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<main-class>" + LauncherMainTester.class.getName() + "</main-class>" +
                "</java>";
        final Context context = createContext(actionXml, null);
        final String runningJob = submitAction(context);
        ActionExecutor ae = new JavaActionExecutor();
        ae.kill(context, context.getAction());
        assertEquals(WorkflowAction.Status.DONE, context.getAction().getStatus());
        assertEquals("KILLED", context.getAction().getExternalStatus());
        assertTrue(ae.isCompleted(context.getAction().getExternalStatus()));
        waitUntilYarnAppKilledAndAssertSuccess(runningJob);
    }


    public void testRecovery() throws Exception {
        final String actionXml = "<java>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<main-class>" + LauncherMainTester.class.getName() + "</main-class>" +
                "</java>";
        final Context context = createContext(actionXml, null);
        String launcherId =  submitAction(context);

        waitFor(60 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                JavaActionExecutor ae = new JavaActionExecutor();
                Configuration conf = ae.createBaseHadoopConf(context, XmlUtils.parseXml(actionXml));
                return LauncherHelper.getRecoveryId(conf, context.getActionDir(), context.getRecoveryId()) != null;
            }
        });

        final String runningJob2 = submitAction(context);

        assertEquals(launcherId, runningJob2);
        assertEquals(launcherId, context.getAction().getExternalId());

        waitUntilYarnAppDoneAndAssertSuccess(launcherId);
        ActionExecutor ae = new JavaActionExecutor();
        ae.check(context, context.getAction());
        assertEquals("SUCCEEDED", context.getAction().getExternalStatus());
        assertNull(context.getAction().getData());

        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());
    }

    public void testPrepare() throws Exception {
        FileSystem fs = getFileSystem();
        Path mkdir = new Path(getFsTestCaseDir(), "mkdir");
        Path delete = new Path(getFsTestCaseDir(), "delete");
        fs.mkdirs(delete);

        String actionXml = "<java>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<prepare>" +
                "<mkdir path='" + mkdir + "'/>" +
                "<delete path='" + delete + "'/>" +
                "</prepare>" +
                "<configuration>" +
                "<property>" +
                "<name>dfs.umaskmode</name>" +
                "<value>026</value>" +
                "</property>" +
                "<property>" +
                "<name>fs.hdfs.impl.disable.cache</name>" +
                "<value>true</value>" +
                "</property>" +
                "</configuration>" +
                "<main-class>" + LauncherMainTester.class.getName() + "</main-class>" +
                "</java>";
        Context context = createContext(actionXml, null);
        final String runningJob = submitAction(context);
        waitUntilYarnAppDoneAndAssertSuccess(runningJob);
        ActionExecutor ae = new JavaActionExecutor();
        ae.check(context, context.getAction());
        assertEquals("SUCCEEDED", context.getAction().getExternalStatus());
        assertNull(context.getAction().getData());

        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());

        assertTrue(fs.exists(mkdir));
        // Check if the action configuration is applied in the prepare block
        assertEquals("rwxr-x--x", fs.getFileStatus(mkdir).getPermission().toString());
        assertFalse(fs.exists(delete));
    }

    public void testCredentialsModule() throws Exception {
        String actionXml = "<workflow-app xmlns='uri:oozie:workflow:0.2.5' name='pig-wf'>" + "<credentials>"
                + "<credential name='abcname' type='abc'>" + "<property>" + "<name>property1</name>"
                + "<value>value1</value>" + "</property>" + "<property>" + "<name>property2</name>"
                + "<value>value2</value>" + "</property>" + "<property>" + "<name>${property3}</name>"
                + "<value>${value3}</value>" + "</property>" + "</credential>" + "</credentials>"
                + "<start to='pig1' />" + "<action name='pig1' cred='abcname'>" + "<pig>" + "</pig>"
                + "<ok to='end' />" + "<error to='fail' />" + "</action>" + "<kill name='fail'>"
                + "<message>Pig failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>" + "</kill>"
                + "<end name='end' />" + "</workflow-app>";

        JavaActionExecutor ae = new JavaActionExecutor();
        WorkflowJobBean wfBean = addRecordToWfJobTable("test1", actionXml);
        WorkflowActionBean action = (WorkflowActionBean) wfBean.getActions().get(0);
        action.setType(ae.getType());
        action.setCred("abcname");
        String actionxml = "<pig>" + "<job-tracker>${jobTracker}</job-tracker>" + "<name-node>${nameNode}</name-node>"
                + "<prepare>" + "<delete path='outputdir' />" + "</prepare>" + "<configuration>" + "<property>"
                + "<name>mapred.compress.map.output</name>" + "<value>true</value>" + "</property>" + "<property>"
                + "<name>mapred.job.queue.name</name>" + "<value>${queueName}</value>" + "</property>"
                + "</configuration>" + "<script>org/apache/oozie/examples/pig/id.pig</script>"
                + "<param>INPUT=${inputDir}</param>" + "<param>OUTPUT=${outputDir}/pig-output</param>" + "</pig>";
        action.setConf(actionxml);
        Context context = new Context(wfBean, action);

        Element actionXmlconf = XmlUtils.parseXml(action.getConf());
        // action job configuration
        Configuration actionConf = ae.createBaseHadoopConf(context, actionXmlconf);

        // Setting the credential properties in launcher conf
        Map<String, CredentialsProperties> credProperties = ae.setCredentialPropertyToActionConf(context,
                action, actionConf);

        assertNotNull(credProperties);
        CredentialsProperties prop = credProperties.get("abcname");
        assertEquals("value1", prop.getProperties().get("property1"));
        assertEquals("value2", prop.getProperties().get("property2"));
        assertEquals("val3", prop.getProperties().get("prop3"));

        // Try to load the token without it being defined in oozie-site; should get an exception
        CredentialsProviderFactory.destroy();
        JobConf credentialsConf = new JobConf();
        Credentials credentials = new Credentials();
        Configuration launcherConf = ae.createBaseHadoopConf(context, actionXmlconf);
        XConfiguration.copy(launcherConf, credentialsConf);
        try {
            ae.setCredentialTokens(credentials, credentialsConf, context, action, credProperties);
            fail("Should have gotten an exception but did not");
        }
        catch (ActionExecutorException aee) {
            assertEquals("JA020", aee.getErrorCode());
            assertTrue(aee.getMessage().contains("type [abc]"));
            assertTrue(aee.getMessage().contains("name [abcname]"));
        }
        CredentialsProviderFactory.destroy();

        // Define 'abc' token type in oozie-site
        ConfigurationService.set("oozie.credentials.credentialclasses", "abc=org.apache.oozie.action.hadoop.InsertTestToken");

        // Try to load the token after being defined in oozie-site; should work correctly
        credentialsConf = new JobConf();
        credentials = new Credentials();
        launcherConf = ae.createBaseHadoopConf(context, actionXmlconf);
        XConfiguration.copy(launcherConf, credentialsConf);
        ae.setCredentialTokens(credentials, credentialsConf, context, action, credProperties);
        Token<? extends TokenIdentifier> tk = credentials.getToken(new Text("ABC Token"));
        assertNotNull(tk);

        byte[] secKey = credentials.getSecretKey(new Text(InsertTestToken.DUMMY_SECRET_KEY));
        assertNotNull(secKey);
        assertEquals(InsertTestToken.DUMMY_SECRET_KEY, new String(secKey, StandardCharsets.UTF_8));
    }

    public void testCredentialsInvalid() throws Exception {
        String workflowXml = "<workflow-app xmlns='uri:oozie:workflow:0.2.5' name='pig-wf'>" + "<credentials>"
                + "<credential name='abcname' type='abc'>" + "<property>" + "<name>property1</name>"
                + "<value>value1</value>" + "</property>" + "<property>" + "<name>property2</name>"
                + "<value>value2</value>" + "</property>" + "<property>" + "<name>${property3}</name>"
                + "<value>${value3}</value>" + "</property>" + "</credential>" + "</credentials>"
                + "<start to='pig1' />" + "<action name='pig1' cred='abcname'>" + "<pig>" + "</pig>"
                + "<ok to='end' />" + "<error to='fail' />" + "</action>" + "<kill name='fail'>"
                + "<message>Pig failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>" + "</kill>"
                + "<end name='end' />" + "</workflow-app>";

        JavaActionExecutor ae = new JavaActionExecutor();
        WorkflowJobBean wfBean = addRecordToWfJobTable("test1", workflowXml);
        WorkflowActionBean action = (WorkflowActionBean) wfBean.getActions().get(0);
        action.setType(ae.getType());
        action.setCred("invalidabcname");
        String actionXml = "<pig>" + "<job-tracker>${jobTracker}</job-tracker>" + "<name-node>${nameNode}</name-node>"
                + "<prepare>" + "<delete path='outputdir' />" + "</prepare>" + "<configuration>" + "<property>"
                + "<name>mapred.compress.map.output</name>" + "<value>true</value>" + "</property>" + "<property>"
                + "<name>mapred.job.queue.name</name>" + "<value>${queueName}</value>" + "</property>"
                + "</configuration>" + "<script>org/apache/oozie/examples/pig/id.pig</script>"
                + "<param>INPUT=${inputDir}</param>" + "<param>OUTPUT=${outputDir}/pig-output</param>" + "</pig>";
        action.setConf(actionXml);
        Context context = new Context(wfBean, action);

        Element actionXmlconf = XmlUtils.parseXml(action.getConf());
        // action job configuration
        Configuration actionConf = ae.createBaseHadoopConf(context, actionXmlconf);

        try {
            // Setting the credential properties in launcher conf should fail
            ae.setCredentialPropertyToActionConf(context, action, actionConf);
        }
        catch (ActionExecutorException e) {
            assertEquals(e.getErrorCode(), "JA021");
        }
    }


    public void testCredentialsWithoutCredTag() throws Exception {
        // create a workflow with credentials
        // add a pig action without cred tag
        String workflowXml = "<workflow-app xmlns='uri:oozie:workflow:0.2.5' name='pig-wf'>" + "<credentials>"
                + "<credential name='abcname' type='abc'>" + "<property>" + "<name>property1</name>"
                + "<value>value1</value>" + "</property>" + "<property>" + "<name>property2</name>"
                + "<value>value2</value>" + "</property>" + "<property>" + "<name>${property3}</name>"
                + "<value>${value3}</value>" + "</property>" + "</credential>" + "</credentials>"
                + "<start to='pig1' />" + "<action name='pig1'>" + "<pig>" + "</pig>"
                + "<ok to='end' />" + "<error to='fail' />" + "</action>" + "<kill name='fail'>"
                + "<message>Pig failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>" + "</kill>"
                + "<end name='end' />" + "</workflow-app>";

        JavaActionExecutor ae = new JavaActionExecutor();
        WorkflowJobBean wfBean = addRecordToWfJobTable("test1", workflowXml);
        WorkflowActionBean action = (WorkflowActionBean) wfBean.getActions().get(0);
        action.setType(ae.getType());
        String actionXml = "<pig>" + "<job-tracker>${jobTracker}</job-tracker>" + "<name-node>${nameNode}</name-node>"
                + "<prepare>" + "<delete path='outputdir' />" + "</prepare>" + "<configuration>" + "<property>"
                + "<name>mapred.compress.map.output</name>" + "<value>true</value>" + "</property>" + "<property>"
                + "<name>mapred.job.queue.name</name>" + "<value>${queueName}</value>" + "</property>"
                + "</configuration>" + "<script>org/apache/oozie/examples/pig/id.pig</script>"
                + "<param>INPUT=${inputDir}</param>" + "<param>OUTPUT=${outputDir}/pig-output</param>" + "</pig>";
        action.setConf(actionXml);
        Context context = new Context(wfBean, action);

        Element actionXmlconf = XmlUtils.parseXml(action.getConf());
        // action job configuration
        Configuration actionConf = ae.createBaseHadoopConf(context, actionXmlconf);

        // should not throw JA021 exception
        ae.setCredentialPropertyToActionConf(context, action, actionConf);
    }

    public void testCredentialsSkip() throws Exception {
        // providerCache must be re-initialized
        CredentialsProviderFactory.destroy();

        // Try setting oozie.credentials.skip at different levels, and verifying the correct behavior
        // oozie-site: false -- job-level: null -- action-level: null
        _testCredentialsSkip(false, null, null, true);

        // oozie-site: false -- job-level: null -- action-level: false
        _testCredentialsSkip(false, null, "false", true);

        // oozie-site: false -- job-level: null -- action-level: true
        _testCredentialsSkip(false, null, "true", false);

        // oozie-site: false -- job-level: false -- action-level: null
        _testCredentialsSkip(false, "false", null, true);

        // oozie-site: false -- job-level: false -- action-level: false
        _testCredentialsSkip(false, "false", "false", true);

        // oozie-site: false -- job-level: false -- action-level: true
        _testCredentialsSkip(false, "false", "true", false);

        // oozie-site: false -- job-level: true -- action-level: null
        _testCredentialsSkip(false, "true", null, false);

        // oozie-site: false -- job-level: true -- action-level: false
        _testCredentialsSkip(false, "true", "false", true);

        // oozie-site: false -- job-level: true -- action-level: true
        _testCredentialsSkip(false, "true", "true", false);

        // oozie-site: true -- job-level: null -- action-level: null
        _testCredentialsSkip(true, null, null, false);

        // oozie-site: true -- job-level: null -- action-level: false
        _testCredentialsSkip(true, null, "false", true);

        // oozie-site: true -- job-level: null -- action-level: true
        _testCredentialsSkip(true, null, "true", false);

        // oozie-site: true -- job-level: false -- action-level: null
        _testCredentialsSkip(true, "false", null, true);

        // oozie-site: true -- job-level: false -- action-level: false
        _testCredentialsSkip(true, "false", "false", true);

        // oozie-site: true -- job-level: false -- action-level: true
        _testCredentialsSkip(true, "false", "true", false);

        // oozie-site: true -- job-level: true -- action-level: null
        _testCredentialsSkip(true, "true", null, false);

        // oozie-site: true -- job-level: true -- action-level: false
        _testCredentialsSkip(true, "true", "false", true);

        // oozie-site: true -- job-level: true -- action-level: true
        _testCredentialsSkip(true, "true", "true", false);
    }

    private void _testCredentialsSkip(boolean skipSite, String skipJob, String skipAction, boolean expectingTokens)
            throws Exception {
        String actionLevelSkipConf = (skipAction == null) ? "" :
                "<property><name>oozie.credentials.skip</name><value>" + skipAction + "</value></property>";
        String actionxml = "<pig>" + "<job-tracker>${jobTracker}</job-tracker>" + "<name-node>${nameNode}</name-node>"
                + "<prepare>" + "<delete path='outputdir' />" + "</prepare>" + "<configuration>" + "<property>"
                + "<name>mapred.compress.map.output</name>" + "<value>true</value>" + "</property>" + "<property>"
                + "<name>mapred.job.queue.name</name>" + "<value>${queueName}</value>" + "</property>" + actionLevelSkipConf
                + "</configuration>" + "<script>org/apache/oozie/examples/pig/id.pig</script>"
                + "<param>INPUT=${inputDir}</param>" + "<param>OUTPUT=${outputDir}/pig-output</param>" + "</pig>";
        String workflowXml = "<workflow-app xmlns='uri:oozie:workflow:0.2.5' name='pig-wf'>" + "<credentials>"
                + "<credential name='abcname' type='abc'>" + "<property>" + "<name>property1</name>"
                + "<value>value1</value>" + "</property>" + "<property>" + "<name>property2</name>"
                + "<value>value2</value>" + "</property>" + "<property>" + "<name>${property3}</name>"
                + "<value>${value3}</value>" + "</property>" + "</credential>" + "</credentials>"
                + "<start to='pig1' />" + "<action name='pig1' cred='abcname'>" + actionxml
                + "<ok to='end' />" + "<error to='fail' />" + "</action>" + "<kill name='fail'>"
                + "<message>Pig failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>" + "</kill>"
                + "<end name='end' />" + "</workflow-app>";

        JavaActionExecutor ae = new JavaActionExecutor();
        WorkflowJobBean wfBean = addRecordToWfJobTable("test1", workflowXml,
                (skipJob == null) ? null : Collections.singletonMap("oozie.credentials.skip", skipJob));
        WorkflowActionBean action = (WorkflowActionBean) wfBean.getActions().get(0);
        action.setType(ae.getType());
        action.setCred("abcname");
        action.setConf(actionxml);
        Context context = new Context(wfBean, action);

        Element actionXmlconf = XmlUtils.parseXml(action.getConf());
        // action job configuration
        Configuration actionConf = ae.createBaseHadoopConf(context, actionXmlconf);
        actionConf = ae.setupActionConf(actionConf, context, actionXmlconf, new Path("/tmp/foo"));

        // Define 'abc' token type in oozie-site
        ConfigurationService.set("oozie.credentials.credentialclasses", "abc=org.apache.oozie.action.hadoop.InsertTestToken");
        ConfigurationService.setBoolean("oozie.credentials.skip", skipSite);
        // Setting the credential properties in launcher conf
        Map<String, CredentialsProperties> credProperties = ae.setCredentialPropertyToActionConf(context,
                action, actionConf);

        // Try to load the token without it being defined in oozie-site; should get an exception
        JobConf credentialsConf = new JobConf();
        Credentials credentials = new Credentials();
        Configuration launcherConf = ae.createBaseHadoopConf(context, actionXmlconf);
        XConfiguration.copy(launcherConf, credentialsConf);
        ae.setCredentialTokens(credentials, credentialsConf, context, action, credProperties);
        Token<? extends TokenIdentifier> tk = credentials.getToken(new Text("ABC Token"));
        if (expectingTokens) {
            assertNotNull(tk);
        } else {
            assertNull(tk);
        }
    }

    private WorkflowJobBean addRecordToWfJobTable(String wfId, String wfxml) throws Exception {
        return addRecordToWfJobTable(wfId, wfxml, null);
    }

    private WorkflowJobBean addRecordToWfJobTable(String wfId, String wfxml, Map<String, String> otherProps) throws Exception {
        WorkflowApp app = new LiteWorkflowApp("testApp", wfxml,
            new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class, "start")).
                addNode(new EndNodeDef("end", LiteWorkflowStoreService.LiteControlNodeHandler.class));
        Configuration conf = getHadoopAccessorService().
                createConfiguration(new URI(getNameNodeUri()).getAuthority());
        conf.set(OozieClient.APP_PATH, getNameNodeUri() + "/testPath");
        conf.set(OozieClient.LOG_TOKEN, "testToken");
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("property3", "prop3");
        conf.set("value3", "val3");
        if (otherProps != null) {
            for (Map.Entry<String, String> ent : otherProps.entrySet()) {
                conf.set(ent.getKey(), ent.getValue());
            }
        }

        WorkflowJobBean wfBean = createWorkflow(app, conf, "auth");
        wfBean.setId(wfId);
        wfBean.setStatus(WorkflowJob.Status.SUCCEEDED);
        WorkflowActionBean action = new WorkflowActionBean();
        action.setName("test");
        action.setId(Services.get().get(UUIDService.class).generateChildId(wfBean.getId(), "test"));
        wfBean.getActions().add(action);
        return wfBean;
    }

    private WorkflowJobBean createWorkflow(WorkflowApp app, Configuration conf, String authToken) throws Exception {
        WorkflowAppService wps = Services.get().get(WorkflowAppService.class);
        Configuration protoActionConf = wps.createProtoActionConf(conf, true);
        WorkflowLib workflowLib = Services.get().get(WorkflowStoreService.class).getWorkflowLibWithNoDB();
        WorkflowInstance wfInstance;
        wfInstance = workflowLib.createInstance(app, conf);
        WorkflowJobBean workflow = new WorkflowJobBean();
        workflow.setId(wfInstance.getId());
        workflow.setAppName(app.getName());
        workflow.setAppPath(conf.get(OozieClient.APP_PATH));
        workflow.setConf(XmlUtils.prettyPrint(conf).toString());
        workflow.setProtoActionConf(XmlUtils.prettyPrint(protoActionConf).toString());
        workflow.setCreatedTime(new Date());
        workflow.setLogToken(conf.get(OozieClient.LOG_TOKEN, ""));
        workflow.setStatus(WorkflowJob.Status.PREP);
        workflow.setRun(0);
        workflow.setUser(conf.get(OozieClient.USER_NAME));
        workflow.setGroup(conf.get(OozieClient.GROUP_NAME));
        workflow.setWorkflowInstance(wfInstance);
        return workflow;
    }

    public void testActionSharelibResolution() throws Exception {
        JavaActionExecutor ae = new JavaActionExecutor() {
            @Override
            protected String getDefaultShareLibName(Element actionXml) {
                return "java-action-executor";
            }
        };

        WorkflowJobBean wfBean = new WorkflowJobBean();
        String jobConf = "<configuration/>";
        wfBean.setConf(jobConf);

        WorkflowActionBean action = new WorkflowActionBean();
        Context context = new Context(wfBean, action);

        Configuration actionConf = new XConfiguration();

        Assert.assertArrayEquals(new String[] { "java-action-executor" },
                ae.getShareLibNames(context, new Element("java"), actionConf));

        ConfigurationService.set("oozie.action.sharelib.for.java", "java-oozie-conf");
        Assert.assertArrayEquals(new String[] { "java-oozie-conf" },
                ae.getShareLibNames(context, new Element("java"), actionConf));

        jobConf = "<configuration>" + "<property>"
               + "<name>oozie.action.sharelib.for.java</name>"
               + "<value>java-job-conf</value>" + "</property>"
               + "</configuration>";
        wfBean.setConf(jobConf);
        ae = new JavaActionExecutor() {
            @Override
            protected String getDefaultShareLibName(Element actionXml) {
                return "java-action-executor";
            }
        };
        Assert.assertArrayEquals(new String[] { "java-job-conf" },
                ae.getShareLibNames(context, new Element("java"), actionConf));

        actionConf.set("oozie.action.sharelib.for.java", "java-action-conf");
        Assert.assertArrayEquals(new String[] { "java-action-conf" },
                ae.getShareLibNames(context, new Element("java"), actionConf));
    }

    public void testJavaOpts() throws Exception {
        String actionXml = "<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>"
                + "<configuration>" + "<property><name>oozie.launcher.a</name><value>LA</value></property>"
                + "<property><name>a</name><value>AA</value></property>"
                + "<property><name>b</name><value>BB</value></property>" + "</configuration>"
                + "<main-class>MAIN-CLASS</main-class>" + "<java-opts>JAVA-OPT1 JAVA-OPT2</java-opts>"
                + "<arg>A1</arg>" + "<arg>A2</arg>" + "<file>f.jar</file>" + "<archive>a.tar</archive>" + "</java>";

        JavaActionExecutor ae = new JavaActionExecutor();

        WorkflowJobBean wfBean = addRecordToWfJobTable("test1", actionXml);
        WorkflowActionBean action = (WorkflowActionBean) wfBean.getActions().get(0);
        action.setType(ae.getType());
        action.setConf(actionXml);

        Context context = new Context(wfBean, action);

        Element actionXmlconf = XmlUtils.parseXml(action.getConf());

        Configuration actionConf = ae.createBaseHadoopConf(context, actionXmlconf);

        Configuration conf = ae.createLauncherConf(getFileSystem(), context, action, actionXmlconf, actionConf);

        assertEquals("-Xmx200m JAVA-OPT1 JAVA-OPT2", conf.get(MAPRED_CHILD_JAVA_OPTS));
        assertEquals("-Xmx200m JAVA-OPT1 JAVA-OPT2", conf.get(MAPREDUCE_MAP_JAVA_OPTS));

        actionXml = "<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>"
                + "<configuration>" + "<property><name>oozie.launcher.a</name><value>LA</value></property>"
                + "<property><name>a</name><value>AA</value></property>"
                + "<property><name>b</name><value>BB</value></property>" + "</configuration>"
                + "<main-class>MAIN-CLASS</main-class>" + "<java-opt>JAVA-OPT1</java-opt>"
                + "<java-opt>JAVA-OPT2</java-opt>" + "<arg>A1</arg>" + "<arg>A2</arg>" + "<file>f.jar</file>"
                + "<archive>a.tar</archive>" + "</java>";

        wfBean = addRecordToWfJobTable("test1", actionXml);
        action = (WorkflowActionBean) wfBean.getActions().get(0);
        action.setType(ae.getType());
        action.setConf(actionXml);

        context = new Context(wfBean, action);

        actionXmlconf = XmlUtils.parseXml(action.getConf());

        actionConf = ae.createBaseHadoopConf(context, actionXmlconf);

        conf = ae.createLauncherConf(getFileSystem(), context, action, actionXmlconf, actionConf);

        assertEquals("-Xmx200m JAVA-OPT1 JAVA-OPT2", conf.get(MAPRED_CHILD_JAVA_OPTS));
        assertEquals("-Xmx200m JAVA-OPT1 JAVA-OPT2", conf.get(MAPREDUCE_MAP_JAVA_OPTS));

        actionXml = "<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>"
                + "<configuration>" + "<property><name>oozie.launcher.a</name><value>LA</value></property>"
                + "<property><name>a</name><value>AA</value></property>"
                + "<property><name>b</name><value>BB</value></property>"
                + "<property><name>oozie.launcher.mapred.child.java.opts</name><value>JAVA-OPT3</value></property>"
                + "</configuration>"
                + "<main-class>MAIN-CLASS</main-class>" + "<java-opt>JAVA-OPT1</java-opt>"
                + "<java-opt>JAVA-OPT2</java-opt>" + "<arg>A1</arg>" + "<arg>A2</arg>" + "<file>f.jar</file>"
                + "<archive>a.tar</archive>" + "</java>";

        wfBean = addRecordToWfJobTable("test1", actionXml);
        action = (WorkflowActionBean) wfBean.getActions().get(0);
        action.setType(ae.getType());
        action.setConf(actionXml);

        context = new Context(wfBean, action);

        actionXmlconf = XmlUtils.parseXml(action.getConf());

        actionConf = ae.createBaseHadoopConf(context, actionXmlconf);

        conf = ae.createLauncherConf(getFileSystem(), context, action, actionXmlconf, actionConf);

        assertEquals("JAVA-OPT3 JAVA-OPT1 JAVA-OPT2", conf.get(MAPRED_CHILD_JAVA_OPTS));
        assertEquals("JAVA-OPT3 JAVA-OPT1 JAVA-OPT2", conf.get(MAPREDUCE_MAP_JAVA_OPTS));

        actionXml = "<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>"
                + "<configuration>" + "<property><name>oozie.launcher.a</name><value>LA</value></property>"
                + "<property><name>a</name><value>AA</value></property>"
                + "<property><name>b</name><value>BB</value></property>"
                + "<property><name>oozie.launcher.mapreduce.map.java.opts</name><value>JAVA-OPT3</value></property>"
                + "</configuration>"
                + "<main-class>MAIN-CLASS</main-class>" + "<java-opt>JAVA-OPT1</java-opt>"
                + "<java-opt>JAVA-OPT2</java-opt>" + "<arg>A1</arg>" + "<arg>A2</arg>" + "<file>f.jar</file>"
                + "<archive>a.tar</archive>" + "</java>";

        wfBean = addRecordToWfJobTable("test1", actionXml);
        action = (WorkflowActionBean) wfBean.getActions().get(0);
        action.setType(ae.getType());
        action.setConf(actionXml);

        context = new Context(wfBean, action);

        actionXmlconf = XmlUtils.parseXml(action.getConf());

        actionConf = ae.createBaseHadoopConf(context, actionXmlconf);

        conf = ae.createLauncherConf(getFileSystem(), context, action, actionXmlconf, actionConf);

        assertEquals("-Xmx200m JAVA-OPT3 JAVA-OPT1 JAVA-OPT2", conf.get(MAPRED_CHILD_JAVA_OPTS));
        assertEquals("-Xmx200m JAVA-OPT3 JAVA-OPT1 JAVA-OPT2", conf.get(MAPREDUCE_MAP_JAVA_OPTS));

        actionXml = "<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>"
                + "<configuration>" + "<property><name>oozie.launcher.a</name><value>LA</value></property>"
                + "<property><name>a</name><value>AA</value></property>"
                + "<property><name>b</name><value>BB</value></property>"
                + "<property><name>oozie.launcher.mapred.child.java.opts</name><value>JAVA-OPT3</value></property>"
                + "<property><name>oozie.launcher.mapreduce.map.java.opts</name><value>JAVA-OPT4</value></property>"
                + "</configuration>"
                + "<main-class>MAIN-CLASS</main-class>" + "<java-opt>JAVA-OPT1</java-opt>"
                + "<java-opt>JAVA-OPT2</java-opt>" + "<arg>A1</arg>" + "<arg>A2</arg>" + "<file>f.jar</file>"
                + "<archive>a.tar</archive>" + "</java>";

        wfBean = addRecordToWfJobTable("test1", actionXml);
        action = (WorkflowActionBean) wfBean.getActions().get(0);
        action.setType(ae.getType());
        action.setConf(actionXml);

        context = new Context(wfBean, action);

        actionXmlconf = XmlUtils.parseXml(action.getConf());

        actionConf = ae.createBaseHadoopConf(context, actionXmlconf);

        conf = ae.createLauncherConf(getFileSystem(), context, action, actionXmlconf, actionConf);

        assertEquals("JAVA-OPT3 JAVA-OPT4 JAVA-OPT1 JAVA-OPT2", conf.get(MAPRED_CHILD_JAVA_OPTS));
        assertEquals("JAVA-OPT3 JAVA-OPT4 JAVA-OPT1 JAVA-OPT2", conf.get(MAPREDUCE_MAP_JAVA_OPTS));
    }

    public void testAddShareLibSchemeAndAuthority() throws Exception {
        JavaActionExecutor ae = new JavaActionExecutor() {
            @Override
            public String getDefaultShareLibName(Element actionXml) {
                return "java-action-executor";
            }
        };
        String actionXml = "<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNode2Uri() + "</name-node>" + "<main-class>" + LauncherMainTester.class.getName()
                + "</main-class>" + "</java>";
        Element eActionXml = XmlUtils.parseXml(actionXml);
        Context context = createContext(actionXml, null);

        // Set sharelib to a relative path (i.e. no scheme nor authority)
        Services.get().destroy();
        setSystemProperty(WorkflowAppService.SYSTEM_LIB_PATH, "/user/" + getTestUser()+ "/share/");
        new Services().init();
        // Create the dir
        WorkflowAppService wps = Services.get().get(WorkflowAppService.class);
        Path systemLibPath = getNewSystemLibPath();
        Path javaShareLibPath = new Path(systemLibPath, "java-action-executor");
        getFileSystem().mkdirs(javaShareLibPath);
        Services.get().setService(ShareLibService.class);

        Configuration conf = ae.createBaseHadoopConf(context, eActionXml);
        // Despite systemLibPath is not fully qualified and the action refers to the
        // second namenode the next line won't throw exception because default fs is used
        ae.addShareLib(conf, new String[] { "java-action-executor" });

        // Set sharelib to a full path (i.e. include scheme and authority)
        Services.get().destroy();
        setSystemProperty(WorkflowAppService.SYSTEM_LIB_PATH, getNameNodeUri() + "/user/" + getTestUser() + "/share/");
        new Services().init();
        Services.get().setService(ShareLibService.class);
        conf = ae.createBaseHadoopConf(context, eActionXml);
        // The next line should not throw an Exception because it will get the scheme and authority from the sharelib path
        ae.addShareLib(conf, new String[] { "java-action-executor" });
    }

    public void testFilesystemScheme() throws Exception {
        try {
            String actionXml = "<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                    + getNameNodeUri() + "</name-node>" + "<main-class>" + LauncherMainTester.class.getName()
                    + "</main-class>" + "</java>";
            Element eActionXml = XmlUtils.parseXml(actionXml);
            Context context = createContext(actionXml, null);
            Path appPath = new Path("localfs://namenode:port/mydir");
            JavaActionExecutor ae = new JavaActionExecutor();
            Configuration conf = ae.createBaseHadoopConf(context, eActionXml);
            Services.get().destroy();
            setSystemProperty(HadoopAccessorService.SUPPORTED_FILESYSTEMS, "hdfs,viewfs");
            new Services().init();
            ae.setupActionConf(conf, context, eActionXml, appPath);

            fail("Supposed to throw exception due to unsupported fs scheme - localfs");
        }
        catch (ActionExecutorException ae) {
            assertTrue(ae.getMessage().contains("E0904"));
            assertTrue(ae.getMessage().contains("Scheme [localfs] not supported"));
        }
    }

    public void testACLDefaults_launcherACLsSetToDefault() throws Exception {
        // CASE: launcher specific ACLs not configured - set defaults
        String actionXml = "<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node> <configuration>" +
                "<property><name>mapreduce.job.acl-view-job</name><value>VIEWER</value></property>" +
                "<property><name>mapreduce.job.acl-modify-job</name><value>MODIFIER</value></property>" +
                "</configuration>" + "<main-class>MAIN-CLASS</main-class>" +
                "</java>";

        WorkflowJobBean wfBean = addRecordToWfJobTable("test1-acl", actionXml);
        WorkflowActionBean action = (WorkflowActionBean) wfBean.getActions().get(0);
        JavaActionExecutor ae = new JavaActionExecutor();
        action.setType(ae.getType());
        Context context = new Context(wfBean, action);

        Element eActionXml = XmlUtils.parseXml(actionXml);

        Configuration actionConf = ae.createBaseHadoopConf(context, eActionXml);
        ae.setupActionConf(actionConf, context, eActionXml, getAppPath());
        Configuration conf = ae.createLauncherConf(getFileSystem(), context, action, eActionXml, actionConf);

        assertEquals("VIEWER", conf.get(JavaActionExecutor.ACL_VIEW_JOB));
        assertEquals("MODIFIER", conf.get(JavaActionExecutor.ACL_MODIFY_JOB));
    }

    public void testACLDefaults_noFalseChange() throws Exception {
        // CASE: launcher specific ACLs configured, but MR job ACLs not configured i.e. null. Check for no false changes to null
        String actionXml = "<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node> <configuration>" +
                "<property><name>oozie.launcher.mapreduce.job.acl-view-job</name><value>V</value></property>" +
                "<property><name>oozie.launcher.mapreduce.job.acl-modify-job</name><value>M</value></property>" +
                "</configuration>" + "<main-class>MAIN-CLASS</main-class>" +
                "</java>";

        WorkflowJobBean wfBean = addRecordToWfJobTable("test2-acl", actionXml);
        WorkflowActionBean action = (WorkflowActionBean) wfBean.getActions().get(0);
        JavaActionExecutor ae = new JavaActionExecutor();
        action.setType(ae.getType());
        Context context = new Context(wfBean, action);

        Element eActionXml = XmlUtils.parseXml(actionXml);

        Configuration actionConf = ae.createBaseHadoopConf(context, eActionXml);
        ae.setupActionConf(actionConf, context, eActionXml, getAppPath());
        Configuration conf = ae.createLauncherConf(getFileSystem(), context, action, eActionXml, actionConf);

        assertNotNull(conf.get(JavaActionExecutor.ACL_VIEW_JOB));
        assertNotNull(conf.get(JavaActionExecutor.ACL_MODIFY_JOB));
    }

    public void testACLDefaults_explicitLauncherAndActionSettings() throws Exception {
        // CASE: launcher specific ACLs configured, as well as MR job ACLs configured. Check that NO overriding with defaults
        String actionXml = "<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node> <configuration>" +
                "<property><name>oozie.launcher.mapreduce.job.acl-view-job</name><value>V</value></property>" +
                "<property><name>oozie.launcher.mapreduce.job.acl-modify-job</name><value>M</value></property>" +
                "<property><name>mapreduce.job.acl-view-job</name><value>VIEWER</value></property>" +
                "<property><name>mapreduce.job.acl-modify-job</name><value>MODIFIER</value></property>" +
                "</configuration>" + "<main-class>MAIN-CLASS</main-class>" +
                "</java>";

        WorkflowJobBean wfBean = addRecordToWfJobTable("test3-acl", actionXml);
        WorkflowActionBean action = (WorkflowActionBean) wfBean.getActions().get(0);
        JavaActionExecutor ae = new JavaActionExecutor();
        action.setType(ae.getType());
        Context context = new Context(wfBean, action);

        Element eActionXml = XmlUtils.parseXml(actionXml);

        Configuration actionConf = ae.createBaseHadoopConf(context, eActionXml);
        ae.setupActionConf(actionConf, context, eActionXml, getAppPath());
        Configuration conf = ae.createLauncherConf(getFileSystem(), context, action, eActionXml, actionConf);

        assertNotSame(conf.get(JavaActionExecutor.ACL_VIEW_JOB), actionConf.get(JavaActionExecutor.ACL_VIEW_JOB));
        assertNotSame(conf.get(JavaActionExecutor.ACL_MODIFY_JOB), actionConf.get(JavaActionExecutor.ACL_MODIFY_JOB));
    }

    // Only "dummyuser" can kill the app, but we try "test2" instead
    public void testCannotKillActionWhenACLSpecified() throws Exception {
        // Only "dummyuser" and the owner have the permission to kill the job
        String actionXml = "<java>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<configuration>" +
                "  <property><name>oozie.launcher.modify.acl</name><value>dummyuser</value></property>" +
                "</configuration>" +
                "<main-class>" + LauncherMainTester.class.getName() + "</main-class>" +
                "<arg>sleep</arg>" +
                "<arg>10000</arg>" +
                "</java>";

        // Submitting WF as the default "test" user
        Context context = createContext(actionXml, null);
        String applicationId = submitAction(context);
        waitUntilYarnAppState(applicationId, EnumSet.of(YarnApplicationState.RUNNING));

        // Modify the user inside the workflow. It's necessary because the Yarn client is always
        // retrieved for the owner of the WF and owner always has the permission to kill its
        // own application
        WorkflowJobBean wfBean = (WorkflowJobBean) context.getWorkflow();
        wfBean.setUser(getTestUser2());

        JavaActionExecutor jae = new JavaActionExecutor();
        jae.kill(context, context.getAction());

        // Kill should fail - wait until the application is finished
        waitUntilYarnAppDoneAndAssertSuccess(applicationId);
    }

    public void testParseJobXmlAndConfiguration() throws Exception {
        String str = "<java>"
                + "<job-xml>job1.xml</job-xml>"
                + "<job-xml>job2.xml</job-xml>"
                + "<configuration>"
                + "<property><name>p1</name><value>v1a</value></property>"
                + "<property><name>p2</name><value>v2</value></property>"
                + "</configuration>"
                + "</java>";
        Element xml = XmlUtils.parseXml(str);
        Path appPath = new Path(getFsTestCaseDir(), "app");
        getFileSystem().mkdirs(appPath);

        XConfiguration jConf = new XConfiguration();
        jConf.set("p1", "v1b");
        jConf.set("p3", "v3a");
        OutputStream os = getFileSystem().create(new Path(appPath, "job1.xml"));
        jConf.writeXml(os);
        os.close();

        jConf = new XConfiguration();
        jConf.set("p4", "v4");
        jConf.set("p3", "v3b");
        os = getFileSystem().create(new Path(appPath, "job2.xml"));
        jConf.writeXml(os);
        os.close();

        Configuration conf = new XConfiguration();
        assertEquals(0, conf.size());
        JavaActionExecutor.parseJobXmlAndConfiguration(createContext("<java/>", null), xml, appPath, conf);
        assertEquals(4, conf.size());
        assertEquals("v1a", conf.get("p1"));
        assertEquals("v2", conf.get("p2"));
        assertEquals("v3b", conf.get("p3"));
        assertEquals("v4", conf.get("p4"));
    }

    public void testParseJobXmlAndConfigurationWithELExpression() throws Exception {
        String str = "<java>"
                + "<job-xml>job1.xml</job-xml>"
                + "<job-xml>job2.xml</job-xml>"
                + "<configuration>"
                + "<property><name>p1</name><value>v1a</value></property>"
                + "<property><name>p2</name><value>v2</value></property>"
                + "</configuration>"
                + "</java>";
        Element xml = XmlUtils.parseXml(str);
        Path appPath = new Path(getFsTestCaseDir(), "app");
        getFileSystem().mkdirs(appPath);

        XConfiguration jConf = new XConfiguration();
        jConf.set("p3", "${v3}");
        jConf.set("p4", "${v4}");
        jConf.set("user", "${wf:user()}");
        OutputStream os = getFileSystem().create(new Path(appPath, "job1.xml"));
        jConf.writeXml(os);
        os.close();

        jConf = new XConfiguration();
        jConf.set("p5", "v5");
        jConf.set("p6", "v6");
        os = getFileSystem().create(new Path(appPath, "job2.xml"));
        jConf.writeXml(os);
        os.close();

        Configuration conf = new XConfiguration();
        assertEquals(0, conf.size());

        JavaActionExecutor.parseJobXmlAndConfiguration(createContext("<configuration>" +
                "<property><name>v3</name><value>v3a</value></property>" +
                "<property><name>v4</name><value>v4a</value></property>" +
                "</configuration>", null), xml, appPath, conf);
        assertEquals(7, conf.size());
        assertEquals("v1a", conf.get("p1"));
        assertEquals("v2", conf.get("p2"));
        assertEquals("v3a", conf.get("p3"));
        assertEquals("v4a", conf.get("p4"));
        assertEquals("v5", conf.get("p5"));
        assertEquals("v6", conf.get("p6"));
        assertEquals("test", conf.get("user"));
    }

    public void testJobXmlWithOozieLauncher() throws Exception {
        String str = "<java>"
                + "<job-xml>job.xml</job-xml>"
                + "<configuration>"
                + "<property><name>oozie.launcher.p2</name><value>v2b</value></property>"
                + "<property><name>p4</name><value>v4</value></property>"
                + "</configuration>"
                + "</java>";
        Element xml = XmlUtils.parseXml(str);
        Path appPath = new Path(getFsTestCaseDir(), "app");
        getFileSystem().mkdirs(appPath);

        XConfiguration jConf = new XConfiguration();
        jConf.set("oozie.launcher.p1", "v1");
        jConf.set("oozie.launcher.p2", "v2a");
        jConf.set("p3", "v3");
        OutputStream os = getFileSystem().create(new Path(appPath, "job.xml"));
        jConf.writeXml(os);
        os.close();

        Configuration conf = new XConfiguration();
        assertEquals(0, conf.size());
        JavaActionExecutor jae = new JavaActionExecutor("java");
        jae.setupLauncherConf(conf, xml, appPath, createContext("<java/>", null));
        assertEquals(4, conf.size());
        assertEquals("v1", conf.get("oozie.launcher.p1"));
        assertEquals("v1", conf.get("p1"));
        assertEquals("v2b", conf.get("oozie.launcher.p2"));
        assertEquals("v2b", conf.get("p2"));
    }

    public void testUpdateConfForTimeLineServiceEnabled() throws Exception {
        Element actionXml = XmlUtils
                .parseXml("<java>"
                        + "<job-tracker>"
                        + getJobTrackerUri()
                        + "</job-tracker>"
                        + "<name-node>"
                        + getNameNodeUri()
                        + "</name-node>"
                        + "<main-class>MAIN-CLASS</main-class>"
                        + "</java>");
        JavaActionExecutor ae = new JavaActionExecutor();
        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());
        WorkflowJobBean wf = createBaseWorkflow(protoConf, "action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());
        Context context = new Context(wf, action);
        Configuration actionConf = new JobConf();

        // Test when server side setting is not enabled
        Configuration launcherConf = ae.createLauncherConf(getFileSystem(), context, action, actionXml, actionConf);
        // disabled by default
        assertEquals("false", launcherConf.get(JavaActionExecutor.HADOOP_YARN_TIMELINE_SERVICE_ENABLED));

        ConfigurationService.set("oozie.action.launcher." + JavaActionExecutor.HADOOP_YARN_TIMELINE_SERVICE_ENABLED, "true");

        // Test when server side setting is enabled but tez-site.xml is not in DistributedCache
        launcherConf = ae.createLauncherConf(getFileSystem(), context, action, actionXml, actionConf);
        assertEquals("false", launcherConf.get(JavaActionExecutor.HADOOP_YARN_TIMELINE_SERVICE_ENABLED));

        final Path tezSite = new Path("/tmp/tez-site.xml");
        final FSDataOutputStream out = getFileSystem().create(tezSite);
        out.close();

        // Test when server side setting is enabled and tez-site.xml is in DistributedCache
        Element actionXmlWithTez = XmlUtils
                .parseXml("<java>"
                        + "<job-tracker>"
                        + getJobTrackerUri()
                        + "</job-tracker>"
                        + "<name-node>"
                        + getNameNodeUri()
                        + "</name-node>"
                        + "<main-class>MAIN-CLASS</main-class>"
                        + "<file>" + tezSite + "</file>"
                        + "</java>");
        launcherConf = ae.createLauncherConf(getFileSystem(), context, action, actionXmlWithTez, actionConf);
        assertTrue(launcherConf.getBoolean(JavaActionExecutor.HADOOP_YARN_TIMELINE_SERVICE_ENABLED, false));

        // Test when server side setting is enabled, tez-site.xml is in DistributedCache
        // but user has disabled in action configuration
        Element actionXmlATSDisabled = XmlUtils
                .parseXml("<java>"
                        + "<job-tracker>"
                        + getJobTrackerUri()
                        + "</job-tracker>"
                        + "<name-node>"
                        + getNameNodeUri()
                        + "</name-node>"
                        + "<configuration>"
                        + "<property><name>oozie.launcher.yarn.timeline-service.enabled</name>"
                        + "<value>false</value></property>"
                        + "</configuration>"
                        + "<main-class>MAIN-CLASS</main-class>"
                        + "<file>" + tezSite + "</file>"
                        + "</java>");
        actionConf = ae.createBaseHadoopConf(context, actionXmlATSDisabled);
        ae.setupActionConf(actionConf, context, actionXmlATSDisabled, new Path("hdfs:///tmp/workflow"));
        launcherConf = ae.createLauncherConf(getFileSystem(), context, action, actionXmlATSDisabled, actionConf);
        assertFalse(launcherConf.getBoolean(JavaActionExecutor.HADOOP_YARN_TIMELINE_SERVICE_ENABLED, false));

        getFileSystem().delete(tezSite, true);
    }

    public void testAddToCache() throws Exception {
        JavaActionExecutor ae = new JavaActionExecutor();
        Configuration conf = new XConfiguration();

        Path appPath = new Path(getFsTestCaseDir(), "wf");
        URI appUri = appPath.toUri();

        // test archive without fragment
        Path archivePath = new Path("test.jar");
        Path archiveFullPath = new Path(appPath, archivePath);
        ae.addToCache(conf, appPath, archiveFullPath.toString(), true);
        assertTrue(conf.get("mapred.cache.archives").contains(archiveFullPath.toString()));
        assertTrue(DistributedCache.getSymlink(conf));

        // test archive with fragment
        Path archiveFragmentPath = new Path("test.jar#a.jar");
        Path archiveFragmentFullPath = new Path(appPath, archiveFragmentPath);
        conf.clear();
        ae.addToCache(conf, appPath, archiveFragmentFullPath.toString(), true);
        assertTrue(conf.get("mapred.cache.archives").contains(archiveFragmentFullPath.toString()));
        assertTrue(DistributedCache.getSymlink(conf));

        // test .so without fragment
        Path appSoPath = new Path("lib/a.so");
        Path appSoFullPath = new Path(appPath, appSoPath);
        conf.clear();
        ae.addToCache(conf, appPath, appSoFullPath.toString(), false);
        assertTrue(conf.get("mapred.cache.files").contains(appSoFullPath.toString()));
        assertTrue(DistributedCache.getSymlink(conf));

        // test .so with fragment
        Path appSoFragmentPath = new Path("lib/a.so#a.so");
        Path appSoFragmentFullPath = new Path(appPath, appSoFragmentPath);
        conf.clear();
        ae.addToCache(conf, appPath, appSoFragmentFullPath.toString(), false);
        assertTrue(conf.get("mapred.cache.files").contains(appSoFragmentFullPath.toString()));
        assertTrue(Arrays.asList(conf.get("mapred.cache.files").split(",")).contains(appSoFragmentFullPath.toString()));
        assertTrue(DistributedCache.getSymlink(conf));

        // test .jar without fragment where app path is on same cluster as jar path
        Path appJarPath = new Path("lib/a.jar");
        Path appJarFullPath = new Path(appPath, appJarPath);
        conf = new Configuration();
        conf.set(WorkflowAppService.HADOOP_USER, getTestUser());
        ae.addToCache(conf, appPath, appJarFullPath.toString(), false);
        // assert that mapred.cache.files contains jar URI path (full on Hadoop-2)
        Path jarPath = new Path(appJarFullPath.toUri());
        assertTrue(conf.get("mapred.cache.files").contains(jarPath.toString()));
        // assert that dist cache classpath contains jar URI path
        Path[] paths = DistributedCache.getFileClassPaths(conf);
        boolean pathFound = false;
        for (Path path : paths) {
          if (path.equals(jarPath)) {
            pathFound = true;
            break;
          }
        }
        assertTrue(pathFound);
        assertTrue(DistributedCache.getSymlink(conf));

        // test .jar without fragment where app path is on a different cluster than jar path
        appJarPath = new Path("lib/a.jar");
        appJarFullPath = new Path(appPath, appJarPath);
        Path appDifferentClusterPath = new Path(new URI(appUri.getScheme(), null, appUri.getHost() + "x",
            appUri.getPort(), appUri.getPath(), appUri.getQuery(), appUri.getFragment()));
        conf.clear();
        conf.set(WorkflowAppService.HADOOP_USER, getTestUser());
        ae.addToCache(conf, appDifferentClusterPath, appJarFullPath.toString(), false);
        // assert that mapred.cache.files contains absolute jar URI
        assertTrue(conf.get("mapred.cache.files").contains(appJarFullPath.toString()));
        assertTrue(DistributedCache.getSymlink(conf));

        // test .jar with fragment
        Path appJarFragmentPath = new Path("lib/a.jar#a.jar");
        Path appJarFragmentFullPath = new Path(appPath, appJarFragmentPath);
        conf.clear();
        conf.set(WorkflowAppService.HADOOP_USER, getTestUser());
        ae.addToCache(conf, appPath, appJarFragmentFullPath.toString(), false);
        assertTrue(conf.get("mapred.cache.files").contains(appJarFragmentFullPath.toString()));
        assertTrue(DistributedCache.getSymlink(conf));

        // test regular file without fragment
        Path appFilePath = new Path("lib/a.txt");
        Path appFileFullPath = new Path(appPath, appFilePath);
        conf.clear();
        ae.addToCache(conf, appPath, appFileFullPath.toString(), false);
        assertTrue(conf.get("mapred.cache.files").contains(appFileFullPath.toString()));
        assertTrue(DistributedCache.getSymlink(conf));

        // test regular file with fragment
        Path appFileFragmentPath = new Path("lib/a.txt#a.txt");
        Path appFileFragmentFullPath = new Path(appPath, appFileFragmentPath);
        conf.clear();
        ae.addToCache(conf, appPath, appFileFragmentFullPath.toString(), false);
        assertTrue(conf.get("mapred.cache.files").contains(appFileFragmentFullPath.toString()));
        assertTrue(DistributedCache.getSymlink(conf));

        // test path starting with "/" for archive
        Path testPath = new Path("/tmp/testpath/a.jar#a.jar");
        conf.clear();
        ae.addToCache(conf, appPath, testPath.toString(), true);
        assertTrue(conf.get("mapred.cache.archives").contains(testPath.toString()));
        assertTrue(DistributedCache.getSymlink(conf));

        // test path starting with "/" for cache.file
        conf.clear();
        ae.addToCache(conf, appPath, testPath.toString(), false);
        assertTrue(conf.get("mapred.cache.files").contains(testPath.toString()));
        assertTrue(DistributedCache.getSymlink(conf));

        // test absolute path for archive
        Path testAbsolutePath = new Path("hftp://namenode.test.com:8020/tmp/testpath/a.jar#a.jar");
        conf.clear();
        ae.addToCache(conf, appPath, testAbsolutePath.toString(), true);
        assertTrue(conf.get("mapred.cache.archives").contains(testAbsolutePath.toString()));
        assertTrue(DistributedCache.getSymlink(conf));

        // test absolute path for cache files
        conf.clear();
        ae.addToCache(conf, appPath, testAbsolutePath.toString(), false);
        assertTrue(conf.get("mapred.cache.files").contains(testAbsolutePath.toString()));
        assertTrue(DistributedCache.getSymlink(conf));

        // test relative path for archive
        conf.clear();
        ae.addToCache(conf, appPath, "lib/a.jar#a.jar", true);
        assertTrue(conf.get("mapred.cache.archives").contains(appUri.getPath() + "/lib/a.jar#a.jar"));
        assertTrue(DistributedCache.getSymlink(conf));

        // test relative path for cache files
        conf.clear();
        ae.addToCache(conf, appPath, "lib/a.jar#a.jar", false);
        assertTrue(conf.get("mapred.cache.files").contains(appUri.getPath() + "/lib/a.jar#a.jar"));
        assertTrue(DistributedCache.getSymlink(conf));
    }

    public void testJobXmlAndNonDefaultNamenode() throws Exception {
        // By default the job.xml file is taken from the workflow application
        // namenode, regadless the namenode specified for the action. To specify
        // a job.xml on another namenode use a fully qualified file path.

        Path appPath = new Path(getFsTestCaseDir(), "app");
        getFileSystem().mkdirs(appPath);

        Path jobXmlAbsolutePath = new Path(getFsTestCaseDir().toUri().getPath(), "jobxmlpath/job.xml");
        assertTrue(jobXmlAbsolutePath.isAbsolute() && jobXmlAbsolutePath.toUri().getAuthority() == null);
        Path jobXmlAbsolutePath2 = new Path(getFsTestCaseDir().toUri().getPath(), "jobxmlpath/job3.xml");
        assertTrue(jobXmlAbsolutePath2.isAbsolute() && jobXmlAbsolutePath2.toUri().getAuthority() == null);
        Path jobXmlQualifiedPath = new Path(getFs2TestCaseDir(), "jobxmlpath/job4.xml");
        assertTrue(jobXmlQualifiedPath.toUri().getAuthority() != null);

        // Use non-default name node (second filesystem) and three job-xml configurations:
        // 1. Absolute (but not fully qualified) path located in the first filesystem
        // 2. Without path (fist filesystem)
        // 3. Absolute (but not fully qualified) path located in the both filesystems
        //   (first should be used)
        // 4. Fully qualified path located in the second filesystem
        String str = "<java>"
                + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>"
                + "<name-node>" + getNameNode2Uri() + "</name-node>"
                + "<job-xml>" + jobXmlAbsolutePath.toString() + "</job-xml>"
                + "<job-xml>job2.xml</job-xml>"
                + "<job-xml>" + jobXmlAbsolutePath2.toString() + "</job-xml>"
                + "<job-xml>" + jobXmlQualifiedPath.toString() + "</job-xml>"
                + "<configuration>"
                + "<property><name>p1</name><value>v1a</value></property>"
                + "<property><name>p2</name><value>v2</value></property>"
                + "</configuration>"
                + "</java>";
        Element xml = XmlUtils.parseXml(str);

        XConfiguration jConf = new XConfiguration();
        jConf.set("p1", "v1b");
        jConf.set("p3", "v3a");
        OutputStream os = getFileSystem().create(jobXmlAbsolutePath);
        jConf.writeXml(os);
        os.close();

        jConf = new XConfiguration();
        jConf.set("p4", "v4");
        jConf.set("p3", "v3b");
        os = getFileSystem().create(new Path(appPath, "job2.xml"));
        jConf.writeXml(os);
        os.close();

        // This configuration is expected to be used
        jConf = new XConfiguration();
        jConf.set("p5", "v5a");
        jConf.set("p6", "v6a");
        os = getFileSystem().create(jobXmlAbsolutePath2);
        jConf.writeXml(os);
        os.close();

        // This configuration is expected to be ignored
        jConf = new XConfiguration();
        jConf.set("p5", "v5b");
        jConf.set("p6", "v6b");
        os = getFileSystem2().create(new Path(jobXmlAbsolutePath2.toUri().getPath()));
        jConf.writeXml(os);
        os.close();

        jConf = new XConfiguration();
        jConf.set("p7", "v7a");
        jConf.set("p8", "v8a");
        os = getFileSystem2().create(jobXmlQualifiedPath);
        jConf.writeXml(os);
        os.close();

        Context context = createContext("<java/>", null);
        Configuration conf = new JavaActionExecutor().createBaseHadoopConf(context, xml);
        int confSize0 = conf.size();
        JavaActionExecutor.parseJobXmlAndConfiguration(context, xml, appPath, conf);
        assertEquals(confSize0 + 8, conf.size());
        assertEquals("v1a", conf.get("p1"));
        assertEquals("v2", conf.get("p2"));
        assertEquals("v3b", conf.get("p3"));
        assertEquals("v4", conf.get("p4"));
        assertEquals("v5a", conf.get("p5"));
        assertEquals("v6a", conf.get("p6"));
        assertEquals("v7a", conf.get("p7"));
        assertEquals("v8a", conf.get("p8"));
    }

    public void testActionShareLibWithNonDefaultNamenode() throws Exception {

        WorkflowAppService wps = Services.get().get(WorkflowAppService.class);

        Path systemLibPath = getNewSystemLibPath();

        File jarFile = IOUtils.createJar(new File(getTestCaseDir()), "sourcejar.jar", LauncherMainTester.class);
        InputStream is = new FileInputStream(jarFile);
        Path javaShareLibPath = new Path(systemLibPath, "java");
        getFileSystem().mkdirs(javaShareLibPath);
        Path jar1Path = new Path(javaShareLibPath, "jar1.jar");
        OutputStream os1 = getFileSystem().create(jar1Path);
        IOUtils.copyStream(is, os1);
        Path jar2Path = new Path(javaShareLibPath, "jar2.jar");
        OutputStream os2 = getFileSystem().create(jar2Path);
        is = new FileInputStream(jarFile); // is not resetable
        IOUtils.copyStream(is, os2);
        Path launcherPath = new Path(systemLibPath, "oozie");
        getFileSystem().mkdirs(launcherPath);
        Path jar3Path = new Path(launcherPath, "jar3.jar");
        OutputStream os3 = getFileSystem().create(jar3Path);
        is = new FileInputStream(jarFile);
        IOUtils.copyStream(is, os3);

        String actionXml = "<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNode2Uri() + "</name-node>" +
                "<job-xml>job.xml</job-xml>" +
                "<main-class>"+ LauncherMainTester.class.getName() + "</main-class>" +
                "</java>";

        XConfiguration jConf = new XConfiguration();
        jConf.set("p", "v");
        OutputStream os = getFileSystem().create(new Path(getAppPath(), "job.xml"));
        jConf.writeXml(os);
        os.close();

        Context context = createContext(actionXml, null);

        Services.get().setService(ShareLibService.class);

        // Test oozie server action sharelib setting
        WorkflowJobBean workflow = (WorkflowJobBean) context.getWorkflow();
        XConfiguration wfConf = new XConfiguration();
        wfConf.set(WorkflowAppService.HADOOP_USER, getTestUser());
        wfConf.set(OozieClient.APP_PATH, new Path(getAppPath(), "workflow.xml").toString());
        wfConf.setBoolean(OozieClient.USE_SYSTEM_LIBPATH, true);
        workflow.setConf(XmlUtils.prettyPrint(wfConf).toString());

        ConfigurationService.set("oozie.action.sharelib.for.java", "java");

        final String runningJob = submitAction(context);
        waitUntilYarnAppDoneAndAssertSuccess(runningJob);
    }

    public void testJobSubmissionWithoutYarnKill() throws Exception {
        Path inputDir = new Path(getFsTestCaseDir(), "input");
        Path outputDir = new Path(getFsTestCaseDir(), "output");

        Writer w = new OutputStreamWriter(getFileSystem().create(new Path(inputDir, "data.txt")),
                StandardCharsets.UTF_8);
        w.write("dummy\n");
        w.write("dummy\n");
        w.close();

        w = new OutputStreamWriter(getFileSystem().create(new Path(inputDir, "id.pig")),
                StandardCharsets.UTF_8);
        w.write("A = load '$INPUT' using PigStorage(':');\n");
        w.write("store B into '$OUTPUT' USING PigStorage();\n");
        w.close();
        String actionXml = "<pig>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>" + "<prepare>" + "<delete path='outputdir' />" + "</prepare>"
                + "<configuration>" + "<property>" + "<name>mapred.compress.map.output</name>" + "<value>true</value>"
                + "</property>" + "<property>" + "<name>mapred.job.queue.name</name>" + "<value>default</value>"
                + "</property>" + "</configuration>" + "<script>" + inputDir.toString() + "/id.pig" + "</script>"
                + "<param>INPUT=" + inputDir.toUri().getPath() + "</param>" + "<param>OUTPUT="
                + outputDir.toUri().getPath() + "/pig-output</param>" + "</pig>";

        PigActionExecutor ae = new PigActionExecutor();
        WorkflowJobBean wfBean = addRecordToWfJobTable("test1", actionXml);
        WorkflowActionBean action = (WorkflowActionBean) wfBean.getActions().get(0);
        action.setType(ae.getType());
        action.setConf(actionXml);
        Context context = new Context(wfBean, action);

        ConfigurationService.setBoolean(JavaActionExecutor.HADOOP_YARN_KILL_CHILD_JOBS_ON_AMRESTART, false);

        final String runningJob = submitAction(context, ae);
        waitUntilYarnAppDoneAndAssertSuccess(runningJob);
    }

    public void testDefaultConfigurationInLauncher() throws Exception {
        JavaActionExecutor ae = new JavaActionExecutor();
        Element actionXmlWithConfiguration = XmlUtils.parseXml(
                "<java>" + "<job-tracker>" + getJobTrackerUri() +"</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<configuration>" +
                "<property><name>oozie.launcher.a</name><value>AA</value></property>" +
                "<property><name>b</name><value>BB</value></property>" +
                "</configuration>" +
                "<main-class>MAIN-CLASS</main-class>" +
                "</java>");
        Element actionXmlWithoutConfiguration = XmlUtils.parseXml(
                "<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<main-class>MAIN-CLASS</main-class>" +
                "</java>");

        Configuration conf = new Configuration(false);
        Assert.assertEquals(0, conf.size());
        conf.set(YARN_RESOURCEMANAGER_ADDRESS, getJobTrackerUri());
        ae.setupLauncherConf(conf, actionXmlWithConfiguration, null, null);
        assertEquals(getJobTrackerUri(), conf.get(YARN_RESOURCEMANAGER_ADDRESS));
        assertEquals("AA", conf.get("oozie.launcher.a"));
        assertEquals("AA", conf.get("a"));
        assertEquals("action.barbar", conf.get("oozie.launcher.action.foofoo"));
        assertEquals("action.barbar", conf.get("action.foofoo"));
        assertEquals("max-attempts", "1", conf.get("oozie.launcher.max.attempts"));
        assertEquals(6, conf.size());

        conf = new Configuration(false);
        Assert.assertEquals(0, conf.size());
        conf.set(YARN_RESOURCEMANAGER_ADDRESS, getJobTrackerUri());
        ae.setupLauncherConf(conf, actionXmlWithoutConfiguration, null, null);
        assertEquals(getJobTrackerUri(), conf.get(YARN_RESOURCEMANAGER_ADDRESS));
        assertEquals("action.barbar", conf.get("oozie.launcher.action.foofoo"));
        assertEquals("action.barbar", conf.get("action.foofoo"));
        assertEquals("max-attempts", "1", conf.get("oozie.launcher.max.attempts"));
        assertEquals(4, conf.size());
    }

    public void testDefaultConfigurationInActionConf() throws Exception {
        JavaActionExecutor ae = new JavaActionExecutor();
        final String dummyConfiguration = "<configuration>" +
                "<property><name>action.foo</name><value>AA</value></property>" +
                "</configuration>";
        String xmlStr = helper.getJavaActionXml(dummyConfiguration);
        Element actionXml = XmlUtils.parseXml(xmlStr);
        Context context = createContext(xmlStr, getTestGroup());
        Configuration conf = new Configuration(true);
        conf.set(YARN_RESOURCEMANAGER_ADDRESS, getJobTrackerUri());
        assertEquals("MapReduce's default value changed", "4", conf.get("mapreduce.map.maxattempts"));
        ae.setupActionConf(conf, context, actionXml, new Path(context.getWorkflow().getAppPath()));
        assertEquals("failed to inject property >action.foo<","AA", conf.get("action.foo"));
        assertEquals("failed to inject property >oozie.launcher.action.foofoo<","action.barbar",
                conf.get("oozie.launcher.action.foofoo"));
        assertEquals("Maxattempts should've been overwritten by setupActionConf", "1",
                conf.get("mapreduce.map.maxattempts"));
    }

    public void testGlobalConfigurationWithActionDefaults() throws Exception {
        try {
            String workflowUri = helper.createTestWorkflowXml(getWorkflowGlobalXml(), helper.getJavaActionXml(""));
            LocalOozie.start();
            final OozieClient wfClient = LocalOozie.getClient();
            Properties conf = wfClient.createConfiguration();
            conf.setProperty(OozieClient.APP_PATH, workflowUri);
            conf.setProperty(OozieClient.USER_NAME, getTestUser());
            conf.setProperty("appName", "var-app-name");
            final String jobId = wfClient.submit(conf);
            wfClient.start(jobId);
            WorkflowJob workflow = wfClient.getJobInfo(jobId);
            waitFor(20 * 1000, new Predicate() {
                @Override
                public boolean evaluate() throws Exception {
                    WorkflowAction javaAction = helper.getJavaAction(wfClient.getJobInfo(jobId));
                    return javaAction != null && !javaAction.getStatus().equals("PREP");
                }
            });
            final WorkflowAction workflowAction = helper.getJavaAction(workflow);
            Element eConf = XmlUtils.parseXml(workflowAction.getConf());
            Element element = eConf.getChild("configuration", eConf.getNamespace());
            Configuration actionConf = new XConfiguration(new StringReader(XmlUtils.prettyPrint(element).toString()));

            assertEquals("Config value set in <global> section is not propagated correctly",
                    "foo2", actionConf.get("action.foo"));
        } finally {
            LocalOozie.stop();
        }
    }

    public void testResourceManagerInGlobalConfigurationCanBeOverridenWithJobTrackerInAction() throws Exception {
        try {
            final String global = "<global>" +
                    "<resource-manager>RM</resource-manager>"+
                    "</global>";

            final String workflowUri = helper.createTestWorkflowXml(global, helper.getJavaActionXml(""));
            LocalOozie.start();
            final OozieClient wfClient = LocalOozie.getClient();
            final Properties conf = wfClient.createConfiguration();
            conf.setProperty(OozieClient.APP_PATH, workflowUri);
            conf.setProperty(OozieClient.USER_NAME, getTestUser());
            conf.setProperty("appName", "var-app-name");
            final String jobId = wfClient.submit(conf);
            wfClient.start(jobId);
            WorkflowJob workflow = wfClient.getJobInfo(jobId);
            waitFor(20 * 1000, new Predicate() {
                @Override
                public boolean evaluate() throws Exception {
                    WorkflowAction javaAction = helper.getJavaAction(wfClient.getJobInfo(jobId));
                    return javaAction != null && !javaAction.getStatus().equals("PREP");
                }
            });
            final WorkflowAction workflowAction = helper.getJavaAction(workflow);
            final String actualConfig = workflowAction.getConf();
            final String actualJobTrackerURI = XmlUtils.parseXml(actualConfig).getChildTextNormalize("job-tracker", null);
            assertEquals(getJobTrackerUri(), actualJobTrackerURI);

        } finally {
            LocalOozie.stop();
        }
    }

    public void testSetRootLoggerLevel() throws Exception {
        String oozieActionRootLogger = "oozie.action." + LauncherAMUtils.ROOT_LOGGER_LEVEL;
        String oozieActionHiveRootLogger = "oozie.action.hive" + LauncherAMUtils.ROOT_LOGGER_LEVEL;

        // default should be INFO
        JavaActionExecutor jae = new JavaActionExecutor();
        Configuration conf = new Configuration(false);
        jae.setRootLoggerLevel(conf);
        assertEquals("INFO", conf.get(oozieActionRootLogger));

        // explicitly set root logger level to DEBUG
        jae = new JavaActionExecutor();
        conf = new Configuration(false);
        conf.set(oozieActionRootLogger, "DEBUG");
        jae.setRootLoggerLevel(conf);
        assertEquals("DEBUG", conf.get(oozieActionRootLogger));

        // explicitly set root logger level to DEBUG for hive action
        HiveActionExecutor hae = new HiveActionExecutor();
        conf = new Configuration(false);
        conf.set(oozieActionHiveRootLogger, "DEBUG");
        hae.setRootLoggerLevel(conf);
        assertEquals("DEBUG", conf.get(oozieActionHiveRootLogger));
    }

    public void testEmptyArgsWithNullArgsNotAllowed() throws Exception {
        testEmptyArgs(false, "SUCCEEDED", WorkflowAction.Status.OK);
    }

    public void testEmptyArgsWithNullArgsAllowed() throws Exception {
        testEmptyArgs(true, "FAILED/KILLED", WorkflowAction.Status.ERROR);
    }

    private void testEmptyArgs(boolean nullArgsAllowed, String expectedExternalStatus, WorkflowAction.Status expectedStatus)
            throws Exception {
        ConfigurationService.setBoolean(LauncherAMUtils.CONF_OOZIE_NULL_ARGS_ALLOWED, nullArgsAllowed);

        String actionXml = "<java>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<main-class>" + LauncherMainTester.class.getName() + "</main-class>" +
                "<arg></arg>" +
                "</java>";

        Context context = createContext(actionXml, null);
        submitAction(context);
        waitUntilYarnAppDoneAndAssertSuccess(context.getAction().getExternalId());
        ActionExecutor ae = new JavaActionExecutor();
        ae.check(context, context.getAction());
        assertTrue(ae.isCompleted(context.getAction().getExternalStatus()));
        assertEquals(expectedExternalStatus, context.getAction().getExternalStatus());
        assertNull(context.getAction().getData());

        ae.end(context, context.getAction());
        assertEquals(expectedStatus, context.getAction().getStatus());
    }

    public void testMaxOutputDataSetByUser() {
        Configuration conf = new Configuration(false);
        conf.set("oozie.action.max.output.data", "10000");

        assertEquals(10000, JavaActionExecutor.getMaxOutputData(conf));
    }

    public void testInvalidMaxOutputDataSetByUser() {
        Configuration conf = new Configuration(false);
        conf.set("oozie.action.max.output.data", "sdasda");

        assertEquals(2048, JavaActionExecutor.getMaxOutputData(conf));
    }

    public void testFileWithSpaces() throws Exception {
        String actPath = JavaActionExecutor.getTrimmedEncodedPath("/user/map dev/test-case/shell/script/shell 1.sh");
        assertEquals("/user/map%20dev/test-case/shell/script/shell%201.sh", actPath);
    }

    public void testSubmitOKWithVcoresAndMemory() throws Exception {
        String actionXml = "<java>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<configuration>" +
                "  <property><name>oozie.launcher.vcores</name><value>1</value></property>" +
                "  <property><name>oozie.launcher.memory.mb</name><value>1024</value></property>" +
                "</configuration>" +
                "<main-class>" + LauncherMainTester.class.getName() + "</main-class>" +
                "</java>";
        Context context = createContext(actionXml, null);
        submitAction(context);
        waitUntilYarnAppDoneAndAssertSuccess(context.getAction().getExternalId());
        ActionExecutor ae = new JavaActionExecutor();
        ae.check(context, context.getAction());
        assertEquals("SUCCEEDED", context.getAction().getExternalStatus());
        assertNull(context.getAction().getData());

        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());
    }

    public void testSubmitOKWithLauncherJavaOpts() throws Exception {
        String actionXml = "<java>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<configuration>" +
                "  <property><name>oozie.launcher.javaopts</name><value>-DtestJavaOpts=true</value></property>" +
                "</configuration>" +
                "<main-class>" + LauncherMainTester.class.getName() + "</main-class>" +
                "</java>";
        Context context = createContext(actionXml, null);
        submitAction(context);
        waitUntilYarnAppDoneAndAssertSuccess(context.getAction().getExternalId());
        ActionExecutor ae = new JavaActionExecutor();
        ae.check(context, context.getAction());
        assertEquals("FAILED/KILLED", context.getAction().getExternalStatus());
        assertNull(context.getAction().getData());

        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.ERROR, context.getAction().getStatus());
    }

    public void testSubmitFailsWithNegativeVcores() throws Exception {
        String actionXml = "<java>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<configuration>" +
                "  <property><name>oozie.launcher.vcores</name><value>-1</value></property>" +
                "</configuration>" +
                "<main-class>" + LauncherMainTester.class.getName() + "</main-class>" +
                "</java>";
        Context context = createContext(actionXml, null);

        boolean exception = false;
        try {
            submitAction(context);
        } catch (ActionExecutorException e) {
            exception = true;
            assertEquals("Illegal exception was thrown", IllegalArgumentException.class, e.getCause().getClass());
        }

        assertTrue("Exception was not caught", exception);
    }

    public void testSubmitFailsWithNegativeMemory() throws Exception {
        String actionXml = "<java>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<configuration>" +
                "  <property><name>oozie.launcher.memory.mb</name><value>-1</value></property>" +
                "</configuration>" +
                "<main-class>" + LauncherMainTester.class.getName() + "</main-class>" +
                "</java>";
        Context context = createContext(actionXml, null);

        boolean exception = false;
        try {
            submitAction(context);
        } catch (ActionExecutorException e) {
            exception = true;
            assertEquals("Illegal exception was thrown", IllegalArgumentException.class, e.getCause().getClass());
        }

        assertTrue("Exception was not caught", exception);
    }

    public void testSubmitOKWithLauncherEnvVars() throws Exception {
        String actionXml = "<java>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<configuration>" +
                "  <property><name>oozie.launcher.env</name><value>A=foo1" + File.pathSeparator + "B=foo2</value></property>" +
                "</configuration>" +
                "<main-class>" + LauncherMainTester.class.getName() + "</main-class>" +
                "</java>";
        Context context = createContext(actionXml, null);
        submitAction(context);
        waitUntilYarnAppDoneAndAssertSuccess(context.getAction().getExternalId());
        ActionExecutor ae = new JavaActionExecutor();
        ae.check(context, context.getAction());
        assertEquals("SUCCEEDED", context.getAction().getExternalStatus());
        assertNull(context.getAction().getData());

        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());
    }

    public void testEnvVarsPropagatedFromLauncherConfig() throws Exception {
        String actionXml = "<java>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<configuration>" +
                "  <property><name>oozie.launcher.env</name><value>LAUNCHER_ENVIRON=foo1" + File.pathSeparator +
                "B=foo2</value></property>" +
                "</configuration>" +
                "<main-class>" + LauncherMainTester.class.getName() + "</main-class>" +
                "</java>";
        Context context = createContext(actionXml, null);
        submitAction(context);
        waitUntilYarnAppDoneAndAssertSuccess(context.getAction().getExternalId());
        ActionExecutor ae = new JavaActionExecutor();
        ae.check(context, context.getAction());
        assertEquals("FAILED/KILLED", context.getAction().getExternalStatus());
        assertNull(context.getAction().getData());

        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.ERROR, context.getAction().getStatus());
    }

    public void testSubmitFailsWithInvalidLauncherEnvVars() throws Exception {
        String actionXml = "<java>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<configuration>" +
                "  <property><name>oozie.launcher.env</name><value>Afoo1" + File.pathSeparator + "B=foo2</value></property>" +
                "</configuration>" +
                "<main-class>" + LauncherMainTester.class.getName() + "</main-class>" +
                "</java>";
        Context context = createContext(actionXml, null);
        try {
            submitAction(context);
            fail();
        }
        catch (ActionExecutorException e) {
            assertTrue(e.getMessage().contains("Invalid launcher setting for environment variables"));
        }
    }

    public void testSubmitWithLauncherQueue() throws Exception {
        String actionXml = "<java>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<configuration>" +
                "  <property><name>oozie.launcher.queue</name><value>default1</value></property>" +
                "</configuration>" +
                "<main-class>" + LauncherMainTester.class.getName() + "</main-class>" +
                "</java>";
        Context context = createContext(actionXml, null);
        submitAction(context);
        final ApplicationId appId = ConverterUtils.toApplicationId(context.getAction().getExternalId());
        Configuration conf = getHadoopAccessorService().createConfiguration(getJobTrackerUri());
        String queue = getHadoopAccessorService().createYarnClient(getTestUser(), conf).getApplicationReport(appId).getQueue();

        if (isFairSchedulerUsed(conf)) {
            assertEquals("root.default1", queue);
        }
        else {
            assertEquals("default1", queue);
        }
    }

    private boolean isFairSchedulerUsed(Configuration conf) {
        return conf.get(org.apache.hadoop.yarn.conf.YarnConfiguration.RM_SCHEDULER).contains(FairScheduler.class.getName());
    }

    public void testSubmitLauncherConfigurationOverridesLauncherMapperProperties() throws Exception {
        final String actionXml = "<java>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<configuration>" +
                "  <property><name>oozie.launcher.queue</name><value>default1</value></property>" +
                "  <property><name>mapreduce.job.queuename</name><value>default2</value></property>" +
                "</configuration>" +
                "<main-class>" + LauncherMainTester.class.getName() + "</main-class>" +
                "</java>";
        final Context context = createContext(actionXml, null);

        String applicationId = submitAction(context);

        final ApplicationId appId = ConverterUtils.toApplicationId(context.getAction().getExternalId());
        final Configuration conf = getHadoopAccessorService().createConfiguration(getJobTrackerUri());

        final String queue = getHadoopAccessorService().createYarnClient(getTestUser(), conf)
                .getApplicationReport(appId).getQueue();

        if (isFairSchedulerUsed(conf)) {
            assertEquals("queue name", "root.default1", queue);
        }

        waitUntilYarnAppDoneAndAssertSuccess(applicationId);
    }

    public void testChildKill() throws Exception {
        final JobConf clusterConf = createJobConf();
        FileSystem fileSystem = FileSystem.get(clusterConf);
        Path confFile = new Path("/tmp/cluster-conf.xml");
        OutputStream out = fileSystem.create(confFile);
        clusterConf.writeXml(out);
        out.close();
        String confFileName = fileSystem.makeQualified(confFile).toString() + "#core-site.xml";
        final String actionXml = "<java>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<main-class> " + SleepJob.class.getName() + " </main-class>" +
                "<arg>-mt</arg>" +
                "<arg>300000</arg>" +
                "<archive>" + confFileName + "</archive>" +
                "</java>";
        final Context context = createContext(actionXml, null);
        final String runningJob = submitAction(context);
        YarnApplicationState state = waitUntilYarnAppState(runningJob, EnumSet.of(YarnApplicationState.RUNNING));
        assertEquals(YarnApplicationState.RUNNING, state);

        WorkflowJob wfJob = context.getWorkflow();
        Configuration conf = null;
        if (wfJob.getConf() != null) {
            conf = new XConfiguration(new StringReader(wfJob.getConf()));
        }
        String launcherTag = LauncherHelper.getActionYarnTag(conf, wfJob.getParentId(), context.getAction());
        JavaActionExecutor ae = new JavaActionExecutor();
        final Configuration jobConf = ae.createBaseHadoopConf(context, XmlUtils.parseXml(actionXml));
        jobConf.set(LauncherMain.CHILD_MAPREDUCE_JOB_TAGS, LauncherHelper.getTag(launcherTag));
        jobConf.setLong(LauncherMain.OOZIE_JOB_LAUNCH_TIME, context.getAction().getStartTime().getTime());

        // We have to use a proper UGI for retrieving the child apps, because the WF is
        // submitted as a test user, not as the current login user
        UserGroupInformationService ugiService = Services.get().get(UserGroupInformationService.class);
        final UserGroupInformation ugi = ugiService.getProxyUser(getTestUser());
        final Set<ApplicationId> childSet = new HashSet<>();

        // wait until we have a child MR job
        waitFor(60_000, new Predicate() {
          @Override
          public boolean evaluate() throws Exception {
            return ugi.doAs(new PrivilegedExceptionAction<Boolean>() {
              @Override
              public Boolean run() throws Exception {
                childSet.clear();
                childSet.addAll(LauncherMain.getChildYarnJobs(jobConf));
                return childSet.size() > 0;
              }
            });
          }
        });
        assertEquals(1, childSet.size());

        // kill the action - based on the job tag, the SleepJob is expected to be killed too
        ae.kill(context, context.getAction());

        HadoopAccessorService hadoopAccessorService = getHadoopAccessorService();
        Configuration config = hadoopAccessorService.createConfiguration(getJobTrackerUri());
        YarnClient yarnClient =  hadoopAccessorService.createYarnClient(getTestUser(), config);

        // check that both the launcher & MR job were successfully killed
        ApplicationId jobId = childSet.iterator().next();
        assertEquals(YarnApplicationState.KILLED, yarnClient.getApplicationReport(jobId).getYarnApplicationState());
        assertTrue(ae.isCompleted(context.getAction().getExternalStatus()));
        assertEquals(WorkflowAction.Status.DONE, context.getAction().getStatus());
        assertEquals(JavaActionExecutor.KILLED, context.getAction().getExternalStatus());
        assertEquals(FinalApplicationStatus.KILLED,
                yarnClient.getApplicationReport(ConverterUtils.toApplicationId(runningJob)).getFinalApplicationStatus());
    }

    public String getWorkflowGlobalXml() {
        return "<global>" +
               "   <configuration>" +
               "        <property>" +
               "            <name>action.foo</name>" +
               "            <value>foo2</value>" +
               "        </property>" +
               "    </configuration>" +
               "</global>";
    }

    public void testSubmitOKWithLauncherJavaOptsExhaustingHeap() throws Exception {
        final String actionXml = "<java>" +
                "    <job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "        <name-node>" + getNameNodeUri() + "</name-node>" +
                "        <configuration>" +
                "            <property>" +
                "                <name>oozie.launcher.javaopts</name>" +
                "                <value>-Xms512m -Xmx1536m -XX:-DisableExplicitGC</value>" +
                "            </property>" +
                "        </configuration>" +
                "        <main-class>" + LauncherMainTester.class.getName() + "</main-class>" +
                "    <arg>-Xmx3072m</arg>" +
                "</java>";
        final Context context = createContext(actionXml, null);
        submitAction(context);
        waitUntilYarnAppDoneAndAssertSuccess(context.getAction().getExternalId());
        ActionExecutor ae = new JavaActionExecutor();
        ae.check(context, context.getAction());
        assertEquals("FAILED/KILLED", context.getAction().getExternalStatus());
        assertNull(context.getAction().getData());

        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.ERROR, context.getAction().getStatus());

        assertTrue("error message should contain: \"Java heap space\"",
                context.getAction().getErrorMessage().contains("Java heap space"));
    }

    public void testKeyValuePairInProperties() {
        JavaActionExecutor ae = new JavaActionExecutor();
        Map<String, String> result = ae.extractEnvVarsFromOozieLauncherProps(
                "FOO=-Dbar=baz" + File.pathSeparatorChar + "BAR=baz");
        assertEquals("-Dbar=baz", result.get("FOO"));
        assertEquals("baz", result.get("BAR"));
    }

}
