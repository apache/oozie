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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.LiteWorkflowStoreService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.ShareLibService;
import org.apache.oozie.service.UUIDService;
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
import org.jdom.Element;
import org.junit.Assert;
import org.junit.Test;

public class TestJavaActionExecutor extends ActionExecutorTestCase {

    @Override
    protected void beforeSetUp() throws Exception {
        super.beforeSetUp();
        setSystemProperty("oozie.test.hadoop.minicluster2", "true");
    }

    @Override
    protected void setSystemProps() throws Exception {
        super.setSystemProps();

        setSystemProperty("oozie.service.ActionService.executor.classes", JavaActionExecutor.class.getName());
        setSystemProperty("oozie.service.HadoopAccessorService.action.configurations",
                          "*=hadoop-conf," + getJobTrackerUri() + "=action-conf");
        setSystemProperty(WorkflowAppService.SYSTEM_LIB_PATH, getFsTestCaseDir().toUri().getPath() + "/systemlib");
        new File(getTestCaseConfDir(), "action-conf").mkdir();
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("test-action-config.xml");
        OutputStream os = new FileOutputStream(new File(getTestCaseConfDir() + "/action-conf", "java.xml"));
        IOUtils.copyStream(is, os);

    }

    @SuppressWarnings("unchecked")
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
        conf.set("mapred.job.tracker", "a");
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

        conf = new XConfiguration();
        conf.set("e", "E");
        conf.set("oozie.launcher.f", "F");
        os = getFileSystem().create(new Path(getFsTestCaseDir(), "job2.xml"));
        conf.writeXml(os);
        os.close();

        conf = ae.createBaseHadoopConf(context, actionXml);
        assertEquals(protoConf.get(WorkflowAppService.HADOOP_USER), conf.get(WorkflowAppService.HADOOP_USER));
        assertEquals(getJobTrackerUri(), conf.get("mapred.job.tracker"));
        assertEquals(getNameNodeUri(), conf.get("fs.default.name"));

        conf = ae.createBaseHadoopConf(context, actionXml);
        ae.setupLauncherConf(conf, actionXml, getFsTestCaseDir(), context);
        assertEquals("LA", conf.get("oozie.launcher.a"));
        assertEquals("LA", conf.get("a"));
        assertNull(conf.get("b"));
        assertNull(conf.get("oozie.launcher.d"));
        assertNull(conf.get("d"));
        assertNull(conf.get("action.foo"));
        assertEquals("action.barbar", conf.get("action.foofoo"));

        conf = ae.createBaseHadoopConf(context, actionXml);
        ae.setupActionConf(conf, context, actionXml, getFsTestCaseDir());
        assertEquals("LA", conf.get("oozie.launcher.a"));
        assertEquals("AA", conf.get("a"));
        assertEquals("BB", conf.get("b"));
        assertEquals("C", conf.get("c"));
        assertEquals("D", conf.get("oozie.launcher.d"));
        assertEquals("E", conf.get("e"));
        assertEquals("F", conf.get("oozie.launcher.f"));
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
        assertTrue(conf.get("mapred.child.java.opts").contains("JAVA-OPTS"));
        assertTrue(conf.get("mapreduce.map.java.opts").contains("JAVA-OPTS"));
        assertEquals(Arrays.asList("A1", "A2"), Arrays.asList(LauncherMapper.getMainArguments(conf)));

        assertTrue(getFileSystem().exists(new Path(context.getActionDir(), LauncherMapper.ACTION_CONF_XML)));

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

    protected RunningJob submitAction(Context context) throws Exception {
        JavaActionExecutor ae = new JavaActionExecutor();

        WorkflowAction action = context.getAction();

        ae.prepareActionDir(getFileSystem(), context);
        ae.submitLauncher(getFileSystem(), context, action);

        String jobId = action.getExternalId();
        String jobTracker = action.getTrackerUri();
        String consoleUrl = action.getConsoleUrl();
        assertNotNull(jobId);
        assertNotNull(jobTracker);
        assertNotNull(consoleUrl);

        JobConf jobConf = Services.get().get(HadoopAccessorService.class).createJobConf(jobTracker);
        jobConf.set("mapred.job.tracker", jobTracker);

        JobClient jobClient =
            Services.get().get(HadoopAccessorService.class).createJobClient(getTestUser(), jobConf);
        final RunningJob runningJob = jobClient.getJob(JobID.forName(jobId));
        assertNotNull(runningJob);
        return runningJob;
    }

    public void testSimpestSleSubmitOK() throws Exception {
        String actionXml = "<java>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<main-class>" + LauncherMainTester.class.getName() + "</main-class>" +
                "</java>";
        Context context = createContext(actionXml, null);
        final RunningJob runningJob = submitAction(context);
        waitFor(60 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return runningJob.isComplete();
            }
        });
        assertTrue(runningJob.isSuccessful());
        ActionExecutor ae = new JavaActionExecutor();
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
        final RunningJob runningJob = submitAction(context);
        waitFor(60 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return runningJob.isComplete();
            }
        });
        assertTrue(runningJob.isSuccessful());
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
        final RunningJob runningJob = submitAction(context);
        waitFor(60 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return runningJob.isComplete();
            }
        });
        assertTrue(runningJob.isSuccessful());
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
        final RunningJob runningJob = submitAction(context);
        ActionExecutor ae = new JavaActionExecutor();
        assertFalse(ae.isCompleted(context.getAction().getExternalStatus()));
        waitFor(60 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return runningJob.isComplete();
            }
        });
        assertTrue(runningJob.isSuccessful());
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
        final RunningJob runningJob = submitAction(context);
        waitFor(60 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return runningJob.isComplete();
            }
        });
        assertTrue(runningJob.isSuccessful());
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
        final RunningJob runningJob = submitAction(context);
        waitFor(60 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return runningJob.isComplete();
            }
        });
        assertTrue(runningJob.isSuccessful());
        assertFalse(LauncherMapperHelper.isMainSuccessful(runningJob));
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
        final RunningJob runningJob = submitAction(context);
        waitFor(60 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return runningJob.isComplete();
            }
        });
        assertTrue(runningJob.isSuccessful());
        assertFalse(LauncherMapperHelper.isMainSuccessful(runningJob));
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
        final RunningJob runningJob = submitAction(context);
        waitFor(60 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return runningJob.isComplete();
            }
        });
        assertTrue(runningJob.isSuccessful());
        assertFalse(LauncherMapperHelper.isMainSuccessful(runningJob));
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
        final RunningJob runningJob = submitAction(context);
        assertFalse(runningJob.isComplete());
        ActionExecutor ae = new JavaActionExecutor();
        ae.kill(context, context.getAction());
        assertEquals(WorkflowAction.Status.DONE, context.getAction().getStatus());
        assertEquals("KILLED", context.getAction().getExternalStatus());
        assertTrue(ae.isCompleted(context.getAction().getExternalStatus()));

        waitFor(60 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return runningJob.isComplete();
            }
        });
        assertFalse(runningJob.isSuccessful());
    }


    public void testRecovery() throws Exception {
        final String actionXml = "<java>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<main-class>" + LauncherMainTester.class.getName() + "</main-class>" +
                "</java>";
        final Context context = createContext(actionXml, null);
        RunningJob runningJob = submitAction(context);
        String launcherId = context.getAction().getExternalId();

        waitFor(60 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                JavaActionExecutor ae = new JavaActionExecutor();
                Configuration conf = ae.createBaseHadoopConf(context, XmlUtils.parseXml(actionXml));
                return LauncherMapperHelper.getRecoveryId(conf, context.getActionDir(), context.getRecoveryId()) != null;
            }
        });

        final RunningJob runningJob2 = submitAction(context);

        assertEquals(launcherId, runningJob2.getJobID().toString());
        assertEquals(launcherId, context.getAction().getExternalId());

        waitFor(60 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return runningJob2.isComplete();
            }
        });
        assertTrue(runningJob.isSuccessful());
        ActionExecutor ae = new JavaActionExecutor();
        ae.check(context, context.getAction());
        assertEquals("SUCCEEDED", context.getAction().getExternalStatus());
        assertNull(context.getAction().getData());

        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());
    }

    public void testLibFileArchives() throws Exception {
        Path root = new Path(getFsTestCaseDir(), "root");

        Path jar = new Path("jar.jar");
        getFileSystem().create(new Path(getAppPath(), jar)).close();
        Path rootJar = new Path(root, "rootJar.jar");
        getFileSystem().create(rootJar).close();

        Path file = new Path("file");
        getFileSystem().create(new Path(getAppPath(), file)).close();
        Path rootFile = new Path(root, "rootFile");
        getFileSystem().create(rootFile).close();

        Path so = new Path("soFile.so");
        getFileSystem().create(new Path(getAppPath(), so)).close();
        Path rootSo = new Path(root, "rootSoFile.so");
        getFileSystem().create(rootSo).close();

        Path so1 = new Path("soFile.so.1");
        getFileSystem().create(new Path(getAppPath(), so1)).close();
        Path rootSo1 = new Path(root, "rootSoFile.so.1");
        getFileSystem().create(rootSo1).close();

        Path archive = new Path("archive.tar");
        getFileSystem().create(new Path(getAppPath(), archive)).close();
        Path rootArchive = new Path(root, "rootArchive.tar");
        getFileSystem().create(rootArchive).close();

        String actionXml = "<java>" +
                "      <job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "      <name-node>" + getNameNodeUri() + "</name-node>" +
                "      <main-class>CLASS</main-class>" +
                "      <file>" + jar.toString() + "</file>\n" +
                "      <file>" + rootJar.toString() + "</file>\n" +
                "      <file>" + file.toString() + "</file>\n" +
                "      <file>" + rootFile.toString() + "</file>\n" +
                "      <file>" + so.toString() + "</file>\n" +
                "      <file>" + rootSo.toString() + "</file>\n" +
                "      <file>" + so1.toString() + "</file>\n" +
                "      <file>" + rootSo1.toString() + "</file>\n" +
                "      <archive>" + archive.toString() + "</archive>\n" +
                "      <archive>" + rootArchive.toString() + "</archive>\n" +
                "</java>";

        Element eActionXml = XmlUtils.parseXml(actionXml);

        Context context = createContext(actionXml, null);

        Path appPath = getAppPath();

        JavaActionExecutor ae = new JavaActionExecutor();

        Configuration jobConf = ae.createBaseHadoopConf(context, eActionXml);
        ae.setupActionConf(jobConf, context, eActionXml, appPath);
        ae.setLibFilesArchives(context, eActionXml, appPath, jobConf);


        assertTrue(DistributedCache.getSymlink(jobConf));

        Path[] filesInClasspath = DistributedCache.getFileClassPaths(jobConf);
        for (Path p : new Path[]{new Path(getAppPath(), jar), rootJar}) {
            boolean found = false;
            for (Path c : filesInClasspath) {
                if (!found && p.toUri().getPath().equals(c.toUri().getPath())) {
                    found = true;
                }
            }
            assertTrue("file " + p.toUri().getPath() + " not found in classpath", found);
        }
        for (Path p : new Path[]{new Path(getAppPath(), file), rootFile, new Path(getAppPath(), so), rootSo,
                                 new Path(getAppPath(), so1), rootSo1}) {
            boolean found = false;
            for (Path c : filesInClasspath) {
                if (!found && p.toUri().getPath().equals(c.toUri().getPath())) {
                    found = true;
                }
            }
            assertFalse("file " + p.toUri().getPath() + " found in classpath", found);
        }

        URI[] filesInCache = DistributedCache.getCacheFiles(jobConf);
        for (Path p : new Path[]{new Path(getAppPath(), jar), rootJar, new Path(getAppPath(), file), rootFile,
                                 new Path(getAppPath(), so), rootSo, new Path(getAppPath(), so1), rootSo1}) {
            boolean found = false;
            for (URI c : filesInCache) {
                if (!found && p.toUri().getPath().equals(c.getPath())) {
                    found = true;
                }
            }
            assertTrue("file " + p.toUri().getPath() + " not found in cache", found);
        }

        URI[] archivesInCache = DistributedCache.getCacheArchives(jobConf);
        for (Path p : new Path[]{new Path(getAppPath(), archive), rootArchive}) {
            boolean found = false;
            for (URI c : archivesInCache) {
                if (!found && p.toUri().getPath().equals(c.getPath())) {
                    found = true;
                }
            }
            assertTrue("archive " + p.toUri().getPath() + " not found in cache", found);
        }
    }

    /**
     * https://issues.apache.org/jira/browse/OOZIE-87
     * @throws Exception
     */
    public void testCommaSeparatedFilesAndArchives() throws Exception {
        Path root = new Path(getFsTestCaseDir(), "root");

        Path jar = new Path("jar.jar");
        getFileSystem().create(new Path(getAppPath(), jar)).close();
        Path rootJar = new Path(root, "rootJar.jar");
        getFileSystem().create(rootJar).close();

        Path file = new Path("file");
        getFileSystem().create(new Path(getAppPath(), file)).close();
        Path rootFile = new Path(root, "rootFile");
        getFileSystem().create(rootFile).close();

        Path so = new Path("soFile.so");
        getFileSystem().create(new Path(getAppPath(), so)).close();
        Path rootSo = new Path(root, "rootSoFile.so");
        getFileSystem().create(rootSo).close();

        Path so1 = new Path("soFile.so.1");
        getFileSystem().create(new Path(getAppPath(), so1)).close();
        Path rootSo1 = new Path(root, "rootSoFile.so.1");
        getFileSystem().create(rootSo1).close();

        Path archive = new Path("archive.tar");
        getFileSystem().create(new Path(getAppPath(), archive)).close();
        Path rootArchive = new Path(root, "rootArchive.tar");
        getFileSystem().create(rootArchive).close();

        String actionXml = "<java>" +
                "      <job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "      <name-node>" + getNameNodeUri() + "</name-node>" +
                "      <main-class>CLASS</main-class>" +
                "      <file>" + jar.toString() +
                            "," + rootJar.toString() +
                            "," + file.toString() +
                            ", " + rootFile.toString() + // with leading and trailing spaces
                            "  ," + so.toString() +
                            "," + rootSo.toString() +
                            "," + so1.toString() +
                            "," + rootSo1.toString() + "</file>\n" +
                "      <archive>" + archive.toString() + ", "
                            + rootArchive.toString() + " </archive>\n" + // with leading and trailing spaces
                "</java>";

        Element eActionXml = XmlUtils.parseXml(actionXml);

        Context context = createContext(actionXml, null);

        Path appPath = getAppPath();

        JavaActionExecutor ae = new JavaActionExecutor();

        Configuration jobConf = ae.createBaseHadoopConf(context, eActionXml);
        ae.setupActionConf(jobConf, context, eActionXml, appPath);
        ae.setLibFilesArchives(context, eActionXml, appPath, jobConf);


        assertTrue(DistributedCache.getSymlink(jobConf));

        Path[] filesInClasspath = DistributedCache.getFileClassPaths(jobConf);
        for (Path p : new Path[]{new Path(getAppPath(), jar), rootJar}) {
            boolean found = false;
            for (Path c : filesInClasspath) {
                if (!found && p.toUri().getPath().equals(c.toUri().getPath())) {
                    found = true;
                }
            }
            assertTrue("file " + p.toUri().getPath() + " not found in classpath", found);
        }
        for (Path p : new Path[]{new Path(getAppPath(), file), rootFile, new Path(getAppPath(), so), rootSo,
                                new Path(getAppPath(), so1), rootSo1}) {
            boolean found = false;
            for (Path c : filesInClasspath) {
                if (!found && p.toUri().getPath().equals(c.toUri().getPath())) {
                    found = true;
                }
            }
            assertFalse("file " + p.toUri().getPath() + " found in classpath", found);
        }

        URI[] filesInCache = DistributedCache.getCacheFiles(jobConf);
        for (Path p : new Path[]{new Path(getAppPath(), jar), rootJar, new Path(getAppPath(), file), rootFile,
                                new Path(getAppPath(), so), rootSo, new Path(getAppPath(), so1), rootSo1}) {
            boolean found = false;
            for (URI c : filesInCache) {
                if (!found && p.toUri().getPath().equals(c.getPath())) {
                    found = true;
                }
            }
            assertTrue("file " + p.toUri().getPath() + " not found in cache", found);
        }

        URI[] archivesInCache = DistributedCache.getCacheArchives(jobConf);
        for (Path p : new Path[]{new Path(getAppPath(), archive), rootArchive}) {
            boolean found = false;
            for (URI c : archivesInCache) {
                if (!found && p.toUri().getPath().equals(c.getPath())) {
                    found = true;
                }
            }
            assertTrue("archive " + p.toUri().getPath() + " not found in cache", found);
        }
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
        final RunningJob runningJob = submitAction(context);
        waitFor(60 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return runningJob.isComplete();
            }
        });
        assertTrue(runningJob.isSuccessful());
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
        HashMap<String, CredentialsProperties> credProperties = ae.setCredentialPropertyToActionConf(context,
                action, actionConf);

        CredentialsProperties prop = credProperties.get("abcname");
        assertEquals("value1", prop.getProperties().get("property1"));
        assertEquals("value2", prop.getProperties().get("property2"));
        assertEquals("val3", prop.getProperties().get("prop3"));

        // Try to load the token without it being defined in oozie-site; should get an exception
        JobConf credentialsConf = new JobConf();
        Configuration launcherConf = ae.createBaseHadoopConf(context, actionXmlconf);
        XConfiguration.copy(launcherConf, credentialsConf);
        try {
            ae.setCredentialTokens(credentialsConf, context, action, credProperties);
            fail("Should have gotten an exception but did not");
        }
        catch (ActionExecutorException aee) {
            assertEquals("JA020", aee.getErrorCode());
            assertTrue(aee.getMessage().contains("type [abc]"));
            assertTrue(aee.getMessage().contains("name [abcname]"));
        }

        // Define 'abc' token type in oozie-site
        ConfigurationService.set("oozie.credentials.credentialclasses", "abc=org.apache.oozie.action.hadoop.InsertTestToken");

        // Try to load the token after being defined in oozie-site; should work correctly
        credentialsConf = new JobConf();
        launcherConf = ae.createBaseHadoopConf(context, actionXmlconf);
        XConfiguration.copy(launcherConf, credentialsConf);
        ae.setCredentialTokens(credentialsConf, context, action, credProperties);
        Token<? extends TokenIdentifier> tk = credentialsConf.getCredentials().getToken(new Text("ABC Token"));
        assertNotNull(tk);
    }

    private WorkflowJobBean addRecordToWfJobTable(String wfId, String wfxml) throws Exception {
        WorkflowApp app = new LiteWorkflowApp("testApp", wfxml,
            new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class, "start")).
                addNode(new EndNodeDef("end", LiteWorkflowStoreService.LiteControlNodeHandler.class));
        Configuration conf = Services.get().get(HadoopAccessorService.class).
            createJobConf(new URI(getNameNodeUri()).getAuthority());
        conf.set(OozieClient.APP_PATH, getNameNodeUri() + "/testPath");
        conf.set(OozieClient.LOG_TOKEN, "testToken");
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("property3", "prop3");
        conf.set("value3", "val3");

        WorkflowJobBean wfBean = createWorkflow(app, conf, "auth");
        wfBean.setId(wfId);
        wfBean.setStatus(WorkflowJob.Status.SUCCEEDED);
        WorkflowActionBean action = new WorkflowActionBean();
        action.setName("test");
        action.setCred("null");
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
        Assert.assertArrayEquals(new String[] { "java-job-conf" },
                ae.getShareLibNames(context, new Element("java"), actionConf));

        actionConf.set("oozie.action.sharelib.for.java", "java-action-conf");
        Assert.assertArrayEquals(new String[] { "java-action-conf" },
                ae.getShareLibNames(context, new Element("java"), actionConf));
    }

    public void testJavaOpts() throws Exception {
        String actionXml = "<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>" + "<job-xml>job.xml</job-xml>" + "<job-xml>job2.xml</job-xml>"
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

        assertEquals("-Xmx200m JAVA-OPT1 JAVA-OPT2 -Djava.io.tmpdir=./tmp", conf.get("mapred.child.java.opts"));
        assertEquals("-Xmx200m JAVA-OPT1 JAVA-OPT2 -Djava.io.tmpdir=./tmp", conf.get("mapreduce.map.java.opts"));

        actionXml = "<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>" + "<job-xml>job.xml</job-xml>" + "<job-xml>job2.xml</job-xml>"
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

        assertEquals("-Xmx200m JAVA-OPT1 JAVA-OPT2 -Djava.io.tmpdir=./tmp", conf.get("mapred.child.java.opts"));
        assertEquals("-Xmx200m JAVA-OPT1 JAVA-OPT2 -Djava.io.tmpdir=./tmp", conf.get("mapreduce.map.java.opts"));

        actionXml = "<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>" + "<job-xml>job.xml</job-xml>" + "<job-xml>job2.xml</job-xml>"
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

        assertEquals("JAVA-OPT3 JAVA-OPT1 JAVA-OPT2 -Djava.io.tmpdir=./tmp", conf.get("mapred.child.java.opts"));
        assertEquals("JAVA-OPT3 JAVA-OPT1 JAVA-OPT2 -Djava.io.tmpdir=./tmp", conf.get("mapreduce.map.java.opts"));

        actionXml = "<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>" + "<job-xml>job.xml</job-xml>" + "<job-xml>job2.xml</job-xml>"
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

        assertEquals("-Xmx200m JAVA-OPT3 JAVA-OPT1 JAVA-OPT2 -Djava.io.tmpdir=./tmp", conf.get("mapred.child.java.opts"));
        assertEquals("-Xmx200m JAVA-OPT3 JAVA-OPT1 JAVA-OPT2 -Djava.io.tmpdir=./tmp", conf.get("mapreduce.map.java.opts"));

        actionXml = "<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>" + "<job-xml>job.xml</job-xml>" + "<job-xml>job2.xml</job-xml>"
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

        assertEquals("JAVA-OPT3 JAVA-OPT4 JAVA-OPT1 JAVA-OPT2 -Djava.io.tmpdir=./tmp", conf.get("mapred.child.java.opts"));
        assertEquals("JAVA-OPT3 JAVA-OPT4 JAVA-OPT1 JAVA-OPT2 -Djava.io.tmpdir=./tmp", conf.get("mapreduce.map.java.opts"));
    }

    public void testActionLibsPath() throws Exception {
        // Test adding a directory
        Path actionLibPath = new Path(getFsTestCaseDir(), "actionlibs");
        getFileSystem().mkdirs(actionLibPath);
        Path jar1Path = new Path(actionLibPath, "jar1.jar");
        getFileSystem().create(jar1Path).close();
        Path jar2Path = new Path(actionLibPath, "jar2.jar");
        getFileSystem().create(jar2Path).close();

        String actionXml = "<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<job-xml>job.xml</job-xml>" + "<job-xml>job2.xml</job-xml>" + "<configuration>" +
                "<property><name>oozie.launcher.oozie.libpath</name><value>" + actionLibPath + "</value></property>" +
                "</configuration>" + "<main-class>MAIN-CLASS</main-class>" +
                "</java>";
        Element eActionXml = XmlUtils.parseXml(actionXml);
        Context context = createContext(actionXml, null);

        JavaActionExecutor ae = new JavaActionExecutor();

        Configuration jobConf = ae.createBaseHadoopConf(context, eActionXml);
        ae.setupLauncherConf(jobConf, eActionXml, getAppPath(), context);
        ae.setLibFilesArchives(context, eActionXml, getAppPath(), jobConf);

        URI[] cacheFiles = DistributedCache.getCacheFiles(jobConf);
        String cacheFilesStr = Arrays.toString(cacheFiles);
        assertTrue(cacheFilesStr.contains(jar1Path.toString()));
        assertTrue(cacheFilesStr.contains(jar2Path.toString()));

        // Test adding a file
        Path jar3Path = new Path(getFsTestCaseDir(), "jar3.jar");
        getFileSystem().create(jar3Path).close();

        actionXml = "<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<job-xml>job.xml</job-xml>" + "<job-xml>job2.xml</job-xml>" + "<configuration>" +
                "<property><name>oozie.launcher.oozie.libpath</name><value>" + jar3Path + "</value></property>" +
                "</configuration>" + "<main-class>MAIN-CLASS</main-class>" +
                "</java>";
        eActionXml = XmlUtils.parseXml(actionXml);
        context = createContext(actionXml, null);

        ae = new JavaActionExecutor();

        jobConf = ae.createBaseHadoopConf(context, eActionXml);
        ae.setupLauncherConf(jobConf, eActionXml, getAppPath(), context);
        ae.setLibFilesArchives(context, eActionXml, getAppPath(), jobConf);

        cacheFiles = DistributedCache.getCacheFiles(jobConf);
        cacheFilesStr = Arrays.toString(cacheFiles);
        assertTrue(cacheFilesStr.contains(jar3Path.toString()));

        // Test adding a directory and a file (comma separated)
        actionXml = "<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<job-xml>job.xml</job-xml>" + "<job-xml>job2.xml</job-xml>" + "<configuration>" +
                "<property><name>oozie.launcher.oozie.libpath</name><value>" + actionLibPath + "," + jar3Path +
                "</value></property>" +
                "</configuration>" + "<main-class>MAIN-CLASS</main-class>" +
                "</java>";
        eActionXml = XmlUtils.parseXml(actionXml);
        context = createContext(actionXml, null);

        ae = new JavaActionExecutor();

        jobConf = ae.createBaseHadoopConf(context, eActionXml);
        ae.setupLauncherConf(jobConf, eActionXml, getAppPath(), context);
        ae.setLibFilesArchives(context, eActionXml, getAppPath(), jobConf);

        cacheFiles = DistributedCache.getCacheFiles(jobConf);
        cacheFilesStr = Arrays.toString(cacheFiles);
        assertTrue(cacheFilesStr.contains(jar1Path.toString()));
        assertTrue(cacheFilesStr.contains(jar2Path.toString()));
        assertTrue(cacheFilesStr.contains(jar3Path.toString()));
    }

    @Test
    public void testAddActionShareLib() throws Exception {

        WorkflowAppService wps = Services.get().get(WorkflowAppService.class);

        Path systemLibPath = new Path(wps.getSystemLibPath(), ShareLibService.SHARE_LIB_PREFIX
                + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()).toString());

        Path javaShareLibPath = new Path(systemLibPath, "java");
        getFileSystem().mkdirs(javaShareLibPath);
        Path jar1Path = new Path(javaShareLibPath, "jar1.jar");
        getFileSystem().create(jar1Path).close();
        Path jar2Path = new Path(javaShareLibPath, "jar2.jar");
        getFileSystem().create(jar2Path).close();

        Path hcatShareLibPath = new Path(systemLibPath, "hcat");
        getFileSystem().mkdirs(hcatShareLibPath);
        Path jar3Path = new Path(hcatShareLibPath, "jar3.jar");
        getFileSystem().create(jar3Path).close();
        Path jar4Path = new Path(hcatShareLibPath, "jar4.jar");
        getFileSystem().create(jar4Path).close();

        Path otherShareLibPath = new Path(systemLibPath, "other");
        getFileSystem().mkdirs(otherShareLibPath);
        Path jar5Path = new Path(otherShareLibPath, "jar5.jar");
        getFileSystem().create(jar5Path).close();

        String actionXml = "<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<job-xml>job.xml</job-xml>" + "<job-xml>job2.xml</job-xml>" +
                "<main-class>MAIN-CLASS</main-class>" +
                "</java>";
        Element eActionXml = XmlUtils.parseXml(actionXml);
        Context context = createContext(actionXml, null);

        Services.get().setService(ShareLibService.class);

        // Test oozie server action sharelib setting
        WorkflowJobBean workflow = (WorkflowJobBean) context.getWorkflow();
        XConfiguration wfConf = new XConfiguration();
        wfConf.set(WorkflowAppService.HADOOP_USER, getTestUser());
        wfConf.set(OozieClient.APP_PATH, new Path(getAppPath(), "workflow.xml").toString());
        wfConf.setBoolean(OozieClient.USE_SYSTEM_LIBPATH, true);
        workflow.setConf(XmlUtils.prettyPrint(wfConf).toString());

        ConfigurationService.set("oozie.action.sharelib.for.java", "java,hcat");

        JavaActionExecutor ae = new JavaActionExecutor();

        Configuration jobConf = ae.createBaseHadoopConf(context, eActionXml);
        ae.setupLauncherConf(jobConf, eActionXml, getAppPath(), context);
        try {
            ae.setLibFilesArchives(context, eActionXml, getAppPath(), jobConf);
            fail();
        } catch (ActionExecutorException aee) {
            assertEquals("EJ001", aee.getErrorCode());
            assertEquals("Could not locate Oozie sharelib", aee.getMessage());
        }
        Path launcherPath = new Path(systemLibPath, "oozie");
        getFileSystem().mkdirs(launcherPath);
        Path jar6Path = new Path(launcherPath, "jar6.jar");
        getFileSystem().create(jar6Path).close();
        Services.get().get(ShareLibService.class).updateShareLib();
        ae.setLibFilesArchives(context, eActionXml, getAppPath(), jobConf);

        URI[] cacheFiles = DistributedCache.getCacheFiles(jobConf);
        String cacheFilesStr = Arrays.toString(cacheFiles);
        assertTrue(cacheFilesStr.contains(jar1Path.toString()));
        assertTrue(cacheFilesStr.contains(jar2Path.toString()));
        assertTrue(cacheFilesStr.contains(jar3Path.toString()));
        assertTrue(cacheFilesStr.contains(jar4Path.toString()));
        assertFalse(cacheFilesStr.contains(jar5Path.toString()));
        assertTrue(cacheFilesStr.contains(jar6Path.toString()));

        // Test per workflow action sharelib setting
        workflow = (WorkflowJobBean) context.getWorkflow();
        wfConf = new XConfiguration();
        wfConf.set(WorkflowAppService.HADOOP_USER, getTestUser());
        wfConf.set(OozieClient.APP_PATH, new Path(getAppPath(), "workflow.xml").toString());
        wfConf.setBoolean(OozieClient.USE_SYSTEM_LIBPATH, true);
        wfConf.set("oozie.action.sharelib.for.java", "other,hcat");
        workflow.setConf(XmlUtils.prettyPrint(wfConf).toString());

        ConfigurationService.set("oozie.action.sharelib.for.java", "java");
        ae = new JavaActionExecutor();

        jobConf = ae.createBaseHadoopConf(context, eActionXml);
        ae.setupLauncherConf(jobConf, eActionXml, getAppPath(), context);
        ae.setLibFilesArchives(context, eActionXml, getAppPath(), jobConf);

        cacheFiles = DistributedCache.getCacheFiles(jobConf);
        cacheFilesStr = Arrays.toString(cacheFiles);
        // The oozie server setting should have been overridden by workflow setting
        assertFalse(cacheFilesStr.contains(jar1Path.toString()));
        assertFalse(cacheFilesStr.contains(jar2Path.toString()));
        assertTrue(cacheFilesStr.contains(jar3Path.toString()));
        assertTrue(cacheFilesStr.contains(jar4Path.toString()));
        assertTrue(cacheFilesStr.contains(jar5Path.toString()));
        assertTrue(cacheFilesStr.contains(jar6Path.toString()));
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
        Path systemLibPath = new Path(wps.getSystemLibPath(), ShareLibService.SHARE_LIB_PREFIX
                + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()).toString());
        Path javaShareLibPath = new Path(systemLibPath, "java-action-executor");
        getFileSystem().mkdirs(javaShareLibPath);
        Services.get().setService(ShareLibService.class);

        JobConf conf = ae.createBaseHadoopConf(context, eActionXml);
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
            JobConf conf = ae.createBaseHadoopConf(context, eActionXml);
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

    public void testACLModifyJob() throws Exception {
        // CASE 1: If user has provided modify-acl value
        // then it should NOT be overridden by group name
        String actionXml = "<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node> <configuration>" +
                "<property><name>mapreduce.job.acl-modify-job</name><value>MODIFIER</value></property>" +
                "</configuration>" + "<main-class>MAIN-CLASS</main-class>" +
                "</java>";

        Context context = createContext(actionXml, "USERS");
        RunningJob job = submitAction(context);
        FileSystem fs = context.getAppFileSystem();
        Configuration jobXmlConf = new XConfiguration(fs.open(new Path(job.getJobFile())));

        String userModifyAcl = jobXmlConf.get(JavaActionExecutor.ACL_MODIFY_JOB); // 'MODIFIER'
        String userGroup = context.getWorkflow().getAcl(); // 'USERS'
        assertFalse(userGroup.equals(userModifyAcl));

        // CASE 2: If user has not provided modify-acl value
        // then it equals group name
        actionXml = "<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node> <configuration>" +
                "</configuration>" + "<main-class>MAIN-CLASS</main-class>" +
                "</java>";
        context = createContext(actionXml, "USERS");
        job = submitAction(context);
        fs = context.getAppFileSystem();
        jobXmlConf = new XConfiguration(fs.open(new Path(job.getJobFile())));

        userModifyAcl = jobXmlConf.get(JavaActionExecutor.ACL_MODIFY_JOB);
        userGroup = context.getWorkflow().getAcl();
        assertTrue(userGroup.equals(userModifyAcl));
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

    public void testInjectLauncherUseUberMode() throws Exception {
        // default -- should set to true
        JavaActionExecutor jae = new JavaActionExecutor();
        Configuration conf = new Configuration(false);
        assertNull(conf.get("mapreduce.job.ubertask.enable"));
        jae.injectLauncherUseUberMode(conf);
        assertEquals("true", conf.get("mapreduce.job.ubertask.enable"));

        // action conf set to true -- should keep at true
        conf = new Configuration(false);
        assertNull(conf.get("mapreduce.job.ubertask.enable"));
        conf.setBoolean("mapreduce.job.ubertask.enable", true);
        jae.injectLauncherUseUberMode(conf);
        assertEquals("true", conf.get("mapreduce.job.ubertask.enable"));

        // action conf set to false -- should keep at false
        conf = new Configuration(false);
        assertNull(conf.get("mapreduce.job.ubertask.enable"));
        conf.setBoolean("mapreduce.job.ubertask.enable", false);
        jae.injectLauncherUseUberMode(conf);
        assertEquals("false", conf.get("mapreduce.job.ubertask.enable"));

        // disable at oozie-site level for just the "test" action
        ConfigurationService.setBoolean("oozie.action.test.launcher.mapreduce.job.ubertask.enable", false);
        JavaActionExecutor tjae = new JavaActionExecutor("test");

        // default -- should not set
        conf = new Configuration(false);
        assertNull(conf.get("mapreduce.job.ubertask.enable"));
        tjae.injectLauncherUseUberMode(conf);
        assertNull(conf.get("mapreduce.job.ubertask.enable"));
        // default -- should be true
        conf = new Configuration(false);
        assertNull(conf.get("mapreduce.job.ubertask.enable"));
        jae.injectLauncherUseUberMode(conf);
        assertEquals("true", conf.get("mapreduce.job.ubertask.enable"));

        // action conf set to true -- should keep at true
        conf = new Configuration(false);
        assertNull(conf.get("mapreduce.job.ubertask.enable"));
        conf.setBoolean("mapreduce.job.ubertask.enable", true);
        tjae.injectLauncherUseUberMode(conf);
        assertEquals("true", conf.get("mapreduce.job.ubertask.enable"));
        // action conf set to true -- should keep at true
        conf = new Configuration(false);
        assertNull(conf.get("mapreduce.job.ubertask.enable"));
        conf.setBoolean("mapreduce.job.ubertask.enable", true);
        jae.injectLauncherUseUberMode(conf);
        assertEquals("true", conf.get("mapreduce.job.ubertask.enable"));

        // action conf set to false -- should keep at false
        conf = new Configuration(false);
        assertNull(conf.get("mapreduce.job.ubertask.enable"));
        conf.setBoolean("mapreduce.job.ubertask.enable", false);
        tjae.injectLauncherUseUberMode(conf);
        assertEquals("false", conf.get("mapreduce.job.ubertask.enable"));
        // action conf set to false -- should keep at false
        conf = new Configuration(false);
        assertNull(conf.get("mapreduce.job.ubertask.enable"));
        conf.setBoolean("mapreduce.job.ubertask.enable", false);
        jae.injectLauncherUseUberMode(conf);
        assertEquals("false", conf.get("mapreduce.job.ubertask.enable"));

        // disable at oozie-site level for all actions except for the "test" action
        ConfigurationService.setBoolean("oozie.action.test.launcher.mapreduce.job.ubertask.enable", true);
        ConfigurationService.setBoolean("oozie.action.launcher.mapreduce.job.ubertask.enable", false);

        // default -- should be true
        conf = new Configuration(false);
        assertNull(conf.get("mapreduce.job.ubertask.enable"));
        tjae.injectLauncherUseUberMode(conf);
        assertEquals("true", conf.get("mapreduce.job.ubertask.enable"));
        // default -- should not set
        conf = new Configuration(false);
        assertNull(conf.get("mapreduce.job.ubertask.enable"));
        jae.injectLauncherUseUberMode(conf);
        assertNull(conf.get("mapreduce.job.ubertask.enable"));

        // action conf set to true -- should keep at true
        conf = new Configuration(false);
        assertNull(conf.get("mapreduce.job.ubertask.enable"));
        conf.setBoolean("mapreduce.job.ubertask.enable", true);
        tjae.injectLauncherUseUberMode(conf);
        assertEquals("true", conf.get("mapreduce.job.ubertask.enable"));
        // action conf set to true -- should keep at true
        conf = new Configuration(false);
        assertNull(conf.get("mapreduce.job.ubertask.enable"));
        conf.setBoolean("mapreduce.job.ubertask.enable", true);
        jae.injectLauncherUseUberMode(conf);
        assertEquals("true", conf.get("mapreduce.job.ubertask.enable"));

        // action conf set to false -- should keep at false
        conf = new Configuration(false);
        assertNull(conf.get("mapreduce.job.ubertask.enable"));
        conf.setBoolean("mapreduce.job.ubertask.enable", false);
        tjae.injectLauncherUseUberMode(conf);
        assertEquals("false", conf.get("mapreduce.job.ubertask.enable"));
        // action conf set to false -- should keep at false
        conf = new Configuration(false);
        assertNull(conf.get("mapreduce.job.ubertask.enable"));
        conf.setBoolean("mapreduce.job.ubertask.enable", false);
        jae.injectLauncherUseUberMode(conf);
        assertEquals("false", conf.get("mapreduce.job.ubertask.enable"));
    }

    public void testUpdateConfForJavaTmpDir() throws Exception {

        //Test UpdateCOnfForJavaTmpDir for launcherConf
        String actionXml1 = "<java>"
                        + "<job-tracker>"
                        + getJobTrackerUri()
                        + "</job-tracker>"
                        + "<name-node>"
                        + getNameNodeUri()
                        + "</name-node>"
                        + "<configuration>"
                        + "<property><name>oozie.launcher.yarn.app.mapreduce.am.command-opts</name>"
                        + "<value>-Xmx1024m -Djava.net.preferIPv4Stack=true -Djava.io.tmpdir=./usr</value></property>"
                        + "<property><name>oozie.launcher.mapred.child.java.opts</name>"
                        + "<value>-Xmx2048m -Djava.net.preferIPv4Stack=true</value></property>"
                        + "<property><name>oozie.launcher.mapreduce.reduce.java.opts</name>"
                        + "<value>-Xmx2560m -XX:NewRatio=8 -Djava.io.tmpdir=./usr</value></property>"
                        + "</configuration>" + "<main-class>MAIN-CLASS</main-class>" + "</java>";
        JavaActionExecutor ae = new JavaActionExecutor();
        WorkflowJobBean wfBean = addRecordToWfJobTable("test1", actionXml1);
        WorkflowActionBean action = (WorkflowActionBean) wfBean.getActions().get(0);
        action.setType(ae.getType());
        action.setConf(actionXml1);

        Context context = new Context(wfBean, action);
        Element actionXmlconf = XmlUtils.parseXml(action.getConf());

        Configuration actionConf = ae.createBaseHadoopConf(context, actionXmlconf);
        Configuration conf = ae.createLauncherConf(getFileSystem(), context, action, actionXmlconf, actionConf);

        assertEquals("-Xmx2048m -Djava.net.preferIPv4Stack=true -Djava.io.tmpdir=./tmp",
                conf.get(JavaActionExecutor.HADOOP_CHILD_JAVA_OPTS));
        assertEquals("-Xmx2048m -Djava.net.preferIPv4Stack=true -Djava.io.tmpdir=./tmp",
                conf.get(JavaActionExecutor.HADOOP_MAP_JAVA_OPTS));
        assertEquals("-Xmx2560m -XX:NewRatio=8 -Djava.io.tmpdir=./usr", conf.get(JavaActionExecutor.HADOOP_REDUCE_JAVA_OPTS));
        assertEquals("-Xmx1024m -Djava.net.preferIPv4Stack=true -Djava.io.tmpdir=./usr -Xmx2048m " +
                        "-Djava.net.preferIPv4Stack=true -Xmx2560m", conf.get(JavaActionExecutor.YARN_AM_COMMAND_OPTS).trim());

        //Test UpdateConfForJavaTmpDIr for actionConf
        String actionXml = "<java>"
                        + "<job-tracker>"
                        + getJobTrackerUri()
                        + "</job-tracker>"
                        + "<name-node>"
                        + getNameNodeUri()
                        + "</name-node>"
                        + "<configuration>"
                        + "<property><name>mapreduce.map.java.opts</name>"
                        + "<value>-Xmx1024m -Djava.io.tmpdir=./usr</value></property>"
                        + "<property><name>mapreduce.reduce.java.opts</name>"
                        + "<value>-Xmx2560m -XX:NewRatio=8</value></property>"
                        + "</configuration>" + "<main-class>MAIN-CLASS</main-class>" + "</java>";
        Element eActionXml = XmlUtils.parseXml(actionXml);
        Context context2 = createContext(actionXml, null);
        Path appPath2 = getAppPath();
        JavaActionExecutor ae2 = new JavaActionExecutor();
        Configuration jobConf = ae2.createBaseHadoopConf(context2, eActionXml);
        ae2.setupActionConf(jobConf, context2, eActionXml, appPath2);

        assertEquals("-Xmx200m -Djava.io.tmpdir=./tmp", jobConf.get(JavaActionExecutor.HADOOP_CHILD_JAVA_OPTS));
        assertEquals("-Xmx1024m -Djava.io.tmpdir=./usr", jobConf.get(JavaActionExecutor.HADOOP_MAP_JAVA_OPTS));
        assertEquals("-Xmx2560m -XX:NewRatio=8 -Djava.io.tmpdir=./tmp", jobConf.get(JavaActionExecutor.HADOOP_REDUCE_JAVA_OPTS));
        assertNull(jobConf.get(JavaActionExecutor.YARN_AM_COMMAND_OPTS));
    }
    public void testUpdateConfForUberMode() throws Exception {
        Element actionXml1 = XmlUtils
                .parseXml("<java>"
                        + "<job-tracker>"
                        + getJobTrackerUri()
                        + "</job-tracker>"
                        + "<name-node>"
                        + getNameNodeUri()
                        + "</name-node>"
                        + "<configuration>"
                        + "<property><name>oozie.launcher.mapreduce.map.memory.mb</name><value>2048</value></property>"
                        + "<property><name>oozie.launcher.mapred.child.java.opts</name>"
                        + "<value>-Xmx2048m -Djava.net.preferIPv4Stack=true</value></property>"
                        + "<property><name>oozie.launcher.mapred.child.env</name><value>A=foo</value></property>"
                        + "</configuration>" + "<main-class>MAIN-CLASS</main-class>" + "</java>");
        JavaActionExecutor ae = new JavaActionExecutor();
        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());

        WorkflowJobBean wf = createBaseWorkflow(protoConf, "action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());

        Context context = new Context(wf, action);
        JobConf launcherConf = new JobConf();
        launcherConf = ae.createLauncherConf(getFileSystem(), context, action, actionXml1, launcherConf);
        // memoryMB (2048 + 512)
        assertEquals("2560", launcherConf.get(JavaActionExecutor.YARN_AM_RESOURCE_MB));
        // heap size in child.opts (2048 + 512)
        int heapSize = ae.extractHeapSizeMB(launcherConf.get(JavaActionExecutor.YARN_AM_COMMAND_OPTS));
        assertEquals("-Xmx2048m -Djava.net.preferIPv4Stack=true -Djava.io.tmpdir=./tmp",
                launcherConf.get("mapred.child.java.opts"));
        assertEquals("-Xmx2048m -Djava.net.preferIPv4Stack=true -Djava.io.tmpdir=./tmp",
                launcherConf.get("mapreduce.map.java.opts"));
        // There's an extra parameter (-Xmx1024m) in here when using YARN that's not here when using MR1
        if (createJobConf().get("yarn.resourcemanager.address") != null) {
            assertEquals("-Xmx1024m -Xmx2048m -Djava.net.preferIPv4Stack=true -Xmx2560m",
                    launcherConf.get(JavaActionExecutor.YARN_AM_COMMAND_OPTS).trim());
        }
        else {
            assertEquals("-Xmx2048m -Djava.net.preferIPv4Stack=true -Xmx2560m -Djava.io.tmpdir=./tmp",
                    launcherConf.get(JavaActionExecutor.YARN_AM_COMMAND_OPTS).trim());
        }
        assertEquals(2560, heapSize);

        // env
        assertEquals("A=foo", launcherConf.get(JavaActionExecutor.YARN_AM_ENV));

        Element actionXml2 = XmlUtils
                .parseXml("<java>"
                        + "<job-tracker>"
                        + getJobTrackerUri()
                        + "</job-tracker>"
                        + "<name-node>"
                        + getNameNodeUri()
                        + "</name-node>"
                        + "<configuration>"
                        + "<property><name>oozie.launcher.yarn.app.mapreduce.am.resource.mb</name><value>3072</value></property>"
                        + "<property><name>oozie.launcher.mapreduce.map.memory.mb</name><value>2048</value></property>"
                        + "<property><name>oozie.launcher.yarn.app.mapreduce.am.command-opts</name>"
                        + "<value>-Xmx1024m -Djava.net.preferIPv4Stack=true </value></property>"
                        + "<property><name>oozie.launcher.mapred.child.java.opts</name><value>-Xmx1536m</value></property>"
                        + "<property><name>oozie.launcher.mapreduce.map.java.opts</name>"
                        + "<value>-Xmx2560m -XX:NewRatio=8</value></property>"
                        + "<property><name>oozie.launcher.yarn.app.mapreduce.am.env</name><value>A=foo</value></property>"
                        + "<property><name>oozie.launcher.mapred.child.env</name><value>B=bar</value></property>"
                        + "</configuration>" + "<main-class>MAIN-CLASS</main-class>" + "</java>");

        launcherConf = ae.createLauncherConf(getFileSystem(), context, action, actionXml2, launcherConf);

        // memoryMB (3072 + 512)
        assertEquals("3584", launcherConf.get(JavaActionExecutor.YARN_AM_RESOURCE_MB));

        // heap size (2560 + 512)
        heapSize = ae.extractHeapSizeMB(launcherConf.get(JavaActionExecutor.YARN_AM_COMMAND_OPTS));
        assertEquals("-Xmx1536m -Xmx2560m -XX:NewRatio=8 -Djava.io.tmpdir=./tmp", launcherConf.get("mapred.child.java.opts"));
        assertEquals("-Xmx1536m -Xmx2560m -XX:NewRatio=8 -Djava.io.tmpdir=./tmp", launcherConf.get("mapreduce.map.java.opts"));
        assertEquals("-Xmx1024m -Djava.net.preferIPv4Stack=true -Xmx1536m -Xmx2560m -XX:NewRatio=8 " +
                        "-Xmx3072m -Djava.io.tmpdir=./tmp", launcherConf.get(JavaActionExecutor.YARN_AM_COMMAND_OPTS).trim());
        assertEquals(3072, heapSize);

        // env (equqls to mapreduce.map.env + am.env)
        assertTrue(launcherConf.get(JavaActionExecutor.YARN_AM_ENV).trim().equals("A=foo,B=bar"));

        // Test limit is applied in case of 32 bit
        Element actionXml3 = XmlUtils
                .parseXml("<java>"
                        + "<job-tracker>"
                        + getJobTrackerUri()
                        + "</job-tracker>"
                        + "<name-node>"
                        + getNameNodeUri()
                        + "</name-node>"
                        + "<configuration>"
                        + "<property><name>oozie.launcher.yarn.app.mapreduce.am.resource.mb</name><value>3072</value></property>"
                        + "<property><name>oozie.launcher.mapreduce.map.memory.mb</name><value>4000</value></property>"
                        + "<property><name>oozie.launcher.yarn.app.mapreduce.am.command-opts</name>"
                        + "<value>-Xmx1024m -Djava.net.preferIPv4Stack=true</value></property>"
                        + "<property><name>oozie.launcher.mapred.child.java.opts</name><value>-Xmx1536m</value></property>"
                        + "<property><name>oozie.launcher.mapreduce.map.java.opts</name>"
                        + "<value>-Xmx4000m -XX:NewRatio=8</value></property>"
                        + "<property><name>oozie.launcher.yarn.app.mapreduce.am.env</name><value>A=foo</value></property>"
                        + "<property><name>oozie.launcher.mapred.child.env</name><value>B=bar</value></property>"
                        + "</configuration>" + "<main-class>MAIN-CLASS</main-class>" + "</java>");

        launcherConf = ae.createBaseHadoopConf(context, actionXml3);
        launcherConf = ae.createLauncherConf(getFileSystem(), context, action, actionXml3, launcherConf);

        // memoryMB (limit to 4096)
        assertEquals("4096", launcherConf.get(JavaActionExecutor.YARN_AM_RESOURCE_MB));

        // heap size (limit to 3584)
        heapSize = ae.extractHeapSizeMB(launcherConf.get(JavaActionExecutor.YARN_AM_COMMAND_OPTS));
        assertEquals("-Xmx1536m -Xmx4000m -XX:NewRatio=8 -Djava.io.tmpdir=./tmp", launcherConf.get("mapred.child.java.opts"));
        assertEquals("-Xmx1536m -Xmx4000m -XX:NewRatio=8 -Djava.io.tmpdir=./tmp", launcherConf.get("mapreduce.map.java.opts"));
        assertEquals("-Xmx1024m -Djava.net.preferIPv4Stack=true -Xmx1536m -Xmx4000m -XX:NewRatio=8 " +
                        "-Xmx3584m -Djava.io.tmpdir=./tmp", launcherConf.get(JavaActionExecutor.YARN_AM_COMMAND_OPTS).trim());
        assertEquals(3584, heapSize);

        // env (equqls to mapreduce.map.env + am.env)
        assertEquals("A=foo,B=bar", launcherConf.get(JavaActionExecutor.YARN_AM_ENV));
    }

    public void testUpdateConfForUberModeWithEnvDup() throws Exception {
        Element actionXml1 = XmlUtils.parseXml("<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>"
                + "<name-node>" + getNameNodeUri() + "</name-node>" + "<configuration>"
                + "<property><name>oozie.launcher.yarn.app.mapreduce.am.env</name>"
                + "<value>JAVA_HOME=/home/blah/java/jdk64/current,A=foo,B=bar</value></property>"
                + "<property><name>oozie.launcher.mapreduce.map.env</name>"
                + "<value>JAVA_HOME=/home/blah/java/jdk64/latest,C=blah</value></property>" + "</configuration>"
                + "<main-class>MAIN-CLASS</main-class>" + "</java>");
        JavaActionExecutor ae = new JavaActionExecutor();
        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());

        WorkflowJobBean wf = createBaseWorkflow(protoConf, "action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());

        Context context = new Context(wf, action);
        JobConf launcherConf = new JobConf();
        launcherConf = ae.createLauncherConf(getFileSystem(), context, action, actionXml1, launcherConf);

        // uber mode should be disabled since JAVA_HOME points to different paths in am.evn and map.env
        assertEquals("false", launcherConf.get(JavaActionExecutor.HADOOP_YARN_UBER_MODE));

        // testing complicated env setting case
        Element actionXml2 = XmlUtils.parseXml("<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>"
                + "<name-node>" + getNameNodeUri() + "</name-node>" + "<configuration>" + "<property>"
                + "<name>oozie.launcher.yarn.app.mapreduce.am.env</name>"
                + "<value>LD_LIBRARY_PATH=$HADOOP_HOME_1/lib/native/`$JAVA_HOME/bin/java -d32 -version;"
                + "if [ $? -eq 0 ]; then echo Linux-i386-32; else echo Linux-amd64-64;fi`</value></property>"
                + "<property>" + "<name>oozie.launcher.mapreduce.map.env</name>"
                + "<value>LD_LIBRARY_PATH=$HADOOP_HOME_2/lib/native/`$JAVA_HOME/bin/java -d32 -version;"
                + "if [ $? -eq 0 ]; then echo Linux-i386-32; else echo Linux-amd64-64;fi`</value></property>"
                + "</configuration>" + "<main-class>MAIN-CLASS</main-class>" + "</java>");

        launcherConf = ae.createLauncherConf(getFileSystem(), context, action, actionXml2, launcherConf);

        // uber mode should be disabled since LD_LIBRARY_PATH is different in am.evn and map.env
        assertEquals("false", launcherConf.get(JavaActionExecutor.HADOOP_YARN_UBER_MODE));

        Element actionXml3 = XmlUtils
                .parseXml("<java>"
                        + "<job-tracker>"
                        + getJobTrackerUri()
                        + "</job-tracker>"
                        + "<name-node>"
                        + getNameNodeUri()
                        + "</name-node>"
                        + "<configuration>"
                        + "<property><name>oozie.launcher.yarn.app.mapreduce.am.env</name>"
                        + "<value>JAVA_HOME=/home/blah/java/jdk64/current,PATH=A,PATH=B</value></property>"
                        + "<property><name>oozie.launcher.mapreduce.map.env</name>"
                        + "<value>JAVA_HOME=/home/blah/java/jdk64/current,PATH=A</value></property>"
                        + "</configuration>" + "<main-class>MAIN-CLASS</main-class>" + "</java>");

        launcherConf = ae.createBaseHadoopConf(context, actionXml3);
        launcherConf = ae.createLauncherConf(getFileSystem(), context, action, actionXml3, launcherConf);

        // uber mode should be enabled since JAVA_HOME is the same, and PATH doesn't conflict
        assertEquals("true", launcherConf.get(JavaActionExecutor.HADOOP_YARN_UBER_MODE));

        // JAVA_HOME, PATH=A duplication is removed
        String a = launcherConf.get(JavaActionExecutor.YARN_AM_ENV);
        assertEquals("JAVA_HOME=/home/blah/java/jdk64/current,PATH=A,PATH=B",
                launcherConf.get(JavaActionExecutor.YARN_AM_ENV));
    }

    public void testUpdateConfForUberModeForJavaOpts() throws Exception {
        Element actionXml1 = XmlUtils
                .parseXml("<java>"
                        + "<job-tracker>"
                        + getJobTrackerUri()
                        + "</job-tracker>"
                        + "<name-node>"
                        + getNameNodeUri()
                        + "</name-node>"
                        + "<configuration>"
                        + "<property><name>oozie.launcher.yarn.app.mapreduce.am.command-opts</name>"
                        + "<value>-Xmx1024m -Djava.net.preferIPv4Stack=true </value></property>"
                        + "<property><name>oozie.launcher.mapreduce.map.java.opts</name><value>-Xmx1536m</value></property>"
                        + "</configuration>" + "<main-class>MAIN-CLASS</main-class>"
                        + "<java-opt>-Xmx2048m</java-opt>"
                        + "<java-opt>-Dkey1=val1</java-opt>"
                        + "<java-opt>-Dkey2=val2</java-opt>"
                        + "</java>");
        JavaActionExecutor ae = new JavaActionExecutor();
        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());

        WorkflowJobBean wf = createBaseWorkflow(protoConf, "action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());

        Context context = new Context(wf, action);
        JobConf launcherConf = new JobConf();
        launcherConf = ae.createLauncherConf(getFileSystem(), context, action, actionXml1, launcherConf);

        // heap size (2048 + 512)
        int heapSize = ae.extractHeapSizeMB(launcherConf.get(JavaActionExecutor.YARN_AM_COMMAND_OPTS));
        assertEquals("-Xmx200m -Xmx1536m -Xmx2048m -Dkey1=val1 -Dkey2=val2 -Djava.io.tmpdir=./tmp",
                launcherConf.get("mapred.child.java.opts"));
        assertEquals("-Xmx200m -Xmx1536m -Xmx2048m -Dkey1=val1 -Dkey2=val2 -Djava.io.tmpdir=./tmp",
                launcherConf.get("mapreduce.map.java.opts"));
        assertEquals("-Xmx1024m -Djava.net.preferIPv4Stack=true -Xmx200m -Xmx1536m -Xmx2048m -Dkey1=val1 -Dkey2=val2 -Xmx2560m " +
                        "-Djava.io.tmpdir=./tmp", launcherConf.get(JavaActionExecutor.YARN_AM_COMMAND_OPTS).trim());
        assertEquals(2560, heapSize);

        Element actionXml2 = XmlUtils
                .parseXml("<java>"
                        + "<job-tracker>"
                        + getJobTrackerUri()
                        + "</job-tracker>"
                        + "<name-node>"
                        + getNameNodeUri()
                        + "</name-node>"
                        + "<configuration>"
                        + "<property><name>oozie.launcher.yarn.app.mapreduce.am.command-opts</name>"
                        + "<value>-Xmx1024m -Djava.net.preferIPv4Stack=true </value></property>"
                        + "<property><name>oozie.launcher.mapreduce.map.java.opts</name><value>-Xmx1536m</value></property>"
                        + "</configuration>" + "<main-class>MAIN-CLASS</main-class>"
                        + "<java-opts>-Xmx2048m -Dkey1=val1</java-opts>"
                        + "</java>");

        launcherConf = ae.createLauncherConf(getFileSystem(), context, action, actionXml2, launcherConf);

        // heap size (2048 + 512)
        heapSize = ae.extractHeapSizeMB(launcherConf.get(JavaActionExecutor.YARN_AM_COMMAND_OPTS));
        assertEquals("-Xmx200m -Xmx1536m -Xmx2048m -Dkey1=val1 -Djava.io.tmpdir=./tmp",
                launcherConf.get("mapred.child.java.opts"));
        assertEquals("-Xmx200m -Xmx1536m -Xmx2048m -Dkey1=val1 -Djava.io.tmpdir=./tmp",
                launcherConf.get("mapreduce.map.java.opts"));
        assertEquals("-Xmx1024m -Djava.net.preferIPv4Stack=true -Xmx200m -Xmx1536m -Xmx2048m -Dkey1=val1 -Xmx2560m " +
                        "-Djava.io.tmpdir=./tmp", launcherConf.get(JavaActionExecutor.YARN_AM_COMMAND_OPTS).trim());
        assertEquals(2560, heapSize);

        Element actionXml3 = XmlUtils
                .parseXml("<java>"
                        + "<job-tracker>"
                        + getJobTrackerUri()
                        + "</job-tracker>"
                        + "<name-node>"
                        + getNameNodeUri()
                        + "</name-node>"
                        + "<configuration>"
                        + "<property><name>oozie.launcher.yarn.app.mapreduce.am.command-opts</name>"
                        + "<value>-Xmx2048m -Djava.net.preferIPv4Stack=true </value></property>"
                        + "<property><name>oozie.launcher.mapreduce.map.java.opts</name><value>-Xmx3072m</value></property>"
                        + "</configuration>" + "<main-class>MAIN-CLASS</main-class>"
                        + "<java-opts>-Xmx1024m -Dkey1=val1</java-opts>"
                        + "</java>");

        launcherConf = ae.createLauncherConf(getFileSystem(), context, action, actionXml3, launcherConf);

        // heap size (2048 + 512)
        heapSize = ae.extractHeapSizeMB(launcherConf.get(JavaActionExecutor.YARN_AM_COMMAND_OPTS));
        assertEquals("-Xmx200m -Xmx3072m -Xmx1024m -Dkey1=val1 -Djava.io.tmpdir=./tmp",
                launcherConf.get("mapred.child.java.opts"));
        assertEquals("-Xmx200m -Xmx3072m -Xmx1024m -Dkey1=val1 -Djava.io.tmpdir=./tmp",
                launcherConf.get("mapreduce.map.java.opts"));
        assertEquals("-Xmx2048m -Djava.net.preferIPv4Stack=true -Xmx200m -Xmx3072m -Xmx1024m -Dkey1=val1 -Xmx2560m " +
                        "-Djava.io.tmpdir=./tmp", launcherConf.get(JavaActionExecutor.YARN_AM_COMMAND_OPTS).trim());
        assertEquals(2560, heapSize);
    }

    public void testDisableUberForProperties() throws Exception {
        Element actionXml1 = XmlUtils.parseXml("<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>"
                + "<name-node>" + getNameNodeUri() + "</name-node>"
                + "<configuration>"
                + "<property><name>oozie.launcher.mapreduce.job.classloader</name>"
                + "<value>true</value></property>"
                + "</configuration>"
                + "<main-class>MAIN-CLASS</main-class>" + "</java>");
        JavaActionExecutor ae = new JavaActionExecutor();
        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());

        WorkflowJobBean wf = createBaseWorkflow(protoConf, "action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());

        Context context = new Context(wf, action);
        JobConf launcherConf = new JobConf();
        launcherConf = ae.createLauncherConf(getFileSystem(), context, action, actionXml1, launcherConf);

        // uber mode should be disabled since oozie.launcher.mapreduce.job.classloader=true
        assertEquals("false", launcherConf.get(JavaActionExecutor.HADOOP_YARN_UBER_MODE));

        Element actionXml2 = XmlUtils.parseXml("<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>"
                + "<name-node>" + getNameNodeUri() + "</name-node>"
                + "<configuration>"
                + "<property><name>oozie.launcher.mapreduce.user.classpath.first</name>"
                + "<value>true</value></property>"
                + "</configuration>"
                + "<main-class>MAIN-CLASS</main-class>" + "</java>");
        ae = new JavaActionExecutor();
        protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());

        wf = createBaseWorkflow(protoConf, "action");
        action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());

        context = new Context(wf, action);
        launcherConf = new JobConf();
        launcherConf = ae.createLauncherConf(getFileSystem(), context, action, actionXml2, launcherConf);

        // uber mode should be disabled since oozie.launcher.mapreduce.user.classpath.first=true
        assertEquals("false", launcherConf.get(JavaActionExecutor.HADOOP_YARN_UBER_MODE));
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
        JobConf actionConf = new JobConf();

        // Test when server side setting is not enabled
        JobConf launcherConf = ae.createLauncherConf(getFileSystem(), context, action, actionXml, actionConf);
        assertNull(launcherConf.get(JavaActionExecutor.HADOOP_YARN_TIMELINE_SERVICE_ENABLED));

        ConfigurationService.set("oozie.action.launcher." + JavaActionExecutor.HADOOP_YARN_TIMELINE_SERVICE_ENABLED, "true");

        // Test when server side setting is enabled but tez-site.xml is not in DistributedCache
        launcherConf = ae.createLauncherConf(getFileSystem(), context, action, actionXml, actionConf);
        assertNull(launcherConf.get(JavaActionExecutor.HADOOP_YARN_TIMELINE_SERVICE_ENABLED));

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
        assertTrue(DistributedCache.getSymlink(conf));

        // test .jar without fragment where app path is on same cluster as jar path
        Path appJarPath = new Path("lib/a.jar");
        Path appJarFullPath = new Path(appPath, appJarPath);
        conf.clear();
        conf.set(WorkflowAppService.HADOOP_USER, getTestUser());
        ae.addToCache(conf, appPath, appJarFullPath.toString(), false);
        // assert that mapred.cache.files contains jar URI path (full on Hadoop-2)
        Path jarPath = createJobConf().get("yarn.resourcemanager.address") == null ?
                new Path(appJarFullPath.toUri().getPath()) :
                new Path(appJarFullPath.toUri());
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

        Path systemLibPath = new Path(wps.getSystemLibPath(), ShareLibService.SHARE_LIB_PREFIX
                + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()).toString());

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

        final RunningJob runningJob = submitAction(context);
        waitFor(60 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return runningJob.isComplete();
            }
        });
        assertTrue(runningJob.isSuccessful());
    }
}
