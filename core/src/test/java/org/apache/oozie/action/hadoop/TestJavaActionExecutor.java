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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
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
import org.apache.oozie.action.hadoop.ActionExecutorTestCase.Context;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.LiteWorkflowStoreService;
import org.apache.oozie.service.Services;
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

public class TestJavaActionExecutor extends ActionExecutorTestCase {

    @Override
    protected void setSystemProps() throws Exception {
        super.setSystemProps();
        setSystemProperty("oozie.service.ActionService.executor.classes", JavaActionExecutor.class.getName());
        setSystemProperty("oozie.service.HadoopAccessorService.action.configurations",
                          "*=hadoop-conf," + getJobTrackerUri() + "=action-conf");
        new File(getTestCaseConfDir(), "action-conf").mkdir();
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("test-action-config.xml");
        OutputStream os = new FileOutputStream(new File(getTestCaseConfDir() + "/action-conf", "java.xml"));
        IOUtils.copyStream(is, os);
    }

    public void testLauncherJar() throws Exception {
        JavaActionExecutor ae = new JavaActionExecutor();
        Path jar = new Path(ae.getOozieRuntimeDir(), ae.getLauncherJarName());
        assertTrue(new File(jar.toString()).exists());
    }

    public void testSetupMethods() throws Exception {
        JavaActionExecutor ae = new JavaActionExecutor();
        assertEquals("java", ae.getType());
        assertEquals("java-launcher.jar", ae.getLauncherJarName());
        List<Class> classes = new ArrayList<Class>();
        classes.add(LauncherMapper.class);
        classes.add(LauncherSecurityManager.class);
        classes.add(LauncherException.class);
        classes.add(LauncherMainException.class);
        classes.add(FileSystemActions.class);
        classes.add(PrepareActionsDriver.class);
        classes.add(ActionStats.class);
        classes.add(ActionType.class);
        assertEquals(classes, ae.getLauncherClasses());

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

        assertTrue(ae.getOozieLauncherJar(context).startsWith(context.getActionDir().toString()));
        assertTrue(ae.getOozieLauncherJar(context).endsWith(ae.getLauncherJarName()));

        assertFalse(getFileSystem().exists(context.getActionDir()));
        ae.prepareActionDir(getFileSystem(), context);
        assertTrue(getFileSystem().exists(context.getActionDir()));
        assertTrue(getFileSystem().exists(new Path(context.getActionDir(), ae.getLauncherJarName())));

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
        assertEquals("MAIN-CLASS", ae.getLauncherMain(conf, actionXml));
        assertTrue(conf.get("mapred.child.java.opts").contains("JAVA-OPTS"));
        assertEquals(Arrays.asList("A1", "A2"), Arrays.asList(LauncherMapper.getMainArguments(conf)));
        assertNotNull(conf.get(LauncherMapper.CONF_OOZIE_ACTION_SUPPORTED_FILESYSTEMS));

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
        assertFalse(LauncherMapper.isMainSuccessful(runningJob));
        ActionExecutor ae = new JavaActionExecutor();
        ae.check(context, context.getAction());
        assertTrue(ae.isCompleted(context.getAction().getExternalStatus()));
        assertEquals("FAILED/KILLED", context.getAction().getExternalStatus());
        assertEquals("1", context.getAction().getErrorCode());
        assertNull(context.getAction().getData());

        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.ERROR, context.getAction().getStatus());
    }

    public void testExceptionSubmitError() throws Exception {
        String actionXml = "<java>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<main-class>" + LauncherMainTester.class.getName() + "</main-class>" +
                "<arg>ex</arg>" +
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
        assertFalse(LauncherMapper.isMainSuccessful(runningJob));
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
                return LauncherMapper.getRecoveryId(conf, context.getActionDir(), context.getRecoveryId()) != null;
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

        Configuration conf = Services.get().getConf();
        conf.set("oozie.credentials.credentialclasses", "abc=org.apache.oozie.action.hadoop.InsertTestToken");

        // Adding if action need to set more credential tokens
        JobConf credentialsConf = new JobConf();
        Configuration launcherConf = ae.createBaseHadoopConf(context, actionXmlconf);
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
        Configuration protoActionConf = wps.createProtoActionConf(conf, authToken, true);
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
        workflow.setAuthToken(authToken);
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

        assertEquals("java-action-executor", ae.getShareLibName(context, new Element("java"), actionConf));

        Services.get().getConf().set("oozie.action.sharelib.for.java", "java-oozie-conf");
        assertEquals("java-oozie-conf", ae.getShareLibName(context, new Element("java"), actionConf));

        jobConf = "<configuration>" + "<property>"
               + "<name>oozie.action.sharelib.for.java</name>"
               + "<value>java-job-conf</value>" + "</property>"
               + "</configuration>";
        wfBean.setConf(jobConf);
        assertEquals("java-job-conf", ae.getShareLibName(context, new Element("java"), actionConf));

        actionConf.set("oozie.action.sharelib.for.java", "java-action-conf");
        assertEquals("java-action-conf", ae.getShareLibName(context, new Element("java"), actionConf));
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

        assertTrue(conf.get("mapred.child.java.opts").contains("JAVA-OPT1"));
        assertTrue(conf.get("mapred.child.java.opts").contains("JAVA-OPT2"));

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

        assertTrue(conf.get("mapred.child.java.opts").contains("JAVA-OPT1"));
        assertTrue(conf.get("mapred.child.java.opts").contains("JAVA-OPT2"));
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

    public void testAddShareLibSchemeAndAuthority() throws Exception {
        JavaActionExecutor ae = new JavaActionExecutor() {
            @Override
            protected String getDefaultShareLibName(Element actionXml) {
                return "java-action-executor";
            }
        };
        String actionXml = "<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>" + "<main-class>" + LauncherMainTester.class.getName()
                + "</main-class>" + "</java>";
        Element eActionXml = XmlUtils.parseXml(actionXml);
        Context context = createContext(actionXml, null);

        // Set sharelib to a relative path (i.e. no scheme nor authority)
        Services.get().destroy();
        setSystemProperty(WorkflowAppService.SYSTEM_LIB_PATH, "/user/" + getOozieUser() + "/share/");
        new Services().init();
        Path appPath = getAppPath();
        JobConf conf = ae.createBaseHadoopConf(context, eActionXml);
        // The next line should not throw an Exception because it will get the scheme and authority from the appPath, and not the
        // sharelib path because it doesn't have a scheme or authority
        ae.addShareLib(appPath, conf, "java-action-executor");

        appPath = new Path("foo://bar:1234/blah");
        conf = ae.createBaseHadoopConf(context, eActionXml);
        // The next line should throw an Exception because it will get the scheme and authority from the appPath, which is obviously
        // invalid, and not the sharelib path because it doesn't have a scheme or authority
        try {
            ae.addShareLib(appPath, conf, "java-action-executor");
        }
        catch (ActionExecutorException aee) {
            assertEquals("E0902", aee.getErrorCode());
            assertTrue(aee.getMessage().contains("[No FileSystem for scheme: foo]"));
        }

        // Set sharelib to a full path (i.e. include scheme and authority)
        Services.get().destroy();
        setSystemProperty(WorkflowAppService.SYSTEM_LIB_PATH, getNameNodeUri() + "/user/" + getOozieUser() + "/share/");
        new Services().init();
        appPath = new Path("foo://bar:1234/blah");
        conf = ae.createBaseHadoopConf(context, eActionXml);
        // The next line should not throw an Exception because it will get the scheme and authority from the sharelib path (and not
        // from the obviously invalid appPath)
        ae.addShareLib(appPath, conf, "java-action-executor");
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
}
