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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.streaming.StreamJob;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.ClassUtils;
import org.jdom.Element;

import java.io.File;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.Writer;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.jar.JarOutputStream;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import org.apache.hadoop.fs.FileStatus;
import org.apache.oozie.action.ActionExecutorException;

public class TestMapReduceActionExecutor extends ActionExecutorTestCase {

    @Override
    protected void setSystemProps() throws Exception {
        super.setSystemProps();
        setSystemProperty("oozie.service.ActionService.executor.classes", MapReduceActionExecutor.class.getName());
        setSystemProperty("oozie.credentials.credentialclasses", "cred=org.apache.oozie.action.hadoop.CredentialForTest");
    }

    public void testLauncherJar() throws Exception {
        MapReduceActionExecutor ae = new MapReduceActionExecutor();
        Path jar = new Path(ae.getOozieRuntimeDir(), ae.getLauncherJarName());
        assertTrue(new File(jar.toString()).exists());
    }

    public Element createUberJarActionXML(String uberJarPath, String additional) throws Exception{
        return XmlUtils.parseXml("<map-reduce>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>"
                + "<name-node>" + getNameNodeUri() + "</name-node>" + additional + "<configuration>"
                + "<property><name>oozie.mapreduce.uber.jar</name><value>" + uberJarPath + "</value></property>"
                + "</configuration>" + "</map-reduce>");
    }

    public void testSetupMethods() throws Exception {
        MapReduceActionExecutor ae = new MapReduceActionExecutor();

        assertEquals("map-reduce", ae.getType());

        assertEquals("map-reduce-launcher.jar", ae.getLauncherJarName());

        List<Class> classes = new ArrayList<Class>();
        classes.add(LauncherMapper.class);
        classes.add(LauncherSecurityManager.class);
        classes.add(LauncherException.class);
        classes.add(LauncherMainException.class);
        classes.add(FileSystemActions.class);
        classes.add(PrepareActionsDriver.class);
        classes.add(ActionStats.class);
        classes.add(ActionType.class);
        classes.add(LauncherMain.class);
        classes.add(MapReduceMain.class);
        classes.add(StreamingMain.class);
        classes.add(PipesMain.class);
        assertEquals(classes, ae.getLauncherClasses());

        Element actionXml = XmlUtils.parseXml("<map-reduce>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>"
                + "<name-node>" + getNameNodeUri() + "</name-node>" + "<configuration>"
                + "<property><name>mapred.input.dir</name><value>IN</value></property>"
                + "<property><name>mapred.output.dir</name><value>OUT</value></property>" + "</configuration>"
                + "</map-reduce>");

        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());


        WorkflowJobBean wf = createBaseWorkflow(protoConf, "mr-action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());

        Context context = new Context(wf, action);

        Configuration conf = ae.createBaseHadoopConf(context, actionXml);
        ae.setupActionConf(conf, context, actionXml, getFsTestCaseDir());
        assertEquals("IN", conf.get("mapred.input.dir"));

        // Enable uber jars to test that MapReduceActionExecutor picks up the oozie.mapreduce.uber.jar property correctly
        Services serv = Services.get();
        boolean originalUberJarDisabled = serv.getConf().getBoolean("oozie.action.mapreduce.uber.jar.enable", false);
        serv.getConf().setBoolean("oozie.action.mapreduce.uber.jar.enable", true);

        actionXml = createUberJarActionXML(getNameNodeUri() + "/app/job.jar", "");
        conf = ae.createBaseHadoopConf(context, actionXml);
        ae.setupActionConf(conf, context, actionXml, getFsTestCaseDir());
        assertEquals(getNameNodeUri() + "/app/job.jar", conf.get("oozie.mapreduce.uber.jar"));  // absolute path with namenode
        JobConf launcherJobConf = ae.createLauncherConf(getFileSystem(), context, action, actionXml, conf);
        assertEquals(getNameNodeUri() + "/app/job.jar", launcherJobConf.getJar());              // same for launcher conf

        actionXml = createUberJarActionXML("/app/job.jar", "");
        conf = ae.createBaseHadoopConf(context, actionXml);
        ae.setupActionConf(conf, context, actionXml, getFsTestCaseDir());
        assertEquals(getNameNodeUri() + "/app/job.jar", conf.get("oozie.mapreduce.uber.jar"));  // absolute path without namenode
        launcherJobConf = ae.createLauncherConf(getFileSystem(), context, action, actionXml, conf);
        assertEquals(getNameNodeUri() + "/app/job.jar", launcherJobConf.getJar());              // same for launcher conf

        actionXml = createUberJarActionXML("job.jar", "");
        conf = ae.createBaseHadoopConf(context, actionXml);
        ae.setupActionConf(conf, context, actionXml, getFsTestCaseDir());
        assertEquals(getFsTestCaseDir() + "/job.jar", conf.get("oozie.mapreduce.uber.jar"));    // relative path
        launcherJobConf = ae.createLauncherConf(getFileSystem(), context, action, actionXml, conf);
        assertEquals(getFsTestCaseDir() + "/job.jar", launcherJobConf.getJar());                // same for launcher

        actionXml = createUberJarActionXML("job.jar", "<streaming></streaming>");
        conf = ae.createBaseHadoopConf(context, actionXml);
        ae.setupActionConf(conf, context, actionXml, getFsTestCaseDir());
        assertEquals("", conf.get("oozie.mapreduce.uber.jar"));                                 // ignored for streaming
        launcherJobConf = ae.createLauncherConf(getFileSystem(), context, action, actionXml, conf);
        assertNull(launcherJobConf.getJar());                                                   // same for launcher conf (not set)

        actionXml = createUberJarActionXML("job.jar", "<pipes></pipes>");
        conf = ae.createBaseHadoopConf(context, actionXml);
        ae.setupActionConf(conf, context, actionXml, getFsTestCaseDir());
        assertEquals("", conf.get("oozie.mapreduce.uber.jar"));                                 // ignored for pipes
        launcherJobConf = ae.createLauncherConf(getFileSystem(), context, action, actionXml, conf);
        assertNull(launcherJobConf.getJar());                                                   // same for launcher conf (not set)

        actionXml = XmlUtils.parseXml("<map-reduce>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>"
                + "<name-node>" + getNameNodeUri() + "</name-node>" + "</map-reduce>");
        conf = ae.createBaseHadoopConf(context, actionXml);
        ae.setupActionConf(conf, context, actionXml, getFsTestCaseDir());
        assertNull(conf.get("oozie.mapreduce.uber.jar"));                                       // doesn't resolve if not set
        launcherJobConf = ae.createLauncherConf(getFileSystem(), context, action, actionXml, conf);
        assertNull(launcherJobConf.getJar());                                                   // same for launcher conf

        // Disable uber jars to test that MapReduceActionExecutor won't allow the oozie.mapreduce.uber.jar property
        serv.getConf().setBoolean("oozie.action.mapreduce.uber.jar.enable", false);
        try {
            actionXml = createUberJarActionXML(getNameNodeUri() + "/app/job.jar", "");
            conf = ae.createBaseHadoopConf(context, actionXml);
            ae.setupActionConf(conf, context, actionXml, getFsTestCaseDir());
            fail("ActionExecutorException expected because uber jars are disabled");
        } catch (ActionExecutorException aee) {
            assertEquals("MR003", aee.getErrorCode());
            assertEquals(ActionExecutorException.ErrorType.ERROR, aee.getErrorType());
            assertTrue(aee.getMessage().contains("oozie.action.mapreduce.uber.jar.enable"));
            assertTrue(aee.getMessage().contains("oozie.mapreduce.uber.jar"));
        }
        serv.getConf().setBoolean("oozie.action.mapreduce.uber.jar.enable", originalUberJarDisabled);

        actionXml = XmlUtils.parseXml("<map-reduce>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>"
                + "<name-node>" + getNameNodeUri() + "</name-node>" + "<streaming>" + "<mapper>M</mapper>"
                + "<reducer>R</reducer>" + "<record-reader>RR</record-reader>"
                + "<record-reader-mapping>RRM1=1</record-reader-mapping>"
                + "<record-reader-mapping>RRM2=2</record-reader-mapping>" + "<env>e=E</env>" + "<env>ee=EE</env>"
                + "</streaming>" + "<configuration>"
                + "<property><name>mapred.input.dir</name><value>IN</value></property>"
                + "<property><name>mapred.output.dir</name><value>OUT</value></property>" + "</configuration>"
                + "</map-reduce>");

        conf = ae.createBaseHadoopConf(context, actionXml);
        ae.setupActionConf(conf, context, actionXml, getFsTestCaseDir());
        assertEquals("M", conf.get("oozie.streaming.mapper"));
        assertEquals("R", conf.get("oozie.streaming.reducer"));
        assertEquals("RR", conf.get("oozie.streaming.record-reader"));
        assertEquals("2", conf.get("oozie.streaming.record-reader-mapping.size"));
        assertEquals("2", conf.get("oozie.streaming.env.size"));

        actionXml = XmlUtils.parseXml("<map-reduce>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>"
                + "<name-node>" + getNameNodeUri() + "</name-node>" + "<pipes>" + "<map>M</map>" + "<reduce>R</reduce>"
                + "<inputformat>IF</inputformat>" + "<partitioner>P</partitioner>" + "<writer>W</writer>"
                + "<program>PP</program>" + "</pipes>" + "<configuration>"
                + "<property><name>mapred.input.dir</name><value>IN</value></property>"
                + "<property><name>mapred.output.dir</name><value>OUT</value></property>" + "</configuration>"
                + "</map-reduce>");

        conf = ae.createBaseHadoopConf(context, actionXml);
        ae.setupActionConf(conf, context, actionXml, getFsTestCaseDir());
        assertEquals("M", conf.get("oozie.pipes.map"));
        assertEquals("R", conf.get("oozie.pipes.reduce"));
        assertEquals("IF", conf.get("oozie.pipes.inputformat"));
        assertEquals("P", conf.get("oozie.pipes.partitioner"));
        assertEquals("W", conf.get("oozie.pipes.writer"));
        assertEquals(getFsTestCaseDir()+"/PP", conf.get("oozie.pipes.program"));
    }

    protected Context createContext(String name, String actionXml) throws Exception {
        JavaActionExecutor ae = new JavaActionExecutor();

        Path appJarPath = new Path("lib/test.jar");
        File jarFile = IOUtils.createJar(new File(getTestCaseDir()), "test.jar", MapperReducerForTest.class);
        InputStream is = new FileInputStream(jarFile);
        OutputStream os = getFileSystem().create(new Path(getAppPath(), "lib/test.jar"));
        IOUtils.copyStream(is, os);

        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());

        protoConf.setStrings(WorkflowAppService.APP_LIB_PATH_LIST, appJarPath.toString());

        WorkflowJobBean wf = createBaseWorkflow(protoConf, "mr-action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setName(name);
        action.setType(ae.getType());
        action.setConf(actionXml);

        return new Context(wf, action);
    }

    protected Context createContextWithCredentials(String name, String actionXml) throws Exception {
        JavaActionExecutor ae = new JavaActionExecutor();

        Path appJarPath = new Path("lib/test.jar");
        File jarFile = IOUtils.createJar(new File(getTestCaseDir()), "test.jar", MapperReducerForTest.class);
        InputStream is = new FileInputStream(jarFile);
        OutputStream os = getFileSystem().create(new Path(getAppPath(), "lib/test.jar"));
        IOUtils.copyStream(is, os);

        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());
        protoConf.setStrings(WorkflowAppService.APP_LIB_PATH_LIST, appJarPath.toString());


        WorkflowJobBean wf = createBaseWorkflowWithCredentials(protoConf, "mr-action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setName(name);
        action.setType(ae.getType());
        action.setConf(actionXml);
        action.setCred("testcred");

        return new Context(wf, action);
    }

    protected RunningJob submitAction(Context context) throws Exception {
        MapReduceActionExecutor ae = new MapReduceActionExecutor();

        WorkflowAction action = context.getAction();

        ae.prepareActionDir(getFileSystem(), context);
        ae.submitLauncher(getFileSystem(), context, action);

        String jobId = action.getExternalId();
        String jobTracker = action.getTrackerUri();
        String consoleUrl = action.getConsoleUrl();
        assertNotNull(jobId);
        assertNotNull(jobTracker);
        assertNotNull(consoleUrl);

        Element e = XmlUtils.parseXml(action.getConf());
        XConfiguration conf = new XConfiguration(new StringReader(XmlUtils.prettyPrint(e.getChild("configuration"))
                .toString()));
        conf.set("mapred.job.tracker", e.getChildTextTrim("job-tracker"));
        conf.set("fs.default.name", e.getChildTextTrim("name-node"));
        conf.set("user.name", context.getProtoActionConf().get("user.name"));
        conf.set("group.name", getTestGroup());

        conf.set("mapreduce.framework.name", "yarn");
        JobConf jobConf = Services.get().get(HadoopAccessorService.class).createJobConf(jobTracker);
        XConfiguration.copy(conf, jobConf);
        String user = jobConf.get("user.name");
        String group = jobConf.get("group.name");
        JobClient jobClient = Services.get().get(HadoopAccessorService.class).createJobClient(user, jobConf);
        final RunningJob runningJob = jobClient.getJob(JobID.forName(jobId));
        assertNotNull(runningJob);
        return runningJob;
    }

    private String _testSubmit(String name, String actionXml) throws Exception {

        Context context = createContext(name, actionXml);
        final RunningJob launcherJob = submitAction(context);
        String launcherId = context.getAction().getExternalId();
        waitFor(120 * 2000, new Predicate() {
            public boolean evaluate() throws Exception {
                return launcherJob.isComplete();
            }
        });
        assertTrue(launcherJob.isSuccessful());

        assertTrue(LauncherMapper.hasIdSwap(launcherJob));

        MapReduceActionExecutor ae = new MapReduceActionExecutor();
        ae.check(context, context.getAction());
        assertFalse(launcherId.equals(context.getAction().getExternalId()));

        JobConf conf = ae.createBaseHadoopConf(context, XmlUtils.parseXml(actionXml));
        String user = conf.get("user.name");
        String group = conf.get("group.name");
        JobClient jobClient = Services.get().get(HadoopAccessorService.class).createJobClient(user, conf);
        final RunningJob mrJob = jobClient.getJob(JobID.forName(context.getAction().getExternalId()));

        waitFor(120 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return mrJob.isComplete();
            }
        });
        assertTrue(mrJob.isSuccessful());
        ae.check(context, context.getAction());

        assertEquals("SUCCEEDED", context.getAction().getExternalStatus());
        assertNull(context.getAction().getData());

        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());

        //hadoop.counters will always be set in case of MR action.
        assertNotNull(context.getVar("hadoop.counters"));
        String counters = context.getVar("hadoop.counters");
        assertTrue(counters.contains("Counter"));

        //External Child IDs will always be null in case of MR action.
        assertNull(context.getExternalChildIDs());

        return mrJob.getID().toString();
    }

    private void _testSubmitWithCredentials(String name, String actionXml) throws Exception {

        Context context = createContextWithCredentials("map-reduce", actionXml);
        final RunningJob launcherJob = submitAction(context);
        String launcherId = context.getAction().getExternalId();
        waitFor(120 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return launcherJob.isComplete();
            }
        });
        assertTrue(launcherJob.isSuccessful());

        assertTrue(LauncherMapper.hasIdSwap(launcherJob));

        MapReduceActionExecutor ae = new MapReduceActionExecutor();
        ae.check(context, context.getAction());
        assertFalse(launcherId.equals(context.getAction().getExternalId()));

        JobConf conf = ae.createBaseHadoopConf(context, XmlUtils.parseXml(actionXml));
        String user = conf.get("user.name");
        String group = conf.get("group.name");
        JobClient jobClient = Services.get().get(HadoopAccessorService.class).createJobClient(user, conf);
        final RunningJob mrJob = jobClient.getJob(JobID.forName(context.getAction().getExternalId()));

        waitFor(120 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return mrJob.isComplete();
            }
        });
        assertTrue(mrJob.isSuccessful());
        ae.check(context, context.getAction());

        assertEquals("SUCCEEDED", context.getAction().getExternalStatus());
        assertNull(context.getAction().getData());

        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());

        assertTrue(MapperReducerCredentialsForTest.hasCredentials(mrJob));
    }

    protected XConfiguration getMapReduceConfig(String inputDir, String outputDir) {
        XConfiguration conf = new XConfiguration();
        conf.set("mapred.mapper.class", MapperReducerForTest.class.getName());
        conf.set("mapred.reducer.class", MapperReducerForTest.class.getName());
        conf.set("mapred.input.dir", inputDir);
        conf.set("mapred.output.dir", outputDir);
        return conf;
    }

    protected XConfiguration getMapReduceCredentialsConfig(String inputDir, String outputDir) {
        XConfiguration conf = new XConfiguration();
        conf.set("mapred.mapper.class", MapperReducerCredentialsForTest.class.getName());
        conf.set("mapred.reducer.class", MapperReducerForTest.class.getName());
        conf.set("mapred.input.dir", inputDir);
        conf.set("mapred.output.dir", outputDir);
        return conf;
    }

    protected XConfiguration getMapReduceUberJarConfig(String inputDir, String outputDir) throws Exception{
        XConfiguration conf = new XConfiguration();
        conf.set("mapred.mapper.class", MapperReducerUberJarForTest.class.getName());
        conf.set("mapred.reducer.class", MapperReducerUberJarForTest.class.getName());
        conf.set("mapred.input.dir", inputDir);
        conf.set("mapred.output.dir", outputDir);
        conf.set("oozie.mapreduce.uber.jar", createAndUploadUberJar().toUri().toString());
        return conf;
    }

    public void testMapReduce() throws Exception {
        FileSystem fs = getFileSystem();

        Path inputDir = new Path(getFsTestCaseDir(), "input");
        Path outputDir = new Path(getFsTestCaseDir(), "output");

        Writer w = new OutputStreamWriter(fs.create(new Path(inputDir, "data.txt")));
        w.write("dummy\n");
        w.write("dummy\n");
        w.close();

        String actionXml = "<map-reduce>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>"
                + getMapReduceConfig(inputDir.toString(), outputDir.toString()).toXmlString(false) + "</map-reduce>";
        _testSubmit("map-reduce", actionXml);
    }

    public void testMapReduceWithCredentials() throws Exception {
        FileSystem fs = getFileSystem();

        Path inputDir = new Path(getFsTestCaseDir(), "input");
        Path outputDir = new Path(getFsTestCaseDir(), "output");

        Writer w = new OutputStreamWriter(fs.create(new Path(inputDir, "data.txt")));
        w.write("dummy\n");
        w.write("dummy\n");
        w.close();

        String actionXml = "<map-reduce>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>"
                + getMapReduceCredentialsConfig(inputDir.toString(), outputDir.toString()).toXmlString(false)
                + "</map-reduce>";
        _testSubmitWithCredentials("map-reduce", actionXml);
    }

    protected Path createAndUploadUberJar() throws Exception {
        Path localJobJarPath = makeUberJarWithLib(getTestCaseDir());
        Path remoteJobJarPath = new Path(getAppPath(), localJobJarPath.getName());
        getFileSystem().moveFromLocalFile(localJobJarPath, remoteJobJarPath);
        File localJobJarFile = new File(localJobJarPath.toUri().toString());
        if (localJobJarFile.exists()) {     // just to make sure
            localJobJarFile.delete();
        }
        return remoteJobJarPath;
    }

    private Path makeUberJarWithLib(String testDir) throws Exception {
        Path jobJarPath = new Path(testDir, "uber.jar");
        FileOutputStream fos = new FileOutputStream(new File(jobJarPath.toUri().getPath()));
        JarOutputStream jos = new JarOutputStream(fos);
        // Have to put in real jar files or it will complain
        createAndAddJarToJar(jos, new File(new Path(testDir, "lib1.jar").toUri().getPath()));
        createAndAddJarToJar(jos, new File(new Path(testDir, "lib2.jar").toUri().getPath()));
        jos.close();
        return jobJarPath;
    }

    private void createAndAddJarToJar(JarOutputStream jos, File jarFile) throws Exception {
        FileOutputStream fos2 = new FileOutputStream(jarFile);
        JarOutputStream jos2 = new JarOutputStream(fos2);
        // Have to have at least one entry or it will complain
        ZipEntry ze = new ZipEntry(jarFile.getName() + ".inside");
        jos2.putNextEntry(ze);
        jos2.closeEntry();
        jos2.close();
        ze = new ZipEntry("lib/" + jarFile.getName());
        jos.putNextEntry(ze);
        FileInputStream in = new FileInputStream(jarFile);
        byte buf[] = new byte[1024];
        int numRead;
        do {
            numRead = in.read(buf);
            if (numRead >= 0) {
                jos.write(buf, 0, numRead);
            }
        } while (numRead != -1);
        in.close();
        jos.closeEntry();
        jarFile.delete();
    }

    public void _testMapReduceWithUberJar() throws Exception {
        FileSystem fs = getFileSystem();

        Path inputDir = new Path(getFsTestCaseDir(), "input");
        Path outputDir = new Path(getFsTestCaseDir(), "output");

        Writer w = new OutputStreamWriter(fs.create(new Path(inputDir, "data.txt")));
        w.write("dummy\n");
        w.write("dummy\n");
        w.close();

        String actionXml = "<map-reduce>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>"
                + getMapReduceUberJarConfig(inputDir.toString(), outputDir.toString()).toXmlString(false) + "</map-reduce>";
        String jobID = _testSubmit("map-reduce", actionXml);

        boolean containsLib1Jar = false;
        String lib1JarStr = "jobcache/" + jobID + "/jars/lib/lib1.jar";
        Pattern lib1JarPatYarn = Pattern.compile(
                ".*appcache/application_" + jobID.replaceFirst("job_", "") + "/filecache/.*/uber.jar/lib/lib1.jar");
        boolean containsLib2Jar = false;
        String lib2JarStr = "jobcache/" + jobID + "/jars/lib/lib1.jar";
        Pattern lib2JarPatYarn = Pattern.compile(
                ".*appcache/application_" + jobID.replaceFirst("job_", "") + "/filecache/.*/uber.jar/lib/lib2.jar");

        FileStatus[] fstats = getFileSystem().listStatus(outputDir);
        for (FileStatus fstat : fstats) {
            Path p = fstat.getPath();
            if (getFileSystem().isFile(p) && p.getName().startsWith("part-")) {
                InputStream is = getFileSystem().open(p);
                Scanner sc = new Scanner(is);
                while (sc.hasNextLine()) {
                    String line = sc.nextLine();
                    containsLib1Jar = (containsLib1Jar || line.endsWith(lib1JarStr) || lib1JarPatYarn.matcher(line).matches());
                    containsLib2Jar = (containsLib2Jar || line.endsWith(lib2JarStr) || lib2JarPatYarn.matcher(line).matches());
                }
                sc.close();
                is.close();
            }
        }

        assertTrue("lib/lib1.jar should have been unzipped from the uber jar and added to the classpath but was not",
                containsLib1Jar);
        assertTrue("lib/lib2.jar should have been unzipped from the uber jar and added to the classpath but was not",
                containsLib2Jar);
    }

    // With the oozie.action.mapreduce.uber.jar.enable property set to false, a workflow with an uber jar should fail
    // (this happens before we get to Hadoop, so not having the correct version of Hadoop doesn't matter here; the
    // TestMapReduceActionExecutorUberJar.testMapReduceWithUberJarEnabled() test actually tests the uber jar functionality, but
    // this test is excluded by default)
    public void testMapReduceWithUberJarDisabled() throws Exception {
        Services serv = Services.get();
        boolean originalUberJarDisabled = serv.getConf().getBoolean("oozie.action.mapreduce.uber.jar.enable", false);
        try {
            serv.getConf().setBoolean("oozie.action.mapreduce.uber.jar.enable", false);
            _testMapReduceWithUberJar();
        } catch (ActionExecutorException aee) {
            assertEquals("MR003", aee.getErrorCode());
            assertEquals(ActionExecutorException.ErrorType.ERROR, aee.getErrorType());
            assertTrue(aee.getMessage().contains("oozie.action.mapreduce.uber.jar.enable"));
            assertTrue(aee.getMessage().contains("oozie.mapreduce.uber.jar"));
        } catch (Exception e) {
            throw e;
        } finally {
            serv.getConf().setBoolean("oozie.action.mapreduce.uber.jar.enable", originalUberJarDisabled);
        }
    }

    protected XConfiguration getStreamingConfig(String inputDir, String outputDir) {
        XConfiguration conf = new XConfiguration();
        conf.set("mapred.input.dir", inputDir);
        conf.set("mapred.output.dir", outputDir);
        return conf;
    }

    public void testStreaming() throws Exception {
        FileSystem fs = getFileSystem();
        Path streamingJar = new Path(getFsTestCaseDir(), "jar/hadoop-streaming.jar");

        InputStream is = new FileInputStream(ClassUtils.findContainingJar(StreamJob.class));
        OutputStream os = fs.create(new Path(getAppPath(), streamingJar));
        IOUtils.copyStream(is, os);

        Path inputDir = new Path(getFsTestCaseDir(), "input");
        Path outputDir = new Path(getFsTestCaseDir(), "output");

        Writer w = new OutputStreamWriter(fs.create(new Path(inputDir, "data.txt")));
        w.write("dummy\n");
        w.write("dummy\n");
        w.close();

        String actionXml = "<map-reduce>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>" + "      <streaming>" + "        <mapper>cat</mapper>"
                + "        <reducer>wc</reducer>" + "      </streaming>"
                + getStreamingConfig(inputDir.toString(), outputDir.toString()).toXmlString(false) + "<file>"
                + streamingJar + "</file>" + "</map-reduce>";
        _testSubmit("streaming", actionXml);
    }

    protected XConfiguration getPipesConfig(String inputDir, String outputDir) {
        XConfiguration conf = new XConfiguration();
        conf.setBoolean("hadoop.pipes.java.recordreader", true);
        conf.setBoolean("hadoop.pipes.java.recordwriter", true);
        conf.set("mapred.input.dir", inputDir);
        conf.set("mapred.output.dir", outputDir);
        return conf;
    }

    private XConfiguration getOozieActionExternalStatsWriteProperty(String inputDir, String outputDir,
            String oozieProperty) {
        XConfiguration conf = new XConfiguration();
        conf.set("mapred.input.dir", inputDir);
        conf.set("mapred.output.dir", outputDir);
        conf.set("oozie.action.external.stats.write", oozieProperty);
        return conf;
    }

    public void testPipes() throws Exception {
        Path programPath = new Path(getFsTestCaseDir(), "wordcount-simple");

        FileSystem fs = getFileSystem();

        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("wordcount-simple");
        if (is != null) {
            OutputStream os = fs.create(programPath);
            IOUtils.copyStream(is, os);

            Path inputDir = new Path(getFsTestCaseDir(), "input");
            Path outputDir = new Path(getFsTestCaseDir(), "output");

            Writer w = new OutputStreamWriter(fs.create(new Path(inputDir, "data.txt")));
            w.write("dummy\n");
            w.write("dummy\n");
            w.close();

            String actionXml = "<map-reduce>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                    + getNameNodeUri() + "</name-node>" + "      <pipes>" + "        <program>" + programPath
                    + "#wordcount-simple" + "</program>" + "      </pipes>"
                    + getPipesConfig(inputDir.toString(), outputDir.toString()).toXmlString(false) + "<file>"
                    + programPath + "</file>" + "</map-reduce>";
            _testSubmit("pipes", actionXml);
        }
        else {
            System.out.println(
                "SKIPPING TEST: TestMapReduceActionExecutor.testPipes(), " +
                "binary 'wordcount-simple' not available in the classpath");
        }
    }

    // Test to assert that executionStats is set when user has specified stats
    // write property as true.
    public void testSetExecutionStats_when_user_has_specified_stats_write_TRUE() throws Exception {
        FileSystem fs = getFileSystem();

        Path inputDir = new Path(getFsTestCaseDir(), "input");
        Path outputDir = new Path(getFsTestCaseDir(), "output");

        Writer w = new OutputStreamWriter(fs.create(new Path(inputDir, "data.txt")));
        w.write("dummy\n");
        w.write("dummy\n");
        w.close();

        // set user stats write property as true explicitly in the
        // configuration.
        String actionXml = "<map-reduce>"
                + "<job-tracker>"
                + getJobTrackerUri()
                + "</job-tracker>"
                + "<name-node>"
                + getNameNodeUri()
                + "</name-node>"
                + getOozieActionExternalStatsWriteProperty(inputDir.toString(), outputDir.toString(), "true")
                        .toXmlString(false) + "</map-reduce>";

        Context context = createContext("map-reduce", actionXml);
        final RunningJob launcherJob = submitAction(context);
        String launcherId = context.getAction().getExternalId();
        waitFor(120 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return launcherJob.isComplete();
            }
        });
        assertTrue(launcherJob.isSuccessful());

        assertTrue(LauncherMapper.hasIdSwap(launcherJob));

        MapReduceActionExecutor ae = new MapReduceActionExecutor();
        ae.check(context, context.getAction());
        assertFalse(launcherId.equals(context.getAction().getExternalId()));

        JobConf conf = ae.createBaseHadoopConf(context, XmlUtils.parseXml(actionXml));
        String user = conf.get("user.name");
        String group = conf.get("group.name");
        JobClient jobClient = Services.get().get(HadoopAccessorService.class).createJobClient(user, conf);
        final RunningJob mrJob = jobClient.getJob(JobID.forName(context.getAction().getExternalId()));

        waitFor(120 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return mrJob.isComplete();
            }
        });
        assertTrue(mrJob.isSuccessful());
        ae.check(context, context.getAction());

        assertEquals("SUCCEEDED", context.getAction().getExternalStatus());
        assertNull(context.getAction().getData());

        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());

        // Assert for stats info stored in the context.
        assertNotNull(context.getExecutionStats());
        assertTrue(context.getExecutionStats().contains("ACTION_TYPE"));
        assertTrue(context.getExecutionStats().contains("Counter"));

        // External Child IDs will always be null in case of MR action.
        assertNull(context.getExternalChildIDs());

        // hadoop.counters will always be set in case of MR action.
        assertNotNull(context.getVar("hadoop.counters"));
        String counters = context.getVar("hadoop.counters");
        assertTrue(counters.contains("Counter"));
    }

    // Test to assert that executionStats is not set when user has specified
    // stats write property as false.
    public void testSetExecutionStats_when_user_has_specified_stats_write_FALSE() throws Exception {
        FileSystem fs = getFileSystem();

        Path inputDir = new Path(getFsTestCaseDir(), "input");
        Path outputDir = new Path(getFsTestCaseDir(), "output");

        Writer w = new OutputStreamWriter(fs.create(new Path(inputDir, "data.txt")));
        w.write("dummy\n");
        w.write("dummy\n");
        w.close();

        // set user stats write property as false explicitly in the
        // configuration.
        String actionXml = "<map-reduce>"
                + "<job-tracker>"
                + getJobTrackerUri()
                + "</job-tracker>"
                + "<name-node>"
                + getNameNodeUri()
                + "</name-node>"
                + getOozieActionExternalStatsWriteProperty(inputDir.toString(), outputDir.toString(), "false")
                        .toXmlString(false) + "</map-reduce>";

        Context context = createContext("map-reduce", actionXml);
        final RunningJob launcherJob = submitAction(context);
        String launcherId = context.getAction().getExternalId();
        waitFor(120 * 2000, new Predicate() {
            public boolean evaluate() throws Exception {
                return launcherJob.isComplete();
            }
        });
        assertTrue(launcherJob.isSuccessful());

        assertTrue(LauncherMapper.hasIdSwap(launcherJob));

        MapReduceActionExecutor ae = new MapReduceActionExecutor();
        ae.check(context, context.getAction());
        assertFalse(launcherId.equals(context.getAction().getExternalId()));

        JobConf conf = ae.createBaseHadoopConf(context, XmlUtils.parseXml(actionXml));
        String user = conf.get("user.name");
        String group = conf.get("group.name");
        JobClient jobClient = Services.get().get(HadoopAccessorService.class).createJobClient(user, conf);
        final RunningJob mrJob = jobClient.getJob(JobID.forName(context.getAction().getExternalId()));

        waitFor(120 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return mrJob.isComplete();
            }
        });
        assertTrue(mrJob.isSuccessful());
        ae.check(context, context.getAction());

        assertEquals("SUCCEEDED", context.getAction().getExternalStatus());
        assertNull(context.getAction().getData());

        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());

        // Assert for stats info stored in the context.
        assertNull(context.getExecutionStats());

        // External Child IDs will always be null in case of MR action.
        assertNull(context.getExternalChildIDs());

        // hadoop.counters will always be set in case of MR action.
        assertNotNull(context.getVar("hadoop.counters"));
        String counters = context.getVar("hadoop.counters");
        assertTrue(counters.contains("Counter"));
    }

    /**
     * Test "oozie.launcher.mapred.job.name" and "mapred.job.name" can be set in
     * the action configuration and not overridden by the action executor
     *
     * @throws Exception
     */
    public void testSetMapredJobName() throws Exception {
        final String launcherJobName = "MapReduceLauncherTest";
        final String mapredJobName = "MapReduceTest";

        FileSystem fs = getFileSystem();

        Path inputDir = new Path(getFsTestCaseDir(), "input");
        Path outputDir = new Path(getFsTestCaseDir(), "output");

        Writer w = new OutputStreamWriter(fs.create(new Path(inputDir,
                "data.txt")));
        w.write("dummy\n");
        w.write("dummy\n");
        w.close();

        XConfiguration mrConfig = getMapReduceConfig(inputDir.toString(),
                outputDir.toString());
        mrConfig.set("oozie.launcher.mapred.job.name", launcherJobName);
        mrConfig.set("mapred.job.name", mapredJobName);

        StringBuilder sb = new StringBuilder("<map-reduce>")
                .append("<job-tracker>").append(getJobTrackerUri())
                .append("</job-tracker>").append("<name-node>")
                .append(getNameNodeUri()).append("</name-node>")
                .append(mrConfig.toXmlString(false)).append("</map-reduce>");
        String actionXml = sb.toString();

        Context context = createContext("map-reduce", actionXml);
        final RunningJob launcherJob = submitAction(context);
        String launcherId = context.getAction().getExternalId();
        waitFor(120 * 2000, new Predicate() {
            public boolean evaluate() throws Exception {
                return launcherJob.isComplete();
            }
        });

        assertTrue(launcherJob.isSuccessful());
        assertTrue(LauncherMapper.hasIdSwap(launcherJob));
        // Assert launcher job name has been set
        System.out.println("Launcher job name: " + launcherJob.getJobName());
        assertTrue(launcherJob.getJobName().equals(launcherJobName));

        MapReduceActionExecutor ae = new MapReduceActionExecutor();
        ae.check(context, context.getAction());
        assertFalse(launcherId.equals(context.getAction().getExternalId()));

        JobConf conf = ae.createBaseHadoopConf(context,
                XmlUtils.parseXml(actionXml));
        String user = conf.get("user.name");

        JobClient jobClient = Services.get().get(HadoopAccessorService.class)
                .createJobClient(user, conf);
        final RunningJob mrJob = jobClient.getJob(JobID.forName(context
                .getAction().getExternalId()));

        waitFor(120 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return mrJob.isComplete();
            }
        });
        assertTrue(mrJob.isSuccessful());
        ae.check(context, context.getAction());

        assertEquals("SUCCEEDED", context.getAction().getExternalStatus());
        assertNull(context.getAction().getData());

        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());

        // Assert Mapred job name has been set
        System.out.println("Mapred job name: " + mrJob.getJobName());
        assertTrue(mrJob.getJobName().equals(mapredJobName));

        // Assert for stats info stored in the context.
        assertNull(context.getExecutionStats());

        // External Child IDs will always be null in case of MR action.
        assertNull(context.getExternalChildIDs());

        // hadoop.counters will always be set in case of MR action.
        assertNotNull(context.getVar("hadoop.counters"));
        String counters = context.getVar("hadoop.counters");
        assertTrue(counters.contains("Counter"));
    }

    public void testDefaultShareLibName() {
        MapReduceActionExecutor ae = new MapReduceActionExecutor();
        Element e = new Element("mapreduce");
        assertNull(ae.getDefaultShareLibName(e));
        e.addContent(new Element("streaming"));
        assertEquals("mapreduce-streaming", ae.getDefaultShareLibName(e));
    }

    /**
     * https://issues.apache.org/jira/browse/OOZIE-87
     * This test covers map-reduce action
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

        String actionXml = "<map-reduce>" +
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
                "</map-reduce>";

        Element eActionXml = XmlUtils.parseXml(actionXml);

        Context context = createContext("map-reduce", actionXml);

        Path appPath = getAppPath();

        MapReduceActionExecutor ae = new MapReduceActionExecutor();

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


}
