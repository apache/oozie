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
import java.io.Writer;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

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

    private void _testSubmit(String name, String actionXml) throws Exception {

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
        if (Boolean.parseBoolean(System.getProperty("oozie.test.hadoop.pipes", "false"))) {
            String wordCountBinary = TestPipesMain.getProgramName(this);
            Path programPath = new Path(getFsTestCaseDir(), "wordcount-simple");

            FileSystem fs = getFileSystem();

            InputStream is = IOUtils.getResourceAsStream(wordCountBinary, -1);
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
}
