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
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.util.IOUtils;
import org.jdom.Element;

import java.io.File;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.StringReader;
import java.io.Writer;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

public class TestMapReduceActionError extends ActionExecutorTestCase {

    @Override
    protected void setSystemProps() throws Exception {
        super.setSystemProps();
        setSystemProperty("oozie.service.ActionService.executor.classes", MapReduceActionExecutor.class.getName());
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

        Element actionXml = XmlUtils.parseXml("<map-reduce>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<configuration>" +
                "<property><name>mapred.input.dir</name><value>IN</value></property>" +
                "<property><name>mapred.output.dir</name><value>OUT</value></property>" +
                "</configuration>" +
                "</map-reduce>");

        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());


        WorkflowJobBean wf = createBaseWorkflow(protoConf, "mr-action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());

        Context context = new Context(wf, action);

        Configuration conf = ae.createBaseHadoopConf(context, actionXml);
        ae.setupActionConf(conf, context, actionXml, getFsTestCaseDir());
        assertEquals("IN", conf.get("mapred.input.dir"));

        actionXml = XmlUtils.parseXml("<map-reduce>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<streaming>" +
                "<mapper>M</mapper>" +
                "<reducer>R</reducer>" +
                "<record-reader>RR</record-reader>" +
                "<record-reader-mapping>RRM1=1</record-reader-mapping>" +
                "<record-reader-mapping>RRM2=2</record-reader-mapping>" +
                "<env>e=E</env>" +
                "<env>ee=EE</env>" +
                "</streaming>" +
                "<configuration>" +
                "<property><name>mapred.input.dir</name><value>IN</value></property>" +
                "<property><name>mapred.output.dir</name><value>OUT</value></property>" +
                "</configuration>" +
                "</map-reduce>");

        conf = ae.createBaseHadoopConf(context, actionXml);
        ae.setupActionConf(conf, context, actionXml, getFsTestCaseDir());
        assertEquals("M", conf.get("oozie.streaming.mapper"));
        assertEquals("R", conf.get("oozie.streaming.reducer"));
        assertEquals("RR", conf.get("oozie.streaming.record-reader"));
        assertEquals("2", conf.get("oozie.streaming.record-reader-mapping.size"));
        assertEquals("2", conf.get("oozie.streaming.env.size"));

        actionXml = XmlUtils.parseXml("<map-reduce>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<pipes>" +
                "<map>M</map>" +
                "<reduce>R</reduce>" +
                "<inputformat>IF</inputformat>" +
                "<partitioner>P</partitioner>" +
                "<writer>W</writer>" +
                "<program>PP</program>" +
                "</pipes>" +
                "<configuration>" +
                "<property><name>mapred.input.dir</name><value>IN</value></property>" +
                "<property><name>mapred.output.dir</name><value>OUT</value></property>" +
                "</configuration>" +
                "</map-reduce>");

        conf = ae.createBaseHadoopConf(context, actionXml);
        ae.setupActionConf(conf, context, actionXml, getFsTestCaseDir());
        assertEquals("M", conf.get("oozie.pipes.map"));
        assertEquals("R", conf.get("oozie.pipes.reduce"));
        assertEquals("IF", conf.get("oozie.pipes.inputformat"));
        assertEquals("P", conf.get("oozie.pipes.partitioner"));
        assertEquals("W", conf.get("oozie.pipes.writer"));
        assertEquals(getFsTestCaseDir()+"/PP", conf.get("oozie.pipes.program"));
    }

    private Context createContext(String actionXml) throws Exception {
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
        action.setType(ae.getType());
        action.setConf(actionXml);

        return new Context(wf, action);
    }

    private RunningJob submitAction(Context context) throws Exception {
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
        XConfiguration conf =
                new XConfiguration(new StringReader(XmlUtils.prettyPrint(e.getChild("configuration")).toString()));
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

    private void _testSubmit(String actionXml) throws Exception {

        Context context = createContext(actionXml);
        final RunningJob launcherJob = submitAction(context);
        String launcherId = context.getAction().getExternalId();
        waitFor(60 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return launcherJob.isComplete();
            }
        });

        MapReduceActionExecutor ae = new MapReduceActionExecutor();
        ae.check(context, context.getAction());

        JobConf conf = ae.createBaseHadoopConf(context, XmlUtils.parseXml(actionXml));
        String user = conf.get("user.name");
        String group = conf.get("group.name");
        JobClient jobClient = Services.get().get(HadoopAccessorService.class).createJobClient(user, conf);
        final RunningJob mrJob = jobClient.getJob(JobID.forName(context.getAction().getExternalId()));

        waitFor(60 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return mrJob.isComplete();
            }
        });
        ae.check(context, context.getAction());

        assertEquals("FAILED/KILLED", context.getAction().getExternalStatus());

        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.ERROR, context.getAction().getStatus());
        assertTrue(context.getAction().getErrorMessage().contains("already exists"));
    }

    public void testMapReduce() throws Exception {
        FileSystem fs = getFileSystem();

        Path inputDir = new Path(getFsTestCaseDir(), "input");
        Path outputDir = new Path(getFsTestCaseDir(), "output1");

        Writer w = new OutputStreamWriter(fs.create(new Path(inputDir, "data.txt")));
        w.write("dummy\n");
        w.write("dummy\n");
        Writer ow = new OutputStreamWriter(fs.create(new Path(outputDir, "data.txt")));
        ow.write("dummy\n");
        ow.write("dummy\n");
        ow.close();

        String actionXml = "<map-reduce>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<configuration>" +
                "<property><name>mapred.mapper.class</name><value>" + MapperReducerForTest.class.getName() +
                "</value></property>" +
                "<property><name>mapred.reducer.class</name><value>" + MapperReducerForTest.class.getName() +
                "</value></property>" +
                "<property><name>mapred.input.dir</name><value>" + inputDir + "</value></property>" +
                "<property><name>mapred.output.dir</name><value>" + outputDir + "</value></property>" +
                "</configuration>" +
                "</map-reduce>";
        _testSubmit(actionXml);
    }

}
