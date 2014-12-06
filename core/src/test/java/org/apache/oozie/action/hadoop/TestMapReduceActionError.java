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

public class TestMapReduceActionError extends ActionExecutorTestCase {

    @Override
    protected void setSystemProps() throws Exception {
        super.setSystemProps();
        setSystemProperty("oozie.service.ActionService.executor.classes", MapReduceActionExecutor.class.getName());
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
