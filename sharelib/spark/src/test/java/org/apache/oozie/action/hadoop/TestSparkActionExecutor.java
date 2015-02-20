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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;

import java.io.OutputStreamWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;

import java.text.MessageFormat;
import java.util.Arrays;

public class TestSparkActionExecutor extends ActionExecutorTestCase {
    private static final String SPARK_FILENAME = "file.txt";
    private static final String OUTPUT = "output";

    @Override
    protected void setSystemProps() throws Exception {
        super.setSystemProps();
        setSystemProperty("oozie.service.ActionService.executor.classes", SparkActionExecutor.class.getName());
    }

    @SuppressWarnings("unchecked")
    public void testSetupMethods() throws Exception {
        SparkActionExecutor ae = new SparkActionExecutor();
        assertEquals(Arrays.asList(SparkMain.class), ae.getLauncherClasses());
    }

    private String getActionXml() {
        String script = "<spark xmlns=''uri:oozie:spark-action:0.1''>" +
                "<job-tracker>{0}</job-tracker>" +
                "<name-node>{1}</name-node>" +
                "<master>local[*]</master>" +
                "<mode>client</mode>" +
                "<name>SparkFileCopy</name>" +
                "<class>org.apache.oozie.example.SparkFileCopy</class>" +
                "<jar>" + getAppPath() +"/lib/test.jar</jar>" +
                "<arg>" + getAppPath() + "/" + SPARK_FILENAME + "</arg>" +
                "<arg>" + getAppPath() + "/" + OUTPUT + "</arg>" +
                "</spark>";
        return MessageFormat.format(script, getJobTrackerUri(), getNameNodeUri());
    }


    public void testSparkAction() throws Exception {
        FileSystem fs = getFileSystem();
        Path file = new Path(getAppPath(), SPARK_FILENAME);
        Writer scriptWriter = new OutputStreamWriter(fs.create(file));
        scriptWriter.write("1,2,3");
        scriptWriter.write("\n");
        scriptWriter.write("2,3,4");
        scriptWriter.close();

        Context context = createContext(getActionXml());
        final RunningJob launcherJob = submitAction(context);
        waitFor(200 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return launcherJob.isComplete();
            }
        });
        assertTrue(launcherJob.isSuccessful());

        SparkActionExecutor ae = new SparkActionExecutor();
        ae.check(context, context.getAction());
        assertEquals("SUCCEEDED", context.getAction().getExternalStatus());
        assertTrue(fs.exists(new Path(getAppPath() + "/" + OUTPUT)));
        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());

    }

    protected Context createContext(String actionXml) throws Exception {
        SparkActionExecutor ae = new SparkActionExecutor();

        File jarFile = IOUtils.createJar(new File(getTestCaseDir()), "test.jar", LauncherMainTester.class);
        InputStream is = new FileInputStream(jarFile);
        OutputStream os = getFileSystem().create(new Path(getAppPath(), "lib/test.jar"));
        IOUtils.copyStream(is, os);

        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());
        SharelibUtils.addToDistributedCache("spark", getFileSystem(), getFsTestCaseDir(), protoConf);

        WorkflowJobBean wf = createBaseWorkflow(protoConf, "spark-action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());
        action.setConf(actionXml);

        return new Context(wf, action);
    }

    protected RunningJob submitAction(Context context) throws Exception {
        SparkActionExecutor ae = new SparkActionExecutor();

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


}
