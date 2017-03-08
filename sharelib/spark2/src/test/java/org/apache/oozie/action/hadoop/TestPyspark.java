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

import java.io.StringReader;
import java.text.MessageFormat;
import java.util.ArrayList;

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

public class TestPyspark extends ActionExecutorTestCase {


    public static String PY4J_ZIP = "py4j-0.9-src.zip";
    public static String PYSPARK_ZIP = "pyspark.zip";
    public static String PI_EXAMPLE = "pi.py";


    @Override
    protected void setSystemProps() throws Exception {
        super.setSystemProps();
        setSystemProperty("oozie.service.ActionService.executor.classes", SparkActionExecutor.class.getName());
    }

    protected String getActionXml(String sparkOpts) {
        String script = "<spark xmlns=''uri:oozie:spark-action:0.1''>" +
                "<job-tracker>{0}</job-tracker>" +
                "<name-node>{1}</name-node>" +
                "<master>local[*]</master>" +
                "<mode>client</mode>" +
                "<name>PysparkExample</name>" +
                "<jar>" + PI_EXAMPLE + "</jar>" +
                "<spark-opts>" +sparkOpts +"</spark-opts>" +
                "<version>1</version>"+
                "</spark>";
        return MessageFormat.format(script, getJobTrackerUri(), getNameNodeUri());
    }

    public void testPyspark() throws Exception {
        ArrayList<String> listLibFiles = new ArrayList<String>();

        // <spark-opts> does not have any files
        // pyspark and py4j are not present in current directory.
        String sparkOpts = "--conf " + TestSparkActionExecutor.SPARK_TESTING_MEMORY;
        WorkflowJobBean wf = getWorkflow(listLibFiles);
        testPysparkHelper(sparkOpts, wf, "FAILED/KILLED", WorkflowAction.Status.ERROR);

        // <spark-opts> has other files;
        // pyspark and py4j are not present in current directory.
        sparkOpts = "--py-files other.zip,myfunctions.py --conf " + TestSparkActionExecutor.SPARK_TESTING_MEMORY;
        listLibFiles.add("other.zip");
        listLibFiles.add("myfunctions.py");
        wf = getWorkflow(listLibFiles);
        testPysparkHelper(sparkOpts, wf, "FAILED/KILLED", WorkflowAction.Status.ERROR);

        // <spark-opts> does not have any files
        // pyspark and py4j are present in current directory.
        sparkOpts = "--conf " + TestSparkActionExecutor.SPARK_TESTING_MEMORY;
        listLibFiles.clear();
        listLibFiles.add(PY4J_ZIP);
        listLibFiles.add(PYSPARK_ZIP);
        wf = getWorkflow(listLibFiles);
        testPysparkHelper(sparkOpts, wf, "SUCCEEDED", WorkflowAction.Status.OK);

        // <spark-opts> has some other files
        // pyspark and py4j are present in current directory.
        sparkOpts = "--py-files other.zip,myfunctions.py --conf " + TestSparkActionExecutor.SPARK_TESTING_MEMORY;
        listLibFiles.clear();
        listLibFiles.add("other.zip");
        listLibFiles.add("myfunctions.py");
        listLibFiles.add(PY4J_ZIP);
        listLibFiles.add(PYSPARK_ZIP);
        wf = getWorkflow(listLibFiles);
        testPysparkHelper(sparkOpts, wf, "SUCCEEDED", WorkflowAction.Status.OK);
    }

    private void testPysparkHelper(String sparkOpts, WorkflowJobBean wf, String externalStatus,
            WorkflowAction.Status wfStatus)
            throws Exception {
        Context context = createContext(getActionXml(sparkOpts), wf);
        final RunningJob launcherJob = submitAction(context);
        waitFor(200 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return launcherJob.isComplete();
            }
        });
        assertTrue(launcherJob.isSuccessful());
        SparkActionExecutor ae = new SparkActionExecutor();
        ae.check(context, context.getAction());
        assertEquals(externalStatus, context.getAction().getExternalStatus());
        ae.end(context, context.getAction());
        assertEquals(wfStatus, context.getAction().getStatus());
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
        JobClient jobClient = Services.get().get(HadoopAccessorService.class).createJobClient(getTestUser(), jobConf);
        final RunningJob runningJob = jobClient.getJob(JobID.forName(jobId));
        assertNotNull(runningJob);
        return runningJob;
    }

    protected Context createContext(String actionXml, WorkflowJobBean wf) throws Exception {
        SparkActionExecutor ae = new SparkActionExecutor();
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());
        action.setConf(actionXml);
        return new Context(wf, action);
    }

    /**
     * @param listLibFiles list of files to be created in workflow lib/
     *        directory
     * @return
     * @throws Exception
     */
    protected WorkflowJobBean getWorkflow(ArrayList<String> listLibFiles) throws Exception {
        // add the example file as well
        listLibFiles.add(PI_EXAMPLE);
        String[] libPaths = new String[listLibFiles.size()];
        FileSystem fs = getFileSystem();
        for (int i = 0; i < listLibFiles.size(); i++) {
            libPaths[i] = new Path("lib/" + listLibFiles.get(i)).toString();
            if (listLibFiles.get(i).equals(PY4J_ZIP) || listLibFiles.get(i).equals(PYSPARK_ZIP)
                    || listLibFiles.get(i).equals(PI_EXAMPLE)) {
                IOUtils.copyStream(IOUtils.getResourceAsStream(listLibFiles.get(i), -1),
                        fs.create(new Path(getAppPath(), "lib/" + listLibFiles.get(i))));
            }
            else {
                fs.createNewFile(new Path(getAppPath(), "lib/" + listLibFiles.get(i)));
            }
        }
        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());
        SharelibUtils.addToDistributedCache("spark", getFileSystem(), getFsTestCaseDir(), protoConf);
        WorkflowJobBean wf = createBaseWorkflow(protoConf, "spark-action");
        String defaultProtoConf = wf.getProtoActionConf();
        XConfiguration newProtoConf = new XConfiguration(new StringReader(defaultProtoConf));
        newProtoConf.setStrings(WorkflowAppService.APP_LIB_PATH_LIST, libPaths);
        wf.setProtoActionConf(newProtoConf.toXmlString());
        return wf;
    }
}
