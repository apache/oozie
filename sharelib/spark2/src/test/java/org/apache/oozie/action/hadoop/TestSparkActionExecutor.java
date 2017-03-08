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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.SparkConfigurationService;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestSparkActionExecutor extends ActionExecutorTestCase {
    private static final String SPARK_FILENAME = "file.txt";
    private static final String OUTPUT = "output";
    private static Pattern SPARK_OPTS_PATTERN = Pattern.compile("([^= ]+)=([^= ]+)");
    public static String SPARK_TESTING_MEMORY = "spark.testing.memory=512000000"; // 512MB
    @Override
    protected void setSystemProps() throws Exception {
        super.setSystemProps();
        setSystemProperty("oozie.service.ActionService.executor.classes", SparkActionExecutor.class.getName());
    }

    public void testSetupMethods() throws Exception {
        _testSetupMethods("local[*]", new HashMap<String, String>(), "client");
        _testSetupMethods("yarn", new HashMap<String, String>(), "cluster");
        _testSetupMethods("yarn", new HashMap<String, String>(), "client");
        _testSetupMethods("yarn-cluster", new HashMap<String, String>(), null);
        _testSetupMethods("yarn-client", new HashMap<String, String>(), null);
    }

    public void testSetupMethodsWithSparkConfiguration() throws Exception {
        File sparkConfDir = new File(getTestCaseConfDir(), "spark-conf");
        sparkConfDir.mkdirs();
        File sparkConf = new File(sparkConfDir, "spark-defaults.conf");
        Properties sparkConfProps = new Properties();
        sparkConfProps.setProperty("a", "A");
        sparkConfProps.setProperty("b", "B");
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(sparkConf);
            sparkConfProps.store(fos, "");
        } finally {
            IOUtils.closeSafely(fos);
        }
        SparkConfigurationService scs = Services.get().get(SparkConfigurationService.class);
        scs.destroy();
        ConfigurationService.set("oozie.service.SparkConfigurationService.spark.configurations",
                getJobTrackerUri() + "=" + sparkConfDir.getAbsolutePath());
        scs.init(Services.get());

        _testSetupMethods("local[*]", new HashMap<String, String>(), "client");
        Map<String, String> extraSparkOpts = new HashMap<String, String>(2);
        extraSparkOpts.put("a", "A");
        extraSparkOpts.put("b", "B");
        _testSetupMethods("yarn-cluster", extraSparkOpts, null);
        _testSetupMethods("yarn-client", extraSparkOpts, null);
    }

    @SuppressWarnings("unchecked")
    private void _testSetupMethods(String master, Map<String, String> extraSparkOpts, String mode) throws Exception {
        SparkActionExecutor ae = new SparkActionExecutor();
        assertEquals(Arrays.asList(SparkMain2.class), ae.getLauncherClasses());

        Element actionXml = XmlUtils.parseXml("<spark>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<master>" + master + "</master>" +
                (mode != null ? "<mode>" + mode + "</mode>" : "") +
                "<name>Some Name</name>" +
                "<class>org.apache.oozie.foo</class>" +
                "<jar>" + getNameNodeUri() + "/foo.jar</jar>" +
                "<spark-opts>--conf foo=bar</spark-opts>" +
                "<version>1</version>"+
                "</spark>");

        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());

        WorkflowJobBean wf = createBaseWorkflow(protoConf, "spark-action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());

        Context context = new Context(wf, action);

        Configuration conf = ae.createBaseHadoopConf(context, actionXml);
        ae.setupActionConf(conf, context, actionXml, getFsTestCaseDir());
        assertEquals(master, conf.get("oozie.spark.master"));
        assertEquals(mode, conf.get("oozie.spark.mode"));
        assertEquals("Some Name", conf.get("oozie.spark.name"));
        assertEquals("org.apache.oozie.foo", conf.get("oozie.spark.class"));
        assertEquals(getNameNodeUri() + "/foo.jar", conf.get("oozie.spark.jar"));
        Map<String, String> sparkOpts = new HashMap<String, String>();
        sparkOpts.put("foo", "bar");
        sparkOpts.putAll(extraSparkOpts);
        Matcher m = SPARK_OPTS_PATTERN.matcher(conf.get("oozie.spark.spark-opts"));
        int count = 0;
        while (m.find()) {
            count++;
            String key = m.group(1);
            String val = m.group(2);
            assertEquals(sparkOpts.get(key), val);
        }
        assertEquals(sparkOpts.size(), count);
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
                "<spark-opts>--conf " +SPARK_TESTING_MEMORY+"</spark-opts>"+
                "<version>1</version>"+
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
