/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.ClassUtils;
import org.apache.pig.Main;
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
import java.util.Properties;

import jline.ConsoleReaderInputStream;

public class TestPigActionExecutor extends ActionExecutorTestCase {

    protected void setSystemProps() {
        super.setSystemProps();
        setSystemProperty("oozie.service.ActionService.executor.classes", PigActionExecutor.class.getName());
    }

    public void testLauncherJar() throws Exception {
        PigActionExecutor ae = new PigActionExecutor();
        Path jar = new Path(ae.getOozieRuntimeDir(), ae.getLauncherJarName());
        assertTrue(new File(jar.toString()).exists());
    }

    public void testSetupMethods() throws Exception {
        PigActionExecutor ae = new PigActionExecutor();

        assertEquals("pig", ae.getType());

        assertEquals("pig-launcher.jar", ae.getLauncherJarName());

        List<Class> classes = new ArrayList<Class>();
        classes.add(LauncherMapper.class);
        classes.add(LauncherSecurityManager.class);
        classes.add(LauncherException.class);
        classes.add(LauncherMain.class);
        classes.add(MapReduceMain.class);
        classes.add(PigMain.class);
        assertEquals(classes, ae.getLauncherClasses());


        Element actionXml = XmlUtils.parseXml("<pig>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<script>SCRIPT</script>" +
                "<param>a=A</param>" +
                "<param>b=B</param>" +
                "</pig>");

        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());
        protoConf.set(WorkflowAppService.HADOOP_UGI, getTestUser() + "," + getTestGroup());
        protoConf.set(OozieClient.GROUP_NAME, getTestGroup());
        injectKerberosInfo(protoConf);

        WorkflowJobBean wf = createBaseWorkflow(protoConf, "pig-action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());

        Context context = new Context(wf, action);

        Configuration conf = ae.createBaseHadoopConf(context, actionXml);
        ae.setupActionConf(conf, context, actionXml, getFsTestCaseDir());
        assertEquals("SCRIPT", conf.get("oozie.pig.script"));
        assertEquals("2", conf.get("oozie.pig.params.size"));
        assertEquals("a=A", conf.get("oozie.pig.params.0"));
        assertEquals("b=B", conf.get("oozie.pig.params.1"));
    }

    private Context createContext(String actionXml) throws Exception {
        PigActionExecutor ae = new PigActionExecutor();

        FileSystem fs = getFileSystem();

        Path pigJar = new Path(getAppPath(), "lib/pig.jar");
        InputStream is = new FileInputStream(ClassUtils.findContainingJar(Main.class));
        OutputStream os = fs.create(new Path(getAppPath(), pigJar));
        IOUtils.copyStream(is, os);

        Path jLineJar = new Path(getAppPath(), "lib/jline.jar");
        is = new FileInputStream(ClassUtils.findContainingJar(ConsoleReaderInputStream.class));
        os = fs.create(new Path(getAppPath(), jLineJar));
        IOUtils.copyStream(is, os);


        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());
        protoConf.set(WorkflowAppService.HADOOP_UGI, getTestUser() + "," + getTestGroup());
        protoConf.set(OozieClient.GROUP_NAME, getTestGroup());
        injectKerberosInfo(protoConf);
        protoConf.setStrings(WorkflowAppService.APP_LIB_JAR_PATH_LIST, pigJar.toString(), jLineJar.toString());

        WorkflowJobBean wf = createBaseWorkflow(protoConf, "pig-action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());
        action.setConf(actionXml);

        return new Context(wf, action);
    }

    private RunningJob submitAction(Context context) throws Exception {
        PigActionExecutor ae = new PigActionExecutor();

        WorkflowAction action = context.getAction();

        ae.prepareActionDir(getFileSystem(), context);
        ae.submitLauncher(context, action);

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
        injectKerberosInfo(conf);
        JobConf jobConf = new JobConf(conf);
        String user = jobConf.get("user.name");
        String group = jobConf.get("group.name");
        JobClient jobClient = Services.get().get(HadoopAccessorService.class).createJobClient(user, group, jobConf);
        final RunningJob runningJob = jobClient.getJob(JobID.forName(jobId));
        assertNotNull(runningJob);
        return runningJob;
    }

    private void _testSubmit(String actionXml) throws Exception {

        Context context = createContext(actionXml);
        final RunningJob launcherJob = submitAction(context);
        String launcherId = context.getAction().getExternalId();
        waitFor(120 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return launcherJob.isComplete();
            }
        });
        assertTrue(launcherJob.isSuccessful());

        assertFalse(LauncherMapper.hasIdSwap(launcherJob));
        assertTrue(LauncherMapper.hasOutputData(launcherJob));

        PigActionExecutor ae = new PigActionExecutor();
        ae.check(context, context.getAction());
        assertTrue(launcherId.equals(context.getAction().getExternalId()));
        assertEquals("SUCCEEDED", context.getAction().getExternalStatus());
        assertNotNull(context.getAction().getData());
        Properties outputData = new Properties();
        outputData.load(new StringReader(context.getAction().getData()));
        assertTrue(outputData.containsKey("hadoopJobs"));
        assertNotSame("", outputData.getProperty("hadoopJobs"));
        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());
    }

    private static final String PIG_SCRIPT = "set job.name 'test'\n" + "set debug on\n" +
            "A = load '$IN' using PigStorage(':');\n" +
            "B = foreach A generate $0 as id;\n" +
            "store B into '$OUT' USING PigStorage();\n";

    protected XConfiguration getPigConfig() {
        XConfiguration conf = new XConfiguration();
        conf.set("oozie.pig.log.level", "INFO");
        return conf;
    }

    public void testPig() throws Exception {
        FileSystem fs = getFileSystem();

        Path script = new Path(getAppPath(), "script.pig");
        Writer w = new OutputStreamWriter(fs.create(script));
        w.write(PIG_SCRIPT);
        w.close();

        Path inputDir = new Path(getFsTestCaseDir(), "input");
        Path outputDir = new Path(getFsTestCaseDir(), "output");

        w = new OutputStreamWriter(fs.create(new Path(inputDir, "data.txt")));
        w.write("dummy\n");
        w.write("dummy\n");
        w.close();

        String actionXml = "<pig>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                getPigConfig().toXmlString(false) +
                "<script>" + script.getName() + "</script>" +
                "<param>IN=" + inputDir.toUri().getPath() + "</param>" +
                "<param>OUT=" + outputDir.toUri().getPath() + "</param>" +
                "</pig>";
        _testSubmit(actionXml);
    }

    private static final String UDF_PIG_SCRIPT = "register udf.jar\n" +
            "set job.name 'test'\n" + "set debug on\n" +
            "A = load '$IN' using PigStorage(':');\n" +
            "B = foreach A generate" +
            "       org.apache.oozie.action.hadoop.UDFTester($0) as id;\n" +
            "store B into '$OUT' USING PigStorage();\n";

    public void testUdfPig() throws Exception {
        FileSystem fs = getFileSystem();

        Path udfJar = new Path(getFsTestCaseDir(), "udf.jar");
        File jarFile = IOUtils.createJar(new File(getTestCaseDir()), "udf.jar", UDFTester.class);
        InputStream is = new FileInputStream(jarFile);
        OutputStream os = getFileSystem().create(udfJar);
        IOUtils.copyStream(is, os);


        Path script = new Path(getAppPath(), "script.pig");
        Writer w = new OutputStreamWriter(fs.create(script));
        w.write(UDF_PIG_SCRIPT);
        w.close();

        Path inputDir = new Path(getFsTestCaseDir(), "input");
        Path outputDir = new Path(getFsTestCaseDir(), "output");

        w = new OutputStreamWriter(fs.create(new Path(inputDir, "data.txt")));
        w.write("dummy\n");
        w.write("dummy\n");
        w.close();

        String actionXml = "<pig>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                getPigConfig().toXmlString(false) +
                "<script>" + script.getName() + "</script>" +
                "<param>IN=" + inputDir.toUri().getPath() + "</param>" +
                "<param>OUT=" + outputDir.toUri().getPath() + "</param>" +
                "<file>" + udfJar.toString() + "#" + udfJar.getName() + "</file>" +
                "</pig>";
        _testSubmit(actionXml);
    }

}