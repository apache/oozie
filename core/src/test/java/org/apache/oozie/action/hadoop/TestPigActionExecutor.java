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
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.util.IOUtils;
import org.jdom.Element;
import org.json.simple.JSONValue;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.Writer;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestPigActionExecutor extends ActionExecutorTestCase {

    private static final String PIG_SCRIPT = "set job.name 'test'\n" + "set debug on\n" +
            "A = load '$IN' using PigStorage(':');\n" +
            "B = foreach A generate $0 as id;\n" +
            "store B into '$OUT' USING PigStorage();\n";

    private static final String ERROR_PIG_SCRIPT = "set job.name 'test'\n" + "set debug on\n" +
            "A = load '$IN' using PigStorage(':');\n" +
            "ERROR @#$@#$;\n";

    private static final String UDF_PIG_SCRIPT = "register udf.jar\n" +
            "set job.name 'test'\n" + "set debug on\n" +
            "A = load '$IN' using PigStorage(':');\n" +
            "B = foreach A generate" +
            "       org.apache.oozie.action.hadoop.UDFTester($0) as id;\n" +
            "store B into '$OUT' USING PigStorage();\n";

    @Override
    protected void setSystemProps() throws Exception {
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
        classes.add(LauncherMainException.class);
        classes.add(FileSystemActions.class);
        classes.add(PrepareActionsDriver.class);
        classes.add(ActionStats.class);
        classes.add(ActionType.class);
        classes.add(LauncherMain.class);
        classes.add(MapReduceMain.class);
        classes.add(PigMain.class);
        classes.add(OoziePigStats.class);
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

        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());


        SharelibUtils.addToDistributedCache("pig", fs, getFsTestCaseDir(), protoConf);

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
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("user.name", context.getProtoActionConf().get("user.name"));
        conf.set("group.name", getTestGroup());

        JobConf jobConf = Services.get().get(HadoopAccessorService.class).createJobConf(jobTracker);
        XConfiguration.copy(conf, jobConf);
        String user = jobConf.get("user.name");
        String group = jobConf.get("group.name");
        JobClient jobClient = Services.get().get(HadoopAccessorService.class).createJobClient(user, jobConf);
        final RunningJob runningJob = jobClient.getJob(JobID.forName(jobId));
        assertNotNull(runningJob);
        return runningJob;
    }

    private void _testSubmit(String actionXml, boolean checkForSuccess) throws Exception {

        Context context = createContext(actionXml);
        final RunningJob launcherJob = submitAction(context);
        String launcherId = context.getAction().getExternalId();
        evaluateLauncherJob(launcherJob);
        assertTrue(launcherJob.isSuccessful());

        Thread.sleep(2000);
        assertFalse(LauncherMapper.hasIdSwap(launcherJob));
        if (checkForSuccess) {
            assertTrue(LauncherMapper.hasStatsData(launcherJob));
        }

        PigActionExecutor ae = new PigActionExecutor();
        ae.check(context, context.getAction());
        ae.end(context, context.getAction());
        assertTrue(launcherId.equals(context.getAction().getExternalId()));
        if (checkForSuccess) {
            assertEquals("SUCCEEDED", context.getAction().getExternalStatus());
            assertNull(context.getAction().getData());

        }
        else {
            assertEquals("FAILED/KILLED", context.getAction().getExternalStatus());
            assertNotNull(context.getAction().getErrorMessage());
        }
        if (checkForSuccess) {
            assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());
        }
        else {
            assertEquals(WorkflowAction.Status.ERROR, context.getAction().getStatus());
        }
    }

    /*
     * Test the stats retrieved from a Pig job
     */
    public void testExecutionStats() throws Exception {
        // Set the action xml with the option for retrieving stats to true
        String actionXml = setPigActionXml(PIG_SCRIPT, true);
        Context context = createContext(actionXml);
        final RunningJob launcherJob = submitAction(context);
        evaluateLauncherJob(launcherJob);
        assertTrue(launcherJob.isSuccessful());
        assertTrue(LauncherMapper.hasStatsData(launcherJob));

        PigActionExecutor ae = new PigActionExecutor();
        WorkflowAction wfAction = context.getAction();
        ae.check(context, wfAction);
        ae.end(context, wfAction);

        assertEquals("SUCCEEDED", wfAction.getExternalStatus());
        String stats = wfAction.getStats();
        assertNotNull(stats);
        // check for some of the expected key values in the stats
        Map m = (Map)JSONValue.parse(stats);
        // check for expected 1st level JSON keys
        assertTrue(m.containsKey("PIG_VERSION"));

        String expectedChildIDs = wfAction.getExternalChildIDs();
        String[] childIDs = expectedChildIDs.split(",");
        assertTrue(m.containsKey(childIDs[0]));

        Map q = (Map)m.get(childIDs[0]);
        // check for expected 2nd level JSON keys
        assertTrue(q.containsKey("HADOOP_COUNTERS"));
    }


    /*
     * Test the Hadoop IDs obtained from the Pig job
     */
    public void testExternalChildIds() throws Exception {
        // Set the action xml with the option for retrieving stats to false
        String actionXml = setPigActionXml(PIG_SCRIPT, false);
        Context context = createContext(actionXml);
        final RunningJob launcherJob = submitAction(context);
        evaluateLauncherJob(launcherJob);
        assertTrue(launcherJob.isSuccessful());

        PigActionExecutor ae = new PigActionExecutor();
        WorkflowAction wfAction = context.getAction();
        ae.check(context, wfAction);
        ae.end(context, wfAction);

        assertEquals("SUCCEEDED", wfAction.getExternalStatus());
        String externalIds = wfAction.getExternalChildIDs();
        assertNotNull(externalIds);
        assertNotSame("", externalIds);
        // check for the expected prefix of hadoop jobIDs
        assertTrue(externalIds.contains("job_"));

    }

    /*
     * Test the stats after setting the maximum allowed size of stats to a small
     * value
     */
    public void testExecutionStatsWithMaxStatsSizeLimit() throws Exception {
        // Set a very small value for max size of stats
        setSystemProperty(JavaActionExecutor.MAX_EXTERNAL_STATS_SIZE, new String("1"));
        new Services().init();
        // Set the action xml with the option for retrieving stats to true
        String actionXml = setPigActionXml(PIG_SCRIPT, true);
        Context context = createContext(actionXml);
        final RunningJob launcherJob = submitAction(context);
        evaluateLauncherJob(launcherJob);
        assertTrue(launcherJob.isSuccessful());

        PigActionExecutor ae = new PigActionExecutor();
        WorkflowAction wfAction = context.getAction();
        ae.check(context, wfAction);
        ae.end(context, wfAction);

        // action should fail as the size of pig stats will always be greater
        // than 1 byte
        assertEquals("FAILED/KILLED", wfAction.getExternalStatus());
        assertNull(wfAction.getStats());
    }

    /*
     * Test the stats with retrieve stats option set to false
     */
    public void testExecutionStatsWithRetrieveStatsFalse() throws Exception {
        // Set the action xml with the option for retrieving stats to false
        String actionXml = setPigActionXml(PIG_SCRIPT, false);
        Context context = createContext(actionXml);
        final RunningJob launcherJob = submitAction(context);
        evaluateLauncherJob(launcherJob);
        assertTrue(launcherJob.isSuccessful());
        assertFalse(LauncherMapper.hasStatsData(launcherJob));

        PigActionExecutor ae = new PigActionExecutor();
        WorkflowAction wfAction = context.getAction();
        ae.check(context, wfAction);
        ae.end(context, wfAction);

        assertEquals("SUCCEEDED", wfAction.getExternalStatus());
        assertNotNull(wfAction.getExternalChildIDs());
    }

    private void evaluateLauncherJob(final RunningJob launcherJob) throws Exception{
        waitFor(180 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return launcherJob.isComplete();
            }
        });
        Thread.sleep(2000);
    }

    protected XConfiguration setPigConfig(boolean writeStats) {
        XConfiguration conf = new XConfiguration();
        conf.set("oozie.pig.log.level", "INFO");
        conf.set(PigMain.EXTERNAL_STATS_WRITE, String.valueOf(writeStats));
        return conf;
    }

    public void testPig() throws Exception {
        // Set the action xml with the option for retrieving stats to true
        String actionXml = setPigActionXml(PIG_SCRIPT, true);
        _testSubmit(actionXml, true);
    }

    public void testPigError() throws Exception {
        // Set the action xml with the option for retrieving stats to true
        String actionXml = setPigActionXml(ERROR_PIG_SCRIPT, true);
        _testSubmit(actionXml, false);
    }

    private String setPigActionXml(String pigScript, boolean writeStats) throws IOException{
        FileSystem fs = getFileSystem();

        Path script = new Path(getAppPath(), "script.pig");
        Writer w = new OutputStreamWriter(fs.create(script));
        w.write(pigScript);
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
                setPigConfig(writeStats).toXmlString(false) +
                "<script>" + script.getName() + "</script>" +
                "<param>IN=" + inputDir.toUri().getPath() + "</param>" +
                "<param>OUT=" + outputDir.toUri().getPath() + "</param>" +
                "</pig>";

        return actionXml;
    }

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
                setPigConfig(true).toXmlString(false) +
                "<script>" + script.getName() + "</script>" +
                "<param>IN=" + inputDir.toUri().getPath() + "</param>" +
                "<param>OUT=" + outputDir.toUri().getPath() + "</param>" +
                "<file>" + udfJar.toString() + "#" + udfJar.getName() + "</file>" +
                "</pig>";
        _testSubmit(actionXml, true);
    }

}
