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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.JobID;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.URIHandlerService;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.util.IOUtils;
import org.codehaus.jackson.JsonParser;
import org.jdom.Element;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.Writer;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.net.URI;
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
    protected void setUp() throws Exception {
        System.setProperty("oozie.test.metastore.server", "false");
        super.setUp();
    }

    @Override
    protected void setSystemProps() throws Exception {
        super.setSystemProps();
        setSystemProperty("oozie.service.ActionService.executor.classes", PigActionExecutor.class.getName());
    }

    public void testSetupMethods() throws Exception {
        PigActionExecutor ae = new PigActionExecutor();
        List<Class> classes = new ArrayList<Class>();
        classes.add(PigMain.class);
        classes.add(JSONParser.class);
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

        sleep(2000);

        PigActionExecutor ae = new PigActionExecutor();
        ae.check(context, context.getAction());
        ae.end(context, context.getAction());

        if (checkForSuccess) {
            assertFalse(context.getExternalChildIDs().equals(launcherId));
            assertNotNull(context.getAction().getStats());
        }

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

        Configuration conf = new XConfiguration();
        conf.set("user.name", getTestUser());
        Map<String, String> actionData = LauncherMapperHelper.getActionData(getFileSystem(), context.getActionDir(),
                conf);
        assertTrue(LauncherMapperHelper.hasStatsData(actionData));

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


    public void testActionConfLoadDefaultResources() throws Exception {
        ConfigurationService.setBoolean(
                "oozie.service.HadoopAccessorService.action.configurations.load.default.resources", false);
        String actionXml = setPigActionXml(PIG_SCRIPT, true);
        Context context = createContext(actionXml);
        submitAction(context);
        FileSystem fs = getFileSystem();
        FSDataInputStream os = fs.open(new Path(context.getActionDir(), LauncherMapper.ACTION_CONF_XML));
        XConfiguration conf = new XConfiguration();
        conf.addResource(os);
        assertNull(conf.get("oozie.HadoopAccessorService.created"));
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
        Services.get().destroy();
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

        Configuration conf = new XConfiguration();
        conf.set("user.name", getTestUser());
        Map<String, String> actionData = LauncherMapperHelper.getActionData(getFileSystem(), context.getActionDir(),
                conf);
        assertFalse(LauncherMapperHelper.hasStatsData(actionData));

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
        sleep(2000);
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

    /**
     * https://issues.apache.org/jira/browse/OOZIE-87
     * This test covers pig action
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

        String actionXml = "<pig>" +
                "      <job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "      <name-node>" + getNameNodeUri() + "</name-node>" +
                "      <script>id.pig</script>" +
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
                "</pig>";

        Element eActionXml = XmlUtils.parseXml(actionXml);

        Context context = createContext(actionXml);

        Path appPath = getAppPath();

        PigActionExecutor ae = new PigActionExecutor();

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
