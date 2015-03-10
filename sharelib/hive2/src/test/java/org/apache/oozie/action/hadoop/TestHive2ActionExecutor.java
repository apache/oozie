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

import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.Writer;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.Namespace;

public class TestHive2ActionExecutor extends ActionExecutorTestCase {

    private static final String NEW_LINE =
        System.getProperty("line.separator", "\n");

    private static final String SAMPLE_DATA_TEXT =
        "3\n4\n6\n1\n2\n7\n9\n0\n8\n";

    private static final String HIVE_SCRIPT_FILENAME = "script.q";

    private static final String INPUT_DIRNAME = "input";
    private static final String OUTPUT_DIRNAME = "output";
    private static final String DATA_FILENAME = "data.txt";

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void setSystemProps() throws Exception {
        super.setSystemProps();
        setSystemProperty("oozie.service.ActionService.executor.classes", Hive2ActionExecutor.class.getName());
    }

    @SuppressWarnings("unchecked")
    public void testSetupMethods() throws Exception {
        Hive2ActionExecutor ae = new Hive2ActionExecutor();
        List<Class> classes = new ArrayList<Class>();
        classes.add(Hive2Main.class);
        assertEquals(classes, ae.getLauncherClasses());

        Element actionXml = XmlUtils.parseXml("<hive2>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<jdbc-url>jdbc:hive2://foo:1234/bar</jdbc-url>" +
                "<password>pass</password>" +
                "<script>script.q</script>" +
                "<param>a=A</param>" +
                "<param>b=B</param>" +
                "<argument>-c</argument>" +
                "<argument>--dee</argument>" +
                "</hive2>");

        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());

        WorkflowJobBean wf = createBaseWorkflow(protoConf, "hive2-action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());

        Context context = new Context(wf, action);

        Configuration conf = ae.createBaseHadoopConf(context, actionXml);
        ae.setupActionConf(conf, context, actionXml, getFsTestCaseDir());
        assertEquals("jdbc:hive2://foo:1234/bar", conf.get("oozie.hive2.jdbc.url"));
        assertEquals("pass", conf.get("oozie.hive2.password"));
        assertEquals("script.q", conf.get("oozie.hive2.script"));
        assertEquals("2", conf.get("oozie.hive2.params.size"));
        assertEquals("a=A", conf.get("oozie.hive2.params.0"));
        assertEquals("b=B", conf.get("oozie.hive2.params.1"));
        assertEquals("2", conf.get("oozie.hive2.args.size"));
        assertEquals("-c", conf.get("oozie.hive2.args.0"));
        assertEquals("--dee", conf.get("oozie.hive2.args.1"));
    }

    private String getHive2Script(String inputPath, String outputPath) {
        StringBuilder buffer = new StringBuilder(NEW_LINE);
        buffer.append("set -v;").append(NEW_LINE);
        buffer.append("CREATE EXTERNAL TABLE test (a INT) STORED AS");
        buffer.append(NEW_LINE).append("TEXTFILE LOCATION '");
        buffer.append(inputPath).append("';").append(NEW_LINE);
        buffer.append("INSERT OVERWRITE DIRECTORY '");
        buffer.append(outputPath).append("'").append(NEW_LINE);
        buffer.append("SELECT (a-1) FROM test;").append(NEW_LINE);

        return buffer.toString();
    }

    private String getActionXml() {
        String script = "<hive2 xmlns=''uri:oozie:hive2-action:0.1''>" +
        "<job-tracker>{0}</job-tracker>" +
        "<name-node>{1}</name-node>" +
        "<configuration></configuration>" +
        "<jdbc-url>{2}</jdbc-url>" +
        "<password>dummy</password>" +
        "<script>" + HIVE_SCRIPT_FILENAME + "</script>" +
        "</hive2>";
        return MessageFormat.format(script, getJobTrackerUri(), getNameNodeUri(), getHiveServer2JdbcURL(""));
    }

    @SuppressWarnings("deprecation")
    public void testHive2Action() throws Exception {
        setupHiveServer2();
        Path inputDir = new Path(getFsTestCaseDir(), INPUT_DIRNAME);
        Path outputDir = new Path(getFsTestCaseDir(), OUTPUT_DIRNAME);

        FileSystem fs = getFileSystem();
        Path script = new Path(getAppPath(), HIVE_SCRIPT_FILENAME);
        Writer scriptWriter = new OutputStreamWriter(fs.create(script));
        scriptWriter.write(getHive2Script(inputDir.toString(), outputDir.toString()));
        scriptWriter.close();

        Writer dataWriter = new OutputStreamWriter(fs.create(new Path(inputDir, DATA_FILENAME)));
        dataWriter.write(SAMPLE_DATA_TEXT);
        dataWriter.close();

        Context context = createContext(getActionXml());
        final RunningJob launcherJob = submitAction(context);
        String launcherId = context.getAction().getExternalId();
        waitFor(200 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return launcherJob.isComplete();
            }
        });
        assertTrue(launcherJob.isSuccessful());
        Configuration conf = new XConfiguration();
        conf.set("user.name", getTestUser());
        Map<String, String> actionData = LauncherMapperHelper.getActionData(getFileSystem(), context.getActionDir(),
                conf);
        assertFalse(LauncherMapperHelper.hasIdSwap(actionData));

        Hive2ActionExecutor ae = new Hive2ActionExecutor();
        ae.check(context, context.getAction());
        assertTrue(launcherId.equals(context.getAction().getExternalId()));
        assertEquals("SUCCEEDED", context.getAction().getExternalStatus());
        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());

        assertNotNull(context.getAction().getData());
        Properties outputData = new Properties();
        outputData.load(new StringReader(context.getAction().getData()));
        assertTrue(outputData.containsKey(LauncherMain.HADOOP_JOBS));
        assertEquals(outputData.get(LauncherMain.HADOOP_JOBS), context.getExternalChildIDs());

        assertTrue(fs.exists(outputDir));
        assertTrue(fs.isDirectory(outputDir));
    }

    private RunningJob submitAction(Context context) throws Exception {
        Hive2ActionExecutor ae = new Hive2ActionExecutor();

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
        Namespace ns = Namespace.getNamespace("uri:oozie:hive2-action:0.1");
        XConfiguration conf =
                new XConfiguration(new StringReader(XmlUtils.prettyPrint(e.getChild("configuration", ns)).toString()));
        conf.set("mapred.job.tracker", e.getChildTextTrim("job-tracker", ns));
        conf.set("fs.default.name", e.getChildTextTrim("name-node", ns));
        conf.set("user.name", context.getProtoActionConf().get("user.name"));
        conf.set("group.name", getTestGroup());

        JobConf jobConf = Services.get().get(HadoopAccessorService.class).createJobConf(jobTracker);
        XConfiguration.copy(conf, jobConf);
        String user = jobConf.get("user.name");
        JobClient jobClient = Services.get().get(HadoopAccessorService.class).createJobClient(user, jobConf);
        final RunningJob runningJob = jobClient.getJob(JobID.forName(jobId));
        assertNotNull(runningJob);
        return runningJob;
    }

    private Context createContext(String actionXml) throws Exception {
        Hive2ActionExecutor ae = new Hive2ActionExecutor();

        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());

        SharelibUtils.addToDistributedCache("hive2", getFileSystem(), getFsTestCaseDir(), protoConf);

        WorkflowJobBean wf = createBaseWorkflow(protoConf, "hive2-action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());
        action.setConf(actionXml);

        return new Context(wf, action);
    }
}
