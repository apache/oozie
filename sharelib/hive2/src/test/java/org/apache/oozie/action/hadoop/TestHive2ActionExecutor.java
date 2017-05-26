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
import java.io.Writer;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowAction;
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

    public void testSetupMethodsForScript() throws Exception {
        Hive2ActionExecutor ae = new Hive2ActionExecutor();
        List<Class<?>> classes = new ArrayList<>();
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

    public void testSetupMethodsForQuery() throws Exception {
        Hive2ActionExecutor ae = new Hive2ActionExecutor();
        List<Class<?>> classes = new ArrayList<>();
        classes.add(Hive2Main.class);
        assertEquals(classes, ae.getLauncherClasses());

        String sampleQuery = "SELECT count(*) from foobar";
        Element actionXml = XmlUtils.parseXml("<hive2  xmlns=\"uri:oozie:hive2-action:0.2\">" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<jdbc-url>jdbc:hive2://foo:1234/bar</jdbc-url>" +
                "<password>pass</password>" +
                "<query>" + sampleQuery + "</query>" +
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
        assertEquals(sampleQuery, conf.get("oozie.hive2.query"));
        assertNull(conf.get("oozie.hive2.script"));
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
        buffer.append("CREATE DATABASE IF NOT EXISTS default;").append(NEW_LINE);
        buffer.append("DROP TABLE IF EXISTS test;").append(NEW_LINE);
        buffer.append("CREATE EXTERNAL TABLE test (a INT) STORED AS");
        buffer.append(NEW_LINE).append("TEXTFILE LOCATION '");
        buffer.append(inputPath).append("';").append(NEW_LINE);
        buffer.append("INSERT OVERWRITE DIRECTORY '");
        buffer.append(outputPath).append("'").append(NEW_LINE);
        buffer.append("SELECT (a-1) FROM test;").append(NEW_LINE);
        return buffer.toString();
    }

    private String getScriptActionXml() {
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

    private String getQueryActionXml(String query) {
        String script = "<hive2 xmlns=\"uri:oozie:hive2-action:0.2\">" +
        "<job-tracker>{0}</job-tracker>" +
        "<name-node>{1}</name-node>" +
        "<configuration></configuration>" +
        "<jdbc-url>{2}</jdbc-url>" +
        "<password>dummy</password>";
        String expanded = MessageFormat.format(script, getJobTrackerUri(), getNameNodeUri(), getHiveServer2JdbcURL(""));
        // MessageFormat strips single quotes, which causes issues with the hive query parser
        return expanded +
            "<query>" + query + "</query>" + "</hive2>";
    }

    public void testHive2Action() throws Exception {
        setupHiveServer2();
        Path inputDir = new Path(getFsTestCaseDir(), INPUT_DIRNAME);
        Path outputDir = new Path(getFsTestCaseDir(), OUTPUT_DIRNAME);
        FileSystem fs = getFileSystem();

        {
            String query = getHive2Script(inputDir.toString(), outputDir.toString());
            Writer dataWriter = new OutputStreamWriter(fs.create(new Path(inputDir, DATA_FILENAME)));
            dataWriter.write(SAMPLE_DATA_TEXT);
            dataWriter.close();
            Context context = createContext(getQueryActionXml(query));
            final String launcherId = submitAction(context,
                Namespace.getNamespace("uri:oozie:hive2-action:0.2"));
            waitUntilYarnAppDoneAndAssertSuccess(launcherId);
            Configuration conf = new XConfiguration();
            conf.set("user.name", getTestUser());
            Map<String, String> actionData = LauncherHelper.getActionData(getFileSystem(), context.getActionDir(),
                conf);
            assertFalse(LauncherHelper.hasIdSwap(actionData));
            Hive2ActionExecutor ae = new Hive2ActionExecutor();
            ae.check(context, context.getAction());
            assertTrue(launcherId.equals(context.getAction().getExternalId()));
            assertEquals("SUCCEEDED", context.getAction().getExternalStatus());
            ae.end(context, context.getAction());
            assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());
            // Disabled external child id check until Hive version is upgraded to 0.14+
            //assertNotNull(context.getExternalChildIDs());
            assertTrue(fs.exists(outputDir));
            assertTrue(fs.isDirectory(outputDir));
        }
        {
            Path script = new Path(getAppPath(), HIVE_SCRIPT_FILENAME);
            Writer scriptWriter = new OutputStreamWriter(fs.create(script));
            scriptWriter.write(getHive2Script(inputDir.toString(), outputDir.toString()));
            scriptWriter.close();

            Writer dataWriter = new OutputStreamWriter(fs.create(new Path(inputDir, DATA_FILENAME)));
            dataWriter.write(SAMPLE_DATA_TEXT);
            dataWriter.close();
            Context context = createContext(getScriptActionXml());
            final String launcherId = submitAction(context,
                Namespace.getNamespace("uri:oozie:hive2-action:0.1"));
            waitUntilYarnAppDoneAndAssertSuccess(launcherId);
            Configuration conf = new XConfiguration();
            conf.set("user.name", getTestUser());
            Map<String, String> actionData = LauncherHelper.getActionData(getFileSystem(), context.getActionDir(),
                conf);
            assertFalse(LauncherHelper.hasIdSwap(actionData));
            Hive2ActionExecutor ae = new Hive2ActionExecutor();
            ae.check(context, context.getAction());
            assertTrue(launcherId.equals(context.getAction().getExternalId()));
            assertEquals("SUCCEEDED", context.getAction().getExternalStatus());
            ae.end(context, context.getAction());
            assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());
            // Disabled external child id check until Hive version is upgraded to 0.14+
            //assertNotNull(context.getExternalChildIDs());
            assertTrue(fs.exists(outputDir));
            assertTrue(fs.isDirectory(outputDir));
        }
    }

    public void testHive2ActionFails() throws Exception {
        setupHiveServer2();
        Path inputDir = new Path(getFsTestCaseDir(), INPUT_DIRNAME);
        Path outputDir = new Path(getFsTestCaseDir(), OUTPUT_DIRNAME);
        FileSystem fs = getFileSystem();

        String query = getHive2BadScript(inputDir.toString(), outputDir.toString());
        Writer dataWriter = new OutputStreamWriter(fs.create(new Path(inputDir, DATA_FILENAME)));
        dataWriter.write(SAMPLE_DATA_TEXT);
        dataWriter.close();
        Context context = createContext(getQueryActionXml(query));
        final String launcherId = submitAction(context, Namespace.getNamespace("uri:oozie:hive2-action:0.2"));
        waitUntilYarnAppDoneAndAssertSuccess(launcherId);
        Configuration conf = new XConfiguration();
        conf.set("user.name", getTestUser());
        Map<String, String> actionData = LauncherHelper.getActionData(getFileSystem(), context.getActionDir(),
                conf);
        assertFalse(LauncherHelper.hasIdSwap(actionData));
        Hive2ActionExecutor ae = new Hive2ActionExecutor();
        ae.check(context, context.getAction());
        assertTrue(launcherId.equals(context.getAction().getExternalId()));
        assertEquals("FAILED/KILLED", context.getAction().getExternalStatus());
        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.ERROR, context.getAction().getStatus());
        assertNull(context.getExternalChildIDs());
    }

    private String getHive2BadScript(String inputPath, String outputPath) {
        StringBuilder buffer = new StringBuilder(NEW_LINE);
        buffer.append("set -v;").append(NEW_LINE);
        buffer.append("DROP TABLE IF EXISTS test;").append(NEW_LINE);
        buffer.append("CREATE EXTERNAL TABLE test (a INT) STORED AS");
        buffer.append(NEW_LINE).append("TEXTFILE LOCATION '");
        buffer.append(inputPath).append("';").append(NEW_LINE);
        buffer.append("INSERT OVERWRITE DIRECTORY '");
        buffer.append(outputPath).append("'").append(NEW_LINE);
        buffer.append("SELECT (a-1) FROM test-bad;").append(NEW_LINE);
        return buffer.toString();
    }

    private String submitAction(Context context, Namespace ns) throws Exception {
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
        return jobId;
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
