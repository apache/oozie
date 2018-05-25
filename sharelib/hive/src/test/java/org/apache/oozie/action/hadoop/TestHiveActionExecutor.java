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
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.util.XConfiguration;
import org.jdom.Namespace;

public class TestHiveActionExecutor extends ActionExecutorTestCase {

    private static final String NEW_LINE =
        System.getProperty("line.separator", "\n");

    private static final String SAMPLE_DATA_TEXT =
        "3\n4\n6\n1\n2\n7\n9\n0\n8\n";

    private static final String HIVE_SCRIPT_FILENAME = "script.q";

    private static final String INPUT_DIRNAME = "input";
    private static final String OUTPUT_DIRNAME = "output";
    private static final String DATA_FILENAME = "data.txt";

    public void testSetupMethods() throws Exception {
        HiveActionExecutor ae = new HiveActionExecutor();
        assertEquals(Arrays.asList(HiveMain.class), ae.getLauncherClasses());
        assertEquals("hive", ae.getType());
    }

    private String getHiveScript(String inputPath, String outputPath) {
        StringBuilder buffer = new StringBuilder(NEW_LINE);
        buffer.append("set -v;").append(NEW_LINE);
        buffer.append("DROP TABLE IF EXISTS test;").append(NEW_LINE);
        buffer.append("CREATE EXTERNAL TABLE test (a INT) STORED AS");
        buffer.append(NEW_LINE).append("TEXTFILE LOCATION '");
        buffer.append(inputPath).append("';").append(NEW_LINE);
        buffer.append("INSERT OVERWRITE DIRECTORY '");
        buffer.append(outputPath).append("'").append(NEW_LINE);
        buffer.append("SELECT (a-1) FROM test;").append(NEW_LINE);

        return buffer.toString();
    }

    private String getActionScriptXml() {
        String script = "<hive xmlns=''uri:oozie:hive-action:0.2''>" +
        "<job-tracker>{0}</job-tracker>" +
        "<name-node>{1}</name-node>" +
        "<configuration>" +
        "<property>" +
        "<name>javax.jdo.option.ConnectionURL</name>" +
        "<value>jdbc:derby:" + getTestCaseDir() + "/db;create=true</value>" +
        "</property>" +
        "<property>" +
        "<name>javax.jdo.option.ConnectionDriverName</name>" +
        "<value>org.apache.derby.jdbc.EmbeddedDriver</value>" +
        "</property>" +
        "<property>" +
        "<name>javax.jdo.option.ConnectionUserName</name>" +
        "<value>sa</value>" +
        "</property>" +
        "<property>" +
        "<name>javax.jdo.option.ConnectionPassword</name>" +
        "<value> </value>" +
        "</property>" +
        "<property>" +
        "<name>oozie.hive.log.level</name>" +
        "<value>DEBUG</value>" +
        "</property>" +
        "</configuration>" +
        "<script>" + HIVE_SCRIPT_FILENAME + "</script>" +
        "</hive>";
        return MessageFormat.format(script, getJobTrackerUri(), getNameNodeUri());
    }

    private String getActionQueryXml(String query) {
        String script = "<hive xmlns=''uri:oozie:hive-action:0.6''>" +
            "<job-tracker>{0}</job-tracker>" +
            "<name-node>{1}</name-node>" +
            "<configuration>" +
            "<property>" +
            "<name>javax.jdo.option.ConnectionURL</name>" +
            "<value>jdbc:derby:" + getTestCaseDir() + "/db;create=true</value>" +
            "</property>" +
            "<property>" +
            "<name>javax.jdo.option.ConnectionDriverName</name>" +
            "<value>org.apache.derby.jdbc.EmbeddedDriver</value>" +
            "</property>" +
            "<property>" +
            "<name>javax.jdo.option.ConnectionUserName</name>" +
            "<value>sa</value>" +
            "</property>" +
            "<property>" +
            "<name>javax.jdo.option.ConnectionPassword</name>" +
            "<value> </value>" +
            "</property>" +
            "<property>" +
            "<name>oozie.hive.log.level</name>" +
            "<value>DEBUG</value>" +
            "</property>" +
            "</configuration>";
        return MessageFormat.format(script, getJobTrackerUri(), getNameNodeUri())
            + "<query>" + query + "</query>" +
            "</hive>";
    }

    public void testHiveAction() throws Exception {
        Path inputDir = new Path(getFsTestCaseDir(), INPUT_DIRNAME);
        Path outputDir = new Path(getFsTestCaseDir(), OUTPUT_DIRNAME);
        String hiveScript = getHiveScript(inputDir.toString(), outputDir.toString());
        FileSystem fs = getFileSystem();

        {
            Path script = new Path(getAppPath(), HIVE_SCRIPT_FILENAME);
            Writer scriptWriter = new OutputStreamWriter(fs.create(script));
            scriptWriter.write(hiveScript);
            scriptWriter.close();
            Writer dataWriter = new OutputStreamWriter(fs.create(new Path(inputDir, DATA_FILENAME)));
            dataWriter.write(SAMPLE_DATA_TEXT);
            dataWriter.close();
            Context context = createContext(getActionScriptXml());
            Namespace ns = Namespace.getNamespace("uri:oozie:hive-action:0.2");
            final String launcherId = submitAction(context, ns);
            waitUntilYarnAppDoneAndAssertSuccess(launcherId, 180 * 1000);
            Configuration conf = new XConfiguration();
            conf.set("user.name", getTestUser());
            Map<String, String> actionData = LauncherHelper.getActionData(getFileSystem(), context.getActionDir(),
                conf);
            assertFalse(LauncherHelper.hasIdSwap(actionData));
            HiveActionExecutor ae = new HiveActionExecutor();
            ae.check(context, context.getAction());
            assertTrue(launcherId.equals(context.getAction().getExternalId()));
            assertEquals("SUCCEEDED", context.getAction().getExternalStatus());
            ae.end(context, context.getAction());
            assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());
            assertNotNull(context.getExternalChildIDs());
            //while this works in a real cluster, it does not with miniMR
            //assertTrue(outputData.getProperty(LauncherMain.HADOOP_JOBS).trim().length() > 0);
            //assertTrue(!actionData.get(LauncherAMUtils.ACTION_DATA_EXTERNAL_CHILD_IDS).isEmpty());
            assertTrue(fs.exists(outputDir));
            assertTrue(fs.isDirectory(outputDir));
        }
        {
            Context context = createContext(getActionQueryXml(hiveScript));
            Namespace ns = Namespace.getNamespace("uri:oozie:hive-action:0.6");
            final String launcherId = submitAction(context, ns);
            waitUntilYarnAppDoneAndAssertSuccess(launcherId, 180 * 1000);
            Configuration conf = new XConfiguration();
            conf.set("user.name", getTestUser());
            Map<String, String> actionData = LauncherHelper.getActionData(getFileSystem(), context.getActionDir(),
                conf);
            assertFalse(LauncherHelper.hasIdSwap(actionData));
            HiveActionExecutor ae = new HiveActionExecutor();
            ae.check(context, context.getAction());
            assertTrue(launcherId.equals(context.getAction().getExternalId()));
            assertEquals("SUCCEEDED", context.getAction().getExternalStatus());
            ae.end(context, context.getAction());
            assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());
            assertNotNull(context.getAction().getExternalChildIDs());
            //while this works in a real cluster, it does not with miniMR
            //assertTrue(outputData.getProperty(LauncherMain.HADOOP_JOBS).trim().length() > 0);
            //assertTrue(!actionData.get(LauncherAMUtils.ACTION_DATA_EXTERNAL_CHILD_IDS).isEmpty());
            assertTrue(fs.exists(outputDir));
            assertTrue(fs.isDirectory(outputDir));
        }
    }

    private String submitAction(Context context, Namespace ns) throws Exception {
        HiveActionExecutor ae = new HiveActionExecutor();

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
        HiveActionExecutor ae = new HiveActionExecutor();

        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());

        SharelibUtils.addToDistributedCache("hive", getFileSystem(), getFsTestCaseDir(), protoConf);

        WorkflowJobBean wf = createBaseWorkflow(protoConf, "hive-action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());
        action.setConf(actionXml);

        return new Context(wf, action);
    }

    public void testActionConfLoadDefaultResources() throws Exception {
        ConfigurationService.setBoolean(
                "oozie.service.HadoopAccessorService.action.configurations.load.default.resources", false);
        Path inputDir = new Path(getFsTestCaseDir(), INPUT_DIRNAME);
        Path outputDir = new Path(getFsTestCaseDir(), OUTPUT_DIRNAME);

        FileSystem fs = getFileSystem();
        Path script = new Path(getAppPath(), HIVE_SCRIPT_FILENAME);
        Writer scriptWriter = new OutputStreamWriter(fs.create(script));
        scriptWriter.write(getHiveScript(inputDir.toString(), outputDir.toString()));
        scriptWriter.close();

        Writer dataWriter = new OutputStreamWriter(fs.create(new Path(inputDir, DATA_FILENAME)));
        dataWriter.write(SAMPLE_DATA_TEXT);
        dataWriter.close();

        Context context = createContext(getActionScriptXml());
        Namespace ns = Namespace.getNamespace("uri:oozie:hive-action:0.2");
        submitAction(context, ns);
        FSDataInputStream os = fs.open(new Path(context.getActionDir(), LauncherAMUtils.ACTION_CONF_XML));
        XConfiguration conf = new XConfiguration();
        conf.addResource(os);
        assertNull(conf.get("oozie.HadoopAccessorService.created"));
    }
}
