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

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.Writer;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
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
import org.apache.oozie.util.ClassUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
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

    protected void setSystemProps() throws Exception {
        super.setSystemProps();
        setSystemProperty("oozie.service.ActionService.executor.classes",
                HiveActionExecutor.class.getName());
    }

    public void testVariableSubstitutionSimple() throws Exception {
        String key = "test.random.key";
        String keyExpression = "${" + key + "}";
        Map<String, String> varMap = new HashMap<String, String>();
        varMap.put(key, keyExpression);
        String value = HiveMain.substitute(varMap, keyExpression);
        assertTrue("Unexpected value " + value, value.equals(keyExpression));
    }

    public void testSetupMethods() throws Exception {
        HiveActionExecutor ae = new HiveActionExecutor();
        assertEquals("hive", ae.getType());
    }

    public void testLauncherJar() throws Exception {
        HiveActionExecutor ae = new HiveActionExecutor();
        Path jar = new Path(ae.getOozieRuntimeDir(), ae.getLauncherJarName());
        assertTrue(new File(jar.toString()).exists());
    }

    private String getHiveScript(String inputPath, String outputPath) {
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
        "<property>" +
        "<name>oozie.hive.defaults</name>" +
        "<value>user-hive-default.xml</value>" +
        "</property>" +
        "</configuration>" +
        "<script>" + HIVE_SCRIPT_FILENAME + "</script>" +
        "</hive>";
        return MessageFormat.format(script, getJobTrackerUri(), getNameNodeUri());
    }

    public void testHiveAction() throws Exception {
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

        InputStream is = IOUtils.getResourceAsStream("user-hive-default.xml", -1);
        OutputStream os = fs.create(new Path(getAppPath(), "user-hive-default.xml"));
        IOUtils.copyStream(is, os);

        Context context = createContext(getActionXml());
        final RunningJob launcherJob = submitAction(context);
        String launcherId = context.getAction().getExternalId();
        waitFor(200 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return launcherJob.isComplete();
            }
        });
        assertTrue(launcherJob.isSuccessful());

        assertFalse(LauncherMapper.hasIdSwap(launcherJob));

        HiveActionExecutor ae = new HiveActionExecutor();
        ae.check(context, context.getAction());
        assertTrue(launcherId.equals(context.getAction().getExternalId()));
        assertEquals("SUCCEEDED", context.getAction().getExternalStatus());
        assertNotNull(context.getAction().getData());
        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());

        assertNotNull(context.getAction().getData());
        Properties outputData = new Properties();
        outputData.load(new StringReader(context.getAction().getData()));
        assertTrue(outputData.containsKey(LauncherMain.HADOOP_JOBS));
        //while this works in a real cluster, it does not with miniMR
        //assertTrue(outputData.getProperty(LauncherMain.HADOOP_JOBS).trim().length() > 0);

        assertTrue(fs.exists(outputDir));
        assertTrue(fs.isDirectory(outputDir));
    }

    private RunningJob submitAction(Context context) throws Exception {
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
        Element e = XmlUtils.parseXml(action.getConf());
        Namespace ns = Namespace.getNamespace("uri:oozie:hive-action:0.2");
        XConfiguration conf =
                new XConfiguration(new StringReader(XmlUtils.prettyPrint(e.getChild("configuration", ns)).toString()));
        conf.set("mapred.job.tracker", e.getChildTextTrim("job-tracker", ns));
        conf.set("fs.default.name", e.getChildTextTrim("name-node", ns));
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

    private String copyJar(String targetFile, Class<?> anyContainedClass)
            throws Exception {
        String file = ClassUtils.findContainingJar(anyContainedClass);
        System.out.println("[copy-jar] class: " + anyContainedClass
                + ", local jar ==> " + file);
        Path targetPath = new Path(getAppPath(), targetFile);
        FileSystem fs = getFileSystem();
        InputStream is = new FileInputStream(file);
        OutputStream os = fs.create(new Path(getAppPath(), targetPath));
        IOUtils.copyStream(is, os);
        return targetPath.toString();
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

}
