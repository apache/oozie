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

import java.io.File;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.util.PropertiesUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.junit.Assert;

public class TestShellActionExecutor extends ActionExecutorTestCase {

    private static final String SHELL_EXEC = Shell.WINDOWS ? "cmd.exe" : "sh";
    private static final String SHELL_PARAM = Shell.WINDOWS ? "/c" : "-c";
    private static final String SHELL_SCRIPTNAME = Shell.WINDOWS ? "script.cmd" : "script.sh";
    private static final String SHELL_SCRIPT_CONTENT = Shell.WINDOWS
            ? "dir /s /b\necho %1 %2\necho %PATH%\ntype %0"
            : "ls -ltr\necho $1 $2\necho $PATH\npwd\ntype sh";
    private static final String SHELL_SCRIPT_CONTENT_ENVVAR = Shell.WINDOWS
            ? "dir /s /b\necho var1=%var1%\necho var2=%var2%"
            : "ls -ltr\necho var1=$var1\necho var2=$var2";
    private static final String SHELL_SCRIPT_CONTENT_ERROR = Shell.WINDOWS
            ? "dir /s /b\necho %1 %2\nexit 1"
            : "ls -ltr\necho $1 $2\nexit 1";
    private static final String PERL_SCRIPT_CONTENT = "print \"MY_VAR=TESTING\";";
    private static final String SHELL_SCRIPT_HADOOP_CONF_DIR_CONTENT = Shell.WINDOWS
            ? "echo OOZIE_ACTION_CONF_XML=%OOZIE_ACTION_CONF_XML%\necho HADOOP_CONF_DIR=%HADOOP_CONF_DIR%\n"
            : "echo OOZIE_ACTION_CONF_XML=$OOZIE_ACTION_CONF_XML\necho HADOOP_CONF_DIR=$HADOOP_CONF_DIR\n";
    private static final String SHELL_SCRIPT_YARN_CONF_DIR_CONTENT = Shell.WINDOWS
            ? "echo YARN_CONF_DIR=%YARN_CONF_DIR%\n"
            : "echo YARN_CONF_DIR=$YARN_CONF_DIR\n";
    private static final String SHELL_SCRIPT_LOG4J_EXISTENCE_CHECKER = Shell.WINDOWS
            ? "IF EXIST %HADOOP_CONF_DIR%\\log4j.properties echo L4J_EXISTS=yes\n"
            : "if [ -f $HADOOP_CONF_DIR/log4j.properties ]; then echo L4J_EXISTS=yes; fi\n";
    private static final String SHELL_SCRIPT_LOG4J_CONTENT_COUNTER = Shell.WINDOWS
            ? "FOR /f %%i IN ('TYPE %HADOOP_CONF_DIR%\\log4j.properties " +
              "^| FIND /c /v \"~DOESNOTMATCH~\"') DO " +
              "SET L4J_LC=%%i\necho L4J_LC=%L4J_LC%\n" +
              "FOR /f %%i IN ('FINDSTR \"CLA CLRA\" %HADOOP_CONF_DIR%\\log4j.properties " +
              "^| FIND /c /v \"~DOESNOTMATCH~\"') DO " +
              "SET L4J_APPENDER=%%i\necho L4J_APPENDER=%L4J_APPENDER%\n"
            : "echo L4J_LC=$(cat $HADOOP_CONF_DIR/log4j.properties | wc -l)\n" +
              "echo L4J_APPENDER=$(grep -e 'CLA' -e 'CLRA' -c " +
              "$HADOOP_CONF_DIR/log4j.properties)\n";

    /**
     * Verify if the ShellActionExecutor indeed setups the basic stuffs
     *
     * @throws Exception
     */
    public void testSetupMethods() throws Exception {
        ShellActionExecutor ae = new ShellActionExecutor();
        assertNull(ae.getLauncherClasses());
        Element actionXml = XmlUtils.parseXml("<shell>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>"
                + "<name-node>" + getNameNodeUri() + "</name-node>" + "<exec>SCRIPT</exec>"
                + "<argument>a=A</argument>" + "<argument>b=B</argument>" + "</shell>");

        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());


        WorkflowJobBean wf = createBaseWorkflow(protoConf, "pig-action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());

        Context context = new Context(wf, action);

        Configuration conf = ae.createBaseHadoopConf(context, actionXml);
        ae.setupActionConf(conf, context, actionXml, getFsTestCaseDir());
        assertEquals("SCRIPT", conf.get("oozie.shell.exec"));
        assertEquals("2", conf.get("oozie.shell.args.size"));
        assertEquals("a=A", conf.get("oozie.shell.args.0"));
        assertEquals("b=B", conf.get("oozie.shell.args.1"));
        assertEquals("Expected HADOOP_CONF_DIR setup switch to be disabled",
                "false", conf.get("oozie.action.shell.setup.hadoop.conf.dir"));
        assertEquals("Expected log4j.properties write switch to be enabled",
                "true", conf.get("oozie.action.shell.setup.hadoop.conf.dir.write.log4j.properties"));
        assertNotNull("Expected a default config to exist for log4j.properties",
                conf.get("oozie.action.shell.setup.hadoop.conf.dir.log4j.content"));
    }

    /**
     * test if a sample shell script could run successfully
     *
     * @throws Exception
     */
    public void testShellScript() throws Exception {
        FileSystem fs = getFileSystem();
        // Create the script file with canned shell command
        Path script = new Path(getAppPath(), SHELL_SCRIPTNAME);
        Writer w = new OutputStreamWriter(fs.create(script));
        w.write(SHELL_SCRIPT_CONTENT);
        w.close();

        // Create sample Shell action xml
        String actionXml = "<shell>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>" + "<exec>" + SHELL_EXEC + "</exec>" + "<argument>" + SHELL_PARAM + "</argument>"
                + "<argument>" + SHELL_SCRIPTNAME + "</argument>" + "<argument>A</argument>" + "<argument>B</argument>"
                + "<env-var>var1=val1</env-var>" + "<env-var>var2=val2</env-var>" + "<file>" + script.toString()
                + "#" + script.getName() + "</file>" + "</shell>";
        // Submit and verify the job's status
        _testSubmit(actionXml, true, "");
    }

    /**
     * Test if a shell script could run successfully with {@link ShellMain#CONF_OOZIE_SHELL_SETUP_HADOOP_CONF_DIR} enabled.
     *
     * @throws Exception
     */
    public void testShellScriptHadoopConfDir() throws Exception {
        FileSystem fs = getFileSystem();
        // Create the script file with canned shell command
        Path script = new Path(getAppPath(), SHELL_SCRIPTNAME);
        Writer w = new OutputStreamWriter(fs.create(script));
        w.write(SHELL_SCRIPT_HADOOP_CONF_DIR_CONTENT);
        w.write(SHELL_SCRIPT_YARN_CONF_DIR_CONTENT);
        w.write(SHELL_SCRIPT_LOG4J_EXISTENCE_CHECKER);
        w.write(SHELL_SCRIPT_LOG4J_CONTENT_COUNTER);
        w.close();

        // Create sample Shell action xml
        String actionXml = "<shell>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>" + "<configuration>"
                + "<property><name>oozie.action.shell.setup.hadoop.conf.dir</name><value>true</value></property>"
                + "</configuration>" + "<exec>" + SHELL_EXEC + "</exec>" + "<argument>" + SHELL_PARAM + "</argument>"
                + "<argument>" + SHELL_SCRIPTNAME + "</argument>" + "<file>" + script.toString()
                + "#" + script.getName() + "</file>" + "<capture-output/>" + "</shell>";
        // Submit and verify the job's status
        WorkflowAction action = _testSubmit(actionXml, true, "");
        String oozieActionConfXml = PropertiesUtils.stringToProperties(action.getData()).getProperty("OOZIE_ACTION_CONF_XML");
        String hadoopConfDir = PropertiesUtils.stringToProperties(action.getData()).getProperty("HADOOP_CONF_DIR");
        String yarnConfDir = PropertiesUtils.stringToProperties(action.getData()).getProperty("YARN_CONF_DIR");
        String log4jExists = PropertiesUtils.stringToProperties(action.getData()).getProperty("L4J_EXISTS");
        String log4jFileLineCount = PropertiesUtils.stringToProperties(action.getData()).getProperty("L4J_LC");
        String log4BadAppenderCount = PropertiesUtils.stringToProperties(action.getData()).getProperty("L4J_APPENDER");
        assertNotNull(oozieActionConfXml);
        assertNotNull(hadoopConfDir);
        String s = new File(oozieActionConfXml).getParent() + File.separator + "oozie-hadoop-conf-";
        Assert.assertTrue(
                "Expected HADOOP_CONF_DIR to start with " + s + " but was " + hadoopConfDir,
                hadoopConfDir.startsWith(s));
        Assert.assertTrue(
                "Expected YARN_CONF_DIR to start with " + s + " but was " + yarnConfDir,
                yarnConfDir.startsWith(s));
        Assert.assertEquals(
                "Expected log4j.properties file to exist", "yes", log4jExists);
        Assert.assertTrue(
                "Expected log4j.properties to have non-zero line count, but has: " + log4jFileLineCount,
                Integer.parseInt(log4jFileLineCount) > 0);
        Assert.assertEquals(
                "Expected log4j.properties to have no container appender references (CLA/CLRA)",
                0, Integer.parseInt(log4BadAppenderCount));
    }

    /**
     * Test if a shell script could run successfully with {@link ShellMain#CONF_OOZIE_SHELL_SETUP_HADOOP_CONF_DIR} enabled.
     * Run with {@link ShellMain#CONF_OOZIE_SHELL_SETUP_HADOOP_CONF_DIR_WRITE_LOG4J_PROPERTIES} disabled.
     *
     * @throws Exception
     */
    public void testShellScriptHadoopConfDirWithNoL4J() throws Exception {
        FileSystem fs = getFileSystem();
        // Create the script file with canned shell command
        Path script = new Path(getAppPath(), SHELL_SCRIPTNAME);
        Writer w = new OutputStreamWriter(fs.create(script));
        w.write(SHELL_SCRIPT_LOG4J_EXISTENCE_CHECKER);
        w.close();

        // Create sample Shell action xml
        String actionXml = "<shell>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>" + "<configuration>"
                + "<property><name>oozie.action.shell.setup.hadoop.conf.dir</name><value>true</value></property>"
                + "<property><name>oozie.action.shell.setup.hadoop.conf.dir.write.log4j.properties"
                + "</name><value>false</value></property>"
                + "</configuration>" + "<exec>" + SHELL_EXEC + "</exec>" + "<argument>" + SHELL_PARAM + "</argument>"
                + "<argument>" + SHELL_SCRIPTNAME + "</argument>" + "<file>" + script.toString()
                + "#" + script.getName() + "</file>" + "<capture-output/>" + "</shell>";
        // Submit and verify the job's status
        WorkflowAction action = _testSubmit(actionXml, true, "");
        String log4jExists = PropertiesUtils.stringToProperties(action.getData()).getProperty("L4J_EXISTS");
        Assert.assertNull(
                "Expected no log4j.properties file to exist", log4jExists);
    }

    /**
     * test if a sample shell script could run with error when the script return
     * non-zero exit code
     *
     * @throws Exception
     */
    public void testShellScriptError() throws Exception {
        FileSystem fs = getFileSystem();
        // Create the script file with canned shell command
        Path script = new Path(getAppPath(), SHELL_SCRIPTNAME);
        Writer w = new OutputStreamWriter(fs.create(script));
        w.write(SHELL_SCRIPT_CONTENT_ERROR);
        w.close();

        // Create sample shell action xml
        String actionXml = "<shell>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>" + "<exec>" + SHELL_EXEC + "</exec>" + "<argument>" + SHELL_PARAM + "</argument>"
                + "<argument>" + SHELL_SCRIPTNAME + "</argument>" + "<argument>A</argument>" + "<argument>B</argument>" + "<file>"
                + script.toString() + "#" + script.getName() + "</file>" + "</shell>";
        // Submit and verify the job's status
        _testSubmit(actionXml, false, "");
    }

    /**
     * test if a perl script could run successfully
     *
     * @throws Exception
     */
    public void testPerlScript() throws Exception {
        if (Shell.WINDOWS) {
            System.out.println("Windows cannot natively execute perl. Skipping test");
            return;
        }

        FileSystem fs = getFileSystem();
        // Create a sample perl script
        Path script = new Path(getAppPath(), "script.pl");
        Writer w = new OutputStreamWriter(fs.create(script));
        w.write(PERL_SCRIPT_CONTENT);
        w.close();
        // Create a Sample Shell action using the perl script
        String actionXml = "<shell>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>" + "<exec>perl</exec>" + "<argument>script.pl</argument>"
                + "<argument>A</argument>" + "<argument>B</argument>" + "<env-var>my_var1=my_val1</env-var>" + "<file>"
                + script.toString() + "#" + script.getName() + "</file>" + "<capture-output/>" + "</shell>";
        _testSubmit(actionXml, true, "TESTING");
    }

    /**
     * Test that env variable can contain '=' symbol within value
     *
     * @throws Exception
     */
    public void testEnvVar() throws Exception {
        Services.get().destroy();
        Services services = new Services();
        services.getConf().setInt(LauncherAMUtils.CONF_OOZIE_ACTION_MAX_OUTPUT_DATA, 8 * 1042);
        services.init();

        FileSystem fs = getFileSystem();
        // Create the script file with canned shell command
        Path script = new Path(getAppPath(), SHELL_SCRIPTNAME);
        Writer w = new OutputStreamWriter(fs.create(script));
        w.write(SHELL_SCRIPT_CONTENT_ENVVAR);
        w.close();

        String envValueHavingEqualSign = "a=b;c=d";
        // Create sample shell action xml
        String actionXml = "<shell>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>" + "<exec>" + SHELL_EXEC + "</exec>" + "<argument>" + SHELL_PARAM + "</argument>"
                + "<argument>" + SHELL_SCRIPTNAME + "</argument>" + "<argument>A</argument>" + "<argument>B</argument>"
                + "<env-var>var1=val1</env-var>" + "<env-var>var2=" + envValueHavingEqualSign + "</env-var>" + "<file>"
                + script.toString()
                + "#" + script.getName() + "</file>" + "<capture-output />" + "</shell>";

        Context context = createContext(actionXml);
        // Submit the action
        final String launcherId = submitAction(context);
        waitUntilYarnAppDoneAndAssertSuccess(launcherId);
        ShellActionExecutor ae = new ShellActionExecutor();
        WorkflowAction action = context.getAction();
        ae.check(context, action);
        ae.end(context, action);

        // Checking action data from shell script output
        assertEquals(envValueHavingEqualSign, PropertiesUtils.stringToProperties(action.getData())
                    .getProperty("var2"));

    }

    /**
     * Submit the WF with a Shell action and very of the job succeeds
     *
     * @param actionXml
     * @param checkForSuccess
     * @throws Exception
     */
    private WorkflowAction _testSubmit(String actionXml, boolean checkForSuccess, String capture_output) throws Exception {

        Context context = createContext(actionXml);
        final String launcherId = submitAction(context);// Submit the action
        waitUntilYarnAppDoneAndAssertSuccess(launcherId);

        Configuration conf = new XConfiguration();
        conf.set("user.name", getTestUser());
        Map<String, String> actionData = LauncherHelper.getActionData(getFileSystem(), context.getActionDir(),
                conf);
        assertFalse(LauncherHelper.hasIdSwap(actionData));

        ShellActionExecutor ae = new ShellActionExecutor();
        ae.check(context, context.getAction());
        ae.end(context, context.getAction());
        assertTrue(launcherId.equals(context.getAction().getExternalId()));

        if (checkForSuccess) { // Postive test cases
            assertEquals("SUCCEEDED", context.getAction().getExternalStatus());
            // Testing capture output
            if (capture_output != null && capture_output.length() > 0) {
                assertEquals(capture_output, PropertiesUtils.stringToProperties(context.getAction().getData())
                        .getProperty("MY_VAR"));
            }
        }
        else { // Negative test cases
            assertEquals("FAILED/KILLED", context.getAction().getExternalStatus());
            assertNotNull(context.getAction().getErrorMessage());
        }
        if (checkForSuccess) { // Positive test cases
            assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());
        }
        else {// Negative test cases
            assertEquals(WorkflowAction.Status.ERROR, context.getAction().getStatus());
        }
        return context.getAction();
    }

    /**
     * Create WF using the Shell action and return the corresponding contest
     * structure
     *
     * @param actionXml :Shell action
     * @return :Context
     * @throws Exception
     */
    private Context createContext(String actionXml) throws Exception {
        ShellActionExecutor ae = new ShellActionExecutor();

        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());
        // Make sure Kerbores prinicpal is in the conf


        WorkflowJobBean wf = createBaseWorkflow(protoConf, "shell-action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());
        action.setConf(actionXml);

        return new Context(wf, action);
    }

    /**
     * Submit the Shell action using Shell ActionExecutor
     *
     * @param context
     * @return The RunningJob of the Launcher Mapper
     * @throws Exception
     */
    private String submitAction(Context context) throws Exception {
        ShellActionExecutor ae = new ShellActionExecutor();

        WorkflowAction action = context.getAction();

        ae.prepareActionDir(getFileSystem(), context);
        ae.submitLauncher(getFileSystem(), context, action); // Submit the action

        String jobId = action.getExternalId();
        String jobTracker = action.getTrackerUri();
        String consoleUrl = action.getConsoleUrl();

        assertNotNull(jobId);
        assertNotNull(jobTracker);
        assertNotNull(consoleUrl);

        return jobId;
    }
}
