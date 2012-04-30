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
import java.util.ArrayList;
import java.util.List;

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
import org.apache.oozie.util.PropertiesUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;

public class TestShellActionExecutor extends ActionExecutorTestCase {

    private static final String SHELL_SCRIPT_CONTENT = "ls -ltr\necho $1 $2\necho $PATH\npwd\ntype sh";
    private static final String SHELL_SCRIPT_CONTENT_ERROR = "ls -ltr\necho $1 $2\nexit 1";
    private static final String PERL_SCRIPT_CONTENT = "print \"MY_VAR=TESTING\";";

    @Override
    protected void setSystemProps() throws Exception {
        super.setSystemProps();
        setSystemProperty("oozie.service.ActionService.executor.classes", ShellActionExecutor.class.getName());
    }

    /**
     * Verify if the launcher jar is created.
     *
     * @throws Exception
     */
    public void testLauncherJar() throws Exception {
        ShellActionExecutor ae = new ShellActionExecutor();
        Path jar = new Path(ae.getOozieRuntimeDir(), ae.getLauncherJarName());
        assertTrue(new File(jar.toString()).exists());
    }

    /**
     * Verify if the ShellActionExecutor indeed setups the basic stuffs
     *
     * @throws Exception
     */
    public void testSetupMethods() throws Exception {
        ShellActionExecutor ae = new ShellActionExecutor();

        assertEquals("shell", ae.getType());// ActionExcutor type is 'shell'
        // Verify the launcher jar filename
        assertEquals("shell-launcher.jar", ae.getLauncherJarName());

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
        classes.add(ShellMain.class);
        classes.add(ShellMain.OutputWriteThread.class);
        assertEquals(classes, ae.getLauncherClasses());// Verify the class

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
    }

    /**
     * test if a sample shell script could run successfully
     *
     * @throws Exception
     */
    public void testShellScript() throws Exception {
        FileSystem fs = getFileSystem();
        // Create the script file with canned shell command
        Path script = new Path(getAppPath(), "script.sh");
        Writer w = new OutputStreamWriter(fs.create(script));
        w.write(SHELL_SCRIPT_CONTENT);
        w.close();

        // Create sample Shell action xml
        String actionXml = "<shell>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>" + "<exec>sh</exec>" + "<argument>-c</argument>"
                + "<argument>script.sh</argument>" + "<argument>A</argument>" + "<argument>B</argument>"
                + "<env-var>var1=val1</env-var>" + "<env-var>var2=val2</env-var>" + "<file>" + script.toString()
                + "#" + script.getName() + "</file>" + "</shell>";
        // Submit and verify the job's status
        _testSubmit(actionXml, true, "");
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
        Path script = new Path(getAppPath(), "script.sh");
        Writer w = new OutputStreamWriter(fs.create(script));
        w.write(SHELL_SCRIPT_CONTENT_ERROR);
        w.close();

        // Create sample shell action xml
        String actionXml = "<shell>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>" + "<exec>sh</exec>" + "<argument>-c</argument>"
                + "<argument>script.sh</argument>" + "<argument>A</argument>" + "<argument>B</argument>" + "<file>"
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

        String shellScript = "ls -ltr\necho var1=$var1\necho var2=$var2";
        FileSystem fs = getFileSystem();
        // Create the script file with canned shell command
        Path script = new Path(getAppPath(), "script.sh");
        Writer w = new OutputStreamWriter(fs.create(script));
        w.write(shellScript);
        w.close();

        String envValueHavingEqualSign = "a=b;c=d";
        // Create sample shell action xml
        String actionXml = "<shell>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>" + "<exec>sh</exec>" + "<argument>-c</argument>"
                + "<argument>script.sh</argument>" + "<argument>A</argument>" + "<argument>B</argument>"
                + "<env-var>var1=val1</env-var>" + "<env-var>var2=" + envValueHavingEqualSign + "</env-var>" + "<file>" + script.toString()
                + "#" + script.getName() + "</file>" + "<capture-output />" + "</shell>";

        Context context = createContext(actionXml);
        // Submit the action
        final RunningJob launcherJob = submitAction(context);
        waitFor(30 * 1000, new Predicate() { // Wait for the external job to
                    // finish
                    public boolean evaluate() throws Exception {
                        return launcherJob.isComplete();
                    }
                });

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
    private void _testSubmit(String actionXml, boolean checkForSuccess, String capture_output) throws Exception {

        Context context = createContext(actionXml);
        final RunningJob launcherJob = submitAction(context);// Submit the
        // action
        String launcherId = context.getAction().getExternalId(); // Get LM id
        waitFor(180 * 1000, new Predicate() { // Wait for the external job to
                    // finish
                    public boolean evaluate() throws Exception {
                        return launcherJob.isComplete();
                    }
                });
        // Thread.sleep(2000);
        assertTrue(launcherJob.isSuccessful());

        Thread.sleep(2000);// Wait more to make sure no ID swap happens
        assertFalse(LauncherMapper.hasIdSwap(launcherJob));

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
    private RunningJob submitAction(Context context) throws Exception {
        ShellActionExecutor ae = new ShellActionExecutor();

        WorkflowAction action = context.getAction();

        ae.prepareActionDir(getFileSystem(), context);
        ae.submitLauncher(getFileSystem(), context, action); // Submit the
        // Launcher Mapper

        String jobId = action.getExternalId();
        String jobTracker = action.getTrackerUri();
        String consoleUrl = action.getConsoleUrl();

        assertNotNull(jobId);
        assertNotNull(jobTracker);
        assertNotNull(consoleUrl);

        Element e = XmlUtils.parseXml(action.getConf());
        XConfiguration conf = new XConfiguration();
        conf.set("mapred.job.tracker", e.getChildTextTrim("job-tracker"));
        conf.set("fs.default.name", e.getChildTextTrim("name-node"));
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

}
