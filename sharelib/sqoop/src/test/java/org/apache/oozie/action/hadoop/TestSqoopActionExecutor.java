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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestSqoopActionExecutor extends ActionExecutorTestCase {

    private static final String SQOOP_IMPORT_COMMAND =
            "import --connect {0} --table TT --target-dir {1} -m 1";

    private static final String SQOOP_ACTION_COMMAND_XML =
            "<sqoop xmlns=\"uri:oozie:sqoop-action:0.1\">" +
            "<job-tracker>{0}</job-tracker>" +
            "<name-node>{1}</name-node>" +
            "<configuration>" +
            "<property>" +
            "<name>{2}</name>" +
            "<value>{3}</value>" +
            "</property>" +
            "</configuration>" +
            "<command>{4}</command>" +
            "</sqoop>";

    private static final String SQOOP_ACTION_ARGS_XML =
            "<sqoop xmlns=\"uri:oozie:sqoop-action:0.1\">" +
            "<job-tracker>{0}</job-tracker>" +
            "<name-node>{1}</name-node>" +
            "<configuration>" +
            "<property>" +
            "<name>oozie.sqoop.log.level</name>" +
            "<value>INFO</value>" +
            "</property>" +
            "</configuration>" +
            "{2}" +
            "<arg>--connect</arg>" +
            "<arg>{3}</arg>" +
            "<arg>--username</arg>" +
            "<arg>sa</arg>" +
            "<arg>--password</arg>" +
            "<arg></arg>" +
            "<arg>--verbose</arg>" +
            "<arg>--query</arg>" +
            "<arg>{4}</arg>" +
            "<arg>--target-dir</arg>" +
            "<arg>{5}</arg>" +
            "<arg>--split-by</arg>" +
            "<arg>I</arg>" +
            "</sqoop>";

    private static final String SQOOP_ACTION_EVAL_XML =
            "<sqoop xmlns=\"uri:oozie:sqoop-action:0.1\">" +
            "<job-tracker>{0}</job-tracker>" +
            "<name-node>{1}</name-node>" +
            "<configuration>" +
            "<property>" +
            "<name>oozie.sqoop.log.level</name>" +
            "<value>INFO</value>" +
            "</property>" +
            "</configuration>" +
            "<arg>eval</arg>" +
            "<arg>--connect</arg>" +
            "<arg>{2}</arg>" +
            "<arg>--username</arg>" +
            "<arg>sa</arg>" +
            "<arg>--password</arg>" +
            "<arg></arg>" +
            "<arg>--verbose</arg>" +
            "<arg>--query</arg>" +
            "<arg>{3}</arg>" +
            "</sqoop>";

    public void testSetupMethods() throws Exception {
        SqoopActionExecutor ae = new SqoopActionExecutor();
        assertEquals(SqoopMain.class, ae.getLauncherClasses().get(0));
        assertEquals("sqoop", ae.getType());
    }

    private String getDbFile() {
        return "db.hsqldb";
    }

    private String getDbPath() {
        return new File(getTestCaseDir(), getDbFile()).getAbsolutePath();
    }

    private String getLocalJdbcUri() {
        return "jdbc:hsqldb:file:" + getDbPath() + ";shutdown=true";
    }

    private String getActionJdbcUri() {
        return "jdbc:hsqldb:file:" + getDbFile();
    }

    private String getSqoopOutputDir() {
        return new Path(getFsTestCaseDir(), "output").toString();
    }

    private String getActionXml() {
        String command = MessageFormat.format(SQOOP_IMPORT_COMMAND, getActionJdbcUri(), getSqoopOutputDir());
        return MessageFormat.format(SQOOP_ACTION_COMMAND_XML, getJobTrackerUri(), getNameNodeUri(),
                                    "dummy", "dummyValue", command);
    }

    private String getRedundantCommandActionXml() {
        String command = "sqoop " +
                MessageFormat.format(SQOOP_IMPORT_COMMAND, getActionJdbcUri(), getSqoopOutputDir());
        return MessageFormat.format(SQOOP_ACTION_COMMAND_XML, getJobTrackerUri(), getNameNodeUri(),
                "dummy", "dummyValue", command);
    }

    private String getBadCommandActionXml() {
        String command = MessageFormat.format(SQOOP_IMPORT_COMMAND,
                getActionJdbcUri(), getSqoopOutputDir()).replace("import", "sqoop");
        return MessageFormat.format(SQOOP_ACTION_COMMAND_XML, getJobTrackerUri(), getNameNodeUri(),
                "dummy", "dummyValue", command);
    }

    private String getActionXmlEval() {
      String query = "select TT.I, TT.S from TT";
      return MessageFormat.format(SQOOP_ACTION_EVAL_XML, getJobTrackerUri(), getNameNodeUri(),
        getActionJdbcUri(), query);
    }

    private String getArgsActionXmlFreeFromQuery(boolean redundant) {
        String query = "select TT.I, TT.S from TT where $CONDITIONS";
        return MessageFormat.format(SQOOP_ACTION_ARGS_XML, getJobTrackerUri(), getNameNodeUri(),
                                    (redundant ? "<arg>sqoop</arg>" : "") + "<arg>import</arg>",
                                    getActionJdbcUri(), query, getSqoopOutputDir());
    }

    private String getBadArgsActionXml() {
        String query = "select TT.I, TT.S from TT where $CONDITIONS";
        return MessageFormat.format(SQOOP_ACTION_ARGS_XML, getJobTrackerUri(), getNameNodeUri(),
                "<arg>sqoop</arg>",
                getActionJdbcUri(), query, getSqoopOutputDir());
    }

    private void createDB() throws Exception {
        Class.forName("org.hsqldb.jdbcDriver");
        Connection conn = DriverManager.getConnection(getLocalJdbcUri(), "sa", "");
        Statement st = conn.createStatement();
        st.executeUpdate("CREATE TABLE TT (I INTEGER PRIMARY KEY, S VARCHAR(256))");
        st.executeUpdate("INSERT INTO TT (I, S) VALUES (1, 'a')");
        st.executeUpdate("INSERT INTO TT (I, S) VALUES (2, 'a')");
        st.executeUpdate("INSERT INTO TT (I, S) VALUES (3, 'a')");
        st.close();
        conn.close();
    }

    /**
     * Tests a bad command of 'sqoop --username ...' style.
     * Test asserts that the job will fail.
     */
    public void testSqoopActionWithBadCommand() throws Exception {
        runSqoopActionWithBadCommand(getBadCommandActionXml());
    }

    private void runSqoopActionWithBadCommand(String actionXml) throws Exception {
        createDB();

        Context context = createContext(actionXml);
        final String launcherId = submitAction(context);
        waitUntilYarnAppDoneAndAssertSuccess(launcherId);

        Map<String, String> actionData = LauncherHelper.getActionData(getFileSystem(), context.getActionDir(),
                context.getProtoActionConf());
        assertFalse(LauncherHelper.hasIdSwap(actionData));

        SqoopActionExecutor ae = new SqoopActionExecutor();
        ae.check(context, context.getAction());
        assertTrue(launcherId.equals(context.getAction().getExternalId()));
        assertEquals("FAILED/KILLED", context.getAction().getExternalStatus());
        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.ERROR, context.getAction().getStatus());
    }

    /**
     * Tests a normal command of 'import --username ...'.
     */
    public void testSqoopAction() throws Exception {
        runSqoopAction(getActionXml());
    }

    /**
     * Tests a redundant command of 'sqoop import --username ...'.
     * The test guarantees a success, since the redundant 'sqoop' must get removed.
     */
    public void testSqoopActionWithRedundantPrefix() throws Exception {
        runSqoopAction(getRedundantCommandActionXml());
    }

    private void runSqoopAction(String actionXml) throws Exception {
        createDB();

        Context context = createContext(actionXml);
        final String launcherId = submitAction(context);
        waitUntilYarnAppDoneAndAssertSuccess(launcherId);
        Map<String, String> actionData = LauncherHelper.getActionData(getFileSystem(), context.getActionDir(),
                context.getProtoActionConf());
        assertFalse(LauncherHelper.hasIdSwap(actionData));

        SqoopActionExecutor ae = new SqoopActionExecutor();
        ae.check(context, context.getAction());
        assertTrue(launcherId.equals(context.getAction().getExternalId()));
        assertEquals("SUCCEEDED", context.getAction().getExternalStatus());
        assertNotNull(context.getAction().getExternalChildIDs());
        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());

        String hadoopCounters = context.getVar(MapReduceActionExecutor.HADOOP_COUNTERS);
        assertNotNull(hadoopCounters);
        assertFalse(hadoopCounters.isEmpty());

        FileSystem fs = getFileSystem();
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(getSqoopOutputDir(), "part-m-00000"))));
        int count = 0;
        String line = br.readLine();
        while (line != null) {
            assertTrue(line.contains("a"));
            count++;
            line = br.readLine();
        }
        br.close();
        assertEquals(3, count);
    }

    public void testSqoopEval() throws Exception {
        createDB();

        Context context = createContext(getActionXmlEval());
        final String launcherId = submitAction(context);
        waitUntilYarnAppDoneAndAssertSuccess(launcherId);
        Map<String, String> actionData = LauncherHelper.getActionData(getFileSystem(), context.getActionDir(),
                context.getProtoActionConf());
        assertFalse(LauncherHelper.hasIdSwap(actionData));

        SqoopActionExecutor ae = new SqoopActionExecutor();
        ae.check(context, context.getAction());
        assertTrue(launcherId.equals(context.getAction().getExternalId()));
        assertEquals("SUCCEEDED", context.getAction().getExternalStatus());
        assertNull(context.getAction().getExternalChildIDs());
        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());

        String hadoopCounters = context.getVar(MapReduceActionExecutor.HADOOP_COUNTERS);
        assertNotNull(hadoopCounters);
        assertTrue(hadoopCounters.isEmpty());
    }

    /**
     * Runs a job with arg-style command of 'sqoop --username ...' form that's invalid.
     * The test ensures it fails.
     */
    public void testSqoopActionWithBadRedundantArgsAndFreeFormQuery() throws Exception {
        runSqoopActionWithBadCommand(getBadArgsActionXml());
    }

    /**
     * Runs a job with the arg-style command of 'sqoop import --username ...'.
     * The test guarantees that the redundant 'sqoop' is auto-removed (job passes).
     */
    public void testSqoopActionWithRedundantArgsAndFreeFormQuery() throws Exception {
        runSqoopActionFreeFormQuery(getArgsActionXmlFreeFromQuery(true));
    }

    /**
     * Runs a job with the normal arg-style command of 'import --username ...'.
     */
    public void testSqoopActionWithArgsAndFreeFormQuery() throws Exception {
        runSqoopActionFreeFormQuery(getArgsActionXmlFreeFromQuery(false));
    }

    private void runSqoopActionFreeFormQuery(String actionXml) throws Exception {
        createDB();

        Context context = createContext(actionXml);
        final String launcherId = submitAction(context);
        waitUntilYarnAppDoneAndAssertSuccess(launcherId);
        Map<String, String> actionData = LauncherHelper.getActionData(getFileSystem(), context.getActionDir(),
                context.getProtoActionConf());
        assertFalse(LauncherHelper.hasIdSwap(actionData));

        SqoopActionExecutor ae = new SqoopActionExecutor();
        ae.check(context, context.getAction());
        assertTrue(launcherId.equals(context.getAction().getExternalId()));
        assertEquals("SUCCEEDED", context.getAction().getExternalStatus());
        assertNotNull(context.getAction().getExternalChildIDs());
        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());

        String hadoopCounters = context.getVar(MapReduceActionExecutor.HADOOP_COUNTERS);
        assertNotNull(hadoopCounters);
        assertFalse(hadoopCounters.isEmpty());

        FileSystem fs = getFileSystem();
        FileStatus[] parts = fs.listStatus(new Path(getSqoopOutputDir()), new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().startsWith("part-");
            }
        });
        int count = 0;
        for (FileStatus part : parts) {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(part.getPath())));
            String line = br.readLine();
            while (line != null) {
                assertTrue(line.contains("a"));
                count++;
                line = br.readLine();
            }
            br.close();
        }
        assertEquals(3, count);
    }


    private String submitAction(Context context) throws Exception {
        SqoopActionExecutor ae = new SqoopActionExecutor();

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
        SqoopActionExecutor ae = new SqoopActionExecutor();

        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());


        FileSystem fs = getFileSystem();
        SharelibUtils.addToDistributedCache("sqoop", fs, getFsTestCaseDir(), protoConf);

        protoConf.setStrings(WorkflowAppService.APP_LIB_PATH_LIST, copyDbToHdfs());

        WorkflowJobBean wf = createBaseWorkflow(protoConf, "sqoop-action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());
        action.setConf(actionXml);

        return new Context(wf, action);
    }

    private String[] copyDbToHdfs() throws Exception {
        List<String> files = new ArrayList<String>();
        String[] exts = {".script", ".properties"};
        for (String ext : exts) {
            String file = getDbPath() + ext;
            String name = getDbFile() + ext;
            Path targetPath = new Path(getAppPath(), name);
            FileSystem fs = getFileSystem();
            InputStream is = new FileInputStream(file);
            OutputStream os = fs.create(new Path(getAppPath(), targetPath));
            IOUtils.copyStream(is, os);
            files.add(targetPath.toString() + "#" + name);
        }
        return files.toArray(new String[files.size()]);
    }
}
