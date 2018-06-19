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
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.action.hadoop.security.LauncherSecurityManager;
import org.apache.oozie.test.MiniHCatServer;
import org.apache.oozie.util.XConfiguration;

public class TestHiveMain extends MainTestCase {
    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    @Override
    protected List<File> getFilesToDelete() {
        List<File> filesToDelete = super.getFilesToDelete();
        filesToDelete.add(new File(HiveMain.HIVE_SITE_CONF));
        return filesToDelete;
    }

    private static final String NEW_LINE =
        System.getProperty("line.separator", "\n");

    private String getHiveScript(String inputPath, String outputPath) {
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

    @Override
    public Void call() throws Exception {
        if (System.getenv("HADOOP_HOME") == null) {
            System.out.println("WARNING: 'HADOOP_HOME' env var not defined, TestHiveMain test is not running");
        }
        else {
            FileSystem fs = getFileSystem();

            Path inputDir = new Path(getFsTestCaseDir(), "input");
            fs.mkdirs(inputDir);
            Writer writer = new OutputStreamWriter(fs.create(new Path(inputDir, "data.txt")));
            writer.write("3\n4\n6\n1\n2\n7\n9\n0\n8\n");
            writer.close();

            Path outputDir = new Path(getFsTestCaseDir(), "output");

            Path script = new Path(getTestCaseDir(), "script.q");
            Writer w = new FileWriter(script.toString());
            w.write(getHiveScript("${IN}", "${OUT}"));
            w.close();

            XConfiguration jobConf = new XConfiguration();
            XConfiguration.copy(createJobConf(), jobConf);

            jobConf.set("oozie.hive.log.level", "DEBUG");

            jobConf.set("user.name", getTestUser());
            jobConf.set("group.name", getTestGroup());
            jobConf.setInt("mapred.map.tasks", 1);
            jobConf.setInt("mapred.map.max.attempts", 1);
            jobConf.setInt("mapred.reduce.max.attempts", 1);
            jobConf.set("javax.jdo.option.ConnectionURL", "jdbc:derby:" + getTestCaseDir() + "/db;create=true");
            jobConf.set("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver");
            jobConf.set("javax.jdo.option.ConnectionUserName", "sa");
            jobConf.set("javax.jdo.option.ConnectionPassword", " ");

            jobConf.set("mapreduce.job.tags", "" + System.currentTimeMillis());
            setSystemProperty("oozie.job.launch.time", "" + System.currentTimeMillis());

            SharelibUtils.addToDistributedCache("hive", fs, getFsTestCaseDir(), jobConf);

            jobConf.set(HiveActionExecutor.HIVE_SCRIPT, script.toString());
            ActionUtils.setStrings(jobConf, HiveActionExecutor.HIVE_PARAMS, new String[]{
                "IN=" + inputDir.toUri().getPath(),
                "OUT=" + outputDir.toUri().getPath()});
            ActionUtils.setStrings(jobConf, HiveActionExecutor.HIVE_ARGS,
                new String[]{ "-v" });

            File actionXml = new File(getTestCaseDir(), "action.xml");
            OutputStream os = new FileOutputStream(actionXml);
            jobConf.writeXml(os);
            os.close();

            //needed in the testcase classpath
            URL url = Thread.currentThread().getContextClassLoader().getResource("HiveMain.txt");
            File classPathDir = new File(url.getPath()).getParentFile();
            assertTrue(classPathDir.exists());
            Properties props = jobConf.toProperties();
            assertEquals(props.getProperty("oozie.hive.args.size"), "1");
            File hiveSite = new File(classPathDir, "hive-site.xml");

            File externalChildIdsFile = new File(getTestCaseDir(), "externalChildIDs");

            setSystemProperty("oozie.launcher.job.id", "" + System.currentTimeMillis());
            setSystemProperty("oozie.action.conf.xml", actionXml.getAbsolutePath());
            setSystemProperty("oozie.action.externalChildIDs", externalChildIdsFile.getAbsolutePath());

            LauncherSecurityManager launcherSecurityManager = new LauncherSecurityManager();
            launcherSecurityManager.enable();
            String user = System.getProperty("user.name");
            try {
                os = new FileOutputStream(hiveSite);
                jobConf.writeXml(os);
                os.close();
                MiniHCatServer.resetHiveConfStaticVariables();
                HiveMain.main(null);
            }
            catch (SecurityException ex) {
                if (launcherSecurityManager.getExitInvoked()) {
                    System.out.println("Intercepting System.exit(" + launcherSecurityManager.getExitCode() + ")");
                    System.err.println("Intercepting System.exit(" + launcherSecurityManager.getExitCode() + ")");
                    if (launcherSecurityManager.getExitCode() != 0) {
                        fail();
                    }
                }
                else {
                    throw ex;
                }
            }
            finally {
                System.setProperty("user.name", user);
                hiveSite.delete();
                MiniHCatServer.resetHiveConfStaticVariables();
                launcherSecurityManager.disable();
            }

            assertTrue(externalChildIdsFile.exists());
            assertNotNull(LauncherAMUtils.getLocalFileContentStr(externalChildIdsFile, "", -1));

//TODO: I cannot figure out why when log file is not created in this testcase, it works when running in Launcher
//            Properties props = new Properties();
//            props.load(new FileReader(outputDataFile));
//            assertTrue(props.containsKey(LauncherMain.HADOOP_JOBS));
//            assertTrue(props.getProperty(LauncherMain.HADOOP_JOBS).trim().length() > 0);
        }
        return null;
    }

    public void testJobIDPattern() {
        List<String> lines = new ArrayList<String>();
        lines.add("Ended Job = job_001");
        lines.add("Submitted application application_002");
        // Non-matching ones
        lines.add("Ended Job = . job_003");
        lines.add("Ended Job = abc004");
        lines.add("Submitted application = job_005");
        lines.add("Submitted application. job_006");
        Set<String> jobIds = new LinkedHashSet<String>();
        for (String line : lines) {
            LauncherMain.extractJobIDs(line, HiveMain.HIVE_JOB_IDS_PATTERNS, jobIds);
        }
        Set<String> expected = new LinkedHashSet<String>();
        expected.add("job_001");
        expected.add("job_002");
        assertEquals(expected, jobIds);
    }
}
