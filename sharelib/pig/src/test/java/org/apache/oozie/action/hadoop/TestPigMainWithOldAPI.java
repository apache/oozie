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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.action.hadoop.MainTestCase;
import org.apache.oozie.action.hadoop.MapReduceMain;
import org.apache.oozie.action.hadoop.PigMainWithOldAPI;
import org.apache.oozie.action.hadoop.SharelibUtils;
import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.util.XConfiguration;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.Writer;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.Callable;


/**
 * Test PigMainWithOldAPI class should run a Pig script and write results to
 * output
 */
public class TestPigMainWithOldAPI extends XFsTestCase implements Callable<Void> {
    private SecurityManager SECURITY_MANAGER;

    protected void setUp() throws Exception {
        super.setUp();
        SECURITY_MANAGER = System.getSecurityManager();
    }

    protected void tearDown() throws Exception {
        System.setSecurityManager(SECURITY_MANAGER);
        super.tearDown();
    }

    public void testPigScript() throws Exception {
        MainTestCase.execute(getTestUser(), this);
    }

    @Override
    public Void call() throws Exception {
        FileSystem fs = getFileSystem();

        Path script = new Path(getTestCaseDir(), "script.pig");
        Writer w = new FileWriter(script.toString());
        String pigScript = "set job.name 'test'\n set debug on\n A = load '$IN' using PigStorage(':');\n"
                + "B = foreach A generate $0 as id;\n store B into '$OUT' USING PigStorage();";
        w.write(pigScript);
        w.close();

        Path inputDir = new Path(getFsTestCaseDir(), "input");
        fs.mkdirs(inputDir);
        Writer writer = new OutputStreamWriter(fs.create(new Path(inputDir, "data.txt")));
        writer.write("hello");
        writer.close();

        Path outputDir = new Path(getFsTestCaseDir(), "output");

        XConfiguration jobConfiguration = new XConfiguration();
        XConfiguration.copy(createJobConf(), jobConfiguration);

        jobConfiguration.set("user.name", getTestUser());
        jobConfiguration.set("group.name", getTestGroup());
        jobConfiguration.setInt("mapred.map.tasks", 1);
        jobConfiguration.setInt("mapred.map.max.attempts", 1);
        jobConfiguration.setInt("mapred.reduce.max.attempts", 1);
        jobConfiguration.set("oozie.pig.script", script.toString());
        // option to specify whether stats should be stored or not

        SharelibUtils.addToDistributedCache("pig", fs, getFsTestCaseDir(), jobConfiguration);

        String[] params = { "IN=" + inputDir.toUri().getPath(), "OUT=" + outputDir.toUri().getPath() };
        MapReduceMain.setStrings(jobConfiguration, "oozie.pig.params", params);
        String[] args = { "-v" };
        MapReduceMain.setStrings(jobConfiguration, "oozie.pig.args", args);

        File actionXml = new File(getTestCaseDir(), "action.xml");
        OutputStream os = new FileOutputStream(actionXml);
        jobConfiguration.writeXml(os);
        os.close();

        File jobIdsFile = new File(getTestCaseDir(), "jobIds.properties");

        setSystemProperty("oozie.launcher.job.id", "" + System.currentTimeMillis());
        setSystemProperty("oozie.action.conf.xml", actionXml.getAbsolutePath());
        setSystemProperty("oozie.action.output.properties", jobIdsFile.getAbsolutePath());

        URL url = Thread.currentThread().getContextClassLoader().getResource("PigMain.txt");
        File classPathDir = new File(url.getPath()).getParentFile();
        assertTrue(classPathDir.exists());
        Properties props = jobConfiguration.toProperties();
        assertEquals(props.getProperty("oozie.pig.args.size"), "1");
        File pigProps = new File(classPathDir, "pig.properties");

        new LauncherSecurityManager();
        String user = System.getProperty("user.name");
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        PrintStream oldPrintStream = System.out;
        System.setOut(new PrintStream(data));
        try {
            Writer wr = new FileWriter(pigProps);
            props.store(wr, "");
            wr.close();
            PigMainWithOldAPI.main(null);
        }
        catch (SecurityException ex) {
            if (LauncherSecurityManager.getExitInvoked()) {
                System.out.println("Intercepting System.exit(" + LauncherSecurityManager.getExitCode() + ")");
                System.err.println("Intercepting System.exit(" + LauncherSecurityManager.getExitCode() + ")");
                if (LauncherSecurityManager.getExitCode() != 0) {
                    fail("Exit code should be 0");
                }
            }
            else {
                throw ex;
            }
        }
        finally {
            pigProps.delete();
            System.setProperty("user.name", user);
            System.setOut(oldPrintStream);
        }

        assertTrue(jobIdsFile.exists());
        Properties prop = new Properties();
        prop.load(new FileReader(jobIdsFile));
        String jobId = prop.getProperty("hadoopJobs");
        assertTrue(data.toString().contains(jobId));
        assertTrue(data.toString().contains("Success!"));

        return null;
    }

}
