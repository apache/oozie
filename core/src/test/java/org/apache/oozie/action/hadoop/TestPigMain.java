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
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.IOUtils;
import org.json.simple.JSONValue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.io.FileWriter;
import java.io.FileReader;
import java.util.Map;
import java.util.Properties;
import java.net.URL;

public class TestPigMain extends PigTestCase {
    private SecurityManager SECURITY_MANAGER;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        SECURITY_MANAGER = System.getSecurityManager();
    }

    @Override
    protected void tearDown() throws Exception {
        System.setSecurityManager(SECURITY_MANAGER);
        super.tearDown();
    }

    @Override
    public Void call() throws Exception {
        FileSystem fs = getFileSystem();

        Path script = new Path(getTestCaseDir(), "script.pig");
        Writer w = new FileWriter(script.toString());
        w.write(pigScript);
        w.close();

        Path inputDir = new Path(getFsTestCaseDir(), "input");
        fs.mkdirs(inputDir);
        Writer writer = new OutputStreamWriter(fs.create(new Path(inputDir, "data.txt")));
        writer.write("hello");
        writer.close();

        Path outputDir = new Path(getFsTestCaseDir(), "output");

        XConfiguration jobConf = new XConfiguration();

        jobConf.set("user.name", getTestUser());
        jobConf.set("group.name", getTestGroup());
        jobConf.setInt("mapred.map.tasks", 1);
        jobConf.setInt("mapred.map.max.attempts", 1);
        jobConf.setInt("mapred.reduce.max.attempts", 1);
        jobConf.set("mapred.job.tracker", getJobTrackerUri());
        jobConf.set("fs.default.name", getNameNodeUri());

        jobConf.set("mapreduce.framework.name", "yarn");

        // option to specify whether stats should be stored or not
        jobConf.set("oozie.action.external.stats.write", Boolean.toString(writeStats));



        SharelibUtils.addToDistributedCache("pig", fs, getFsTestCaseDir(), jobConf);

        PigMain.setPigScript(jobConf, script.toString(), new String[] { "IN=" + inputDir.toUri().getPath(),
                "OUT=" + outputDir.toUri().getPath() }, new String[] { "-v" });

        File actionXml = new File(getTestCaseDir(), "action.xml");
        OutputStream os = new FileOutputStream(actionXml);
        jobConf.writeXml(os);
        os.close();

        File statsDataFile = new File(getTestCaseDir(), "statsdata.properties");

        File hadoopIdsFile = new File(getTestCaseDir(), "hadoopIds.properties");

        setSystemProperty("oozie.launcher.job.id", "" + System.currentTimeMillis());
        setSystemProperty("oozie.action.conf.xml", actionXml.getAbsolutePath());
        setSystemProperty("oozie.action.stats.properties", statsDataFile.getAbsolutePath());
        setSystemProperty("oozie.action.externalChildIDs.properties", hadoopIdsFile.getAbsolutePath());


        URL url = Thread.currentThread().getContextClassLoader().getResource("PigMain.txt");
        File classPathDir = new File(url.getPath()).getParentFile();
        assertTrue(classPathDir.exists());
        Properties props = jobConf.toProperties();
        assertEquals(props.getProperty("oozie.pig.args.size"), "1");
        File pigProps = new File(classPathDir, "pig.properties");

        new LauncherSecurityManager();
        String user = System.getProperty("user.name");
        try {
            Writer wr = new FileWriter(pigProps);
            props.store(wr, "");
            wr.close();
            PigMain.main(null);
        }
        catch (SecurityException ex) {
            if (LauncherSecurityManager.getExitInvoked()) {
                System.out.println("Intercepting System.exit(" + LauncherSecurityManager.getExitCode() + ")");
                System.err.println("Intercepting System.exit(" + LauncherSecurityManager.getExitCode() + ")");
                if (LauncherSecurityManager.getExitCode() != 0) {
                    fail();
                }
            }
            else {
                throw ex;
            }
        }
        finally {
            pigProps.delete();
            System.setProperty("user.name", user);
        }

        // Stats should be stored only if option to write stats is set to true
        if (writeStats) {
            assertTrue(statsDataFile.exists());
            String stats = IOUtils.getReaderAsString(new FileReader(statsDataFile), -1);
            // check for some of the expected key values in the stats
            Map m = (Map) JSONValue.parse(stats);
            // check for expected 1st level JSON keys
            assertTrue(m.containsKey("PIG_VERSION"));
        } else {
            assertFalse(statsDataFile.exists());
        }
        // HadoopIds should always be stored
        assertTrue(hadoopIdsFile.exists());
        String externalChildIds = IOUtils.getReaderAsString(new FileReader(hadoopIdsFile), -1);
        assertTrue(externalChildIds.contains("job_"));
        return null;
    }

}
