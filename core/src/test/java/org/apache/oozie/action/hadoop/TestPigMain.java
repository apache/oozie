/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.action.hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.ClassUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.pig.Main;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.io.FileWriter;
import java.io.FileReader;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.net.URL;

import jline.ConsoleReaderInputStream;

public class TestPigMain extends MainTestCase {
    private SecurityManager SECURITY_MANAGER;

    protected void setUp() throws Exception {
        super.setUp();
        SECURITY_MANAGER = System.getSecurityManager();
    }

    protected void tearDown() throws Exception {
        System.setSecurityManager(SECURITY_MANAGER);
        super.tearDown();
    }

    private static final String PIG_SCRIPT =
            "set job.name 'test'\n" +
                    "set debug on\n" +
                    "A = load '$IN' using PigStorage(':');\n" +
                    "B = foreach A generate $0 as id;\n" +
                    "store B into '$OUT' USING PigStorage();\n";

    public Void call() throws Exception {
        FileSystem fs = getFileSystem();

        Path pigJar = new Path(getFsTestCaseDir(), "pig.jar");
        InputStream is = new FileInputStream(ClassUtils.findContainingJar(Main.class));
        OutputStream os = fs.create(pigJar);
        IOUtils.copyStream(is, os);

        Path jlineJar = new Path(getFsTestCaseDir(), "jline.jar");
        is = new FileInputStream(ClassUtils.findContainingJar(ConsoleReaderInputStream.class));
        os = fs.create(jlineJar);
        IOUtils.copyStream(is, os);

        Path script = new Path(getTestCaseDir(), "script.pig");
        Writer w = new FileWriter(script.toString());
        w.write(PIG_SCRIPT);
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

        DistributedCache.addFileToClassPath(new Path(pigJar.toUri().getPath()), getFileSystem().getConf());
        DistributedCache.addFileToClassPath(new Path(jlineJar.toUri().getPath()), getFileSystem().getConf());

        PigMain.setPigScript(jobConf, script.toString(), new String[]{"IN=" + inputDir.toUri().getPath(),
                "OUT=" + outputDir.toUri().getPath()}, new String[]{"-v"});

        File actionXml = new File(getTestCaseDir(), "action.xml");
        os = new FileOutputStream(actionXml);
        jobConf.writeXml(os);
        os.close();

        File outputDataFile = new File(getTestCaseDir(), "outputdata.properties");

        setSystemProperty("oozie.launcher.job.id", "" + System.currentTimeMillis());
        setSystemProperty("oozie.action.conf.xml", actionXml.getAbsolutePath());
        setSystemProperty("oozie.action.output.properties", outputDataFile.getAbsolutePath());


        URL url = Thread.currentThread().getContextClassLoader().getResource("PigMain.txt");
        File classPathDir = new File(url.getPath()).getParentFile();
        assertTrue(classPathDir.exists());
        Properties props = jobConf.toProperties();
        assertEquals(props.getProperty("oozie.pig.args.size"), "1");
        Writer wr = new FileWriter(new File(classPathDir, "pig.properties"));
        props.store(wr, "");
        wr.close();

        new LauncherSecurityManager();
        String user = System.getProperty("user.name");
        try {
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
            System.setProperty("user.name", user);
        }

        assertTrue(outputDataFile.exists());
        props = new Properties();
        props.load(new FileReader(outputDataFile));
        assertTrue(props.containsKey("hadoopJobs"));
        assertNotSame("", props.getProperty("hadoopJobs"));

        return null;
    }

}
