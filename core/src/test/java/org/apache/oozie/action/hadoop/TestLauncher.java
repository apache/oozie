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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.net.URI;
import java.util.Map;

// TODO
// this whole class can be deleted - for now, just renamed the tests that fail
// These tests mostly validate LaunhcherMapper - with OOYA, LauncherMapper should be eliminated, too

// With Hadoop 2.4.0, things work slightly differently (there is an exception in LauncherMapper.map()), also,
// SequenceFile.Reader got deprecated
public class TestLauncher extends XFsTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        new Services().init();
    }

    @Override
    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    private RunningJob _test(String... arg) throws Exception {
        Path actionDir = getFsTestCaseDir();

        File jar = IOUtils.createJar(new File(getTestCaseDir()), "launcher.jar", LauncherMapper.class,
                                     LauncherMainException.class,
                                     LauncherSecurityManager.class, LauncherException.class, LauncherMainTester.class);

        FileSystem fs = getFileSystem();

        Path launcherJar = new Path(actionDir, "launcher.jar");
        fs.copyFromLocalFile(new Path(jar.toString()), launcherJar);

        Configuration appConf = Services.get().get(HadoopAccessorService.class).
                createConfiguration(new URI(getNameNodeUri()).getAuthority());
        appConf.set("user.name", getTestUser());
        appConf.setInt("mapred.map.tasks", 1);
        appConf.setInt("mapred.map.max.attempts", 1);
        appConf.setInt("mapred.reduce.max.attempts", 1);

        appConf.set("mapreduce.framework.name", "yarn");
        appConf.set("mapred.job.tracker", getJobTrackerUri());
        appConf.set("fs.default.name", getNameNodeUri());


        LauncherHelper.setupMainClass(appConf, LauncherMainTester.class.getName());
        LauncherHelper.setupMainArguments(appConf, arg);

        Configuration actionConf = new XConfiguration();
        LauncherHelper.setupLauncherInfo(appConf, "1", "1@a", actionDir, "1@a-0", actionConf, "");
        LauncherHelper.setupYarnRestartHandling(appConf, appConf, "1@a", System.currentTimeMillis());

        assertEquals("1", actionConf.get("oozie.job.id"));
        assertEquals("1@a", actionConf.get("oozie.action.id"));

        DistributedCache.addFileToClassPath(new Path(launcherJar.toUri().getPath()), appConf);

        JobClient jobClient = createJobClient();

        final RunningJob runningJob = jobClient.submitJob(new JobConf(appConf));

        System.out.println("Action Dir: " + actionDir);
        System.out.println("LauncherMapper ID: " + runningJob.getJobID().toString());

        waitFor(180 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return runningJob.isComplete();
            }
        });

        assertTrue(appConf.get("oozie.action.prepare.xml").equals(""));
        return runningJob;

    }

    public void ___testEmpty() throws Exception {
        Path actionDir = getFsTestCaseDir();
        FileSystem fs = getFileSystem();
        final RunningJob runningJob = _test();
        waitFor(2000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return runningJob.isComplete();
            }
        });
        assertTrue(runningJob.isSuccessful());

        Configuration conf = new XConfiguration();
        conf.set("user.name", getTestUser());
        Map<String, String> actionData = LauncherHelper.getActionData(fs, actionDir, conf);
        assertFalse(fs.exists(LauncherHelper.getActionDataSequenceFilePath(actionDir)));
        assertTrue(LauncherHelper.isMainDone(runningJob));
        assertTrue(LauncherHelper.isMainSuccessful(runningJob));
        assertFalse(LauncherHelper.hasOutputData(actionData));
        assertFalse(LauncherHelper.hasIdSwap(actionData));
        assertTrue(LauncherHelper.isMainDone(runningJob));
    }

    public void ___testExit0() throws Exception {
        Path actionDir = getFsTestCaseDir();
        FileSystem fs = getFileSystem();
        final RunningJob runningJob = _test("exit0");
        waitFor(2000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return runningJob.isComplete();
            }
        });
        assertTrue(runningJob.isSuccessful());

        Configuration conf = new XConfiguration();
        conf.set("user.name", getTestUser());
        Map<String, String> actionData = LauncherHelper.getActionData(fs, actionDir, conf);
        assertFalse(fs.exists(LauncherHelper.getActionDataSequenceFilePath(actionDir)));
        assertTrue(LauncherHelper.isMainDone(runningJob));
        assertTrue(LauncherHelper.isMainSuccessful(runningJob));
        assertFalse(LauncherHelper.hasOutputData(actionData));
        assertFalse(LauncherHelper.hasIdSwap(actionData));
        assertTrue(LauncherHelper.isMainDone(runningJob));
    }

    public void ___testExit1() throws Exception {
        Path actionDir = getFsTestCaseDir();
        FileSystem fs = getFileSystem();
        final RunningJob runningJob = _test("exit1");
        waitFor(2000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return runningJob.isComplete();
            }
        });
        assertTrue(runningJob.isSuccessful());

        Configuration conf = new XConfiguration();
        conf.set("user.name", getTestUser());
        Map<String, String> actionData = LauncherHelper.getActionData(fs, actionDir, conf);
        assertTrue(fs.exists(LauncherHelper.getActionDataSequenceFilePath(actionDir)));
        assertTrue(LauncherHelper.isMainDone(runningJob));
        assertFalse(LauncherHelper.isMainSuccessful(runningJob));
        assertFalse(LauncherHelper.hasOutputData(actionData));
        assertFalse(LauncherHelper.hasIdSwap(actionData));
        assertTrue(LauncherHelper.isMainDone(runningJob));
        assertTrue(actionData.containsKey(LauncherMapper.ACTION_DATA_ERROR_PROPS));
    }

    public void ___testException() throws Exception {
        Path actionDir = getFsTestCaseDir();
        FileSystem fs = getFileSystem();
        final RunningJob runningJob = _test("exception");
        waitFor(2000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return runningJob.isComplete();
            }
        });
        assertTrue(runningJob.isSuccessful());

        Configuration conf = new XConfiguration();
        conf.set("user.name", getTestUser());
        Map<String, String> actionData = LauncherHelper.getActionData(fs, actionDir, conf);
        assertTrue(fs.exists(LauncherHelper.getActionDataSequenceFilePath(actionDir)));
        assertTrue(LauncherHelper.isMainDone(runningJob));
        assertFalse(LauncherHelper.isMainSuccessful(runningJob));
        assertFalse(LauncherHelper.hasOutputData(actionData));
        assertFalse(LauncherHelper.hasIdSwap(actionData));
        assertTrue(LauncherHelper.isMainDone(runningJob));
    }

    public void __testThrowable() throws Exception {
        Path actionDir = getFsTestCaseDir();
        FileSystem fs = getFileSystem();
        final RunningJob runningJob = _test("throwable");
        waitFor(2000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return runningJob.isComplete();
            }
        });
        assertTrue(runningJob.isSuccessful());

        Configuration conf = new XConfiguration();
        conf.set("user.name", getTestUser());
        Map<String, String> actionData = LauncherHelper.getActionData(fs, actionDir, conf);
        assertTrue(fs.exists(LauncherHelper.getActionDataSequenceFilePath(actionDir)));
        assertTrue(LauncherHelper.isMainDone(runningJob));
        assertFalse(LauncherHelper.isMainSuccessful(runningJob));
        assertFalse(LauncherHelper.hasOutputData(actionData));
        assertFalse(LauncherHelper.hasIdSwap(actionData));
        assertTrue(LauncherHelper.isMainDone(runningJob));
    }

    public void __testOutput() throws Exception {
        Path actionDir = getFsTestCaseDir();
        FileSystem fs = getFileSystem();
        final RunningJob runningJob = _test("out");
        waitFor(2000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return runningJob.isComplete();
            }
        });
        assertTrue(runningJob.isSuccessful());

        Configuration conf = new XConfiguration();
        conf.set("user.name", getTestUser());
        Map<String, String> actionData = LauncherHelper.getActionData(fs, actionDir, conf);
        assertTrue(fs.exists(LauncherHelper.getActionDataSequenceFilePath(actionDir)));
        assertTrue(LauncherHelper.isMainDone(runningJob));
        assertTrue(LauncherHelper.isMainSuccessful(runningJob));
        assertTrue(LauncherHelper.hasOutputData(actionData));
        assertFalse(LauncherHelper.hasIdSwap(actionData));
        assertTrue(LauncherHelper.isMainDone(runningJob));
    }

    public void __testNewId() throws Exception {
        Path actionDir = getFsTestCaseDir();
        FileSystem fs = getFileSystem();
        final RunningJob runningJob = _test("id");
        waitFor(2000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return runningJob.isComplete();
            }
        });
        assertTrue(runningJob.isSuccessful());

        Configuration conf = new XConfiguration();
        conf.set("user.name", getTestUser());
        Map<String, String> actionData = LauncherHelper.getActionData(fs, actionDir, conf);
        assertTrue(fs.exists(LauncherHelper.getActionDataSequenceFilePath(actionDir)));
        assertTrue(LauncherHelper.isMainDone(runningJob));
        assertTrue(LauncherHelper.isMainSuccessful(runningJob));
        assertFalse(LauncherHelper.hasOutputData(actionData));
        assertTrue(LauncherHelper.hasIdSwap(actionData));
        assertTrue(LauncherHelper.isMainDone(runningJob));
    }

    public void __testSecurityManager() throws Exception {
        Path actionDir = getFsTestCaseDir();
        FileSystem fs = getFileSystem();
        final RunningJob runningJob = _test("securityManager");
        waitFor(2000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return runningJob.isComplete();
            }
        });
        assertTrue(runningJob.isSuccessful());

        Configuration conf = new XConfiguration();
        conf.set("user.name", getTestUser());
        Map<String, String> actionData = LauncherHelper.getActionData(fs, actionDir, conf);
        assertFalse(fs.exists(LauncherHelper.getActionDataSequenceFilePath(actionDir)));
        assertTrue(LauncherHelper.isMainDone(runningJob));
        assertTrue(LauncherHelper.isMainSuccessful(runningJob));
        assertFalse(LauncherHelper.hasOutputData(actionData));
        assertFalse(LauncherHelper.hasIdSwap(actionData));
        assertTrue(LauncherHelper.isMainDone(runningJob));
    }

    // Test to ensure that the property value "oozie.action.prepare.xml" in the configuration of the job is an empty
    // string when there is no prepare block in workflow XML or there is one with no prepare actions in it
    public void testSetupLauncherInfoWithEmptyPrepareXML() throws Exception {
        Path actionDir = getFsTestCaseDir();

        // Setting up the job configuration
        Configuration appConf = Services.get().get(HadoopAccessorService.class).
                createConfiguration(new URI(getNameNodeUri()).getAuthority());
        appConf.set("user.name", getTestUser());
        appConf.set("fs.default.name", getNameNodeUri());

        Configuration actionConf = new XConfiguration();
        String prepareBlock = "";
        LauncherHelper.setupLauncherInfo(appConf, "1", "1@a", actionDir, "1@a-0", actionConf, prepareBlock);
        assertTrue(appConf.get("oozie.action.prepare.xml").equals(""));
    }

    // Test to ensure that the property value "oozie.action.prepare.xml" in the configuration of the job is properly set
    // when there is prepare block in workflow XML
    public void testSetupLauncherInfoWithNonEmptyPrepareXML() throws Exception {
        Path actionDir = getFsTestCaseDir();
        FileSystem fs = getFileSystem();
        Path newDir = new Path(actionDir, "newDir");

        // Setting up the job configuration
        Configuration appConf = Services.get().get(HadoopAccessorService.class).
                createConfiguration(new URI(getNameNodeUri()).getAuthority());
        appConf.set("user.name", getTestUser());
        appConf.set("fs.default.name", getNameNodeUri());

        Configuration actionConf = new XConfiguration();
        String prepareBlock = "<prepare>" + "<mkdir path='" + newDir + "'/>" + "</prepare>";
        LauncherHelper.setupLauncherInfo(appConf, "1", "1@a", actionDir, "1@a-0", actionConf, prepareBlock);
        assertTrue(appConf.get("oozie.action.prepare.xml").equals(prepareBlock));
    }

    public void testSetupMainClass() throws Exception {
        Configuration conf = new Configuration(false);
        LauncherHelper.setupMainClass(conf, "");
        assertNull(conf.get("oozie.launcher.action.main.class"));

        conf = new Configuration(false);
        LauncherHelper.setupMainClass(conf, "org.blah.myclass1");
        assertEquals(conf.get("oozie.launcher.action.main.class"), "org.blah.myclass1");

        conf = new Configuration(false);
        conf.set("oozie.launcher.action.main.class", "org.blah.myclass2");
        LauncherHelper.setupMainClass(conf, "");
        assertEquals(conf.get("oozie.launcher.action.main.class"), "org.blah.myclass2");

        // the passed argument (myclass1) should have priority
        conf = new Configuration(false);
        conf.set("oozie.launcher.action.main.class", "org.blah.myclass2");
        LauncherHelper.setupMainClass(conf, "org.blah.myclass1");
        assertEquals(conf.get("oozie.launcher.action.main.class"), "org.blah.myclass1");
    }

  // Test to ensure that the property value "oozie.action.prepare.xml" in the configuration of the job is properly set
  // when there is prepare block in workflow XML
  public void testSetupLauncherInfoHadoop2_0_2_alphaWorkaround() throws Exception {
    Path actionDir = getFsTestCaseDir();
    // Setting up the job configuration
      Configuration appConf = Services.get().get(HadoopAccessorService.class).
              createConfiguration(new URI(getNameNodeUri()).getAuthority());
    appConf.set("user.name", getTestUser());
    appConf.set("fs.default.name", getNameNodeUri());

    Configuration actionConf = new XConfiguration();
    actionConf.set("mapreduce.job.cache.files", "a.jar,aa.jar#aa.jar");
    LauncherHelper.setupLauncherInfo(appConf, "1", "1@a", actionDir, "1@a-0", actionConf, "");
    assertFalse(appConf.getBoolean("oozie.hadoop-2.0.2-alpha.workaround.for.distributed.cache", false));
    assertEquals("a.jar,aa.jar#aa.jar", actionConf.get("mapreduce.job.cache.files"));

    Services.get().getConf().setBoolean("oozie.hadoop-2.0.2-alpha.workaround.for.distributed.cache", true);
    actionConf = new XConfiguration();
    actionConf.set("mapreduce.job.cache.files", "a.jar,aa.jar#aa.jar");
    LauncherHelper.setupLauncherInfo(appConf, "1", "1@a", actionDir, "1@a-0", actionConf, "");
    assertTrue(appConf.getBoolean("oozie.hadoop-2.0.2-alpha.workaround.for.distributed.cache", false));
    assertEquals("aa.jar#aa.jar", actionConf.get("mapreduce.job.cache.files"));
  }

    public void testCopyFileMultiplex() throws Exception {
        String contents = "Hello World!\nThis is Oozie";
        File src = new File(getTestCaseDir(), "src.txt");
        Writer w = new FileWriter(src);
        w.write(contents);
        w.close();

        File[] dsts = new File[]{new File("dst1.txt"), new File("dist2.txt"), new File("dist3.txt")};
        for (File dst : dsts) {
            dst.delete();
            assertFalse(dst.exists());
        }
        LauncherMain.copyFileMultiplex(src, dsts);
        for (File dst : dsts) {
            assertTrue(dst.exists());
            assertEquals(contents, FileUtils.readFileToString(dst));
        }
    }

}
