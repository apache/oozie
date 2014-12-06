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
import java.net.URI;
import java.util.Map;

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

        JobConf jobConf = Services.get().get(HadoopAccessorService.class).
            createJobConf(new URI(getNameNodeUri()).getAuthority());
//        jobConf.setJar(jar.getAbsolutePath());
        jobConf.set("user.name", getTestUser());
        jobConf.setInt("mapred.map.tasks", 1);
        jobConf.setInt("mapred.map.max.attempts", 1);
        jobConf.setInt("mapred.reduce.max.attempts", 1);

        jobConf.set("mapreduce.framework.name", "yarn");
        jobConf.set("mapred.job.tracker", getJobTrackerUri());
        jobConf.set("fs.default.name", getNameNodeUri());


        LauncherMapperHelper.setupMainClass(jobConf, LauncherMainTester.class.getName());
        LauncherMapperHelper.setupMainArguments(jobConf, arg);

        Configuration actionConf = new XConfiguration();
        LauncherMapperHelper.setupLauncherInfo(jobConf, "1", "1@a", actionDir, "1@a-0", actionConf, "");
        LauncherMapperHelper.setupYarnRestartHandling(jobConf, jobConf, "1@a");

        assertEquals("1", actionConf.get("oozie.job.id"));
        assertEquals("1@a", actionConf.get("oozie.action.id"));

        DistributedCache.addFileToClassPath(new Path(launcherJar.toUri().getPath()), jobConf);

        JobClient jobClient = createJobClient();

        final RunningJob runningJob = jobClient.submitJob(jobConf);

        System.out.println("Action Dir: " + actionDir);
        System.out.println("LauncherMapper ID: " + runningJob.getJobID().toString());

        waitFor(180 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return runningJob.isComplete();
            }
        });

        assertTrue(jobConf.get("oozie.action.prepare.xml").equals(""));
        return runningJob;

    }

    public void testEmpty() throws Exception {
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
        Map<String, String> actionData = LauncherMapperHelper.getActionData(fs, actionDir, conf);
        assertFalse(fs.exists(LauncherMapperHelper.getActionDataSequenceFilePath(actionDir)));
        assertTrue(LauncherMapperHelper.isMainDone(runningJob));
        assertTrue(LauncherMapperHelper.isMainSuccessful(runningJob));
        assertFalse(LauncherMapperHelper.hasOutputData(actionData));
        assertFalse(LauncherMapperHelper.hasIdSwap(actionData));
        assertTrue(LauncherMapperHelper.isMainDone(runningJob));
    }

    public void testExit0() throws Exception {
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
        Map<String, String> actionData = LauncherMapperHelper.getActionData(fs, actionDir, conf);
        assertFalse(fs.exists(LauncherMapperHelper.getActionDataSequenceFilePath(actionDir)));
        assertTrue(LauncherMapperHelper.isMainDone(runningJob));
        assertTrue(LauncherMapperHelper.isMainSuccessful(runningJob));
        assertFalse(LauncherMapperHelper.hasOutputData(actionData));
        assertFalse(LauncherMapperHelper.hasIdSwap(actionData));
        assertTrue(LauncherMapperHelper.isMainDone(runningJob));
    }

    public void testExit1() throws Exception {
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
        Map<String, String> actionData = LauncherMapperHelper.getActionData(fs, actionDir, conf);
        assertTrue(fs.exists(LauncherMapperHelper.getActionDataSequenceFilePath(actionDir)));
        assertTrue(LauncherMapperHelper.isMainDone(runningJob));
        assertFalse(LauncherMapperHelper.isMainSuccessful(runningJob));
        assertFalse(LauncherMapperHelper.hasOutputData(actionData));
        assertFalse(LauncherMapperHelper.hasIdSwap(actionData));
        assertTrue(LauncherMapperHelper.isMainDone(runningJob));
        assertTrue(actionData.containsKey(LauncherMapper.ACTION_DATA_ERROR_PROPS));
    }

    public void testException() throws Exception {
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
        Map<String, String> actionData = LauncherMapperHelper.getActionData(fs, actionDir, conf);
        assertTrue(fs.exists(LauncherMapperHelper.getActionDataSequenceFilePath(actionDir)));
        assertTrue(LauncherMapperHelper.isMainDone(runningJob));
        assertFalse(LauncherMapperHelper.isMainSuccessful(runningJob));
        assertFalse(LauncherMapperHelper.hasOutputData(actionData));
        assertFalse(LauncherMapperHelper.hasIdSwap(actionData));
        assertTrue(LauncherMapperHelper.isMainDone(runningJob));
    }

    public void testThrowable() throws Exception {
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
        Map<String, String> actionData = LauncherMapperHelper.getActionData(fs, actionDir, conf);
        assertTrue(fs.exists(LauncherMapperHelper.getActionDataSequenceFilePath(actionDir)));
        assertTrue(LauncherMapperHelper.isMainDone(runningJob));
        assertFalse(LauncherMapperHelper.isMainSuccessful(runningJob));
        assertFalse(LauncherMapperHelper.hasOutputData(actionData));
        assertFalse(LauncherMapperHelper.hasIdSwap(actionData));
        assertTrue(LauncherMapperHelper.isMainDone(runningJob));
    }

    public void testOutput() throws Exception {
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
        Map<String, String> actionData = LauncherMapperHelper.getActionData(fs, actionDir, conf);
        assertTrue(fs.exists(LauncherMapperHelper.getActionDataSequenceFilePath(actionDir)));
        assertTrue(LauncherMapperHelper.isMainDone(runningJob));
        assertTrue(LauncherMapperHelper.isMainSuccessful(runningJob));
        assertTrue(LauncherMapperHelper.hasOutputData(actionData));
        assertFalse(LauncherMapperHelper.hasIdSwap(actionData));
        assertTrue(LauncherMapperHelper.isMainDone(runningJob));
    }

    public void testNewId() throws Exception {
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
        Map<String, String> actionData = LauncherMapperHelper.getActionData(fs, actionDir, conf);
        assertTrue(fs.exists(LauncherMapperHelper.getActionDataSequenceFilePath(actionDir)));
        assertTrue(LauncherMapperHelper.isMainDone(runningJob));
        assertTrue(LauncherMapperHelper.isMainSuccessful(runningJob));
        assertFalse(LauncherMapperHelper.hasOutputData(actionData));
        assertTrue(LauncherMapperHelper.hasIdSwap(actionData));
        assertTrue(LauncherMapperHelper.isMainDone(runningJob));
    }

    public void testSecurityManager() throws Exception {
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
        Map<String, String> actionData = LauncherMapperHelper.getActionData(fs, actionDir, conf);
        assertFalse(fs.exists(LauncherMapperHelper.getActionDataSequenceFilePath(actionDir)));
        assertTrue(LauncherMapperHelper.isMainDone(runningJob));
        assertTrue(LauncherMapperHelper.isMainSuccessful(runningJob));
        assertFalse(LauncherMapperHelper.hasOutputData(actionData));
        assertFalse(LauncherMapperHelper.hasIdSwap(actionData));
        assertTrue(LauncherMapperHelper.isMainDone(runningJob));
    }

    // Test to ensure that the property value "oozie.action.prepare.xml" in the configuration of the job is an empty
    // string when there is no prepare block in workflow XML or there is one with no prepare actions in it
    public void testSetupLauncherInfoWithEmptyPrepareXML() throws Exception {
        Path actionDir = getFsTestCaseDir();

        // Setting up the job configuration
        JobConf jobConf = Services.get().get(HadoopAccessorService.class).
            createJobConf(new URI(getNameNodeUri()).getAuthority());
        jobConf.set("user.name", getTestUser());
        jobConf.set("fs.default.name", getNameNodeUri());

        Configuration actionConf = new XConfiguration();
        String prepareBlock = "";
        LauncherMapperHelper.setupLauncherInfo(jobConf, "1", "1@a", actionDir, "1@a-0", actionConf, prepareBlock);
        assertTrue(jobConf.get("oozie.action.prepare.xml").equals(""));
    }

    // Test to ensure that the property value "oozie.action.prepare.xml" in the configuration of the job is properly set
    // when there is prepare block in workflow XML
    public void testSetupLauncherInfoWithNonEmptyPrepareXML() throws Exception {
        Path actionDir = getFsTestCaseDir();
        FileSystem fs = getFileSystem();
        Path newDir = new Path(actionDir, "newDir");

        // Setting up the job configuration
        JobConf jobConf = Services.get().get(HadoopAccessorService.class).
            createJobConf(new URI(getNameNodeUri()).getAuthority());
        jobConf.set("user.name", getTestUser());
        jobConf.set("fs.default.name", getNameNodeUri());

        Configuration actionConf = new XConfiguration();
        String prepareBlock = "<prepare>" + "<mkdir path='" + newDir + "'/>" + "</prepare>";
        LauncherMapperHelper.setupLauncherInfo(jobConf, "1", "1@a", actionDir, "1@a-0", actionConf, prepareBlock);
        assertTrue(jobConf.get("oozie.action.prepare.xml").equals(prepareBlock));
    }

    public void testSetupMainClass() throws Exception {
        Configuration conf = new Configuration(false);
        LauncherMapperHelper.setupMainClass(conf, "");
        assertNull(conf.get("oozie.launcher.action.main.class"));

        conf = new Configuration(false);
        LauncherMapperHelper.setupMainClass(conf, "org.blah.myclass1");
        assertEquals(conf.get("oozie.launcher.action.main.class"), "org.blah.myclass1");

        conf = new Configuration(false);
        conf.set("oozie.launcher.action.main.class", "org.blah.myclass2");
        LauncherMapperHelper.setupMainClass(conf, "");
        assertEquals(conf.get("oozie.launcher.action.main.class"), "org.blah.myclass2");

        // the passed argument (myclass1) should have priority
        conf = new Configuration(false);
        conf.set("oozie.launcher.action.main.class", "org.blah.myclass2");
        LauncherMapperHelper.setupMainClass(conf, "org.blah.myclass1");
        assertEquals(conf.get("oozie.launcher.action.main.class"), "org.blah.myclass1");
    }

  // Test to ensure that the property value "oozie.action.prepare.xml" in the configuration of the job is properly set
  // when there is prepare block in workflow XML
  public void testSetupLauncherInfoHadoop2_0_2_alphaWorkaround() throws Exception {
    Path actionDir = getFsTestCaseDir();
    // Setting up the job configuration
    JobConf jobConf = Services.get().get(HadoopAccessorService.class).
      createJobConf(new URI(getNameNodeUri()).getAuthority());
    jobConf.set("user.name", getTestUser());
    jobConf.set("fs.default.name", getNameNodeUri());

    Configuration actionConf = new XConfiguration();
    actionConf.set("mapreduce.job.cache.files", "a.jar,aa.jar#aa.jar");
    LauncherMapperHelper.setupLauncherInfo(jobConf, "1", "1@a", actionDir, "1@a-0", actionConf, "");
    assertFalse(jobConf.getBoolean("oozie.hadoop-2.0.2-alpha.workaround.for.distributed.cache", false));
    assertEquals("a.jar,aa.jar#aa.jar", actionConf.get("mapreduce.job.cache.files"));

    Services.get().getConf().setBoolean("oozie.hadoop-2.0.2-alpha.workaround.for.distributed.cache", true);
    actionConf = new XConfiguration();
    actionConf.set("mapreduce.job.cache.files", "a.jar,aa.jar#aa.jar");
    LauncherMapperHelper.setupLauncherInfo(jobConf, "1", "1@a", actionDir, "1@a-0", actionConf, "");
    assertTrue(jobConf.getBoolean("oozie.hadoop-2.0.2-alpha.workaround.for.distributed.cache", false));
    assertEquals("aa.jar#aa.jar", actionConf.get("mapreduce.job.cache.files"));
  }

}
