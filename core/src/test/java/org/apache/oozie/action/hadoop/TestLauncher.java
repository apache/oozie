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
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.Services;

import java.io.File;
import java.io.IOException;
import java.net.URI;

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


        LauncherMapper lm = new LauncherMapper();
        lm.setupMainClass(jobConf, LauncherMainTester.class.getName());
        lm.setupMainArguments(jobConf, arg);

        Configuration actionConf = new XConfiguration();
        lm.setupLauncherInfo(jobConf, "1", "1@a", actionDir, "1@a-0", actionConf, "");

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
        RunningJob runningJob = _test();
        Thread.sleep(2000);
        assertTrue(runningJob.isSuccessful());

        assertTrue(LauncherMapper.isMainDone(runningJob));
        assertTrue(LauncherMapper.isMainSuccessful(runningJob));
        assertFalse(LauncherMapper.hasOutputData(runningJob));
        assertFalse(LauncherMapper.hasIdSwap(runningJob));
        assertTrue(LauncherMapper.isMainDone(runningJob));
        assertFalse(fs.exists(LauncherMapper.getErrorPath(actionDir)));
        assertFalse(fs.exists(LauncherMapper.getIdSwapPath(actionDir)));
        assertFalse(fs.exists(LauncherMapper.getOutputDataPath(actionDir)));
    }

    public void testExit0() throws Exception {
        Path actionDir = getFsTestCaseDir();
        FileSystem fs = getFileSystem();
        RunningJob runningJob = _test("exit0");
        Thread.sleep(2000);
        assertTrue(runningJob.isSuccessful());

        assertTrue(LauncherMapper.isMainDone(runningJob));
        assertTrue(LauncherMapper.isMainSuccessful(runningJob));
        assertFalse(LauncherMapper.hasOutputData(runningJob));
        assertFalse(LauncherMapper.hasIdSwap(runningJob));
        assertTrue(LauncherMapper.isMainDone(runningJob));
        assertFalse(fs.exists(LauncherMapper.getErrorPath(actionDir)));
        assertFalse(fs.exists(LauncherMapper.getIdSwapPath(actionDir)));
        assertFalse(fs.exists(LauncherMapper.getOutputDataPath(actionDir)));
    }

    public void testExit1() throws Exception {
        Path actionDir = getFsTestCaseDir();
        FileSystem fs = getFileSystem();
        RunningJob runningJob = _test("exit1");
        Thread.sleep(2000);
        assertTrue(runningJob.isSuccessful());

        assertTrue(LauncherMapper.isMainDone(runningJob));
        assertFalse(LauncherMapper.isMainSuccessful(runningJob));
        assertFalse(LauncherMapper.hasOutputData(runningJob));
        assertFalse(LauncherMapper.hasIdSwap(runningJob));
        assertTrue(LauncherMapper.isMainDone(runningJob));
        assertTrue(fs.exists(LauncherMapper.getErrorPath(actionDir)));
        assertFalse(fs.exists(LauncherMapper.getIdSwapPath(actionDir)));
        assertFalse(fs.exists(LauncherMapper.getOutputDataPath(actionDir)));
    }

    public void testException() throws Exception {
        Path actionDir = getFsTestCaseDir();
        FileSystem fs = getFileSystem();
        RunningJob runningJob = _test("ex");
        Thread.sleep(2000);
        assertTrue(runningJob.isSuccessful());

        assertTrue(LauncherMapper.isMainDone(runningJob));
        assertFalse(LauncherMapper.isMainSuccessful(runningJob));
        assertFalse(LauncherMapper.hasOutputData(runningJob));
        assertFalse(LauncherMapper.hasIdSwap(runningJob));
        assertTrue(LauncherMapper.isMainDone(runningJob));
        assertTrue(fs.exists(LauncherMapper.getErrorPath(actionDir)));
        assertFalse(fs.exists(LauncherMapper.getIdSwapPath(actionDir)));
        assertFalse(fs.exists(LauncherMapper.getOutputDataPath(actionDir)));
    }

    public void testOutput() throws Exception {
        Path actionDir = getFsTestCaseDir();
        FileSystem fs = getFileSystem();
        RunningJob runningJob = _test("out");
        Thread.sleep(2000);
        assertTrue(runningJob.isSuccessful());

        assertTrue(LauncherMapper.isMainDone(runningJob));
        assertTrue(LauncherMapper.isMainSuccessful(runningJob));
        assertTrue(LauncherMapper.hasOutputData(runningJob));
        assertFalse(LauncherMapper.hasIdSwap(runningJob));
        assertTrue(LauncherMapper.isMainDone(runningJob));
        assertFalse(fs.exists(LauncherMapper.getErrorPath(actionDir)));
        assertFalse(fs.exists(LauncherMapper.getIdSwapPath(actionDir)));
        assertTrue(fs.exists(LauncherMapper.getOutputDataPath(actionDir)));
    }

    public void testNewId() throws Exception {
        Path actionDir = getFsTestCaseDir();
        FileSystem fs = getFileSystem();
        RunningJob runningJob = _test("id");
        Thread.sleep(2000);
        assertTrue(runningJob.isSuccessful());

        assertTrue(LauncherMapper.isMainDone(runningJob));
        assertTrue(LauncherMapper.isMainSuccessful(runningJob));
        assertFalse(LauncherMapper.hasOutputData(runningJob));
        assertTrue(LauncherMapper.hasIdSwap(runningJob));
        assertTrue(LauncherMapper.isMainDone(runningJob));
        assertFalse(fs.exists(LauncherMapper.getErrorPath(actionDir)));
        assertTrue(fs.exists(LauncherMapper.getIdSwapPath(actionDir)));
        assertFalse(fs.exists(LauncherMapper.getOutputDataPath(actionDir)));
    }

    public void testSecurityManager() throws Exception {
        Path actionDir = getFsTestCaseDir();
        FileSystem fs = getFileSystem();
        RunningJob runningJob = _test("securityManager");
        Thread.sleep(2000);
        assertTrue(runningJob.isSuccessful());

        assertTrue(LauncherMapper.isMainDone(runningJob));
        assertTrue(LauncherMapper.isMainSuccessful(runningJob));
        assertFalse(LauncherMapper.hasOutputData(runningJob));
        assertFalse(LauncherMapper.hasIdSwap(runningJob));
        assertTrue(LauncherMapper.isMainDone(runningJob));
        assertFalse(fs.exists(LauncherMapper.getErrorPath(actionDir)));
        assertFalse(fs.exists(LauncherMapper.getIdSwapPath(actionDir)));
        assertFalse(fs.exists(LauncherMapper.getOutputDataPath(actionDir)));
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

        LauncherMapper lm = new LauncherMapper();
        Configuration actionConf = new XConfiguration();
        String prepareBlock = "";
        lm.setupLauncherInfo(jobConf, "1", "1@a", actionDir, "1@a-0", actionConf, prepareBlock);
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

        LauncherMapper lm = new LauncherMapper();
        Configuration actionConf = new XConfiguration();
        String prepareBlock = "<prepare>" + "<mkdir path='" + newDir + "'/>" + "</prepare>";
        lm.setupLauncherInfo(jobConf, "1", "1@a", actionDir, "1@a-0", actionConf, prepareBlock);
        assertTrue(jobConf.get("oozie.action.prepare.xml").equals(prepareBlock));
    }
}
