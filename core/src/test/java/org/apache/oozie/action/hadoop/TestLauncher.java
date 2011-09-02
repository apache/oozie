/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.action.hadoop;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;

import java.io.File;

public class TestLauncher extends XFsTestCase {

    private RunningJob _test(String... arg) throws Exception {
        Path actionDir = getFsTestCaseDir();

        File jar = IOUtils.createJar(new File(getTestCaseDir()), "launcher.jar", LauncherMapper.class,
                                     LauncherSecurityManager.class, LauncherException.class, LauncherMainTester.class);

        FileSystem fs = getFileSystem();

        Path launcherJar = new Path(actionDir, "launcher.jar");
        fs.copyFromLocalFile(new Path(jar.toString()), launcherJar);

        JobConf jobConf = new JobConf();
        jobConf.setInt("mapred.map.tasks", 1);
        jobConf.setInt("mapred.map.max.attempts", 1);
        jobConf.setInt("mapred.reduce.max.attempts", 1);
        
        jobConf.set("mapred.job.tracker", getJobTrackerUri());
        jobConf.set("fs.default.name", getNameNodeUri());
        LauncherMapper.setupMainClass(jobConf, LauncherMainTester.class.getName());
        LauncherMapper.setupMainArguments(jobConf, arg);
        LauncherMapper.setupLauncherInfo(jobConf, "1", "1@a", actionDir, "1@a-0", new XConfiguration());

        DistributedCache.addFileToClassPath(new Path(launcherJar.toUri().getPath()), jobConf);

        JobClient jobClient = new JobClient(jobConf);

        final RunningJob runningJob = jobClient.submitJob(jobConf);

        System.out.println("Action Dir: " + actionDir);
        System.out.println("LauncherMapper ID: " + runningJob.getJobID().toString());

        waitFor(60 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return runningJob.isComplete();
            }
        });
        return runningJob;

    }

    public void testEmpty() throws Exception {
        Path actionDir = getFsTestCaseDir();
        FileSystem fs = getFileSystem();
        RunningJob runningJob = _test();
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

}
