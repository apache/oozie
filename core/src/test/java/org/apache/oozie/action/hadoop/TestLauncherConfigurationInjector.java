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
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XTestCase;

import static org.apache.oozie.action.hadoop.JavaActionExecutor.HADOOP_USER;

public class TestLauncherConfigurationInjector extends XTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        new Services().init();
    }

    @Override
    protected void tearDown() throws Exception {
        if (Services.get() != null) {
            Services.get().destroy();
        }
        super.tearDown();
    }

    public void testOverrideSwitchedOffSourceCopiedToTargetWithTwoDifferentKeys() throws ActionExecutorException {
        ConfigurationService.setBoolean("oozie.launcher.override", false);

        final Configuration sourceConf = SourceConfigurationFactory.createOverridingAndLauncherEntries();
        final Configuration launcherConf = newConfigurationWithoutDefaults();

        new LauncherConfigurationInjector(sourceConf).inject(launcherConf);

        assertLauncherAndDefaultEntries(launcherConf);
    }

    private static Configuration newConfigurationWithoutDefaults() {
        return new Configuration(false);
    }

    private static Configuration newConfigurationWithDefaults() {
        return new Configuration(true);
    }

    private void assertLauncherAndDefaultEntries(final Configuration launcherConf) {
        assertEquals("launcher max attempts", 1, launcherConf.getInt(LauncherAM.OOZIE_LAUNCHER_MAX_ATTEMPTS, -1));
        assertEquals("max attempts", 1, launcherConf.getInt("max.attempts", -1));
        assertEquals("launcher memory mb", 512, launcherConf.getInt(LauncherAM.OOZIE_LAUNCHER_MEMORY_MB_PROPERTY, -1));
        assertEquals("memory mb", 512, launcherConf.getInt("memory.mb", -1));
        assertEquals("launcher vcores", 4, launcherConf.getInt(LauncherAM.OOZIE_LAUNCHER_VCORES_PROPERTY, -1));
        assertEquals("vcores", 4, launcherConf.getInt("vcores", -1));
        assertEquals("launcher log level", "DEBUG", launcherConf.get(LauncherAM.OOZIE_LAUNCHER_LOG_LEVEL_PROPERTY));
        assertEquals("log level", "DEBUG", launcherConf.get("log.level"));
        assertTrue("launcher java opts",
                launcherConf.get(LauncherAM.OOZIE_LAUNCHER_JAVAOPTS_PROPERTY).contains("-XX:MaxPermSize=128m"));
        assertTrue("java opts", launcherConf.get("javaopts").contains("-XX:MaxPermSize=128m"));
        assertTrue("launcher env", launcherConf.get(LauncherAM.OOZIE_LAUNCHER_ENV_PROPERTY).contains("PATH=/path2:$PATH"));
        assertTrue("env", launcherConf.get("env").contains("PATH=/path2:$PATH"));
        assertEquals("launcher priority", 2, launcherConf.getInt(LauncherAM.OOZIE_LAUNCHER_PRIORITY_PROPERTY, -1));
        assertEquals("priority", 2, launcherConf.getInt("priority", -1));
        assertTrue("launcher queue", launcherConf.get(LauncherAM.OOZIE_LAUNCHER_QUEUE_PROPERTY).contains("default2"));
        assertTrue("queue", launcherConf.get("queue").contains("default2"));
    }

    public void testLauncherConfigSourceCopiedToTarget() throws ActionExecutorException {
        final Configuration sourceConf = SourceConfigurationFactory.createLauncherEntries();
        final Configuration launcherConf = newConfigurationWithoutDefaults();

        new LauncherConfigurationInjector(sourceConf).inject(launcherConf);

        assertLauncherAndDefaultEntries(launcherConf);
    }

    public void testOverridingConfigCopiedToTarget() throws ActionExecutorException {
        final Configuration sourceConf = SourceConfigurationFactory.createOverridingEntries();
        final Configuration launcherConf = newConfigurationWithoutDefaults();

        new LauncherConfigurationInjector(sourceConf).inject(launcherConf);

        assertHigherRankingOverridingAndNoDefaultEntries(launcherConf);
    }

    private void assertHigherRankingOverridingAndNoDefaultEntries(final Configuration launcherConf) {
        assertEquals("launcher max-attempts", 4, launcherConf.getInt(LauncherAM.OOZIE_LAUNCHER_MAX_ATTEMPTS, -1));
        assertEquals("max-attempts", -1, launcherConf.getInt("max.attempts", -1));
        assertEquals("launcher memory mb", 2048, launcherConf.getInt(LauncherAM.OOZIE_LAUNCHER_MEMORY_MB_PROPERTY, -1));
        assertEquals("memory mb", -1, launcherConf.getInt("memory.mb", -1));
        assertEquals("launcher vcores", 16, launcherConf.getInt(LauncherAM.OOZIE_LAUNCHER_VCORES_PROPERTY, -1));
        assertEquals("vcores", -1, launcherConf.getInt("vcores", -1));
        assertEquals("launcher log level", "TRACE", launcherConf.get(LauncherAM.OOZIE_LAUNCHER_LOG_LEVEL_PROPERTY));
        assertNull("log level", launcherConf.get("log.level"));
        assertTrue("launcher java opts",
                launcherConf.get(LauncherAM.OOZIE_LAUNCHER_JAVAOPTS_PROPERTY).contains("-XX:MaxPermSize=256m"));
        assertNull("java opts", launcherConf.get("javaopts"));
        assertTrue("launcher env", launcherConf.get(LauncherAM.OOZIE_LAUNCHER_ENV_PROPERTY).contains("PATH=/path1:$PATH"));
        assertNull("env", launcherConf.get("env"));
        assertEquals("launcher priority", 1, launcherConf.getInt(LauncherAM.OOZIE_LAUNCHER_PRIORITY_PROPERTY, -1));
        assertEquals("priority", -1, launcherConf.getInt("priority", -1));
        assertTrue("launcher queue", launcherConf.get(LauncherAM.OOZIE_LAUNCHER_QUEUE_PROPERTY).contains("default1"));
        assertNull("queue", launcherConf.get("queue"));
        assertEquals("view ACL", "view", launcherConf.get(JavaActionExecutor.LAUNCER_VIEW_ACL));
        assertEquals("modify ACL", "modify", launcherConf.get(JavaActionExecutor.LAUNCER_MODIFY_ACL));
    }

    public void testMultipleOverrideOrder() throws ActionExecutorException {
        final Configuration sourceConf = SourceConfigurationFactory.createMultipleOverridingEntries();
        final Configuration launcherConf = newConfigurationWithoutDefaults();

        new LauncherConfigurationInjector(sourceConf).inject(launcherConf);

        assertHigherRankingOverridingAndNoDefaultEntries(launcherConf);
    }

    public void testPrependLauncherConfigSourcePrependedToTarget() throws ActionExecutorException {
        final Configuration sourceConf = SourceConfigurationFactory.createPrependingAndLauncherEntries();
        final Configuration launcherConf = newConfigurationWithoutDefaults();

        new LauncherConfigurationInjector(sourceConf).inject(launcherConf);

        assertPrependedLauncherAndDefaultEntries(launcherConf);
    }

    private void assertPrependedLauncherAndDefaultEntries(final Configuration launcherConf) {
        assertTrue("launcher java opts",
                launcherConf.get(LauncherAM.OOZIE_LAUNCHER_JAVAOPTS_PROPERTY).contains("-XX:+UseParNewGC -XX:MaxPermSize=128m"));
        assertTrue("java opts", launcherConf.get("javaopts").contains("-XX:MaxPermSize=128m"));
        assertFalse("java opts", launcherConf.get("javaopts").contains("-XX:+UseParNewGC"));
        assertTrue("launcher env",
                launcherConf.get(LauncherAM.OOZIE_LAUNCHER_ENV_PROPERTY).contains("ENV=env:$ENV PATH=/path2:$PATH"));
        assertTrue("env", launcherConf.get("env").contains("PATH=/path2:$PATH"));
        assertFalse("env", launcherConf.get("env").contains("ENV=env:$ENV"));
    }

    public void testLauncherConfigurationFiltering() {
        final Configuration sourceConf = SourceConfigurationFactory.createLauncherEntries();
        sourceConf.set("oozie.launcher." + HADOOP_USER, "should-not-be-present");
        final Configuration launcherConf = newConfigurationWithoutDefaults();

        try {
            new LauncherConfigurationInjector(sourceConf).inject(launcherConf);
            fail(String.format("configuration entry %s should be filtered out", HADOOP_USER));
        }
        catch (final ActionExecutorException e) {
            assertEquals("error code mismatch", "JA010", e.getErrorCode());
        }

    }

    private static class SourceConfigurationFactory {

        private static Configuration createOverridingAndLauncherEntries() {
            final Configuration sourceConf = newConfigurationWithDefaults();

            sourceConf.set("mapreduce.map.maxattempts", "4");
            sourceConf.set(LauncherAM.OOZIE_LAUNCHER_MAX_ATTEMPTS, "1");
            sourceConf.set("yarn.app.mapreduce.am.resource.mb", "2048");
            sourceConf.set(LauncherAM.OOZIE_LAUNCHER_MEMORY_MB_PROPERTY, "512");
            sourceConf.set("yarn.app.mapreduce.am.resource.cpu-vcores", "16");
            sourceConf.set(LauncherAM.OOZIE_LAUNCHER_VCORES_PROPERTY, "4");
            sourceConf.set("mapreduce.map.log.level", "TRACE");
            sourceConf.set(LauncherAM.OOZIE_LAUNCHER_LOG_LEVEL_PROPERTY, "DEBUG");
            sourceConf.set("yarn.app.mapreduce.am.command-opts", "-XX:MaxPermSize=256m");
            sourceConf.set(LauncherAM.OOZIE_LAUNCHER_JAVAOPTS_PROPERTY, "-XX:MaxPermSize=128m");
            sourceConf.set("yarn.app.mapreduce.am.env", "PATH=/path1:$PATH");
            sourceConf.set(LauncherAM.OOZIE_LAUNCHER_ENV_PROPERTY, "PATH=/path2:$PATH");
            sourceConf.set("mapreduce.job.priority", "1");
            sourceConf.set(LauncherAM.OOZIE_LAUNCHER_PRIORITY_PROPERTY, "2");
            sourceConf.set("mapreduce.job.queuename", "default1");
            sourceConf.set(LauncherAM.OOZIE_LAUNCHER_QUEUE_PROPERTY, "default2");

            return sourceConf;
        }

        private static Configuration createLauncherEntries() {
            final Configuration sourceConf = newConfigurationWithDefaults();

            sourceConf.set(LauncherAM.OOZIE_LAUNCHER_MAX_ATTEMPTS, "1");
            sourceConf.set(LauncherAM.OOZIE_LAUNCHER_MEMORY_MB_PROPERTY, "512");
            sourceConf.set(LauncherAM.OOZIE_LAUNCHER_VCORES_PROPERTY, "4");
            sourceConf.set(LauncherAM.OOZIE_LAUNCHER_LOG_LEVEL_PROPERTY, "DEBUG");
            sourceConf.set(LauncherAM.OOZIE_LAUNCHER_JAVAOPTS_PROPERTY, "-XX:MaxPermSize=128m");
            sourceConf.set(LauncherAM.OOZIE_LAUNCHER_ENV_PROPERTY, "PATH=/path2:$PATH");
            sourceConf.set(LauncherAM.OOZIE_LAUNCHER_PRIORITY_PROPERTY, "2");
            sourceConf.set(LauncherAM.OOZIE_LAUNCHER_QUEUE_PROPERTY, "default2");

            return sourceConf;
        }

        private static Configuration createOverridingEntries() {
            final Configuration sourceConf = newConfigurationWithDefaults();

            sourceConf.set("mapreduce.map.maxattempts", "4");
            sourceConf.set("yarn.app.mapreduce.am.resource.mb", "2048");
            sourceConf.set("yarn.app.mapreduce.am.resource.cpu-vcores", "16");
            sourceConf.set("mapreduce.map.log.level", "TRACE");
            sourceConf.set("yarn.app.mapreduce.am.command-opts", "-XX:MaxPermSize=256m");
            sourceConf.set("yarn.app.mapreduce.am.env", "PATH=/path1:$PATH");
            sourceConf.set("mapreduce.job.priority", "1");
            sourceConf.set("mapreduce.job.queuename", "default1");
            sourceConf.set("mapreduce.job.acl-view-job", "view");
            sourceConf.set("mapreduce.job.acl-modify-job", "modify");

            return sourceConf;
        }

        private static Configuration createMultipleOverridingEntries() {
            final Configuration sourceConf = newConfigurationWithDefaults();

            sourceConf.set("mapred.map.max.attempts", "5");
            sourceConf.set("mapreduce.map.maxattempts", "4");
            sourceConf.set("mapred.job.map.memory.mb", "2050");
            sourceConf.set("mapreduce.map.memory.mb", "2049");
            sourceConf.set("yarn.app.mapreduce.am.resource.mb", "2048");
            sourceConf.set("mapreduce.map.cpu.vcores", "17");
            sourceConf.set("yarn.app.mapreduce.am.resource.cpu-vcores", "16");
            sourceConf.set("mapred.map.child.log.level", "DEBUG");
            sourceConf.set("mapreduce.map.log.level", "TRACE");
            sourceConf.set("mapred.child.java.opts", "-XX:MaxPermSize=258m");
            sourceConf.set("mapreduce.map.java.opts", "-XX:MaxPermSize=257m");
            sourceConf.set("yarn.app.mapreduce.am.command-opts", "-XX:MaxPermSize=256m");
            sourceConf.set("mapred.child.env", "PATH=/path3:$PATH");
            sourceConf.set("mapreduce.map.env", "PATH=/path2:$PATH");
            sourceConf.set("yarn.app.mapreduce.am.env", "PATH=/path1:$PATH");
            sourceConf.set("mapred.job.priority", "2");
            sourceConf.set("mapreduce.job.priority", "1");
            sourceConf.set("mapred.job.queue.name", "default2");
            sourceConf.set("mapreduce.job.queuename", "default1");
            sourceConf.set("mapreduce.job.acl-view-job", "view");
            sourceConf.set("mapreduce.job.acl-modify-job", "modify");

            return sourceConf;
        }

        private static Configuration createPrependingAndLauncherEntries() {
            final Configuration sourceConf = newConfigurationWithDefaults();

            sourceConf.set("yarn.app.mapreduce.am.admin-command-opts", "-XX:+UseParNewGC");
            sourceConf.set(LauncherAM.OOZIE_LAUNCHER_JAVAOPTS_PROPERTY, "-XX:MaxPermSize=128m");
            sourceConf.set("yarn.app.mapreduce.am.admin.user.env", "ENV=env:$ENV");
            sourceConf.set(LauncherAM.OOZIE_LAUNCHER_ENV_PROPERTY, "PATH=/path2:$PATH");

            return sourceConf;
        }
    }
}