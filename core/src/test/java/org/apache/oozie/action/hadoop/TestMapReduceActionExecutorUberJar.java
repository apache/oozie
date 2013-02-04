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

import org.apache.oozie.service.Services;

public class TestMapReduceActionExecutorUberJar extends TestMapReduceActionExecutor {

    // This test requires at least Hadoop 2.2.x or 1.2.x or it will fail so it is excluded from the default tests in the pom.xml
    public void testMapReduceWithUberJarEnabled() throws Exception {
        Services serv = Services.get();
        boolean originalUberJarDisabled = serv.getConf().getBoolean("oozie.action.mapreduce.uber.jar.enable", false);
        try {
            serv.getConf().setBoolean("oozie.action.mapreduce.uber.jar.enable", true);
            _testMapReduceWithUberJar();
        } catch (Exception e) {
            throw e;
        } finally {
            serv.getConf().setBoolean("oozie.action.mapreduce.uber.jar.enable", originalUberJarDisabled);
        }
    }

    @Override
    public void testDefaultShareLibName() {
        // skip test
    }

    @Override
    public void testMapReduce() throws Exception {
        // skip test
    }

    @Override
    public void testLauncherJar() throws Exception {
        // skip test
    }

    @Override
    public void testMapReduceWithCredentials() throws Exception {
        // skip test
    }

    @Override
    public void testMapReduceWithUberJarDisabled() throws Exception {
        // skip test
    }

    @Override
    public void testSetExecutionStats_when_user_has_specified_stats_write_FALSE() throws Exception {
        // skip test
    }

    @Override
    public void testSetExecutionStats_when_user_has_specified_stats_write_TRUE() throws Exception {
        // skip test
    }

    @Override
    public void testSetupMethods() throws Exception {
        // skip test
    }

    @Override
    public void testStreaming() throws Exception {
        // skip test
    }

    @Override
    public void testPipes() throws Exception {
        // skip test
    }
}