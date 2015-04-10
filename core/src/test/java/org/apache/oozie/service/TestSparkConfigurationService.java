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

package org.apache.oozie.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Map;
import java.util.Properties;

public class TestSparkConfigurationService extends XTestCase {

    protected void setUp() throws Exception {
        super.setUp();
        Services services = new Services();
        services.init();
    }

    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    public void testSparkConfigsEmpty() throws Exception {
        SparkConfigurationService scs = Services.get().get(SparkConfigurationService.class);
        scs.destroy();
        ConfigurationService.set("oozie.service.SparkConfigurationService.spark.configurations", "");
        scs.init(Services.get());
        Map<String, String> sparkConfigs = scs.getSparkConfig("foo");
        assertEquals(0, sparkConfigs.size());
    }

    public void testSparkConfigs() throws Exception {
        File sparkConf1Dir = new File(getTestCaseConfDir(), "spark-conf-1");
        File sparkConf3Dir = new File(getTestCaseConfDir(), "spark-conf-3");
        File sparkConf4Dir = new File(getTestCaseConfDir(), "spark-conf-4");
        sparkConf1Dir.mkdirs();
        sparkConf3Dir.mkdirs();
        sparkConf4Dir.mkdirs();
        File sparkConf1 = new File(sparkConf1Dir, "spark-defaults.conf");
        Properties sparkConf1Props = new Properties();
        sparkConf1Props.setProperty("a", "A");
        sparkConf1Props.setProperty("b", "B");
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(sparkConf1);
            sparkConf1Props.store(fos, "");
        } finally {
            IOUtils.closeSafely(fos);
        }
        File sparkConf4 = new File(sparkConf4Dir, "spark-defaults.conf");
        Properties sparkConf4Props = new Properties();
        sparkConf4Props.setProperty("y", "Y");
        sparkConf4Props.setProperty("z", "Z");
        fos = null;
        try {
            fos = new FileOutputStream(sparkConf4);
            sparkConf4Props.store(fos, "");
        } finally {
            IOUtils.closeSafely(fos);
        }

        SparkConfigurationService scs = Services.get().get(SparkConfigurationService.class);
        scs.destroy();
        ConfigurationService.set("oozie.service.SparkConfigurationService.spark.configurations",
                "rm1=" + sparkConf1Dir.getAbsolutePath() +   // absolute path
                ",rm2" +                                     // invalid entry
                ",rm3=" + sparkConf3Dir.getAbsolutePath() +  // missing file
                ",rm4=" + sparkConf4Dir.getName());          // relative path
        scs.init(Services.get());
        Map<String, String> sparkConfigs = scs.getSparkConfig("foo");
        assertEquals(0, sparkConfigs.size());
        sparkConfigs = scs.getSparkConfig("rm1");
        assertEquals(2, sparkConfigs.size());
        assertEquals("A", sparkConfigs.get("a"));
        assertEquals("B", sparkConfigs.get("b"));
        sparkConfigs = scs.getSparkConfig("rm2");
        assertEquals(0, sparkConfigs.size());
        sparkConfigs = scs.getSparkConfig("rm3");
        assertEquals(0, sparkConfigs.size());
        sparkConfigs = scs.getSparkConfig("rm4");
        assertEquals(2, sparkConfigs.size());
        assertEquals("Y", sparkConfigs.get("y"));
        assertEquals("Z", sparkConfigs.get("z"));

        scs.destroy();
        ConfigurationService.set("oozie.service.SparkConfigurationService.spark.configurations",
                "rm1=" + sparkConf1Dir.getAbsolutePath() +   // defined
                ",*=" + sparkConf4Dir.getAbsolutePath());    // wildcard
        scs.init(Services.get());
        sparkConfigs = scs.getSparkConfig("rm1");
        assertEquals(2, sparkConfigs.size());
        assertEquals("A", sparkConfigs.get("a"));
        assertEquals("B", sparkConfigs.get("b"));
        sparkConfigs = scs.getSparkConfig("rm2");
        assertEquals(2, sparkConfigs.size());
        assertEquals("Y", sparkConfigs.get("y"));
        assertEquals("Z", sparkConfigs.get("z"));
        sparkConfigs = scs.getSparkConfig("foo");
        assertEquals(2, sparkConfigs.size());
        assertEquals("Y", sparkConfigs.get("y"));
        assertEquals("Z", sparkConfigs.get("z"));
    }
}
