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

import org.apache.oozie.test.XTestCase;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
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
        Properties sparkConfigs = scs.getSparkConfig("foo");
        assertEquals(0, sparkConfigs.size());
    }

    public void testSparkConfigs() throws Exception {
        File sparkConf1Dir = createSparkConfsInDir("spark-conf-1", "a", "A", "b", "B", "spark.yarn.jar", "foo");
        File sparkConf3Dir = createSparkConfsInDir("spark-conf-3");
        File sparkConf4Dir = createSparkConfsInDir("spark-conf-4", "y", "Y", "z", "Z", "spark.yarn.jars", "foo2");

        SparkConfigurationService scs = Services.get().get(SparkConfigurationService.class);
        scs.destroy();
        ConfigurationService.set("oozie.service.SparkConfigurationService.spark.configurations",
                "rm1=" + sparkConf1Dir.getAbsolutePath() +   // absolute path
                ",rm2" +                                     // invalid entry
                ",rm3=" + sparkConf3Dir.getAbsolutePath() +  // missing file
                ",rm4=" + sparkConf4Dir.getName());          // relative path
        scs.init(Services.get());
        Properties sparkConfigs = scs.getSparkConfig("foo");
        assertEquals(sparkConfigs.toString(), 0, sparkConfigs.size());
        sparkConfigs = scs.getSparkConfig("rm1");
        assertEquals(sparkConfigs.toString(), 2, sparkConfigs.size());
        assertEquals("A", sparkConfigs.get("a"));
        assertEquals("B", sparkConfigs.get("b"));
        sparkConfigs = scs.getSparkConfig("rm2");
        assertEquals(sparkConfigs.toString(), 0, sparkConfigs.size());
        sparkConfigs = scs.getSparkConfig("rm3");
        assertEquals(sparkConfigs.toString(), 0, sparkConfigs.size());
        sparkConfigs = scs.getSparkConfig("rm4");
        assertEquals(sparkConfigs.toString(), 2, sparkConfigs.size());
        assertEquals("Y", sparkConfigs.get("y"));
        assertEquals("Z", sparkConfigs.get("z"));
        scs.destroy();
        // Setting this to false should make it not ignore spark.yarn.jar
        ConfigurationService.setBoolean("oozie.service.SparkConfigurationService.spark.configurations.ignore.spark.yarn.jar",
                false);
        ConfigurationService.set("oozie.service.SparkConfigurationService.spark.configurations.blacklist", " ");
        scs.init(Services.get());
        sparkConfigs = scs.getSparkConfig("rm1");
        assertEquals(sparkConfigs.toString(), 3, sparkConfigs.size());
        assertEquals("A", sparkConfigs.get("a"));
        assertEquals("B", sparkConfigs.get("b"));
        assertEquals("foo", sparkConfigs.get("spark.yarn.jar"));
        ConfigurationService.setBoolean(
                "oozie.service.SparkConfigurationService.spark.configurations.ignore.spark.yarn.jar", true);
        ConfigurationService.set(
                "oozie.service.SparkConfigurationService.spark.configurations.blacklist", "spark.yarn.jar,spark.yarn.jars");
        scs.destroy();
        ConfigurationService.set("oozie.service.SparkConfigurationService.spark.configurations",
                "rm1=" + sparkConf1Dir.getAbsolutePath() +   // define
                ",*=" + sparkConf4Dir.getAbsolutePath());    // wildcard
        scs.init(Services.get());
        sparkConfigs = scs.getSparkConfig("rm1");
        assertEquals(sparkConfigs.toString(), 2, sparkConfigs.size());
        assertEquals("A", sparkConfigs.get("a"));
        assertEquals("B", sparkConfigs.get("b"));
        sparkConfigs = scs.getSparkConfig("rm2");
        assertEquals(sparkConfigs.toString(), 2, sparkConfigs.size());
        assertEquals("Y", sparkConfigs.get("y"));
        assertEquals("Z", sparkConfigs.get("z"));
        sparkConfigs = scs.getSparkConfig("foo");
        assertEquals(sparkConfigs.toString(), 2, sparkConfigs.size());
        assertEquals("Y", sparkConfigs.get("y"));
        assertEquals("Z", sparkConfigs.get("z"));
    }

    private File createSparkConfsInDir(String directory, String... props) throws IOException {
        File sparkConf1Dir = new File(getTestCaseConfDir(), directory);
        sparkConf1Dir.mkdirs();
        File sparkConf1 = new File(sparkConf1Dir, "spark-defaults.conf");
        Properties sparkConf1Props = new Properties();
        for (int i = 0; i < props.length; i += 2) {
            sparkConf1Props.setProperty(props[i], props[i + 1]);
        }
        if (!sparkConf1Props.isEmpty()) {
            try (FileOutputStream fos = new FileOutputStream(sparkConf1)) {
                sparkConf1Props.store(fos, "");
            }
        }
        return sparkConf1Dir;
    }

    public void testBlackList() throws Exception {
        File sparkConf1Dir = createSparkConfsInDir("spark-conf-1", "a", "A", "b", "B",
                "spark.yarn.jar", "foo");
        File sparkConf3Dir = createSparkConfsInDir("spark-conf-3");
        File sparkConf4Dir = createSparkConfsInDir("spark-conf-4", "y", "Y", "z", "Z",
                "spark.yarn.jars", "foo2");

        SparkConfigurationService scs = Services.get().get(SparkConfigurationService.class);
        scs.destroy();
        ConfigurationService.set("oozie.service.SparkConfigurationService.spark.configurations",
                "rm1=" + sparkConf1Dir.getAbsolutePath() +   // absolute path
                ",rm2" +                                     // invalid entry
                ",rm3=" + sparkConf3Dir.getAbsolutePath() +  // missing file
                ",rm4=" + sparkConf4Dir.getName());          // relative path
        ConfigurationService.setBoolean("oozie.service.SparkConfigurationService.spark.configurations.ignore.spark.yarn.jar",
                false);
        ConfigurationService.set("oozie.service.SparkConfigurationService.spark.configurations.blacklist", "a,z");
        scs.init(Services.get());
        Properties sparkConfigs = scs.getSparkConfig("foo");
        assertEquals(sparkConfigs.toString(), 0, sparkConfigs.size());
        sparkConfigs = scs.getSparkConfig("rm1");
        assertEquals(sparkConfigs.toString(), 2, sparkConfigs.size());
        assertEquals("B", sparkConfigs.get("b"));
        assertEquals("foo", sparkConfigs.get("spark.yarn.jar"));
        sparkConfigs = scs.getSparkConfig("rm2");
        assertEquals(sparkConfigs.toString(), 0, sparkConfigs.size());
        sparkConfigs = scs.getSparkConfig("rm3");
        assertEquals(sparkConfigs.toString(), 0, sparkConfigs.size());
        sparkConfigs = scs.getSparkConfig("rm4");
        assertEquals(sparkConfigs.toString(), 2, sparkConfigs.size());
        assertEquals("Y", sparkConfigs.get("y"));
        assertEquals("foo2", sparkConfigs.get("spark.yarn.jars"));
        scs.destroy();
    }


}
