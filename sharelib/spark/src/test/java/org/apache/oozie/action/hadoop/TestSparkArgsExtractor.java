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

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestSparkArgsExtractor {

    @Test
    public void testAppendOoziePropertiesToSparkConf() throws Exception {
        final List<String> sparkArgs = new ArrayList<>();
        final Configuration actionConf = new Configuration();
        actionConf.set("foo", "foo-not-to-include");
        actionConf.set("oozie.launcher", "launcher-not-to-include");
        actionConf.set("oozie.spark", "spark-not-to-include");
        actionConf.set("oozie.bar", "bar");

        new SparkArgsExtractor(actionConf).appendOoziePropertiesToSparkConf(sparkArgs);

        assertEquals(Lists.newArrayList("--conf", "spark.oozie.bar=bar"), sparkArgs);
    }

    @Test
    public void testLocalClientArgsParsing() throws Exception {
        final Configuration actionConf = new Configuration();
        actionConf.set(SparkActionExecutor.SPARK_MASTER, "local[*]");
        actionConf.set(SparkActionExecutor.SPARK_MODE, "client");
        actionConf.set(SparkActionExecutor.SPARK_CLASS, "org.apache.oozie.example.SparkFileCopy");
        actionConf.set(SparkActionExecutor.SPARK_JOB_NAME, "Spark Copy File");
        actionConf.set(SparkActionExecutor.SPARK_OPTS, "--driver-memory  1042M " +
                "--conf spark.executor.extraClassPath=aaa " +
                "--conf user.property.after.spark.executor.extraClassPath=bbb " +
                "--conf spark.driver.extraClassPath=ccc " +
                "--conf user.property.after.spark.driver.extraClassPath=ddd " +
                "--conf spark.executor.extraJavaOptions=\"-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp\"");
        actionConf.set(SparkActionExecutor.SPARK_JAR, "/lib/test.jar");

        final String[] mainArgs = {"arg0", "arg1"};
        final List<String> sparkArgs = new SparkArgsExtractor(actionConf).extract(mainArgs);

        assertEquals("Spark args mismatch",
                Lists.newArrayList("--master", "local[*]",
                        "--deploy-mode", "client",
                        "--name", "Spark Copy File",
                        "--class", "org.apache.oozie.example.SparkFileCopy",
                        "--driver-memory", "1042M",
                        "--conf", "spark.executor.extraClassPath=aaa",
                        "--conf", "user.property.after.spark.executor.extraClassPath=bbb",
                        "--conf", "spark.driver.extraClassPath=ccc",
                        "--conf", "user.property.after.spark.driver.extraClassPath=ddd",
                        "--conf", "spark.executor.extraJavaOptions=-XX:+HeapDumpOnOutOfMemoryError " +
                                "-XX:HeapDumpPath=/tmp -Dlog4j.configuration=spark-log4j.properties",
                        "--conf", "spark.yarn.security.tokens.hadoopfs.enabled=false",
                        "--conf", "spark.yarn.security.tokens.hive.enabled=false",
                        "--conf", "spark.yarn.security.tokens.hbase.enabled=false",
                        "--conf", "spark.yarn.security.credentials.hadoopfs.enabled=false",
                        "--conf", "spark.yarn.security.credentials.hive.enabled=false",
                        "--conf", "spark.yarn.security.credentials.hbase.enabled=false",
                        "--conf", "spark.driver.extraJavaOptions=-Dlog4j.configuration=spark-log4j.properties",
                        "--verbose",
                        "/lib/test.jar",
                        "arg0",
                        "arg1"),
                sparkArgs);
    }

    @Test
    public void testYarnClientExecutorAndDriverExtraClasspathsArgsParsing() throws Exception {
        final Configuration actionConf = new Configuration();
        actionConf.set(SparkActionExecutor.SPARK_MASTER, "yarn");
        actionConf.set(SparkActionExecutor.SPARK_MODE, "client");
        actionConf.set(SparkActionExecutor.SPARK_CLASS, "org.apache.oozie.example.SparkFileCopy");
        actionConf.set(SparkActionExecutor.SPARK_JOB_NAME, "Spark Copy File");
        actionConf.set(SparkActionExecutor.SPARK_OPTS, "--driver-memory  1042M " +
                "--conf spark.executor.extraClassPath=aaa " +
                "--conf user.property.after.spark.executor.extraClassPath=bbb " +
                "--conf spark.driver.extraClassPath=ccc " +
                "--conf user.property.after.spark.driver.extraClassPath=ddd " +
                "--conf spark.executor.extraJavaOptions=\"-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp\"");
        actionConf.set(SparkActionExecutor.SPARK_JAR, "/lib/test.jar");

        final String[] mainArgs = {"arg0", "arg1"};
        final List<String> sparkArgs = new SparkArgsExtractor(actionConf).extract(mainArgs);

        assertEquals("Spark args mismatch",
                Lists.newArrayList("--master", "yarn",
                        "--deploy-mode", "client",
                        "--name", "Spark Copy File",
                        "--class", "org.apache.oozie.example.SparkFileCopy",
                        "--driver-memory", "1042M",
                        "--conf", "user.property.after.spark.executor.extraClassPath=bbb",
                        "--conf", "user.property.after.spark.driver.extraClassPath=ddd",
                        "--conf", "spark.executor.extraJavaOptions=-XX:+HeapDumpOnOutOfMemoryError " +
                                "-XX:HeapDumpPath=/tmp -Dlog4j.configuration=spark-log4j.properties",
                        "--conf", "spark.executor.extraClassPath=aaa:$PWD/*",
                        "--conf", "spark.driver.extraClassPath=ccc:$PWD/*",
                        "--conf", "spark.yarn.security.tokens.hadoopfs.enabled=false",
                        "--conf", "spark.yarn.security.tokens.hive.enabled=false",
                        "--conf", "spark.yarn.security.tokens.hbase.enabled=false",
                        "--conf", "spark.yarn.security.credentials.hadoopfs.enabled=false",
                        "--conf", "spark.yarn.security.credentials.hive.enabled=false",
                        "--conf", "spark.yarn.security.credentials.hbase.enabled=false",
                        "--conf", "spark.driver.extraJavaOptions=-Dlog4j.configuration=spark-log4j.properties",
                        "--files", "spark-log4j.properties,hive-site.xml",
                        "--conf", "spark.yarn.jar=null",
                        "--verbose",
                        "/lib/test.jar",
                        "arg0",
                        "arg1"),
                sparkArgs);
    }

    @Test
    public void testYarnClientFilesAndArchivesArgsParsing() throws Exception {
        final Configuration actionConf = new Configuration();
        actionConf.set(SparkActionExecutor.SPARK_MASTER, "yarn");
        actionConf.set(SparkActionExecutor.SPARK_MODE, "client");
        actionConf.set(SparkActionExecutor.SPARK_CLASS, "org.apache.oozie.example.SparkFileCopy");
        actionConf.set(SparkActionExecutor.SPARK_JOB_NAME, "Spark Copy File");
        actionConf.set(SparkActionExecutor.SPARK_OPTS, "--files aaa " +
                "--archives bbb " +
                "--files=ccc " +
                "--archives=ddd");
        actionConf.set(SparkActionExecutor.SPARK_JAR, "/lib/test.jar");

        final String[] mainArgs = {"arg0", "arg1"};
        final List<String> sparkArgs = new SparkArgsExtractor(actionConf).extract(mainArgs);

        assertEquals("Spark args mismatch",
                Lists.newArrayList("--master", "yarn",
                        "--deploy-mode", "client",
                        "--name", "Spark Copy File",
                        "--class", "org.apache.oozie.example.SparkFileCopy",
                        "--conf", "spark.executor.extraClassPath=$PWD/*",
                        "--conf", "spark.driver.extraClassPath=$PWD/*",
                        "--conf", "spark.yarn.security.tokens.hadoopfs.enabled=false",
                        "--conf", "spark.yarn.security.tokens.hive.enabled=false",
                        "--conf", "spark.yarn.security.tokens.hbase.enabled=false",
                        "--conf", "spark.yarn.security.credentials.hadoopfs.enabled=false",
                        "--conf", "spark.yarn.security.credentials.hive.enabled=false",
                        "--conf", "spark.yarn.security.credentials.hbase.enabled=false",
                        "--conf", "spark.executor.extraJavaOptions=-Dlog4j.configuration=spark-log4j.properties",
                        "--conf", "spark.driver.extraJavaOptions=-Dlog4j.configuration=spark-log4j.properties",
                        "--files", "spark-log4j.properties,hive-site.xml,aaa,ccc",
                        "--archives", "bbb,ddd",
                        "--conf", "spark.yarn.jar=null",
                        "--verbose",
                        "/lib/test.jar",
                        "arg0",
                        "arg1"),
                sparkArgs);
    }

    @Test
    public void testDriverClassPathArgsParsing() throws Exception {
        final Configuration actionConf = new Configuration();
        actionConf.set(SparkActionExecutor.SPARK_MASTER, "yarn");
        actionConf.set(SparkActionExecutor.SPARK_MODE, "client");
        actionConf.set(SparkActionExecutor.SPARK_CLASS, "org.apache.oozie.example.SparkFileCopy");
        actionConf.set(SparkActionExecutor.SPARK_JOB_NAME, "Spark Copy File");
        actionConf.set(SparkActionExecutor.SPARK_OPTS, "--driver-class-path aaa");
        actionConf.set(SparkActionExecutor.SPARK_JAR, "/lib/test.jar");

        final String[] mainArgs = {"arg0", "arg1"};
        final List<String> sparkArgs = new SparkArgsExtractor(actionConf).extract(mainArgs);

        assertEquals("Spark args mismatch",
                Lists.newArrayList("--master", "yarn",
                        "--deploy-mode", "client",
                        "--name", "Spark Copy File",
                        "--class", "org.apache.oozie.example.SparkFileCopy",
                        "--conf", "spark.executor.extraClassPath=$PWD/*",
                        "--conf", "spark.driver.extraClassPath=aaa:$PWD/*",
                        "--conf", "spark.yarn.security.tokens.hadoopfs.enabled=false",
                        "--conf", "spark.yarn.security.tokens.hive.enabled=false",
                        "--conf", "spark.yarn.security.tokens.hbase.enabled=false",
                        "--conf", "spark.yarn.security.credentials.hadoopfs.enabled=false",
                        "--conf", "spark.yarn.security.credentials.hive.enabled=false",
                        "--conf", "spark.yarn.security.credentials.hbase.enabled=false",
                        "--conf", "spark.executor.extraJavaOptions=-Dlog4j.configuration=spark-log4j.properties",
                        "--conf", "spark.driver.extraJavaOptions=-Dlog4j.configuration=spark-log4j.properties",
                        "--files", "spark-log4j.properties,hive-site.xml",
                        "--conf", "spark.yarn.jar=null",
                        "--verbose",
                        "/lib/test.jar",
                        "arg0",
                        "arg1"),
                sparkArgs);
    }

    @Test
    public void testQuotedDriverAndExecutorExtraJavaOptionsParsing() throws Exception {
        final Configuration actionConf = new Configuration();
        actionConf.set(SparkActionExecutor.SPARK_MASTER, "yarn");
        actionConf.set(SparkActionExecutor.SPARK_MODE, "client");
        actionConf.set(SparkActionExecutor.SPARK_CLASS, "org.apache.oozie.example.SparkFileCopy");
        actionConf.set(SparkActionExecutor.SPARK_JOB_NAME, "Spark Copy File");
        actionConf.set(SparkActionExecutor.SPARK_OPTS,
                "--conf spark.executor.extraJavaOptions=\"-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp\"" +
                "--conf spark.driver.extraJavaOptions=\"-Xmn2703m -XX:SurvivorRatio=2 -XX:ParallelGCThreads=20\"");
        actionConf.set(SparkActionExecutor.SPARK_JAR, "/lib/test.jar");

        final String[] mainArgs = {"arg0", "arg1"};
        final List<String> sparkArgs = new SparkArgsExtractor(actionConf).extract(mainArgs);

        assertEquals("Spark args mismatch",
                Lists.newArrayList("--master", "yarn",
                        "--deploy-mode", "client",
                        "--name", "Spark Copy File",
                        "--class", "org.apache.oozie.example.SparkFileCopy",
                        "--conf", "spark.executor.extraJavaOptions=-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp " +
                                "-Dlog4j.configuration=spark-log4j.properties",
                        "--conf", "spark.driver.extraJavaOptions=-Xmn2703m -XX:SurvivorRatio=2 -XX:ParallelGCThreads=20 " +
                                "-Dlog4j.configuration=spark-log4j.properties",
                        "--conf", "spark.executor.extraClassPath=$PWD/*",
                        "--conf", "spark.driver.extraClassPath=$PWD/*",
                        "--conf", "spark.yarn.security.tokens.hadoopfs.enabled=false",
                        "--conf", "spark.yarn.security.tokens.hive.enabled=false",
                        "--conf", "spark.yarn.security.tokens.hbase.enabled=false",
                        "--conf", "spark.yarn.security.credentials.hadoopfs.enabled=false",
                        "--conf", "spark.yarn.security.credentials.hive.enabled=false",
                        "--conf", "spark.yarn.security.credentials.hbase.enabled=false",
                        "--files", "spark-log4j.properties,hive-site.xml",
                        "--conf", "spark.yarn.jar=null",
                        "--verbose",
                        "/lib/test.jar",
                        "arg0",
                        "arg1"),
                sparkArgs);
    }
}