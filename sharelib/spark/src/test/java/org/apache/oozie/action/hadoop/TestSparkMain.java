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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

public class TestSparkMain extends MainTestCase {

    private static final String INPUT = "input.txt";
    private static final String OUTPUT = "output";

    @Override
    public Void call() throws Exception {
        XConfiguration jobConf = new XConfiguration();
        XConfiguration.copy(createJobConf(), jobConf);

        FileSystem fs = getFileSystem();
        Path file = new Path(getFsTestCaseDir(), "input.txt");
        Writer scriptWriter = new OutputStreamWriter(fs.create(file));
        scriptWriter.write("1,2,3");
        scriptWriter.write("\n");
        scriptWriter.write("2,3,4");
        scriptWriter.close();

        jobConf.set(JavaMain.JAVA_MAIN_CLASS, "org.apache.spark.deploy.SparkSubmit");

        jobConf.set("mapreduce.job.tags", "" + System.currentTimeMillis());
        setSystemProperty("oozie.job.launch.time", "" + System.currentTimeMillis());

        jobConf.set(SparkActionExecutor.SPARK_MASTER, "local[*]");
        jobConf.set(SparkActionExecutor.SPARK_MODE, "client");
        jobConf.set(SparkActionExecutor.SPARK_CLASS, "org.apache.oozie.example.SparkFileCopy");
        jobConf.set(SparkActionExecutor.SPARK_JOB_NAME, "Spark Copy File");
        jobConf.set(SparkActionExecutor.SPARK_OPTS, "--driver-memory 1024M");
        jobConf.set(SparkActionExecutor.SPARK_JAR, getFsTestCaseDir() + "/lib/test.jar");


        File actionXml = new File(getTestCaseDir(), "action.xml");
        OutputStream os = new FileOutputStream(actionXml);
        jobConf.writeXml(os);
        os.close();

        System.setProperty("oozie.action.conf.xml", actionXml.getAbsolutePath());

        File jarFile = IOUtils.createJar(new File(getTestCaseDir()), "test.jar", LauncherMainTester.class);
        InputStream is = new FileInputStream(jarFile);
        os = getFileSystem().create(new Path(getFsTestCaseDir(), "lib/test.jar"));
        IOUtils.copyStream(is, os);

        String input  = getFsTestCaseDir() + "/" + INPUT;
        String output = getFsTestCaseDir() + "/" + OUTPUT;
        String[] args = {input, output};
        SparkMain.main(args);
        assertTrue(getFileSystem().exists(new Path(getFsTestCaseDir() + "/" + OUTPUT)));
        return null;
    }
}
