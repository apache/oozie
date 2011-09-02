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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.streaming.StreamJob;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.ClassUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.HadoopAccessor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Properties;

public class TestStreamingMain extends XFsTestCase {

    public void testMain() throws Exception {
        FileSystem fs = getFileSystem();

        Path streamingJar = new Path(getFsTestCaseDir(), "hadoop-streaming.jar");
        InputStream is = new FileInputStream(ClassUtils.findContainingJar(StreamJob.class));
        OutputStream os = fs.create(streamingJar);
        IOUtils.copyStream(is, os);

        Path inputDir = new Path(getFsTestCaseDir(), "input");
        fs.mkdirs(inputDir);
        Writer writer = new OutputStreamWriter(fs.create(new Path(inputDir, "data.txt")));
        writer.write("hello");
        writer.close();

        Path outputDir = new Path(getFsTestCaseDir(), "output");

        XConfiguration jobConf = new XConfiguration();

        jobConf.setInt("mapred.map.tasks", 1);
        jobConf.setInt("mapred.map.max.attempts", 1);
        jobConf.setInt("mapred.reduce.max.attempts", 1);
        jobConf.set("mapred.job.tracker", getJobTrackerUri());
        jobConf.set("fs.default.name", getNameNodeUri());

        jobConf.set("oozie.hadoop.accessor.class", HadoopAccessor.class.getName());
        jobConf.set("user.name", System.getProperty("user.name"));
        jobConf.set("hadoop.job.ugi", System.getProperty("user.name") + ",others");

        DistributedCache.addFileToClassPath(new Path(streamingJar.toUri().getPath()), jobConf);
        
        StreamingMain.setStreaming(jobConf, "cat", "wc", null, null, null);

        jobConf.set("mapred.input.dir", inputDir.toString());
        jobConf.set("mapred.output.dir", outputDir.toString());

        File actionXml = new File(getTestCaseDir(), "action.xml");
        os = new FileOutputStream(actionXml);
        jobConf.writeXml(os);
        os.close();

        File newIdProperties = new File(getTestCaseDir(), "newId.properties");

        System.setProperty("oozie.action.conf.xml", actionXml.getAbsolutePath());
        System.setProperty("oozie.action.newId.properties", newIdProperties.getAbsolutePath());

        StreamingMain.main(null);

        assertTrue(newIdProperties.exists());

        is = new FileInputStream(newIdProperties);
        Properties props = new Properties();
        props.load(is);
        is.close();

        assertTrue(props.containsKey("id"));
    }

}
