/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.action.hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.streaming.StreamJob;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.ClassUtils;
import org.apache.oozie.util.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Properties;

public class TestStreamingMain extends MainTestCase {

    public Void call() throws Exception {
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
        injectKerberosInfo(jobConf);
        jobConf.set("user.name", getTestUser());
        jobConf.set("hadoop.job.ugi", getTestUser() + "," + getTestGroup());

        DistributedCache.addFileToClassPath(new Path(streamingJar.toUri().getPath()), fs.getConf());

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
        return null;
    }

}
