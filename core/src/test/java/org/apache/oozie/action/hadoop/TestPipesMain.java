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
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Properties;
import java.net.URI;

public class TestPipesMain extends MainTestCase {

    public Void call() throws Exception {

        Path programPath = new Path(getFsTestCaseDir(), "wordcount-simple");

        FileSystem fs = getFileSystem();

        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("wordcount-simple");
        if (is != null) {
            OutputStream os = fs.create(programPath);
            IOUtils.copyStream(is, os);

            Path inputDir = new Path(getFsTestCaseDir(), "input");
            fs.mkdirs(inputDir);
            Writer writer = new OutputStreamWriter(fs.create(new Path(inputDir, "data.txt")));
            writer.write("hello");
            writer.close();

            Path outputDir = new Path(getFsTestCaseDir(), "output");


            XConfiguration jobConf = new XConfiguration();
            XConfiguration.copy(createJobConf(), jobConf);

            jobConf.set("user.name", getTestUser());

            jobConf.setInt("mapred.map.tasks", 1);
            jobConf.setInt("mapred.map.max.attempts", 1);
            jobConf.setInt("mapred.reduce.max.attempts", 1);

            jobConf.set("mapred.input.dir", inputDir.toString());
            jobConf.set("mapred.output.dir", outputDir.toString());

            jobConf.set("oozie.pipes.program", programPath.toUri().getPath());
            jobConf.setBoolean("hadoop.pipes.java.recordreader", true);


            DistributedCache.addCacheFile(new URI(programPath.toUri().getPath()), fs.getConf());

            File actionXml = new File(getTestCaseDir(), "action.xml");
            os = new FileOutputStream(actionXml);
            jobConf.writeXml(os);
            os.close();

            File newIdProperties = new File(getTestCaseDir(), "newId.properties");

            System.setProperty("oozie.action.conf.xml", actionXml.getAbsolutePath());
            System.setProperty("oozie.action.newId.properties", newIdProperties.getAbsolutePath());

            String[] args = {};

            PipesMain.main(args);

            assertTrue(newIdProperties.exists());

            is = new FileInputStream(newIdProperties);
            Properties props = new Properties();
            props.load(is);
            is.close();

            assertTrue(props.containsKey("id"));
        }
        else {
            System.out.println(
                "SKIPPING TEST: TestPipesMain, binary 'wordcount-simple' not available in the classpath");
        }
        return null;
    }

}
