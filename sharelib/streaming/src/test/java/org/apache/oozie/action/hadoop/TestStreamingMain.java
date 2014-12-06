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
import org.apache.oozie.util.XConfiguration;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

public class TestStreamingMain extends MainTestCase {

    public Void call() throws Exception {
        FileSystem fs = getFileSystem();

        Path inputDir = new Path(getFsTestCaseDir(), "input");
        fs.mkdirs(inputDir);
        Writer writer = new OutputStreamWriter(fs.create(new Path(inputDir, "data.txt")));
        writer.write("hello");
        writer.close();

        Path outputDir = new Path(getFsTestCaseDir(), "output");

        XConfiguration jobConf = new XConfiguration();
        XConfiguration.copy(createJobConf(), jobConf);

        jobConf.setInt("mapred.map.tasks", 1);
        jobConf.setInt("mapred.map.max.attempts", 1);
        jobConf.setInt("mapred.reduce.max.attempts", 1);

        jobConf.set("user.name", getTestUser());
        jobConf.set("hadoop.job.ugi", getTestUser() + "," + getTestGroup());

        jobConf.set("mapreduce.job.tags", "" + System.currentTimeMillis());
        setSystemProperty("oozie.job.launch.time", "" + System.currentTimeMillis());

        SharelibUtils.addToDistributedCache("streaming", fs, getFsTestCaseDir(), jobConf);

        MapReduceActionExecutor.setStreaming(jobConf, "cat", "wc", null, null, null);

        jobConf.set("mapred.input.dir", inputDir.toString());
        jobConf.set("mapred.output.dir", outputDir.toString());

        File actionXml = new File(getTestCaseDir(), "action.xml");
        OutputStream os = new FileOutputStream(actionXml);
        jobConf.writeXml(os);
        os.close();

        File newId = new File(getTestCaseDir(), "newId");

        System.setProperty("oozie.action.conf.xml", actionXml.getAbsolutePath());
        System.setProperty("oozie.action.newId", newId.getAbsolutePath());

        StreamingMain.main(null);

        assertTrue(newId.exists());
        assertNotNull(LauncherMapper.getLocalFileContentStr(newId, "", -1));

        return null;
    }

}
