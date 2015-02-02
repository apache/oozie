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
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.oozie.action.hadoop.DistcpMain;

public class TestDistcpMain extends MainTestCase {

    @Override
    public Void call() throws Exception {

        XConfiguration jobConf = new XConfiguration();
        XConfiguration.copy(createJobConf(), jobConf);

        FileSystem fs = getFileSystem();
        Path inputDir = new Path(getFsTestCaseDir(), "input");
        fs.mkdirs(inputDir);
        Writer writer = new OutputStreamWriter(fs.create(new Path(inputDir, "data.txt")));
        writer.write("hello");
        writer.close();
        Path outputDir = new Path(getFsTestCaseDir(), "output");

        jobConf.set(LauncherMapper.CONF_OOZIE_ACTION_MAIN_CLASS, "org.apache.hadoop.tools.DistCp");

        jobConf.set("mapreduce.job.tags", "" + System.currentTimeMillis());
        setSystemProperty("oozie.job.launch.time", "" + System.currentTimeMillis());

        File actionXml = new File(getTestCaseDir(), "action.xml");
        OutputStream os = new FileOutputStream(actionXml);
        jobConf.writeXml(os);
        os.close();

        System.setProperty("oozie.action.conf.xml", actionXml.getAbsolutePath());

        // Check normal execution
        DistcpMain.main(new String[]{inputDir.toString(), outputDir.toString()});
        assertTrue(getFileSystem().exists(outputDir));
        fs.delete(outputDir,true);

        // Check exception handling
        try {
            DistcpMain.main(new String[0]);
        } catch(RuntimeException re) {
            assertTrue(re.getMessage().indexOf("Returned value from distcp is non-zero") != -1);
        }

        // test -D option
        jobConf.set("mapred.job.queue.name", "non-exist");
        fs.delete(new Path(getTestCaseDir(), "action.xml"), true);
        os = new FileOutputStream(actionXml);
        jobConf.writeXml(os);

        assertFalse(getFileSystem().exists(outputDir));
        String option = "-Dmapred.job.queue.name=default"; // overwrite queue setting
        DistcpMain.main(new String[] { option, inputDir.toString(), outputDir.toString() });
        assertTrue(getFileSystem().exists(outputDir));

        return null;
    }
}
