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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.streaming.StreamJob;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.ClassUtils;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.Writer;
import java.io.OutputStreamWriter;
import org.apache.oozie.util.XConfiguration;

public class TestMapReduceActionExecutorStreaming extends TestMapReduceActionExecutor {

    @Override
    protected void setSystemProps() throws Exception {
        super.setSystemProps();
        setSystemProperty("oozie.service.ActionService.executor.classes", MapReduceActionExecutor.class.getName());
    }

    public void testStreaming() throws Exception {
        FileSystem fs = getFileSystem();
        Path streamingJar = new Path(getFsTestCaseDir(), "jar/hadoop-streaming.jar");

        InputStream is = new FileInputStream(ClassUtils.findContainingJar(StreamJob.class));
        OutputStream os = fs.create(new Path(getAppPath(), streamingJar));
        IOUtils.copyStream(is, os);

        Path inputDir = new Path(getFsTestCaseDir(), "input");
        Path outputDir = new Path(getFsTestCaseDir(), "output");

        Writer w = new OutputStreamWriter(fs.create(new Path(inputDir, "data.txt")));
        w.write("dummy\n");
        w.write("dummy\n");
        w.close();

        String actionXml = "<map-reduce>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>" + "      <streaming>" + "        <mapper>cat</mapper>"
                + "        <reducer>wc</reducer>" + "      </streaming>"
                + getStreamingConfig(inputDir.toString(), outputDir.toString()).toXmlString(false) + "<file>"
                + streamingJar + "</file>" + "</map-reduce>";
        _testSubmit("streaming", actionXml);
    }

    protected XConfiguration getStreamingConfig(String inputDir, String outputDir) {
        XConfiguration conf = new XConfiguration();
        conf.set("mapred.input.dir", inputDir);
        conf.set("mapred.output.dir", outputDir);
        return conf;
    }
}
