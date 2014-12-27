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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

public class StreamingMain extends MapReduceMain {

    public static void main(String[] args) throws Exception {
        run(StreamingMain.class, args);
    }

    @Override
    protected RunningJob submitJob(JobConf jobConf) throws Exception {
        jobConf.set("mapred.mapper.class", "org.apache.hadoop.streaming.PipeMapper");
        jobConf.set("mapred.reducer.class", "org.apache.hadoop.streaming.PipeReducer");
        jobConf.set("mapred.map.runner.class", "org.apache.hadoop.streaming.PipeMapRunner");

        jobConf.set("mapred.input.format.class", "org.apache.hadoop.mapred.TextInputFormat");
        jobConf.set("mapred.output.format.class", "org.apache.hadoop.mapred.TextOutputFormat");
        jobConf.set("mapred.output.value.class", "org.apache.hadoop.io.Text");
        jobConf.set("mapred.output.key.class", "org.apache.hadoop.io.Text");

        jobConf.set("mapred.create.symlink", "yes");
        jobConf.set("mapred.used.genericoptionsparser", "true");

        jobConf.set("stream.addenvironment", "");

        String value = jobConf.get("oozie.streaming.mapper");
        if (value != null) {
            jobConf.set("stream.map.streamprocessor", value);
        }
        value = jobConf.get("oozie.streaming.reducer");
        if (value != null) {
            jobConf.set("stream.reduce.streamprocessor", value);
        }
        value = jobConf.get("oozie.streaming.record-reader");
        if (value != null) {
            jobConf.set("stream.recordreader.class", value);
        }
        String[] values = getStrings(jobConf, "oozie.streaming.record-reader-mapping");
        for (String s : values) {
            String[] kv = s.split("=");
            jobConf.set("stream.recordreader." + kv[0], kv[1]);
        }
        values = getStrings(jobConf, "oozie.streaming.env");
        value = jobConf.get("stream.addenvironment", "");
        if (value.length() > 0) {
            value = value + " ";
        }
        for (String s : values) {
            value = value + s + " ";
        }
        jobConf.set("stream.addenvironment", value);

        // propagate delegation related props from launcher job to MR job
        if (getFilePathFromEnv("HADOOP_TOKEN_FILE_LOCATION") != null) {
            jobConf.set("mapreduce.job.credentials.binary", getFilePathFromEnv("HADOOP_TOKEN_FILE_LOCATION"));
        }

        JobClient jobClient = null;
        RunningJob runJob = null;
        boolean exception = false;
        try {
            jobClient = createJobClient(jobConf);
            runJob = jobClient.submitJob(jobConf);
        }
        catch (Exception ex) {
            exception = true;
            throw ex;
        }
        finally {
            try {
                if (jobClient != null) {
                    jobClient.close();
                }
            }
            catch (Exception ex) {
                if (exception) {
                    System.out.println("JobClient Error: " + ex);
                }
                else {
                    throw ex;
                }
            }
        }
        return runJob;
    }
}
