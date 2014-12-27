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
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.pipes.Submitter;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;

public class PipesMain extends MapReduceMain {

    public static void main(String[] args) throws Exception {
        run(PipesMain.class, args);
    }

    @Override
    protected RunningJob submitJob(JobConf jobConf) throws Exception {
        String value = jobConf.get("oozie.pipes.map");
        if (value != null) {
            jobConf.setBoolean("hadoop.pipes.java.mapper", true);
            jobConf.set("mapred.mapper.class", value);
        }
        value = jobConf.get("oozie.pipes.reduce");
        if (value != null) {
            jobConf.setBoolean("hadoop.pipes.java.reducer", true);
            jobConf.set("mapred.reducer.class", value);
        }
        value = jobConf.get("oozie.pipes.inputformat");
        if (value != null) {
            jobConf.setBoolean("hadoop.pipes.java.recordreader", true);
            jobConf.set("mapred.input.format.class", value);
        }
        value = jobConf.get("oozie.pipes.partitioner");
        if (value != null) {
            jobConf.set("mapred.partitioner.class", value);
        }
        value = jobConf.get("oozie.pipes.writer");
        if (value != null) {
            jobConf.setBoolean("hadoop.pipes.java.recordwriter", true);
            jobConf.set("mapred.output.format.class", value);
        }
        value = jobConf.get("oozie.pipes.program");
        if (value != null) {
            jobConf.set("hadoop.pipes.executable", value);
            if (value.contains("#")) {
                DistributedCache.createSymlink(jobConf);
            }
        }

        //propagate delegation related props from launcher job to MR job
        if (getFilePathFromEnv("HADOOP_TOKEN_FILE_LOCATION") != null) {
            jobConf.set("mapreduce.job.credentials.binary", getFilePathFromEnv("HADOOP_TOKEN_FILE_LOCATION"));
        }

        return Submitter.jobSubmit(jobConf);
    }

    public static void setPipes(Configuration conf, String map, String reduce, String inputFormat, String partitioner,
                                String writer, String program, Path appPath) {
        if (map != null) {
            conf.set("oozie.pipes.map", map);
        }
        if (reduce != null) {
            conf.set("oozie.pipes.reduce", reduce);
        }
        if (inputFormat != null) {
            conf.set("oozie.pipes.inputformat", inputFormat);
        }
        if (partitioner != null) {
            conf.set("oozie.pipes.partitioner", partitioner);
        }
        if (writer != null) {
            conf.set("oozie.pipes.writer", writer);
        }
        if (program != null) {
            Path path = null;
            if (!program.startsWith("/")) {
                path = new Path(appPath, program);
                program = path.toString();
            }
            conf.set("oozie.pipes.program", program);

        }
    }

}
