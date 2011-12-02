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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.File;

public class MapReduceMain extends LauncherMain {

    public static void main(String[] args) throws Exception {
        run(MapReduceMain.class, args);
    }

    protected void run(String[] args) throws Exception {
        System.out.println();
        System.out.println("Oozie Map-Reduce action configuration");
        System.out.println("=======================");

        // loading action conf prepared by Oozie
        Configuration actionConf = new Configuration(false);
        actionConf.addResource(new Path("file:///", System.getProperty("oozie.action.conf.xml")));

        logMasking("Map-Reduce job configuration:", new HashSet<String>(), actionConf);

        System.out.println("Submitting Oozie action Map-Reduce job");
        System.out.println();
        // submitting job
        RunningJob runningJob = submitJob(actionConf);

        // propagating job id back to Oozie
        String jobId = runningJob.getID().toString();
        Properties props = new Properties();
        props.setProperty("id", jobId);
        File idFile = new File(System.getProperty("oozie.action.newId.properties"));
        OutputStream os = new FileOutputStream(idFile);
        props.store(os, "");
        os.close();

        System.out.println("=======================");
        System.out.println();
    }

    protected void addActionConf(JobConf jobConf, Configuration actionConf) {
        for (Map.Entry<String, String> entry : actionConf) {
            jobConf.set(entry.getKey(), entry.getValue());
        }
    }

    protected RunningJob submitJob(Configuration actionConf) throws Exception {
        JobConf jobConf = new JobConf();
        addActionConf(jobConf, actionConf);

        // propagate delegation related props from launcher job to MR job
        if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
            jobConf.set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
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

    @SuppressWarnings("unchecked")
    protected JobClient createJobClient(JobConf jobConf) throws IOException {
        return new JobClient(jobConf);
    }

    // allows any character in the value, the conf.setStrings() does not allow
    // commas
    public static void setStrings(Configuration conf, String key, String[] values) {
        if (values != null) {
            conf.setInt(key + ".size", values.length);
            for (int i = 0; i < values.length; i++) {
                conf.set(key + "." + i, values[i]);
            }
        }
    }

    public static String[] getStrings(Configuration conf, String key) {
        String[] values = new String[conf.getInt(key + ".size", 0)];
        for (int i = 0; i < values.length; i++) {
            values[i] = conf.get(key + "." + i);
            if (values[i] == null) {
                values[i] = "";
            }
        }
        return values;
    }

}
