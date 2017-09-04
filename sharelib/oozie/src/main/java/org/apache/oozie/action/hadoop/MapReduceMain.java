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
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

import java.util.Map;
import java.util.Map.Entry;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.File;
import java.io.OutputStream;

public class MapReduceMain extends LauncherMain {

    public static final String OOZIE_MAPREDUCE_UBER_JAR = "oozie.mapreduce.uber.jar";

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
        setYarnTag(actionConf);

        JobConf jobConf = new JobConf();
        addActionConf(jobConf, actionConf);
        LauncherMain.killChildYarnJobs(jobConf);

        // Run a config class if given to update the job conf
        runConfigClass(jobConf);

        PasswordMasker passwordMasker = new PasswordMasker();
        // Temporary JobConf object, we mask out possible passwords before we print key-value pairs
        JobConf maskedJobConf = new JobConf(false);
        for (Entry<String, String> entry : jobConf) {
            maskedJobConf.set(entry.getKey(), passwordMasker.maskPasswordsIfNecessary(entry.getValue()));
        }

        logMasking("Map-Reduce job configuration:", maskedJobConf);

        File idFile = new File(System.getProperty(LauncherAMUtils.ACTION_PREFIX + LauncherAMUtils.ACTION_DATA_NEW_ID));
        System.out.println("Submitting Oozie action Map-Reduce job");
        System.out.println();
        // submitting job
        RunningJob runningJob = submitJob(jobConf);

        String jobId = runningJob.getID().toString();
        writeJobIdFile(idFile, jobId);

        System.out.println("=======================");
        System.out.println();
    }

    protected void writeJobIdFile(File idFile, String jobId) throws IOException {
        // propagating job id back to Oozie
        OutputStream os = new FileOutputStream(idFile);
        os.write(jobId.getBytes());
        os.close();
    }

    protected void addActionConf(JobConf jobConf, Configuration actionConf) {
        for (Map.Entry<String, String> entry : actionConf) {
            jobConf.set(entry.getKey(), entry.getValue());
        }
    }

    protected RunningJob submitJob(JobConf jobConf) throws Exception {
        // Set for uber jar
        String uberJar = jobConf.get(OOZIE_MAPREDUCE_UBER_JAR);
        if (uberJar != null && uberJar.trim().length() > 0) {
            jobConf.setJar(uberJar);
        }

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

    protected JobClient createJobClient(JobConf jobConf) throws IOException {
        return new JobClient(jobConf);
    }

    /**
     * Will run the user specified OozieActionConfigurator subclass (if one is provided) to update the action configuration.
     *
     * @param actionConf The action configuration to update
     * @throws OozieActionConfiguratorException
     */
    private static void runConfigClass(JobConf actionConf) throws OozieActionConfiguratorException {
        String configClass = actionConf.get(LauncherAMUtils.OOZIE_ACTION_CONFIG_CLASS);
        if (configClass != null) {
            try {
                Class<?> klass = Class.forName(configClass);
                Class<? extends OozieActionConfigurator> actionConfiguratorKlass = klass.asSubclass(OozieActionConfigurator.class);
                OozieActionConfigurator actionConfigurator = actionConfiguratorKlass.newInstance();
                actionConfigurator.configure(actionConf);
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                throw new OozieActionConfiguratorException("An Exception occurred while instantiating the action config class", e);
            }
        }
    }
}
