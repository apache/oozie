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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.URIHandlerService;
import org.apache.oozie.util.XLog;

public class LauncherMapperHelper {

    public static String getRecoveryId(Configuration launcherConf, Path actionDir, String recoveryId)
            throws HadoopAccessorException, IOException {
        String jobId = null;
        Path recoveryFile = new Path(actionDir, recoveryId);
        FileSystem fs = Services.get().get(HadoopAccessorService.class)
                .createFileSystem(launcherConf.get("user.name"),recoveryFile.toUri(), launcherConf);

        if (fs.exists(recoveryFile)) {
            InputStream is = fs.open(recoveryFile);
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            jobId = reader.readLine();
            reader.close();
        }
        return jobId;

    }

    public static void setupMainClass(Configuration launcherConf, String javaMainClass) {
        // Only set the javaMainClass if its not null or empty string (should be the case except for java action), this way the user
        // can override the action's main class via <configuration> property
        if (javaMainClass != null && !javaMainClass.equals("")) {
            launcherConf.set(LauncherMapper.CONF_OOZIE_ACTION_MAIN_CLASS, javaMainClass);
        }
    }

    public static void setupLauncherURIHandlerConf(Configuration launcherConf) {
        for(Map.Entry<String, String> entry : Services.get().get(URIHandlerService.class).getLauncherConfig()) {
            launcherConf.set(entry.getKey(), entry.getValue());
        }
    }

    public static void setupMainArguments(Configuration launcherConf, String[] args) {
        launcherConf.setInt(LauncherMapper.CONF_OOZIE_ACTION_MAIN_ARG_COUNT, args.length);
        for (int i = 0; i < args.length; i++) {
            launcherConf.set(LauncherMapper.CONF_OOZIE_ACTION_MAIN_ARG_PREFIX + i, args[i]);
        }
    }

    public static void setupMaxOutputData(Configuration launcherConf, int maxOutputData) {
        launcherConf.setInt(LauncherMapper.CONF_OOZIE_ACTION_MAX_OUTPUT_DATA, maxOutputData);
    }

    /**
     * Set the maximum value of stats data
     *
     * @param launcherConf the oozie launcher configuration
     * @param maxStatsData the maximum allowed size of stats data
     */
    public static void setupMaxExternalStatsSize(Configuration launcherConf, int maxStatsData){
        launcherConf.setInt(LauncherMapper.CONF_OOZIE_EXTERNAL_STATS_MAX_SIZE, maxStatsData);
    }

    public static void setupLauncherInfo(JobConf launcherConf, String jobId, String actionId, Path actionDir,
            String recoveryId, Configuration actionConf, String prepareXML) throws IOException, HadoopAccessorException {

        launcherConf.setMapperClass(LauncherMapper.class);
        launcherConf.setSpeculativeExecution(false);
        launcherConf.setNumMapTasks(1);
        launcherConf.setNumReduceTasks(0);

        launcherConf.set(LauncherMapper.OOZIE_JOB_ID, jobId);
        launcherConf.set(LauncherMapper.OOZIE_ACTION_ID, actionId);
        launcherConf.set(LauncherMapper.OOZIE_ACTION_DIR_PATH, actionDir.toString());
        launcherConf.set(LauncherMapper.OOZIE_ACTION_RECOVERY_ID, recoveryId);
        launcherConf.set(LauncherMapper.ACTION_PREPARE_XML, prepareXML);

        actionConf.set(LauncherMapper.OOZIE_JOB_ID, jobId);
        actionConf.set(LauncherMapper.OOZIE_ACTION_ID, actionId);

        if (Services.get().getConf().getBoolean("oozie.hadoop-2.0.2-alpha.workaround.for.distributed.cache", false)) {
          List<String> purgedEntries = new ArrayList<String>();
          Collection<String> entries = actionConf.getStringCollection("mapreduce.job.cache.files");
          for (String entry : entries) {
            if (entry.contains("#")) {
              purgedEntries.add(entry);
            }
          }
          actionConf.setStrings("mapreduce.job.cache.files", purgedEntries.toArray(new String[purgedEntries.size()]));
          launcherConf.setBoolean("oozie.hadoop-2.0.2-alpha.workaround.for.distributed.cache", true);
        }

        FileSystem fs =
          Services.get().get(HadoopAccessorService.class).createFileSystem(launcherConf.get("user.name"),
                                                                           actionDir.toUri(), launcherConf);
        fs.mkdirs(actionDir);

        OutputStream os = fs.create(new Path(actionDir, LauncherMapper.ACTION_CONF_XML));
        actionConf.writeXml(os);
        os.close();

        Path inputDir = new Path(actionDir, "input");
        fs.mkdirs(inputDir);
        Writer writer = new OutputStreamWriter(fs.create(new Path(inputDir, "dummy.txt")));
        writer.write("dummy");
        writer.close();

        launcherConf.set("mapred.input.dir", inputDir.toString());
        launcherConf.set("mapred.output.dir", new Path(actionDir, "output").toString());
    }

    public static boolean isMainDone(RunningJob runningJob) throws IOException {
        return runningJob.isComplete();
    }

    public static boolean isMainSuccessful(RunningJob runningJob) throws IOException {
        boolean succeeded = runningJob.isSuccessful();
        if (succeeded) {
            Counters counters = runningJob.getCounters();
            if (counters != null) {
                Counters.Group group = counters.getGroup(LauncherMapper.COUNTER_GROUP);
                if (group != null) {
                    succeeded = group.getCounter(LauncherMapper.COUNTER_LAUNCHER_ERROR) == 0;
                }
            }
        }
        return succeeded;
    }

    public static boolean hasOutputData(RunningJob runningJob) throws IOException {
        boolean output = false;
        Counters counters = runningJob.getCounters();
        if (counters != null) {
            Counters.Group group = counters.getGroup(LauncherMapper.COUNTER_GROUP);
            if (group != null) {
                output = group.getCounter(LauncherMapper.COUNTER_OUTPUT_DATA) == 1;
            }
        }
        return output;
    }

    /**
     * Check whether runningJob has stats data or not
     *
     * @param runningJob the runningJob
     * @return returns whether the running Job has stats data or not
     * @throws IOException
     */
    public static boolean hasStatsData(RunningJob runningJob) throws IOException{
        boolean output = false;
        Counters counters = runningJob.getCounters();
        if (counters != null) {
            Counters.Group group = counters.getGroup(LauncherMapper.COUNTER_GROUP);
            if (group != null) {
                output = group.getCounter(LauncherMapper.COUNTER_STATS_DATA) == 1;
            }
        }
        return output;
    }

    public static boolean hasIdSwap(RunningJob runningJob) throws IOException {
        boolean swap = false;
        Counters counters = runningJob.getCounters();
        if (counters != null) {
            Counters.Group group = counters.getGroup(LauncherMapper.COUNTER_GROUP);
            if (group != null) {
                swap = group.getCounter(LauncherMapper.COUNTER_DO_ID_SWAP) == 1;
            }
        }
        return swap;
    }

    public static boolean hasIdSwap(RunningJob runningJob, String user, String group, Path actionDir)
            throws IOException, HadoopAccessorException {
        boolean swap = false;

        XLog log = XLog.getLog("org.apache.oozie.action.hadoop.LauncherMapper");

        Counters counters = runningJob.getCounters();
        if (counters != null) {
            Counters.Group counterGroup = counters.getGroup(LauncherMapper.COUNTER_GROUP);
            if (counterGroup != null) {
                swap = counterGroup.getCounter(LauncherMapper.COUNTER_DO_ID_SWAP) == 1;
            }
        }
        // additional check for swapped hadoop ID
        // Can't rely on hadoop counters existing
        // we'll check for the newID file in hdfs if the hadoop counters is null
        else {

            Path p = getIdSwapPath(actionDir);
            // log.debug("Checking for newId file in: [{0}]", p);

            HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
            Configuration conf = has.createJobConf(p.toUri().getAuthority());
            FileSystem fs = has.createFileSystem(user, p.toUri(), conf);
            if (fs.exists(p)) {
                log.debug("Hadoop Counters is null, but found newID file.");

                swap = true;
            }
            else {
                log.debug("Hadoop Counters is null, and newID file doesn't exist at: [{0}]", p);
            }
        }
        return swap;
    }

    public static Path getOutputDataPath(Path actionDir) {
        return new Path(actionDir, LauncherMapper.ACTION_OUTPUT_PROPS);
    }

    /**
     * Get the location of stats file
     *
     * @param actionDir the action directory
     * @return the hdfs location of the file
     */
    public static Path getActionStatsDataPath(Path actionDir){
        return new Path(actionDir, LauncherMapper.ACTION_STATS_PROPS);
    }

    /**
     * Get the location of external Child IDs file
     *
     * @param actionDir the action directory
     * @return the hdfs location of the file
     */
    public static Path getExternalChildIDsDataPath(Path actionDir){
        return new Path(actionDir, LauncherMapper.ACTION_EXTERNAL_CHILD_IDS_PROPS);
    }

    public static Path getErrorPath(Path actionDir) {
        return new Path(actionDir, LauncherMapper.ACTION_ERROR_PROPS);
    }

    public static Path getIdSwapPath(Path actionDir) {
        return new Path(actionDir, LauncherMapper.ACTION_NEW_ID_PROPS);
    }
}
