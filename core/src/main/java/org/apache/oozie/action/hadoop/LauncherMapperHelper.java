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
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.URIHandlerService;
import org.apache.oozie.service.UserGroupInformationService;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.PropertiesUtils;

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
        // Only set the javaMainClass if its not null or empty string, this way the user can override the action's main class via
        // <configuration> property
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

    /**
     * Set the maximum number of globbed files/dirs
     *
     * @param launcherConf the oozie launcher configuration
     * @param fsGlobMax the maximum number of files/dirs for FS operation
     */
    public static void setupMaxFSGlob(Configuration launcherConf, int fsGlobMax){
        launcherConf.setInt(LauncherMapper.CONF_OOZIE_ACTION_FS_GLOB_MAX, fsGlobMax);
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
        try {
            actionConf.writeXml(os);
        } finally {
            IOUtils.closeSafely(os);
        }

        launcherConf.setInputFormat(OozieLauncherInputFormat.class);
        launcherConf.set("mapred.output.dir", new Path(actionDir, "output").toString());
    }

    public static void setupYarnRestartHandling(JobConf launcherJobConf, Configuration actionConf, String launcherTag)
            throws NoSuchAlgorithmException {
        launcherJobConf.setLong(LauncherMainHadoopUtils.OOZIE_JOB_LAUNCH_TIME, System.currentTimeMillis());
        // Tags are limited to 100 chars so we need to hash them to make sure (the actionId otherwise doesn't have a max length)
        String tag = getTag(launcherTag);
        // keeping the oozie.child.mapreduce.job.tags instead of mapreduce.job.tags to avoid killing launcher itself.
        // mapreduce.job.tags should only go to child job launch by launcher.
        actionConf.set(LauncherMainHadoopUtils.CHILD_MAPREDUCE_JOB_TAGS, tag);
    }

    private static String getTag(String launcherTag) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("MD5");
        digest.update(launcherTag.getBytes(), 0, launcherTag.length());
        String md5 = "oozie-" + new BigInteger(1, digest.digest()).toString(16);
        return md5;
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

    /**
     * Determine whether action has external child jobs or not
     * @param actionData
     * @return true/false
     * @throws IOException
     */
    public static boolean hasExternalChildJobs(Map<String, String> actionData) throws IOException {
        return actionData.containsKey(LauncherMapper.ACTION_DATA_EXTERNAL_CHILD_IDS);
    }

    /**
     * Determine whether action has output data or not
     * @param actionData
     * @return true/false
     * @throws IOException
     */
    public static boolean hasOutputData(Map<String, String> actionData) throws IOException {
        return actionData.containsKey(LauncherMapper.ACTION_DATA_OUTPUT_PROPS);
    }

    /**
     * Determine whether action has external stats or not
     * @param actionData
     * @return true/false
     * @throws IOException
     */
    public static boolean hasStatsData(Map<String, String> actionData) throws IOException{
        return actionData.containsKey(LauncherMapper.ACTION_DATA_STATS);
    }

    /**
     * Determine whether action has new id (id swap) or not
     * @param actionData
     * @return true/false
     * @throws IOException
     */
    public static boolean hasIdSwap(Map<String, String> actionData) throws IOException {
        return actionData.containsKey(LauncherMapper.ACTION_DATA_NEW_ID);
    }

    /**
     * Get the sequence file path storing all action data
     * @param actionDir
     * @return
     */
    public static Path getActionDataSequenceFilePath(Path actionDir) {
        return new Path(actionDir, LauncherMapper.ACTION_DATA_SEQUENCE_FILE);
    }

    /**
     * Utility function to load the contents of action data sequence file into
     * memory object
     *
     * @param fs Action Filesystem
     * @param actionDir Path
     * @param conf Configuration
     * @return Map action data
     * @throws IOException
     * @throws InterruptedException
     */
    public static Map<String, String> getActionData(final FileSystem fs, final Path actionDir, final Configuration conf)
            throws IOException, InterruptedException {
        UserGroupInformationService ugiService = Services.get().get(UserGroupInformationService.class);
        UserGroupInformation ugi = ugiService.getProxyUser(conf.get(OozieClient.USER_NAME));

        return ugi.doAs(new PrivilegedExceptionAction<Map<String, String>>() {
            @Override
            public Map<String, String> run() throws IOException {
                Map<String, String> ret = new HashMap<String, String>();
                Path seqFilePath = getActionDataSequenceFilePath(actionDir);
                if (fs.exists(seqFilePath)) {
                    SequenceFile.Reader seqFile = new SequenceFile.Reader(fs, seqFilePath, conf);
                    Text key = new Text(), value = new Text();
                    while (seqFile.next(key, value)) {
                        ret.put(key.toString(), value.toString());
                    }
                    seqFile.close();
                }
                else { // maintain backward-compatibility. to be deprecated
                    org.apache.hadoop.fs.FileStatus[] files = fs.listStatus(actionDir);
                    InputStream is;
                    BufferedReader reader = null;
                    Properties props;
                    if (files != null && files.length > 0) {
                        for (int x = 0; x < files.length; x++) {
                            Path file = files[x].getPath();
                            if (file.equals(new Path(actionDir, "externalChildIds.properties"))) {
                                is = fs.open(file);
                                reader = new BufferedReader(new InputStreamReader(is));
                                ret.put(LauncherMapper.ACTION_DATA_EXTERNAL_CHILD_IDS,
                                        IOUtils.getReaderAsString(reader, -1));
                            }
                            else if (file.equals(new Path(actionDir, "newId.properties"))) {
                                is = fs.open(file);
                                reader = new BufferedReader(new InputStreamReader(is));
                                props = PropertiesUtils.readProperties(reader, -1);
                                ret.put(LauncherMapper.ACTION_DATA_NEW_ID, props.getProperty("id"));
                            }
                            else if (file.equals(new Path(actionDir, LauncherMapper.ACTION_DATA_OUTPUT_PROPS))) {
                                int maxOutputData = conf.getInt(LauncherMapper.CONF_OOZIE_ACTION_MAX_OUTPUT_DATA,
                                        2 * 1024);
                                is = fs.open(file);
                                reader = new BufferedReader(new InputStreamReader(is));
                                ret.put(LauncherMapper.ACTION_DATA_OUTPUT_PROPS, PropertiesUtils
                                        .propertiesToString(PropertiesUtils.readProperties(reader, maxOutputData)));
                            }
                            else if (file.equals(new Path(actionDir, LauncherMapper.ACTION_DATA_STATS))) {
                                int statsMaxOutputData = conf.getInt(LauncherMapper.CONF_OOZIE_EXTERNAL_STATS_MAX_SIZE,
                                        Integer.MAX_VALUE);
                                is = fs.open(file);
                                reader = new BufferedReader(new InputStreamReader(is));
                                ret.put(LauncherMapper.ACTION_DATA_STATS, PropertiesUtils
                                        .propertiesToString(PropertiesUtils.readProperties(reader, statsMaxOutputData)));
                            }
                            else if (file.equals(new Path(actionDir, LauncherMapper.ACTION_DATA_ERROR_PROPS))) {
                                is = fs.open(file);
                                reader = new BufferedReader(new InputStreamReader(is));
                                ret.put(LauncherMapper.ACTION_DATA_ERROR_PROPS, IOUtils.getReaderAsString(reader, -1));
                            }
                        }
                    }
                }
                return ret;
            }
        });
    }
}
