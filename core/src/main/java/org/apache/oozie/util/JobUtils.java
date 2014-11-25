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

package org.apache.oozie.util;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.XOozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;

/**
 * Job utilities.
 */
public class JobUtils {
    /**
     * Normalize appPath in job conf with the provided user/group - If it's not jobs via proxy submission, after
     * normalization appPath always points to job's Xml definition file.
     * <p/>
     *
     * @param user user
     * @param group group
     * @param conf job configuration.
     * @throws IOException thrown if normalization can not be done properly.
     */
    public static void normalizeAppPath(String user, String group, Configuration conf) throws IOException {
        if (user == null) {
            throw new IllegalArgumentException("user cannot be null");
        }

        if (conf.get(XOozieClient.IS_PROXY_SUBMISSION) != null) { // do nothing for proxy submission job;
            return;
        }

        String wfPathStr = conf.get(OozieClient.APP_PATH);
        String coordPathStr = conf.get(OozieClient.COORDINATOR_APP_PATH);
        String bundlePathStr = conf.get(OozieClient.BUNDLE_APP_PATH);
        String appPathStr = wfPathStr != null ? wfPathStr : (coordPathStr != null ? coordPathStr : bundlePathStr);

        FileSystem fs = null;
        try {
            URI uri = new Path(appPathStr).toUri();
            HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
            Configuration fsConf = has.createJobConf(uri.getAuthority());
            fs = has.createFileSystem(user, uri, fsConf);
        }
        catch (HadoopAccessorException ex) {
            throw new IOException(ex.getMessage());
        }

        Path appPath = new Path(appPathStr);
        String normalizedAppPathStr = appPathStr;
        if (!fs.exists(appPath)) {
            throw new IOException("Error: " + appPathStr + " does not exist");
        }

        if (wfPathStr != null) {
            conf.set(OozieClient.APP_PATH, normalizedAppPathStr);
        }
        else if (coordPathStr != null) {
            conf.set(OozieClient.COORDINATOR_APP_PATH, normalizedAppPathStr);
        }
        else if (bundlePathStr != null) {
            conf.set(OozieClient.BUNDLE_APP_PATH, normalizedAppPathStr);
        }
    }

    /**
     * This Function will parse the value of the changed values in key value manner. the change value would be
     * key1=value1;key2=value2
     *
     * @param changeValue change value.
     * @return This returns the hash with hash<[key1,value1],[key2,value2]>
     * @throws CommandException thrown if changeValue cannot be parsed properly.
     */
    public static Map<String, String> parseChangeValue(String changeValue) throws CommandException {
        if (changeValue == null || changeValue.trim().equalsIgnoreCase("")) {
            throw new CommandException(ErrorCode.E1015, "change value can not be empty string or null");
        }

        Map<String, String> map = new HashMap<String, String>();

        String[] tokens = changeValue.split(";");
        for (String token : tokens) {
            if (!token.contains("=")) {
                throw new CommandException(ErrorCode.E1015, changeValue,
                        "change value must be name=value pair or name=(empty string)");
            }

            String[] pair = token.split("=");
            String key = pair[0];

            if (map.containsKey(key)) {
                throw new CommandException(ErrorCode.E1015, changeValue, "can not specify repeated change values on "
                        + key);
            }

            if (pair.length == 2) {
                map.put(key, pair[1]);
            }
            else if (pair.length == 1) {
                map.put(key, "");
            }
            else {
                throw new CommandException(ErrorCode.E1015, changeValue, "elements on " + key
                        + " must be name=value pair or name=(empty string)");
            }
        }

        return map;
    }

    /**
     * This method provides a wrapper around hadoop 0.20/1.x and 0.23/2.x implementations.
     * TODO: Remove the workaround when we drop the support for hadoop 0.20.
     * @param file Path of the file to be added
     * @param conf Configuration that contains the classpath setting
     * @param fs FileSystem with respect to which path should be interpreted (may be null)
     * @throws IOException
     */
    public static void addFileToClassPath(Path file, Configuration conf, FileSystem fs) throws IOException {
      Configuration defaultConf = new Configuration();
      XConfiguration.copy(conf, defaultConf);
      if (fs == null) {
        // it fails with conf, therefore we pass defaultConf instead
        fs = file.getFileSystem(defaultConf);
      }
      // Hadoop 0.20/1.x.
      if (defaultConf.get("yarn.resourcemanager.webapp.address") == null) {
          // Duplicate hadoop 1.x code to workaround MAPREDUCE-2361 in Hadoop 0.20
          // Refer OOZIE-1806.
          String filepath = file.toUri().getPath();
          String classpath = conf.get("mapred.job.classpath.files");
          conf.set("mapred.job.classpath.files", classpath == null
              ? filepath
              : classpath + System.getProperty("path.separator") + filepath);
          URI uri = fs.makeQualified(file).toUri();
          DistributedCache.addCacheFile(uri, conf);
      }
      else { // Hadoop 0.23/2.x
          DistributedCache.addFileToClassPath(file, conf, fs);
      }
    }
}
