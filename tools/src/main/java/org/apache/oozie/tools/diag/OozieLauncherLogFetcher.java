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

package org.apache.oozie.tools.diag;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;

// TODO: once OOZIE-2983 ("Stream the Launcher AM Logs") is done, remove it.
public class OozieLauncherLogFetcher {
    private static final String TMP_FILE_SUFFIX = ".tmp";
    final private Configuration hadoopConfig;

    public OozieLauncherLogFetcher(final Configuration hadoopConfig) {
        this.hadoopConfig = hadoopConfig;
    }

    // Borrowed code from org.apache.hadoop.yarn.logaggregation.LogCLIHelpers
    private static void logDirNotExist(String remoteAppLogDir) {
        System.out.println(remoteAppLogDir + "does not exist.");
        System.out.println("Log aggregation has not completed or is not enabled.");
    }
    // Borrowed code from org.apache.hadoop.yarn.logaggregation.LogCLIHelpers
    private static void emptyLogDir(String remoteAppLogDir) {
        System.out.println(remoteAppLogDir + "does not have any log files.");
    }
    // Borrowed code from org.apache.hadoop.yarn.logaggregation.LogAggregationUtils
    public static String getRemoteNodeLogDirSuffix(Configuration conf) {
        return conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX, YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR_SUFFIX);
    }
    // Borrowed code from org.apache.hadoop.yarn.logaggregation.LogAggregationUtils
    public static Path getRemoteLogSuffixedDir(Path remoteRootLogDir, String user, String suffix) {
        return suffix != null && !suffix.isEmpty() ? new Path(getRemoteLogUserDir(remoteRootLogDir, user), suffix) :
                getRemoteLogUserDir(remoteRootLogDir, user);
    }
    // Borrowed code from org.apache.hadoop.yarn.logaggregation.LogAggregationUtils
    public static Path getRemoteLogUserDir(Path remoteRootLogDir, String user) {
        return new Path(remoteRootLogDir, user);
    }

    // Borrowed code from org.apache.hadoop.yarn.logaggregation.LogAggregationUtils
    public static Path getRemoteAppLogDir(Path remoteRootLogDir, ApplicationId appId, String user, String suffix) {
        return new Path(getRemoteLogSuffixedDir(remoteRootLogDir, user, suffix), appId.toString());
    }

    // Borrowed code from org.apache.hadoop.yarn.logaggregation.LogCLIHelpers
    public int dumpAllContainersLogs(ApplicationId appId, String appOwner, PrintStream out) throws IOException {
        Path remoteRootLogDir = new Path(hadoopConfig.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
                YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR));
        String logDirSuffix = getRemoteNodeLogDirSuffix(hadoopConfig);
        Path remoteAppLogDir = getRemoteAppLogDir(remoteRootLogDir, appId, appOwner, logDirSuffix);

        RemoteIterator nodeFiles;
        try {
            Path qualifiedLogDir = FileContext.getFileContext(hadoopConfig).makeQualified(remoteAppLogDir);
            nodeFiles = FileContext.getFileContext(qualifiedLogDir.toUri(), hadoopConfig).listStatus(remoteAppLogDir);
        } catch (FileNotFoundException fileNotFoundException) {
            logDirNotExist(remoteAppLogDir.toString());
            return -1;
        }

        boolean foundAnyLogs = false;

        while(true) {
            FileStatus thisNodeFile;
            do {
                if (!nodeFiles.hasNext()) {
                    if (!foundAnyLogs) {
                        emptyLogDir(remoteAppLogDir.toString());
                        return -1;
                    }

                    return 0;
                }

                thisNodeFile = (FileStatus)nodeFiles.next();
            } while(thisNodeFile.getPath().getName().endsWith(TMP_FILE_SUFFIX));

            AggregatedLogFormat.LogReader reader = new AggregatedLogFormat.LogReader(hadoopConfig, thisNodeFile.getPath());

            try {
                AggregatedLogFormat.LogKey key = new AggregatedLogFormat.LogKey();
                DataInputStream valueStream = reader.next(key);

                while(valueStream != null) {
                    String containerString = "\n\nContainer: " + key + " on " + thisNodeFile.getPath().getName();
                    out.println(containerString);
                    out.println(StringUtils.repeat("=", containerString.length()));

                    while(true) {
                        try {
                            AggregatedLogFormat.LogReader.readAContainerLogsForALogType(valueStream, out,
                                    thisNodeFile.getModificationTime());
                            foundAnyLogs = true;
                        } catch (EOFException eofException) {
                            key = new AggregatedLogFormat.LogKey();
                            valueStream = reader.next(key);
                            break;
                        }
                    }
                }
            } finally {
                reader.close();
            }
        }
    }
}
