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

import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class LauncherAMUtils {

    static final String CONF_OOZIE_ACTION_MAIN_CLASS = "oozie.launcher.action.main.class";

    static final String ACTION_PREFIX = "oozie.action.";
    public static final String CONF_OOZIE_ACTION_MAX_OUTPUT_DATA = ACTION_PREFIX + "max.output.data";
    static final String CONF_OOZIE_ACTION_MAIN_ARG_COUNT = ACTION_PREFIX + "main.arg.count";
    static final String CONF_OOZIE_ACTION_MAIN_ARG_PREFIX = ACTION_PREFIX + "main.arg.";
    static final String CONF_OOZIE_EXTERNAL_STATS_MAX_SIZE = "oozie.external.stats.max.size";
    static final String OOZIE_ACTION_CONFIG_CLASS = ACTION_PREFIX + "config.class";
    static final String CONF_OOZIE_ACTION_FS_GLOB_MAX = ACTION_PREFIX + "fs.glob.max";
    static final String CONF_OOZIE_NULL_ARGS_ALLOWED = ACTION_PREFIX + "null.args.allowed";

    static final String COUNTER_GROUP = "oozie.launcher";
    static final String COUNTER_LAUNCHER_ERROR = "oozie.launcher.error";

    static final String OOZIE_JOB_ID = "oozie.job.id";
    static final String OOZIE_ACTION_ID = ACTION_PREFIX + "id";
    static final String OOZIE_ACTION_RECOVERY_ID = ACTION_PREFIX + "recovery.id";

    static final String OOZIE_ACTION_DIR_PATH = ACTION_PREFIX + "dir.path";
    static final String ACTION_CONF_XML = "action.xml";
    static final String ACTION_PREPARE_XML = "oozie.action.prepare.xml";
    static final String ACTION_DATA_SEQUENCE_FILE = "action-data.seq"; // COMBO FILE
    static final String ACTION_DATA_EXTERNAL_CHILD_IDS = "externalChildIDs";
    static final String ACTION_DATA_OUTPUT_PROPS = "output.properties";
    static final String ACTION_DATA_STATS = "stats.properties";
    static final String ACTION_DATA_NEW_ID = "newId";
    static final String ACTION_DATA_ERROR_PROPS = "error.properties";
    public static final String HADOOP2_WORKAROUND_DISTRIBUTED_CACHE = "oozie.hadoop-2.0.2-alpha.workaround.for.distributed.cache";
    public static final String PROPAGATION_CONF_XML = "propagation-conf.xml";
    public static final String ROOT_LOGGER_LEVEL = "rootlogger.log.level";


    public static String getLocalFileContentStr(File file, String type, int maxLen) throws LauncherException, IOException {
        StringBuffer sb = new StringBuffer();
        FileReader reader = new FileReader(file);
        char[] buffer = new char[2048];
        int read;
        int count = 0;
        while ((read = reader.read(buffer)) > -1) {
            count += read;
            if (maxLen > -1 && count > maxLen) {
                throw new LauncherException(type + " data exceeds its limit ["+ maxLen + "]");
            }
            sb.append(buffer, 0, read);
        }
        reader.close();
        return sb.toString();
    }

    public static String[] getMainArguments(Configuration conf) {
        String[] args = new String[conf.getInt(CONF_OOZIE_ACTION_MAIN_ARG_COUNT, 0)];

        String[] retArray;

        if (conf.getBoolean(CONF_OOZIE_NULL_ARGS_ALLOWED, true)) {
            for (int i = 0; i < args.length; i++) {
                args[i] = conf.get(CONF_OOZIE_ACTION_MAIN_ARG_PREFIX + i);
            }

            retArray = args;
        } else {
            int pos = 0;
            for (int i = 0; i < args.length; i++) {
                String arg = conf.get(CONF_OOZIE_ACTION_MAIN_ARG_PREFIX + i);
                if (!Strings.isNullOrEmpty(arg)) {
                    args[pos++] = conf.get(CONF_OOZIE_ACTION_MAIN_ARG_PREFIX + i);
                }
            }

            // this is to skip null args, that is <arg></arg> in the workflow XML -- in this case,
            // args[] might look like {"arg1", "arg2", null, null} at this point
            retArray = new String[pos];
            System.arraycopy(args, 0, retArray, 0, pos);
        }

        return retArray;
    }
}

