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

package org.apache.oozie.service;

import java.util.regex.Pattern;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.util.XLog;

public class XLogUtil {

    private String logPath;
    private String logFileName;
    private boolean isLogOverEnable = true;
    private int logRotation;

    public static XLog log = new XLog(LogFactory.getLog(XLogUtil.class));

    public XLogUtil(Configuration conf, String logType) {
        extractInfoForLogWebService(conf, logType);
    }

    private void extractInfoForLogWebService(Configuration conf, String logType) {
        String logFile = conf.get("log4j.appender." + logType + ".File");
        if (logFile == null) {
            log.warn("Oozie WS " + logType
                    + " log will be disabled, missing property 'log4j.appender.oozie.File' for 'oozie' " + "appender");
            isLogOverEnable = false;
        }
        else {
            logFile = logFile.trim();
            int i = logFile.lastIndexOf("/");
            if (i == -1) {
                log.warn("Oozie WS " + logType
                        + " log will be disabled, log file is not an absolute path [{0}] for 'oozie' appender", logFile);
                isLogOverEnable = false;
            }
            else {
                String appenderClass = conf.get("log4j.appender." + logType);
                if (appenderClass == null) {
                    log.warn("Oozie WS " + logType + " log will be disabled, missing property [log4j.appender.oozie]");
                    isLogOverEnable = false;
                }
                else if (appenderClass.equals("org.apache.log4j.DailyRollingFileAppender")) {
                    String pattern = conf.get("log4j.appender." + logType + ".DatePattern");
                    if (pattern == null) {
                        log.warn("Oozie WS " + logType
                                + " log will be disabled, missing property [log4j.appender." + logType + ".DatePattern]");
                        isLogOverEnable = false;
                    }
                    else {
                        pattern = pattern.trim();
                        if (pattern.endsWith("HH")) {
                            logRotation = 60 * 60;
                        }
                        else if (pattern.endsWith("dd")) {
                            logRotation = 60 * 60 * 24;
                        }
                        else {
                            log.warn("Oozie WS " + logType
                                    + " log will be disabled, DatePattern [{0}] should end with 'HH' or 'dd'", pattern);
                            isLogOverEnable = false;
                        }
                        if (logRotation > 0) {
                            logPath = logFile.substring(0, i);
                            logFileName = logFile.substring(i + 1);
                        }
                    }
                }
                else if (appenderClass.equals("org.apache.log4j.rolling.RollingFileAppender")) {
                    String pattern = conf.get("log4j.appender." + logType + ".RollingPolicy.FileNamePattern");
                    if (pattern == null) {
                        log.warn("Oozie WS " + logType + " log will be disabled, missing property "
                                + "[log4j.appender." + logType + ".RollingPolicy.FileNamePattern]");
                        isLogOverEnable = false;
                    }
                    else {
                        pattern = pattern.trim();
                        if (pattern.matches(Pattern.quote(logFile) + ".*-%d\\{yyyy-MM-dd-HH\\}(\\.gz)?")) {
                            logRotation = 60 * 60;
                        }
                        else {
                            log.warn(
                                    "Oozie WS "
                                            + logType
                                            + " log will be disabled, RollingPolicy.FileNamePattern [{0}] should end with "
                                            + "'-%d{yyyy-MM-dd-HH}' or '-%d{yyyy-MM-dd-HH}.gz' and also start with the value of "
                                            + "log4j.appender." + logType + ".File [{1}]", pattern, logFile);
                            isLogOverEnable = false;
                        }
                        if (logRotation > 0) {
                            logPath = logFile.substring(0, i);
                            logFileName = logFile.substring(i + 1);
                        }
                    }
                }
                else {
                    log.warn("Oozie WS "
                            + logType
                            + " log will be disabled, log4j.appender.oozie ["
                            + appenderClass
                            + "] should be "
                            + "either org.apache.log4j.DailyRollingFileAppender or org.apache.log4j.rolling.RollingFileAppender "
                            + "to enable it");
                    isLogOverEnable = false;
                }
            }
        }
    }

    public String getLogPath() {
        return logPath;
    }

    public String getLogFileName() {
        return logFileName;
    }

    public boolean isLogOverEnable() {
        return isLogOverEnable;
    }

    public int getLogRotation() {
        return logRotation;
    }

}
