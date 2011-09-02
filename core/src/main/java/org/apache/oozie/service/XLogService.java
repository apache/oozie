/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.service;

import org.apache.commons.logging.LogFactory;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.apache.oozie.util.Instrumentable;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XLogStreamer;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.BuildInfo;
import org.apache.oozie.ErrorCode;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Writer;
import java.net.URL;
import java.util.Properties;
import java.util.Map;
import java.util.Date;

/**
 * Built in service that initializes and manages the log4j.
 * <p/>
 * The log4j configuration file to use is read from the system property {@link #LOG4J_FILE}, if the system propery is
 * not set the default value is {@link #DEFAULT_LOG4J_PROPERTIES}. The log4j configuration file can be a Java Properties
 * file (.properties) or an XML file (.xml).
 * <p/>
 * If the system property {@link ConfigurationService#CONFIG_PATH} is set, the log4j configuration file is read from
 * that directory. Otherwise is read from the classpath root.
 * <p/>
 * The reload interval of the log4j configuration is set by the system property {@link #RELOAD_INTERVAL}, the value is
 * in seconds, the default value is {@link #DEFAULT_RELOAD_INTERVAL}.
 * <p/>
 * The log4j configuration is reloaded only when read from a configuration directory, when read from the classpath
 * reloading is not in effect.
 */
public class XLogService implements Service, Instrumentable {
    private static final String INSTRUMENTATION_GROUP = "logging";

    /**
     * System property that indicates the log4j configuration file to load.
     */
    public static final String LOG4J_FILE = "oozie.log4j.file";

    /**
     * System property that indicates the reload interval of the configuration file.
     */
    public static final String RELOAD_INTERVAL = "oozie.log4j.reload";

    /**
     * Default value for the log4j configuration file if {@link #LOG4J_FILE} is not set.
     */
    public static final String DEFAULT_LOG4J_PROPERTIES = "oozie-log4j.properties";

    /**
     * Default value for the reload interval if {@link #RELOAD_INTERVAL} is not set.
     */
    public static final String DEFAULT_RELOAD_INTERVAL = "10";

    // for testing purposes
    static boolean testingDefaultFile;

    private long interval;
    private boolean fromClasspath;
    private String configFile;

    private static final String STARTUP_MESSAGE =
            "{E}" +
            " ******************************************************************************* {E}" +
            "  STARTUP MSG: Oozie BUILD_VERSION [{0}] compiled by [{1}] on [{2}]{E}" +
            "  STARTUP MSG:       revision [{3}]@[{4}]{E}" +
            "*******************************************************************************";

    private String oozieLogPath;
    private String oozieLogName;
    private int oozieLogRotation = -1;

    public XLogService() {
        testingDefaultFile = false;
    }

    /**
     * Initialize the log service.
     *
     * @param services services instance.
     * @throws ServiceException thrown if the log service could not be initialized.
     */
    public void init(Services services) throws ServiceException {
        try {
            LogManager.resetConfiguration();
            String defaultConfig = (testingDefaultFile) ? "test-" + DEFAULT_LOG4J_PROPERTIES : DEFAULT_LOG4J_PROPERTIES;
            String log4jPathOri = System.getProperty(LOG4J_FILE, defaultConfig);

            String configPath = System.getProperty(ConfigurationService.CONFIG_PATH);

            fromClasspath = configureLog4J(configPath, log4jPathOri);

            XLog log = new XLog(LogFactory.getLog(getClass()));

            String from = (fromClasspath) ? "classpath" : "path";
            long interval = Long.parseLong(System.getProperty(RELOAD_INTERVAL, DEFAULT_RELOAD_INTERVAL));
            String reload = (fromClasspath) ? "disabled" : Long.toString(interval) + " sec";
            log.info(XLog.OPS, STARTUP_MESSAGE,
                     BuildInfo.getBuildInfo().getProperty(BuildInfo.BUILD_VERSION),
                     BuildInfo.getBuildInfo().getProperty(BuildInfo.BUILD_USER_NAME),
                     BuildInfo.getBuildInfo().getProperty(BuildInfo.BUILD_TIME),
                     BuildInfo.getBuildInfo().getProperty(BuildInfo.BUILD_SVN_REVISION),
                     BuildInfo.getBuildInfo().getProperty(BuildInfo.BUILD_SVN_URL));

            log.info("Log4j configuration loaded from [{0}]", from);
            log.info("Log4j configuration file [{0}]", configFile);
            log.info("Log4j reload interval [{0}]", reload);
            if (log.isDebugEnabled()) {
                ClassLoader cl = Thread.currentThread().getContextClassLoader();
                InputStream is = (fromClasspath) ? cl.getResourceAsStream(configFile) : new FileInputStream(configFile);
                BufferedReader br = new BufferedReader(new InputStreamReader(is));
                StringBuffer sb = new StringBuffer();
                String line;
                while ((line = br.readLine()) != null) {
                    sb.append("   ").append(line).append('\n');
                }
                br.close();
                log.trace("Log4j configuration:{E}----{E}{0}----{E}", sb);
            }

            //Getting configuration for oozie log via WS
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            InputStream is = (fromClasspath) ? cl.getResourceAsStream(configFile) : new FileInputStream(configFile);
            Properties props = new Properties();
            props.load(is);
            Configuration conf = new XConfiguration();
            for (Map.Entry entry : props.entrySet()) {
                conf.set((String)entry.getKey(), (String) entry.getValue());
            }
            String logFile = conf.get("log4j.appender.oozie.File");
            if (logFile == null) {
                log.warn("Oozie log via WS not configured properly, missing property [{0}]",
                         "log4j.appender.oozie.File");
            }
            else {
                logFile = logFile.trim();
                int i = logFile.lastIndexOf("/");
                if (i == -1) {
                    log.warn("Oozie apps log via WS not configured properly, log file is not an absolute path [{0}]",
                             logFile);
                }
                else {
                    String pattern = conf.get("log4j.appender.oozie.DatePattern");
                    if (pattern == null) {
                        log.warn("Oozie apps log via WS not configured properly, missing property [{0}]",
                                 "log4j.appender.oozie.DatePattern");
                    }
                    else {
                        pattern = pattern.trim();
                        if (pattern.endsWith("HH")) {
                            oozieLogRotation = 60 * 60;
                        }
                        else if (pattern.endsWith("dd")) {
                            oozieLogRotation = 60 * 60 * 24;
                        }
                        else {
                            log.warn("Oozie apps log via WS not configured properly, invalid DatePatter [{0}], " +
                                     "it should end with 'HH' or 'dd'", pattern);
                        }
                        if (oozieLogRotation > 0) {
                            oozieLogPath = logFile.substring(0, i);
                            oozieLogName = logFile.substring(i + 1);
                        }
                    }
                }
            }
        }
        catch (IOException ex) {
            throw new ServiceException(ErrorCode.E0010, ex.getMessage(), ex);
        }
        XLog.Info.reset();
        XLog.Info.defineParameter(USER);
        XLog.Info.defineParameter(GROUP);
        XLogStreamer.Filter.reset();
        XLogStreamer.Filter.defineParameter(USER);
        XLogStreamer.Filter.defineParameter(GROUP);
    }

    /**
     * Destroy the log service.
     */
    public void destroy() {
        LogManager.shutdown();
        XLog.Info.reset();
        XLogStreamer.Filter.reset();
    }

    /**
     * Group log info constant.
     */
    public static final String USER = "USER";

    /**
     * Group log info constant.
     */
    public static final String GROUP = "GROUP";

    /**
     * Return the public interface for log service.
     *
     * @return {@link XLogService}.
     */
    public Class<? extends Service> getInterface() {
        return XLogService.class;
    }

    private boolean configureLog4J(String configPath, String log4jFile) throws IOException {
        boolean fromClasspath;
        if (configPath != null) {
            interval = Long.parseLong(System.getProperty(RELOAD_INTERVAL, DEFAULT_RELOAD_INTERVAL));
            File file = new File(configPath, log4jFile);
            if (!file.exists()) {
                throw new RuntimeException(
                        XLog.format("Log4j configuration [{0}] not found in path [{1}]", log4jFile, configPath));
            }
            log4jFile = file.getAbsolutePath();
            if (log4jFile.endsWith(".properties")) {
                PropertyConfigurator.configureAndWatch(log4jFile, interval * 1000);
            }
            else {
                throw new RuntimeException(
                        XLog.format("Log4j configuration [{0}] must be a '.properties' file", log4jFile));
            }
            configFile = log4jFile;
            fromClasspath = false;
        }
        else {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            URL log4jUrl = cl.getResource(log4jFile);
            if (log4jUrl != null) {
                if (log4jFile.endsWith(".properties")) {
                    PropertyConfigurator.configure(log4jUrl);
                }
                else {
                    throw new RuntimeException(
                            XLog.format("Log4j configuration [{0}] must be a '.properties' file", log4jFile));
                }
            }
            else {
                throw new RuntimeException(XLog.format("Log4j configuration [{0}] not found in classpath", log4jFile));
            }
            configFile = log4jFile;
            fromClasspath = true;
        }
        return fromClasspath;
    }

    /**
     * Instruments the log service.
     * <p/>
     * It sets instrumentation variables indicating the config file, reload interval and if loaded from the classpath.
     *
     * @param instr instrumentation to use.
     */
    public void instrument(Instrumentation instr) {
        instr.addVariable("oozie", "version", new Instrumentation.Variable<String>() {
            public String getValue() {
                return BuildInfo.getBuildInfo().getProperty(BuildInfo.BUILD_VERSION);
            }
        });
        instr.addVariable(INSTRUMENTATION_GROUP, "config.file", new Instrumentation.Variable<String>() {
            public String getValue() {
                return configFile;
            }
        });
        instr.addVariable(INSTRUMENTATION_GROUP, "reload.interval", new Instrumentation.Variable<Long>() {
            public Long getValue() {
                return interval;
            }
        });
        instr.addVariable(INSTRUMENTATION_GROUP, "from.classpath", new Instrumentation.Variable<Boolean>() {
            public Boolean getValue() {
                return fromClasspath;
            }
        });
    }

    /**
     * Stream the log of a job.
     *
     * @param filter log streamer filter.
     * @param startTime start time for log events to filter.
     * @param endTime end time for log events to filter.
     * @param writer writer to stream the log to.
     * @throws IOException thrown if the log cannot be streamed.
     */
    public void streamLog(XLogStreamer.Filter filter, Date startTime, Date endTime, Writer writer) throws IOException {
        if (oozieLogPath != null) {
            new XLogStreamer(filter, writer, oozieLogPath, oozieLogName, oozieLogRotation).streamLog(startTime,
                                                                                                              endTime);
        }
        else {
            writer.write("Log streaming disabled!!");
        }

    }

}