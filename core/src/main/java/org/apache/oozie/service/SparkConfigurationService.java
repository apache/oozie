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

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SparkConfigurationService implements Service {

    private static XLog LOG = XLog.getLog(SparkConfigurationService.class);

    public static final String SPARK_CONFIGURATION = "oozie.service.SparkConfigurationService.spark.configurations";

    private Map<String, Map<String, String>> sparkConfigs;
    private static final String SPARK_CONFIG_FILE = "spark-defaults.conf";

    @Override
    public void init(Services services) throws ServiceException {
        loadSparkConfigs();
    }

    @Override
    public void destroy() {
        sparkConfigs.clear();
    }

    @Override
    public Class<? extends Service> getInterface() {
        return SparkConfigurationService.class;
    }

    private void loadSparkConfigs() throws ServiceException {
        sparkConfigs = new HashMap<String, Map<String, String>>();
        File configDir = new File(ConfigurationService.getConfigurationDirectory());
        String[] confDefs = ConfigurationService.getStrings(SPARK_CONFIGURATION);
        if (confDefs != null) {
            for (String confDef : confDefs) {
                if (confDef.trim().length() > 0) {
                    String[] parts = confDef.split("=");
                    if (parts.length == 2) {
                        String hostPort = parts[0];
                        String confDir = parts[1];
                        File dir = new File(confDir);
                        if (!dir.isAbsolute()) {
                            dir = new File(configDir, confDir);
                        }
                        if (dir.exists()) {
                            File file = new File(dir, SPARK_CONFIG_FILE);
                            if (file.exists()) {
                                Properties props = new Properties();
                                FileReader fr = null;
                                try {
                                    fr = new FileReader(file);
                                    props.load(fr);
                                    fr.close();
                                    sparkConfigs.put(hostPort, propsToMap(props));
                                    LOG.info("Loaded Spark Configuration: {0}={1}", hostPort, file.getAbsolutePath());
                                } catch (IOException ioe) {
                                    LOG.warn("Spark Configuration could not be loaded for {0}: {1}",
                                            hostPort, ioe.getMessage(), ioe);
                                } finally {
                                    IOUtils.closeSafely(fr);
                                }
                            } else {
                                LOG.warn("Spark Configuration could not be loaded for {0}: {1} does not exist",
                                        hostPort, file.getAbsolutePath());
                            }
                        } else {
                            LOG.warn("Spark Configuration could not be loaded for {0}: {1} does not exist",
                                    hostPort, dir.getAbsolutePath());
                        }
                    } else {
                        LOG.warn("Spark Configuration could not be loaded: invalid value found: {0}", confDef);
                    }
                }
            }
        } else {
            LOG.info("Spark Configuration(s) not specified");
        }
    }

    private Map<String, String> propsToMap(Properties props) {
        Map<String, String> map = new HashMap<String, String>(props.size());
        for (String key : props.stringPropertyNames()) {
            map.put(key, props.getProperty(key));
        }
        return map;
    }

    public Map<String, String> getSparkConfig(String resourceManagerHostPort) {
        resourceManagerHostPort = (resourceManagerHostPort != null) ? resourceManagerHostPort.toLowerCase() : null;
        Map<String, String> config = sparkConfigs.get(resourceManagerHostPort);
        if (config == null) {
            config = sparkConfigs.get("*");
            if (config == null) {
                config = new HashMap<String, String>();
            }
        }
        return config;
    }
}

