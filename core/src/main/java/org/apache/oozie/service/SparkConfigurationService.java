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

import org.apache.oozie.util.XLog;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class SparkConfigurationService implements Service {

    private static XLog LOG = XLog.getLog(SparkConfigurationService.class);

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "SparkConfigurationService.";
    public static final String SPARK_CONFIGURATIONS = CONF_PREFIX + "spark.configurations";
    public static final String SPARK_CONFIGURATIONS_IGNORE_SPARK_YARN_JAR
            = SPARK_CONFIGURATIONS + ".ignore.spark.yarn.jar";
    public static final String SPARK_CONFIGURATIONS_BLACKLIST = SPARK_CONFIGURATIONS + ".blacklist";

    private static final String SPARK_CONFIG_FILE = "spark-defaults.conf";
    private static final String SPARK_YARN_JAR_PROP = "spark.yarn.jar";
    private static final String HOST_WILDCARD = "*";
    private Map<String, Properties> sparkConfigs;
    private Set<String> blacklist;

    @Override
    public void init(Services services) throws ServiceException {
        loadBlacklist();
        loadSparkConfigs();
    }

    @Override
    public void destroy() {
        sparkConfigs.clear();
        blacklist.clear();
    }

    @Override
    public Class<? extends Service> getInterface() {
        return SparkConfigurationService.class;
    }

    private void loadBlacklist() {
        blacklist = new HashSet<>();
        for(String s : ConfigurationService.getStrings(SPARK_CONFIGURATIONS_BLACKLIST)) {
            blacklist.add(s.trim());
        }
        // spark.yarn.jar is added if the old property to ignore it is set.
        if(ConfigurationService.getBoolean(SPARK_CONFIGURATIONS_IGNORE_SPARK_YARN_JAR)){
            LOG.warn("Deprecated property found in configuration: " + SPARK_CONFIGURATIONS_IGNORE_SPARK_YARN_JAR +
                    "Use "+SPARK_CONFIGURATIONS_BLACKLIST+" instead.");
            blacklist.add(SPARK_YARN_JAR_PROP);
        }
    }

    private void loadSparkConfigs() throws ServiceException {
        sparkConfigs = new HashMap<>();
        String[] confDefs = ConfigurationService.getStrings(SPARK_CONFIGURATIONS);
        for (String confDef : confDefs) {
            readEntry(confDef.trim());
        }
    }

    private void readEntry(String confDef) throws ServiceException {
        String[] parts = confDef.split("=");
        if (parts.length == 2) {
            String hostPort = parts[0];
            String confDir = parts[1];
            File dir = getAbsoluteDir(confDir);
            if (dir.exists()) {
                Properties sparkDefaults = readSparkConfigFile(hostPort, dir);
                filterBlackList(sparkDefaults);
                if(!sparkDefaults.isEmpty()) {
                    sparkConfigs.put(hostPort, sparkDefaults);
                }
            } else {
                LOG.warn("Spark Configuration could not be loaded for {0}: {1} does not exist",
                        hostPort, dir.getAbsolutePath());
            }
        } else {
            LOG.warn("Spark Configuration could not be loaded: invalid value found: {0}", confDef);
        }
    }

    private File getAbsoluteDir(String confDir) throws ServiceException {
        File dir = new File(confDir);
        if (!dir.isAbsolute()) {
            File configDir = new File(ConfigurationService.getConfigurationDirectory());
            dir = new File(configDir, confDir);
        }
        return dir;
    }

    private void filterBlackList(Properties sparkDefaults) {
        for(String property : blacklist){
            sparkDefaults.remove(property);
        }
    }

    private Properties readSparkConfigFile(String hostPort, File dir) {
        File file = new File(dir, SPARK_CONFIG_FILE);
        Properties props = new Properties();
        if (file.exists()) {
            try (FileInputStream stream = new FileInputStream(file);
                 InputStreamReader reader = new InputStreamReader(stream, StandardCharsets.UTF_8.name())) {
                props.load(reader);
                LOG.info("Loaded Spark Configuration: {0}={1}", hostPort, file.getAbsolutePath());
            } catch (IOException ioe) {
                LOG.warn("Spark Configuration could not be loaded for {0}: {1}",
                        hostPort, ioe.getMessage(), ioe);
            }
        } else {
            LOG.warn("Spark Configuration could not be loaded for {0}: {1} does not exist",
                    hostPort, file.getAbsolutePath());
        }
        return props;
    }

    public Properties getSparkConfig(String resourceManagerHostPort) {
        resourceManagerHostPort = (resourceManagerHostPort != null) ? resourceManagerHostPort.toLowerCase() : null;
        Properties config = sparkConfigs.get(resourceManagerHostPort);
        if (config == null) {
            config = sparkConfigs.get(HOST_WILDCARD);
            if (config == null) {
                config = new Properties();
            }
        }
        return config;
    }
}

