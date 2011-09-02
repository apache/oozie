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

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.util.Instrumentable;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.ErrorCode;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URL;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Arrays;

/**
 * Built in service that initializes the services configuration. <p/> The configuration loading sequence is identical to
 * Hadoop configuration loading sequence. <p/> First the default values are loaded from the {@link
 * #DEFAULT_CONFIG_FILE}, then the site configured values are loaded from the site configuration file. <p/> The {@link
 * #DEFAULT_CONFIG_FILE} is always loaded from the classpath root. <p/> The site configuration file and loading location
 * is determined as follow: <p/> The site configuration file name is set by the system property {@link #CONFIG_FILE}.
 * The default value is {@link #SITE_CONFIG_FILE}. <p/> The site configuration file is loaded from the directory
 * specified by the system property {@link #CONFIG_PATH}. If not set, no site configuration is loaded. <p/>
 * Configuration properties, prefixed with 'oozie.', passed as system properties overrides default and site values. <p/>
 * The configuration service logs details on how the configuration was loaded as well as what properties were overriden
 * via system properties settings.
 */
public class ConfigurationService implements Service, Instrumentable {
    private static final String INSTRUMENTATION_GROUP = "configuration";

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "ConfigurationService.";

    public static final String CONF_IGNORE_SYS_PROPS = CONF_PREFIX + "ignore.system.properties";

    /**
     * System property that indicates the path where the site configuration should be loaded from.
     */
    public static final String CONFIG_PATH = "oozie.config.dir";

    /**
     * System property that indicates the name of the site configuration file to load.
     */
    public static final String CONFIG_FILE = "oozie.config.file";

    private static final Set<String> IGNORE_SYS_PROPS = new HashSet<String>();
    private static final String IGNORE_SYS_PROPS_PREFIX = "oozie.test.";

    private static final String MASK_PROPS_VALUES_POSTIX = ".password";

    static {
        IGNORE_SYS_PROPS.add(CONFIG_FILE);
        IGNORE_SYS_PROPS.add(CONFIG_PATH);
        IGNORE_SYS_PROPS.add(XLogService.LOG4J_FILE);
        IGNORE_SYS_PROPS.add(XLogService.RELOAD_INTERVAL);
        IGNORE_SYS_PROPS.add(CONF_IGNORE_SYS_PROPS);
    }

    //for testing purposes
    static boolean testingDefaultFile;

    public static final String DEFAULT_CONFIG_FILE = "oozie-default.xml";
    public static final String SITE_CONFIG_FILE = "oozie-site.xml";

    private final XLog log = XLog.getLog(getClass());

    private String configDir;
    private String configFile;

    private LogChangesConfiguration configuration;

    public ConfigurationService() {
        testingDefaultFile = false;
    }

    /**
     * Initialize the log service.
     *
     * @param services services instance.
     * @throws ServiceException thrown if the log service could not be initialized.
     */
    public void init(Services services) throws ServiceException {
        configDir = System.getProperty(CONFIG_PATH);
        configFile = System.getProperty(CONFIG_FILE, SITE_CONFIG_FILE);
        configuration = loadConf();
    }

    /**
     * Destroy the configuration service.
     */
    public void destroy() {
        configuration = null;
    }

    /**
     * Return the public interface for configuration service.
     *
     * @return {@link ConfigurationService}.
     */
    public Class<? extends Service> getInterface() {
        return ConfigurationService.class;
    }

    /**
     * Return the services configuration.
     *
     * @return the services configuration.
     */
    public Configuration getConf() {
        if (configuration == null) {
            throw new IllegalStateException("Not initialized");
        }
        return configuration;
    }

    private InputStream getConfFileFromClassPath(String configFile) throws IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream(configFile);
        if (inputStream != null) {
            URL fileLocation = classLoader.getResource(configFile);
            log.info("Loading configuration file [{0}] from classpath [{1}]", configFile, fileLocation);
        }
        else {
            log.warn("Configuration file [{0}] not available in classpath", configFile);
        }
        return inputStream;
    }

    private InputStream getConfFile(String configFile) throws ServiceException, IOException {
        if (configDir == null) {
            return getConfFileFromClassPath(configFile);
        }
        else {
            if (!configDir.startsWith("/")) {
                throw new ServiceException(ErrorCode.E0020, configDir);
            }
            if (configFile.contains("/")) {
                throw new ServiceException(ErrorCode.E0022, configFile);
            }
            File file = new File(configDir, configFile);
            if (file.exists()) {
                log.info("Loading configuration file from path [{0}]", file.getAbsolutePath());
                return new FileInputStream(file);
            }
            else {
                log.info("Configuration file [{0}] not available at [{1}]", configFile, configDir);
                return null;
            }
        }
    }

    private LogChangesConfiguration loadConf() throws ServiceException {
        XConfiguration configuration;
        try {
            String defaultConfig = (testingDefaultFile) ? "test-" + DEFAULT_CONFIG_FILE : DEFAULT_CONFIG_FILE;
            InputStream inputStream = getConfFileFromClassPath(defaultConfig);
            if (inputStream != null) {
                configuration = new XConfiguration(inputStream);
            }
            else {
                throw new ServiceException(ErrorCode.E0023, defaultConfig);
            }
            if (configFile != null) {
                inputStream = getConfFile(configFile);
                if (inputStream != null) {
                    XConfiguration siteConfiguration = new XConfiguration(inputStream);
                    XConfiguration.injectDefaults(configuration, siteConfiguration);
                    configuration = siteConfiguration;
                }
            }
        }
        catch (IOException ex) {
            throw new ServiceException(ErrorCode.E0024, configFile, ex.getMessage(), ex);
        }

        if (log.isTraceEnabled()) {
            try {
                StringWriter writer = new StringWriter();
                for (Map.Entry<String, String> entry : configuration) {
                    boolean maskValue = entry.getKey().endsWith(MASK_PROPS_VALUES_POSTIX);
                    String value = (maskValue) ? "**MASKED**" : entry.getValue();
                    writer.write(" " + entry.getKey() + " = " + value + "\n");
                }
                writer.close();
                log.trace("Configuration:\n{0}---", writer.toString());
            }
            catch (IOException ex) {
                throw new ServiceException(ErrorCode.E0025, ex.getMessage(), ex);
            }
        }

        String[] ignoreSysProps = configuration.getStrings(CONF_IGNORE_SYS_PROPS);
        if (ignoreSysProps != null) {
            IGNORE_SYS_PROPS.addAll(Arrays.asList(ignoreSysProps));
        }

        for (Map.Entry<String, String> entry : configuration) {
            String sysValue = System.getProperty(entry.getKey());
            if (sysValue != null && !IGNORE_SYS_PROPS.contains(entry.getKey())) {
                log.info("SysProps configuration change, property[{0}]=[{1}]", entry.getKey(), sysValue);
                configuration.set(entry.getKey(), sysValue);
            }
        }
        for (Map.Entry<Object, Object> entry : System.getProperties().entrySet()) {
            String name = (String) entry.getKey();
            if (IGNORE_SYS_PROPS.contains(name) && !name.startsWith(IGNORE_SYS_PROPS_PREFIX)) {
                log.warn("System property [{0}] in ignore list, ignored", name);
            }
            else {
                if (name.startsWith("oozie.")) {
                    if (configuration.get(name) == null) {
                        log.warn("System property [{0}] no defined in Oozie configuration, ignored", name);
                    }
                }
            }
        }

        return new LogChangesConfiguration(configuration);
    }

    private class LogChangesConfiguration extends XConfiguration {

        public LogChangesConfiguration(Configuration conf) {
            for (Map.Entry<String, String> entry : conf) {
                if (get(entry.getKey()) == null) {
                    setValue(entry.getKey(), entry.getValue());
                }
            }
        }

        public String[] getStrings(String name) {
            String s = get(name);
            return (s != null && s.trim().length() > 0) ? super.getStrings(name) : new String[0];
        }

        public String get(String name, String defaultValue) {
            String value = get(name);
            if (value == null) {
                boolean maskValue = name.endsWith(MASK_PROPS_VALUES_POSTIX);
                value = (maskValue) ? "**MASKED**" : defaultValue;
                log.warn(XLog.OPS, "Configuration property [{0}] not found, using default [{1}]", name, value);
            }
            return value;
        }

        public void set(String name, String value) {
            setValue(name, value);
            boolean maskValue = name.endsWith(MASK_PROPS_VALUES_POSTIX);
            value = (maskValue) ? "**MASKED**" : value;
            log.info(XLog.OPS, "Programmatic configuration change, property[{0}]=[{1}]", name, value);
        }

        private void setValue(String name, String value) {
            super.set(name, value);
        }

    }

    /**
     * Instruments the configuration service. <p/> It sets instrumentation variables indicating the config dir and
     * config file used.
     *
     * @param instr instrumentation to use.
     */
    public void instrument(Instrumentation instr) {
        instr.addVariable(INSTRUMENTATION_GROUP, "config.dir", new Instrumentation.Variable<String>() {
            public String getValue() {
                return configDir;
            }
        });
        instr.addVariable(INSTRUMENTATION_GROUP, "config.file", new Instrumentation.Variable<String>() {
            public String getValue() {
                return configFile;
            }
        });
        instr.setConfiguration(configuration);
    }

    /**
     * Return a configuration with all sensitive values masked.
     *
     * @param conf configuration to mask.
     * @return masked configuration.
     */
    public static Configuration maskSecretValues(Configuration conf) {
        XConfiguration maskedConf = new XConfiguration();
        for (Map.Entry<String, String> entry : conf) {
            String name = entry.getKey();
            boolean maskValue = name.endsWith(MASK_PROPS_VALUES_POSTIX);
            String value = (maskValue) ? "**MASKED**" : entry.getValue();
            maskedConf.set(name, value);
        }
        return maskedConf;
    }

}
