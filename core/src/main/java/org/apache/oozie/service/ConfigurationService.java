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
import org.apache.oozie.util.ConfigUtils;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Arrays;

import org.apache.oozie.util.ZKUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * Built in service that initializes the services configuration.
 * <p/>
 * The configuration loading sequence is identical to Hadoop configuration loading sequence.
 * <p/>
 * Default values are loaded from the 'oozie-default.xml' file from the classpath, then site configured values
 * are loaded from a site configuration file from the Oozie configuration directory.
 * <p/>
 * The Oozie configuration directory is resolved using the <code>OOZIE_HOME<code> environment variable as
 * <code>${OOZIE_HOME}/conf</code>. If the <code>OOZIE_HOME<code> environment variable is not defined the
 * initialization of the <code>ConfigurationService</code> fails.
 * <p/>
 * The site configuration is loaded from the <code>oozie-site.xml</code> file in the configuration directory.
 * <p/>
 * The site configuration file name to use can be changed by setting the <code>OOZIE_CONFIG_FILE</code> environment
 * variable to an alternate file name. The alternate file must ber in the Oozie configuration directory.
 * <p/>
 * Configuration properties, prefixed with 'oozie.', passed as system properties overrides default and site values.
 * <p/>
 * The configuration service logs details on how the configuration was loaded as well as what properties were overrode
 * via system properties settings.
 */
public class ConfigurationService implements Service, Instrumentable {
    private static final String INSTRUMENTATION_GROUP = "configuration";

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "ConfigurationService.";

    public static final String CONF_IGNORE_SYS_PROPS = CONF_PREFIX + "ignore.system.properties";

    public static final String CONF_VERIFY_AVAILABLE_PROPS = CONF_PREFIX + "verify.available.properties";

    /**
     * System property that indicates the configuration directory.
     */
    public static final String OOZIE_CONFIG_DIR = "oozie.config.dir";


    /**
     * System property that indicates the data directory.
     */
    public static final String OOZIE_DATA_DIR = "oozie.data.dir";

    /**
     * System property that indicates the name of the site configuration file to load.
     */
    public static final String OOZIE_CONFIG_FILE = "oozie.config.file";

    private static final Set<String> IGNORE_SYS_PROPS = new HashSet<String>();
    private static final Set<String> CONF_SYS_PROPS = new HashSet<String>();

    private static final String IGNORE_TEST_SYS_PROPS = "oozie.test.";
    private static final Set<String> MASK_PROPS = new HashSet<String>();
    private static Map<String,String> defaultConfigs = new HashMap<String,String>();

    static {

        //all this properties are seeded as system properties, no need to log changes
        IGNORE_SYS_PROPS.add(CONF_IGNORE_SYS_PROPS);
        IGNORE_SYS_PROPS.add(Services.OOZIE_HOME_DIR);
        IGNORE_SYS_PROPS.add(OOZIE_CONFIG_DIR);
        IGNORE_SYS_PROPS.add(OOZIE_CONFIG_FILE);
        IGNORE_SYS_PROPS.add(OOZIE_DATA_DIR);
        IGNORE_SYS_PROPS.add(XLogService.OOZIE_LOG_DIR);
        IGNORE_SYS_PROPS.add(XLogService.LOG4J_FILE);
        IGNORE_SYS_PROPS.add(XLogService.LOG4J_RELOAD);

        CONF_SYS_PROPS.add("oozie.http.hostname");
        CONF_SYS_PROPS.add("oozie.http.port");
        CONF_SYS_PROPS.add(ZKUtils.OOZIE_INSTANCE_ID);

        // These properties should be masked when displayed because they contain sensitive info (e.g. password)
        MASK_PROPS.add(JPAService.CONF_PASSWORD);
        MASK_PROPS.add("oozie.authentication.signature.secret");
    }

    public static final String DEFAULT_CONFIG_FILE = "oozie-default.xml";
    public static final String SITE_CONFIG_FILE = "oozie-site.xml";

    private static XLog log = XLog.getLog(ConfigurationService.class);

    private String configDir;
    private String configFile;

    private LogChangesConfiguration configuration;

    public ConfigurationService() {
        log = XLog.getLog(ConfigurationService.class);
    }

    /**
     * Initialize the log service.
     *
     * @param services services instance.
     * @throws ServiceException thrown if the log service could not be initialized.
     */
    public void init(Services services) throws ServiceException {
        configDir = getConfigurationDirectory();
        configFile = System.getProperty(OOZIE_CONFIG_FILE, SITE_CONFIG_FILE);
        if (configFile.contains("/")) {
            throw new ServiceException(ErrorCode.E0022, configFile);
        }
        log.info("Oozie home dir  [{0}]", Services.getOozieHome());
        log.info("Oozie conf dir  [{0}]", configDir);
        log.info("Oozie conf file [{0}]", configFile);
        configFile = new File(configDir, configFile).toString();
        configuration = loadConf();
        if (configuration.getBoolean(CONF_VERIFY_AVAILABLE_PROPS, false)) {
            verifyConfigurationName();
        }
    }

    public static String getConfigurationDirectory() throws ServiceException {
        String oozieHome = Services.getOozieHome();
        String configDir = System.getProperty(OOZIE_CONFIG_DIR, null);
        File file = configDir == null
                ? new File(oozieHome, "conf")
                : new File(configDir);
        if (!file.exists()) {
            throw new ServiceException(ErrorCode.E0024, configDir);
        }
        return file.getPath();
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

    /**
     * Return Oozie configuration directory.
     *
     * @return Oozie configuration directory.
     */
    public String getConfigDir() {
        return configDir;
    }

    private InputStream getDefaultConfiguration() throws ServiceException, IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream(DEFAULT_CONFIG_FILE);
        if (inputStream == null) {
            throw new ServiceException(ErrorCode.E0023, DEFAULT_CONFIG_FILE);
        }
        return inputStream;
    }

    private LogChangesConfiguration loadConf() throws ServiceException {
        XConfiguration configuration;
        try {
            InputStream inputStream = getDefaultConfiguration();
            configuration = loadConfig(inputStream, true);
            File file = new File(configFile);
            if (!file.exists()) {
                log.info("Missing site configuration file [{0}]", configFile);
            }
            else {
                inputStream = new FileInputStream(configFile);
                XConfiguration siteConfiguration = loadConfig(inputStream, false);
                XConfiguration.injectDefaults(configuration, siteConfiguration);
                configuration = siteConfiguration;
            }
        }
        catch (IOException ex) {
            throw new ServiceException(ErrorCode.E0024, configFile, ex.getMessage(), ex);
        }

        if (log.isTraceEnabled()) {
            try {
                StringWriter writer = new StringWriter();
                for (Map.Entry<String, String> entry : configuration) {
                    String value = getValue(configuration, entry.getKey());
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
                log.info("Configuration change via System Property, [{0}]=[{1}]", entry.getKey(), sysValue);
                configuration.set(entry.getKey(), sysValue);
            }
        }
        for (Map.Entry<Object, Object> entry : System.getProperties().entrySet()) {
            String name = (String) entry.getKey();
            if (!IGNORE_SYS_PROPS.contains(name)) {
                if (name.startsWith("oozie.") && !name.startsWith(IGNORE_TEST_SYS_PROPS)) {
                    if (configuration.get(name) == null) {
                        log.warn("System property [{0}] no defined in Oozie configuration, ignored", name);
                    }
                }
            }
        }

        //Backward compatible, we should still support -Dparam.
        for (String key : CONF_SYS_PROPS) {
            String sysValue = System.getProperty(key);
            if (sysValue != null && !IGNORE_SYS_PROPS.contains(key)) {
                log.info("Overriding configuration with system property. Key [{0}], Value [{1}] ", key, sysValue);
                configuration.set(key, sysValue);
            }
        }

        return new LogChangesConfiguration(configuration);
    }

    private XConfiguration loadConfig(InputStream inputStream, boolean defaultConfig) throws IOException, ServiceException {
        XConfiguration configuration;
        configuration = new XConfiguration(inputStream);
        for(Map.Entry<String,String> entry: configuration) {
            if (defaultConfig) {
                defaultConfigs.put(entry.getKey(), entry.getValue());
            }
            else {
                log.debug("Overriding configuration with oozie-site, [{0}]", entry.getKey());
            }
        }
        return configuration;
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

        public String[] getStrings(String name, String[] defaultValue) {
            String s = get(name);
            if (s == null) {
                log.debug(XLog.OPS, "Configuration property [{0}] not found, use given value [{1}]", name,
                        Arrays.asList(defaultValue).toString());
            }
            return (s != null && s.trim().length() > 0) ? super.getStrings(name) : defaultValue;
        }

        public String get(String name, String defaultValue) {
            String value = get(name);
            if (value == null) {
                boolean maskValue = MASK_PROPS.contains(name);
                value = defaultValue;
                String logValue = (maskValue) ? "**MASKED**" : defaultValue;
                log.debug(XLog.OPS, "Configuration property [{0}] not found, use given value [{1}]", name, logValue);
            }
            return value;
        }

        public void set(String name, String value) {
            setValue(name, value);
            boolean maskValue = MASK_PROPS.contains(name);
            value = (maskValue) ? "**MASKED**" : value;
            log.info(XLog.OPS, "Programmatic configuration change, property[{0}]=[{1}]", name, value);
        }

        public boolean getBoolean(String name, boolean defaultValue) {
            String value = get(name);
            if (value == null) {
                log.debug(XLog.OPS, "Configuration property [{0}] not found, use given value [{1}]", name, defaultValue);
            }
            return super.getBoolean(name, defaultValue);
        }

        public int getInt(String name, int defaultValue) {
            String value = get(name);
            if (value == null) {
                log.debug(XLog.OPS, "Configuration property [{0}] not found, use given value [{1}]", name, defaultValue);
            }
            return super.getInt(name, defaultValue);
        }

        public long getLong(String name, long defaultValue) {
            String value = get(name);
            if (value == null) {
                log.debug(XLog.OPS, "Configuration property [{0}] not found, use given value [{1}]", name, defaultValue);
            }
            return super.getLong(name, defaultValue);
        }

        public float getFloat(String name, float defaultValue) {
            String value = get(name);
            if (value == null) {
                log.debug(XLog.OPS, "Configuration property [{0}] not found, use given value [{1}]", name, defaultValue);
            }
            return super.getFloat(name, defaultValue);
        }

        public Class<?>[] getClasses(String name, Class<?> ... defaultValue) {
            String value = get(name);
            if (value == null) {
                log.debug(XLog.OPS, "Configuration property [{0}] not found, use given value [{1}]", name, defaultValue);
            }
            return super.getClasses(name, defaultValue);
        }

        public Class<?> getClass(String name, Class<?> defaultValue) {
            String value = get(name);
            if (value == null) {
                log.debug(XLog.OPS, "Configuration property [{0}] not found, use given value [{1}]", name, defaultValue);
                return defaultValue;
            }
            try {
                return getClassByName(value);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
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
    }

    /**
     * Return a configuration with all sensitive values masked.
     *
     * @return masked configuration.
     */
    public Configuration getMaskedConfiguration() {
        XConfiguration maskedConf = new XConfiguration();
        Configuration conf = getConf();
        for (Map.Entry<String, String> entry : conf) {
            String name = entry.getKey();
            String value = getValue(conf, name);
            maskedConf.set(name, value);
        }
        return maskedConf;
    }

    private String getValue(Configuration config, String key) {
        String value;
        if (MASK_PROPS.contains(key)) {
            value = "**MASKED**";
        }
        else {
            value = config.get(key);
        }
        return value;
    }


    /**
     * Gets the oozie configuration value in oozie-default.
     * @param name
     * @return the configuration value of the <code>name</code> otherwise null
     */
    private String getDefaultOozieConfig(String name) {
        return defaultConfigs.get(name);
    }

    /**
     * Verify the configuration is in oozie-default
     */
    public void verifyConfigurationName() {
        for (Map.Entry<String, String> entry: configuration) {
            if (getDefaultOozieConfig(entry.getKey()) == null) {
                log.warn("Invalid configuration defined, [{0}] ", entry.getKey());
            }
        }
    }

    @VisibleForTesting
    public static void set(String name, String value) {
        Configuration conf = Services.get().getConf();
        conf.set(name, value);
    }

    @VisibleForTesting
    public static void setBoolean(String name, boolean value) {
        Configuration conf = Services.get().getConf();
        conf.setBoolean(name, value);
    }

    public static String get(String name) {
        Configuration conf = Services.get().getConf();
        return get(conf, name);
    }

    public static String get(Configuration conf, String name) {
        return conf.get(name, ConfigUtils.STRING_DEFAULT);
    }

    public static String[] getStrings(String name) {
        Configuration conf = Services.get().getConf();
        return getStrings(conf, name);
    }

    public static String[] getStrings(Configuration conf, String name) {
        return conf.getStrings(name, new String[0]);
    }

    public static boolean getBoolean(String name) {
        Configuration conf = Services.get().getConf();
        return getBoolean(conf, name);
    }

    public static boolean getBoolean(Configuration conf, String name) {
        return conf.getBoolean(name, ConfigUtils.BOOLEAN_DEFAULT);
    }

    public static int getInt(String name) {
        Configuration conf = Services.get().getConf();
        return getInt(conf, name);
    }

    public static int getInt(Configuration conf, String name) {
        return conf.getInt(name, ConfigUtils.INT_DEFAULT);
    }

    public static float getFloat(String name) {
        Configuration conf = Services.get().getConf();
        return conf.getFloat(name, ConfigUtils.FLOAT_DEFAULT);
    }

    public static long getLong(String name) {
        Configuration conf = Services.get().getConf();
        return getLong(conf, name);
    }

    public static long getLong(Configuration conf, String name) {
        return conf.getLong(name, ConfigUtils.LONG_DEFAULT);
    }

    public static Class<?>[] getClasses(String name) {
        Configuration conf = Services.get().getConf();
        return getClasses(conf, name);
    }

    public static Class<?>[] getClasses(Configuration conf, String name) {
        return conf.getClasses(name);
    }

    public static Class<?> getClass(Configuration conf, String name) {
        return conf.getClass(name, Object.class);
    }

}
