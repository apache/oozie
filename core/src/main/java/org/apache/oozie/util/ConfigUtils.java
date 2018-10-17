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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.StatusTransitService;
import org.apache.oozie.servlet.ServicesLoader;

/**
 *
 */
public class ConfigUtils {
    private final static XLog LOG = XLog.getLog(ConfigUtils.class);
    public static final String OOZIE_HTTPS_ENABLED = "oozie.https.enabled";
    public static final String OOZIE_HTTP_HOSTNAME = "oozie.http.hostname";
    public static final String OOZIE_HTTPS_PORT = "oozie.https.port";
    public static final String OOZIE_HTTP_PORT = "oozie.http.port";

    public static boolean BOOLEAN_DEFAULT = false;
    public static String STRING_DEFAULT = "";
    public static int INT_DEFAULT = 0;
    public static float FLOAT_DEFAULT = 0f;
    public static long LONG_DEFAULT = 0l;

    /**
     * Fetches a property using both a deprecated name and the new name. The deprecated property
     * has precedence over the new name. If the deprecated name is used a warning is written to
     * the log.
     *
     * @param conf configuration object.
     * @param newName new property name.
     * @param oldName deprecated property name.
     * @param defaultValue default value.
     * @return the property value, or the default value if not found under the deprecated name and the new name.
     */
    public static String getWithDeprecatedCheck(Configuration conf, String newName, String oldName,
                                                String defaultValue) {
        String value = conf.get(oldName, null);
        if (value == null) {
            value = conf.get(newName, defaultValue);
        }
        else {
            LOG.warn("Using a deprecated configuration property [{0}], should use [{1}].  " +
                     "Please delete the deprecated property in order for the new property to take effect.",
                     oldName, newName);
        }
        return value;
    }

    /**
     * Fetches a property using both a deprecated name and the new name. The deprecated property
     * has precedence over the new name. If the deprecated name is used a warning is written to
     * the log.
     *
     * @param conf configuration object.
     * @param newName new property name.
     * @param oldName deprecated property name.
     * @param defaultValue default value.
     * @return the property value, or the default value if not found under the deprecated name and the new name.
     */
    public static boolean getWithDeprecatedCheck(Configuration conf, String newName, String oldName,
                                                 boolean defaultValue) {
        String value = getWithDeprecatedCheck(conf, newName, oldName, Boolean.toString(defaultValue));
        return Boolean.parseBoolean(value);
    }

    /**
     * Returns the HTTP or HTTPS URL for this Oozie server
     * (http://HOSTNAME:HTTP_PORT/oozie or https://HOSTNAME:HTTPS_PORT/oozie)
     *
     * @param secure true to return the HTTPS URL or false to return the HTTP URL
     * @return the HTTP or HTTPS URL for this Oozie server
     */
    public static String getOozieURL(boolean secure) {
        StringBuilder sb = new StringBuilder();
        if (secure) {
            sb.append("https://");
        }
        else {
            sb.append("http://");
        }
        sb.append(ConfigurationService.get(OOZIE_HTTP_HOSTNAME));
        sb.append(":");
        if (secure) {
            sb.append(ConfigurationService.get(OOZIE_HTTPS_PORT));
        }
        else {
            sb.append(ConfigurationService.get(OOZIE_HTTP_PORT));
        }
        sb.append("/oozie");
        return sb.toString();
    }

    /**
     * Returns the HTTP or HTTPS URL for this Oozie server depending on which is actually configured
     *
     * @return the HTTP or HTTPS URL for this Oozie server
     */
    public static String getOozieEffectiveUrl() {
        return getOozieURL(ServicesLoader.isSSLEnabled() || ConfigurationService.getBoolean(OOZIE_HTTPS_ENABLED));
    }

    public static boolean isBackwardSupportForCoordStatus() {
        return ConfigurationService.getBoolean(StatusTransitService.CONF_BACKWARD_SUPPORT_FOR_COORD_STATUS);
    }

    /**
     * Check {@link Configuration} whether it contains disallowed properties. Given configuration property values of
     * {@code oozie-site.xml#oozie.configuration.check-and-set.DISALLOWED.PROPERTY_KEY}, {@code newValue} and {@code performWrite}
     * values, one of the following will happen:
     * <ul>
     *     <li>{@code base} doesn't contain any disallowed property: NOP</li>
     *     <li>{@code base} contains a disallowed property but its value is the same as {@code newValue}: NOP</li>
     *     <li>{@code base} contains a conflicting disallowed property and configuration tells we should also set, but
     *     {@code performWrite=false}: NOP</li>
     *     <li>{@code base} contains a conflicting disallowed property and configuration tells we should also set, and
     *     {@code performWrite=true}: {@code base} will be overwritten by {@code key=newValue}</li>
     *     <li>{@code base} contains a conflicting disallowed property and configuration tells we should not set:
     *     {@code Exception toThrow} is thrown</li>
     * </ul>
     * @param base the {@link Configuration} to check
     * @param newValue the new value to assign if {@code performWrite=true} and if configuration value
     * {@code oozie.configuration.check-and-set.*} is set
     * @param toThrow the {@link Exception} to throw when {@code oozie.configuration.check-and-set.*} is unset
     * @param performWrite
     * @param <E> {@link Exception} type
     * @throws E the {@link Exception} to throw when {@code oozie.configuration.check-and-set.*} is unset
     */
    public static <E extends Exception> void checkAndSetDisallowedProperties(final Configuration base,
                                                                             final String newValue,
                                                                             final E toThrow,
                                                                             final boolean performWrite) throws E {
        Preconditions.checkNotNull(base, "base");
        Preconditions.checkNotNull(base, "newValue");
        Preconditions.checkNotNull(base, "toThrow");

        for (final String defaultDisallowedProperty : PropertiesUtils.DEFAULT_DISALLOWED_PROPERTIES) {
            checkAndSetConfigValue(base, defaultDisallowedProperty, newValue, toThrow, performWrite);
        }
    }

    private static <E extends Exception> void checkAndSetConfigValue(final Configuration base,
                                                                     final String key,
                                                                     final String newValue,
                                                                     final E toThrow,
                                                                     final boolean performWrite) throws E {
        final boolean shouldCheckAndSet = ConfigurationService.getBoolean("oozie.configuration.check-and-set." + key, false);
        final boolean isPresent = base.get(key) != null;
        if (isPresent && !base.get(key).equals(newValue)) {
            LOG.trace("Base configuration contains config property [{0}={1}] that is different from new value [{2}]",
                    key,
                    base.get(key),
                    newValue);
            if (shouldCheckAndSet && performWrite) {
                LOG.trace("Setting [{0}] to [{1}]", key, newValue);
                base.set(key, newValue);
            }
            else if (!shouldCheckAndSet) {
                LOG.error("Cannot set [{0}] to [{1}]. {2}", key, newValue, toThrow.getMessage());
                throw toThrow;
            }
        }
    }
}
