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

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.servlet.ServicesLoader;

/**
 *
 */
public class ConfigUtils {
    private final static XLog LOG = XLog.getLog(ConfigUtils.class);

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
        sb.append(System.getProperty("oozie.http.hostname"));
        sb.append(":");
        sb.append(System.getProperty("oozie.http.port"));
        sb.append("/oozie");
        return sb.toString();
    }

    /**
     * Returns the HTTP or HTTPS URL for this Oozie server depending on which is actually configured
     *
     * @return the HTTP or HTTPS URL for this Oozie server
     */
    public static String getOozieEffectiveUrl() {
        return getOozieURL(ServicesLoader.isSSLEnabled());
    }
}
