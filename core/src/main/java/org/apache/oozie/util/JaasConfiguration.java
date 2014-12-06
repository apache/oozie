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

import java.util.HashMap;
import java.util.Map;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;


/**
 * Creates a programmatic version of a jaas.conf file.  This can be used instead of writing a jaas.conf file and setting
 * the system property, "java.security.auth.login.config", to point to that file.  It is meant to be used for connecting to
 * ZooKeeper.
 * <p>
 * example usage:
 * JaasConfiguration.addEntry("Client", principal, keytabFile);
 * javax.security.auth.login.Configuration.setConfiguration(JaasConfiguration.getInstance());
 */
public class JaasConfiguration extends Configuration {
    private static Map<String, AppConfigurationEntry> entries = new HashMap<String, AppConfigurationEntry>();
    private static JaasConfiguration me = null;
    private static final String krb5LoginModuleName;

    static  {
        if (System.getProperty("java.vendor").contains("IBM")) {
            krb5LoginModuleName = "com.ibm.security.auth.module.Krb5LoginModule";
        }
        else {
            krb5LoginModuleName = "com.sun.security.auth.module.Krb5LoginModule";
        }
    }

    private JaasConfiguration() {
        // don't need to do anything here but we want to make it private
    }

    /**
     * Return the singleton.  You'd typically use it only to do this:
     * <p>
     * javax.security.auth.login.Configuration.setConfiguration(JaasConfiguration.getInstance());
     *
     * @return
     */
    public static Configuration getInstance() {
        if (me == null) {
            me = new JaasConfiguration();
        }
        return me;
    }

    /**
     * Add an entry to the jaas configuration with the passed in name, principal, and keytab.  The other necessary options will be
     * set for you.
     *
     * @param name The name of the entry (e.g. "Client")
     * @param principal The principal of the user
     * @param keytab The location of the keytab
     */
    public static void addEntry(String name, String principal, String keytab) {
        Map<String, String> options = new HashMap<String, String>();
        options.put("keyTab", keytab);
        options.put("principal", principal);
        options.put("useKeyTab", "true");
        options.put("storeKey", "true");
        options.put("useTicketCache", "false");
        AppConfigurationEntry entry = new AppConfigurationEntry(krb5LoginModuleName,
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options);
        entries.put(name, entry);
    }

    /**
     * Removes the specified entry.
     *
     * @param name  The name of the entry to remove
     */
    public static void removeEntry(String name) {
        entries.remove(name);
    }

    /**
     * Clears all entries.
     */
    public static void clearEntries() {
        entries.clear();
    }

    /**
     * Returns the entries map.
     *
     * @return the entries map
     */
    public static Map<String, AppConfigurationEntry> getEntries() {
        return entries;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        return new AppConfigurationEntry[]{entries.get(name)};
    }
}
