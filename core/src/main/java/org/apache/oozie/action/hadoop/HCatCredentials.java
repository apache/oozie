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

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.action.ActionExecutor.Context;
import org.apache.oozie.service.HCatAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XLog;

/**
 * Credentials implementation to store in jobConf, HCat-specific properties such as Principal and Uri
 * User specifies these credential properties along with the action configuration
 * The jobConf is used further to pass credentials to the tasks while running
 * Oozie server should be configured to use this Credentials class by including it via property
 * 'oozie.credentials.credentialclasses'
 * User can extend the parent class to implement own class as well
 * for handling custom token-based credentials and add to the above server property
 */
public class HCatCredentials implements CredentialsProvider {

    private static final String HCAT_METASTORE_PRINCIPAL = "hcat.metastore.principal";
    private static final String HCAT_METASTORE_URI = "hcat.metastore.uri";
    private static final String HIVE_METASTORE_PRINCIPAL = "hive.metastore.kerberos.principal";
    private static final String HIVE_METASTORE_URI = "hive.metastore.uris";
    private final static Configuration hiveConf = new Configuration(false);
    static {
        hiveConf.addResource("hive-site.xml");
    }

    @Override
    public void updateCredentials(Credentials credentials, Configuration config, CredentialsProperties props,
            Context context) throws Exception {
        try {

            String principal = getProperty(props.getProperties(), HCAT_METASTORE_PRINCIPAL, HIVE_METASTORE_PRINCIPAL);
            if (principal == null || principal.isEmpty()) {
                throw new CredentialException(ErrorCode.E0510,
                        HCAT_METASTORE_PRINCIPAL + " is required to get hcat credential");
            }

            String server = getProperty(props.getProperties(), HCAT_METASTORE_URI, HIVE_METASTORE_URI);
            if (server == null || server.isEmpty()) {
                throw new CredentialException(ErrorCode.E0510,
                        HCAT_METASTORE_URI + " is required to get hcat credential");
            }
            HCatCredentialHelper hcch = new HCatCredentialHelper();
            hcch.set(credentials, config, principal, server);
        }
        catch (Exception e) {
            XLog.getLog(getClass()).warn("Exception in updateCredentials", e);
            throw e;
        }
    }

    /**
     * Returns the value for the oozieConfName if its present in prop map else
     * value of hiveConfName. It will also check HCatAccessorService and
     * HiveConf for hiveConfName.
     *
     * @param prop
     * @param oozieConfName
     * @param hiveConfName
     * @return value for the oozieConfName if its present else value of
     *         hiveConfName. If both are absent then returns null.
     */
    private String getProperty(HashMap<String, String> prop, String oozieConfName, String hiveConfName) {
        String value = prop.get(oozieConfName) == null ? prop.get(hiveConfName) : prop.get(oozieConfName);
        if (value == null || value.isEmpty()) {
            HCatAccessorService hCatService = Services.get().get(HCatAccessorService.class);
            Configuration hCatConf = hCatService.getHCatConf();
            if (hCatConf != null) {
                value = hCatConf.get(hiveConfName);
            }
        }
        if (value == null || value.isEmpty()) {
            value = hiveConf.get(hiveConfName);
        }
        return value;
    }


}
