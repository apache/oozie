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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.oozie.util.XLog;

/**
 * Helper class to handle the HCat credentials
 * Performs internally the heavy-lifting of fetching delegation tokens from Hive Metastore, abstracted from the user
 * Token is added to the credentials
 */
public class HCatCredentialHelper {

    private static final String USER_NAME = "user.name";
    // Some Hive Metastore properties
    private static final String HIVE_METASTORE_SASL_ENABLED = "hive.metastore.sasl.enabled";
    private static final String HIVE_METASTORE_KERBEROS_PRINCIPAL = "hive.metastore.kerberos.principal";
    private static final String HIVE_METASTORE_LOCAL = "hive.metastore.local";
    private static final String HADOOP_RPC_PROTECTION = "hadoop.rpc.protection";

    /**
     * This Function will set the HCat token to the credentials
     * @param credentials - the credentials
     * @param launcherConfig - launcher configuration
     * @param principal - principal for HCat server
     * @param server - Serevr URI for HCat server
     * @throws Exception
     */
    public void set(Credentials credentials, Configuration launcherConfig, String principal, String server) throws Exception {
        HCatClient client = null;
        try {
            client = getHCatClient(launcherConfig, principal, server);
            XLog.getLog(getClass()).debug(
                    "HCatCredentialHelper: set: User name for which token will be asked from HCat: {0}",
                            launcherConfig.get(USER_NAME));
            String tokenStrForm = client.getDelegationToken(launcherConfig.get(USER_NAME), UserGroupInformation
                    .getLoginUser().getShortUserName());
            Token<DelegationTokenIdentifier> hcatToken = new Token<DelegationTokenIdentifier>();
            hcatToken.decodeFromUrlString(tokenStrForm);
            credentials.addToken(CredentialsProviderFactory.getUniqueAlias(hcatToken), hcatToken);
            XLog.getLog(getClass()).debug("Added the HCat token to launcher's credentials");
        }
        catch (Exception ex) {
            XLog.getLog(getClass()).debug("set Exception {0}", ex.getMessage());
            throw ex;
        }
        finally {
            if (client != null) {
                client.close();
            }
        }
    }

    /**
     * Getting the HCat client.
     * @param launcherConfig
     * @param principal
     * @param server
     * @return HCatClient
     * @throws HCatException
     */
    public HCatClient getHCatClient(Configuration launcherConfig,
        String principal, String server) throws HCatException {
        HiveConf hiveConf = null;
        HCatClient hiveclient = null;
        hiveConf = new HiveConf();
        XLog.getLog(getClass()).debug("getHCatClient: Principal: {0} Server: {1}", principal, server);
        // specified a thrift url

        hiveConf.set(HIVE_METASTORE_SASL_ENABLED, "true");
        hiveConf.set(HIVE_METASTORE_KERBEROS_PRINCIPAL, principal);
        hiveConf.set(HIVE_METASTORE_LOCAL, "false");
        hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, server);
        String protection = launcherConfig.get(HADOOP_RPC_PROTECTION,
           SaslRpcServer.QualityOfProtection.AUTHENTICATION.name()
              .toLowerCase());
        XLog.getLog(getClass()).debug("getHCatClient, setting rpc protection to {0}", protection);
        hiveConf.set(HADOOP_RPC_PROTECTION, protection);

        hiveclient = HCatClient.create(hiveConf);
        return hiveclient;
    }
}
