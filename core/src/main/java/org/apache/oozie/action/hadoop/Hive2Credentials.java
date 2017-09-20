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

import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.action.ActionExecutor.Context;
import org.apache.oozie.util.XLog;

/**
 * Credentials implementation, Hive Server 2 specific properties
 * User specifies these credential properties along with the action configuration
 * The credentials is used further to pass credentials to the tasks while running
 * Oozie server should be configured to use this class by including it via property 'oozie.credentials.credentialclasses'
 * User can extend the parent class to implement own class as well
 * for handling custom token-based credentials and add to the above server property
 */
public class Hive2Credentials implements CredentialsProvider {

    private static final String USER_NAME = "user.name";
    private static final String HIVE2_JDBC_URL = "hive2.jdbc.url";
    private static final String HIVE2_SERVER_PRINCIPAL = "hive2.server.principal";

    @Override
    public void updateCredentials(Credentials credentials, Configuration config, CredentialsProperties props,
            Context context) throws Exception {
        try {
            // load the driver
            Class.forName("org.apache.hive.jdbc.HiveDriver");

            String url = props.getProperties().get(HIVE2_JDBC_URL);
            if (url == null || url.isEmpty()) {
                throw new CredentialException(ErrorCode.E0510,
                        HIVE2_JDBC_URL + " is required to get hive server 2 credential");
            }
            String principal = props.getProperties().get(HIVE2_SERVER_PRINCIPAL);
            if (principal == null || principal.isEmpty()) {
                throw new CredentialException(ErrorCode.E0510,
                        HIVE2_SERVER_PRINCIPAL + " is required to get hive server 2 credentials");
            }
            url = url + ";principal=" + principal;
            Connection con = null;
            String tokenStr = null;
            try {
                con = DriverManager.getConnection(url);
                XLog.getLog(getClass()).debug("Connected successfully to " + url);
                // get delegation token for the given proxy user
                tokenStr = ((HiveConnection)con).getDelegationToken(config.get(USER_NAME), principal);
            } finally {
                if (con != null) {
                    con.close();
                }
            }
            XLog.getLog(getClass()).debug("Got token");

            Token<DelegationTokenIdentifier> hive2Token = new Token<DelegationTokenIdentifier>();
            hive2Token.decodeFromUrlString(tokenStr);
            credentials.addToken(CredentialsProviderFactory.getUniqueAlias(hive2Token), hive2Token);
            XLog.getLog(getClass()).debug("Added the Hive Server 2 token to launcher's credential");
        }
        catch (Exception e) {
            XLog.getLog(getClass()).warn("Exception in obtaining Hive2 token", e);
            throw e;
        }
    }

}