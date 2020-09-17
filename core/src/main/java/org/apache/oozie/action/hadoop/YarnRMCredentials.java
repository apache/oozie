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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XLog;

public class YarnRMCredentials implements CredentialsProvider {
    /**
     * Add an RM_DELEGATION_TOKEN to the {@link Credentials} provided.
     *
     * @param credentials the credentials object which is updated
     * @param config launcher AM configuration
     * @param props properties for getting credential token or certificate
     * @param context workflow context
     * @throws Exception thrown if failed
     */
    @Override
    public void updateCredentials(Credentials credentials, Configuration config, CredentialsProperties props,
                                  ActionExecutor.Context context) throws Exception {
        Text rmDelegationTokenService = ClientRMProxy.getRMDelegationTokenService(config);
        if (rmDelegationTokenService == null) {
            throw new CredentialException(ErrorCode.E0512, "Can't create RMDelegationTokenService");
        }
        try (YarnClient yarnClient = Services.get().get(HadoopAccessorService.class)
                .createYarnClient(context.getWorkflow().getUser(), config)) {
            org.apache.hadoop.yarn.api.records.Token rmDelegationToken =
                    yarnClient.getRMDelegationToken(new Text(new HadoopTokenHelper().getServerPrincipal(config)));
            if (rmDelegationToken == null) {
                throw new CredentialException(ErrorCode.E0512, "Returned token is null");
            }
            Token<TokenIdentifier> rmToken = ConverterUtils.convertFromYarn(rmDelegationToken, rmDelegationTokenService);
            credentials.addToken(rmDelegationTokenService, rmToken);
        } catch (Exception e) {
            XLog.getLog(getClass()).debug("Exception in updateCredentials", e);
            throw e;
        }
    }

}
