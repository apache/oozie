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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.v2.api.HSClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UserGroupInformationService;
import org.apache.oozie.util.XLog;

import java.io.IOException;
import java.security.PrivilegedAction;

public class JHSCredentials implements CredentialsProvider {
    protected XLog LOG = XLog.getLog(getClass());


    /**
     * Add an MR_DELEGATION_TOKEN to the {@link Credentials} provided.
     * @param credentials the credentials object which is updated
     * @param config launcher AM configuration
     * @param props properties for getting credential token or certificate
     * @param context workflow context
     * @throws Exception thrown if failed
     */
    @Override
    public void updateCredentials(Credentials credentials, Configuration config, CredentialsProperties props,
                                  ActionExecutor.Context context) throws Exception {
        try {
            LOG.debug("Instantiating JHS Proxy");
            MRClientProtocol hsProxy = instantiateHistoryProxy(config, context);
            Text hsService = SecurityUtil.buildTokenService(hsProxy.getConnectAddress());
            LOG.debug("Getting delegation token for {0}", hsService.toString());
            Token<?> jhsToken = getDelegationTokenFromJHS(hsProxy, new HadoopTokenHelper().getServerPrincipal(config));
            LOG.debug("Acquired token {0}", jhsToken);
            credentials.addToken(hsService, jhsToken);
        } catch (IOException | InterruptedException ex) {
            LOG.debug("exception in updateCredentials", ex);
            throw new CredentialException(ErrorCode.E0512, ex.getMessage(), ex);
        }
    }

    /**
     * Get a Delegation token from the JHS.
     * Copied over from YARNRunner in Hadoop.
     * @param hsProxy protcol used to get the token
     * @return The RM_DELEGATION_TOKEN that can be used to talk to JHS
     * @throws IOException
     * @throws InterruptedException
     */
    private Token<?> getDelegationTokenFromJHS(final MRClientProtocol hsProxy, final String renewer)
            throws IOException, InterruptedException {
        GetDelegationTokenRequest request = RecordFactoryProvider
                .getRecordFactory(null).newRecordInstance(GetDelegationTokenRequest.class);
        LOG.debug("Creating requsest to JHS using renewer [{0}]", renewer);
        request.setRenewer(renewer);
        org.apache.hadoop.yarn.api.records.Token mrDelegationToken = hsProxy.getDelegationToken(request)
                .getDelegationToken();
        LOG.debug("Got token to JHS : {0}. Converting token.", mrDelegationToken);
        return ConverterUtils.convertFromYarn(mrDelegationToken, hsProxy.getConnectAddress());
    }

    /**
     * Create an MRClientProtocol to the JHS
     * Copied over from ClientCache in Hadoop.
     * @return the protocol that can be used to get a token with
     * @throws IOException
     */
    private MRClientProtocol instantiateHistoryProxy(final Configuration configuration, final ActionExecutor.Context context)
            throws IOException {
        final String serviceAddr = configuration.get(JHAdminConfig.MR_HISTORY_ADDRESS);
        if (StringUtils.isEmpty(serviceAddr)) {
            return null;
        }
        LOG.debug("Connecting to JHS at: " + serviceAddr);
        final YarnRPC rpc = YarnRPC.create(configuration);
        LOG.debug("Connected to JHS at: " + serviceAddr);
        UserGroupInformation currentUser = Services.get().get(UserGroupInformationService.class)
                .getProxyUser(context.getWorkflow().getUser());
        return currentUser.doAs(new PrivilegedAction<MRClientProtocol>() {
            @Override
            public MRClientProtocol run() {
                return (MRClientProtocol) rpc.getProxy(HSClientProtocol.class,
                        NetUtils.createSocketAddr(serviceAddr), configuration);
            }
        });
    }
}
