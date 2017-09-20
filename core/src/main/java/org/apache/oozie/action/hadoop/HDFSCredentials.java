/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.oozie.action.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.util.XLog;


public class HDFSCredentials implements CredentialsProvider {
    protected XLog LOG = XLog.getLog(getClass());

    /**
     * Add an HDFS_DELEGATION_TOKEN to the {@link Credentials} provided.
     * This is also important to ensure that log aggregation works correctly from the NM
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
        try (FileSystem fileSystem = context.getAppFileSystem()) {
            final String renewer = new HadoopTokenHelper().getServerPrincipal(config);
            LOG.debug("Server principal present, getting HDFS delegation token. [renewer={0}]", renewer);
            final Token hdfsDelegationToken = fileSystem.getDelegationToken(renewer);
            if (hdfsDelegationToken == null) {
                throw new CredentialException(ErrorCode.E0511, renewer);
            }
            LOG.info("Got HDFS delegation token, setting credentials. [hdfsDelegationToken={0}]", hdfsDelegationToken);
            credentials.addToken(hdfsDelegationToken.getService(), hdfsDelegationToken);
        } catch (Exception e) {
            LOG.debug("exception in updateCredentials", e);
            throw e;
        }
    }

}
