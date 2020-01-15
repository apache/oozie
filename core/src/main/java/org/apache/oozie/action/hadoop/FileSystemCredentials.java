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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.StringUtils;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UserGroupInformationService;
import org.apache.oozie.util.XLog;

import java.security.PrivilegedExceptionAction;
import java.util.Collection;


public class FileSystemCredentials implements CredentialsProvider {
    protected XLog LOG = XLog.getLog(getClass());

    static final String FILESYSTEM_PATH = "filesystem.path";

    /**
     * Add an HDFS_DELEGATION_TOKEN to the {@link Credentials} provided.
     * This is also important to ensure that log aggregation works correctly from the NM.
     *
     * @param credentials the credentials object which is updated
     * @param config      launcher AM configuration
     * @param props       properties for getting credential token or certificate
     * @param context     workflow context
     * @throws Exception thrown if failed
     */
    @Override
    public void updateCredentials(Credentials credentials, Configuration config, CredentialsProperties props,
                                  ActionExecutor.Context context) throws Exception {
        final String[] fileSystemPaths = StringUtils.getStrings(props.getProperties().get(FILESYSTEM_PATH));
        if (fileSystemPaths == null) {
            throw new CredentialException(ErrorCode.E0510,
                    FILESYSTEM_PATH + " property is required to get filesystem type credential");
        }

        final Path[] paths = new Path[fileSystemPaths.length];
        for (int i = 0; i != fileSystemPaths.length; ++i) {
            paths[i] = new Path(fileSystemPaths[i]);
        }

        final UserGroupInformation ugi = Services.get().get(UserGroupInformationService.class)
                .getProxyUser(context.getWorkflow().getUser());
        LOG.info("Obtaining delegation tokens");
        obtainTokens(credentials, config, paths, ugi);
    }

    private void obtainTokens(Credentials credentials, Configuration config, Path[] paths, UserGroupInformation ugi)
            throws java.io.IOException, InterruptedException {
        ugi.doAs(
                new PrivilegedExceptionAction<Void>() {
                    @Override
                    public Void run() throws Exception {
                        TokenCache.obtainTokensForNamenodes(credentials, paths, config);
                        Collection<Token<? extends TokenIdentifier>> creds = credentials.getAllTokens();
                        for (Token tok : creds) {
                            LOG.debug("Tokens in TokenCache: {0}", tok.getService());
                        }
                        return null;
                    }
                }
        );
    }
}
