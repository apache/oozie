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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier;
import org.apache.hadoop.hbase.security.token.TokenUtil;
import org.apache.hadoop.mapred.JobConf;
import org.apache.oozie.action.ActionExecutor.Context;
import org.apache.oozie.action.hadoop.Credentials;
import org.apache.oozie.action.hadoop.CredentialsProperties;
import org.apache.oozie.util.XLog;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;


/**
 * Hbase Credentials implementation to store in jobConf
 * The jobConf is used further to pass credentials to the tasks while running
 * Oozie server should be configured to use this Credentials class by including it via property 'oozie.credentials.credentialclasses'
 *
 */
public class HbaseCredentials extends Credentials {


    /* (non-Javadoc)
     * @see org.apache.oozie.action.hadoop.Credentials#addtoJobConf(org.apache.hadoop.mapred.JobConf, org.apache.oozie.action.hadoop.CredentialsProperties, org.apache.oozie.action.ActionExecutor.Context)
     */
    @Override
    public void addtoJobConf(JobConf jobConf, CredentialsProperties props, Context context) throws Exception {
        try {
            copyHbaseConfToJobConf(jobConf, props);
            obtainToken(jobConf, context);
        }
        catch (Exception e) {
            XLog.getLog(getClass()).warn("Exception in receiving hbase credentials", e);
            throw e;
        }
    }

    void copyHbaseConfToJobConf(JobConf jobConf, CredentialsProperties props) {
        // Create configuration using hbase-site.xml/hbase-default.xml
        Configuration hbaseConf = new Configuration(false);
        HBaseConfiguration.addHbaseResources(hbaseConf);
        // copy cred props to hbaseconf and override if values already exists
        addPropsConf(props, hbaseConf);
        // copy cred props to jobconf and override if values already exist
        addPropsConf(props, jobConf);
        // copy conf from hbaseConf to jobConf without overriding the
        // already existing values of jobConf
        injectConf(hbaseConf, jobConf);
    }

    private void obtainToken(final JobConf jobConf, Context context) throws IOException, InterruptedException {
        String user = context.getWorkflow().getUser();
        UserGroupInformation ugi =  UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser());
        User u = User.create(ugi);
        // A direct doAs is required here vs. User#obtainAuthTokenForJob(...)
        // See OOZIE-2419 for more
        Token<AuthenticationTokenIdentifier> token = u.runAs(
            new PrivilegedExceptionAction<Token<AuthenticationTokenIdentifier>>() {
                public Token<AuthenticationTokenIdentifier> run() throws Exception {
                    return TokenUtil.obtainToken(jobConf);
                }
            }
        );
        jobConf.getCredentials().addToken(token.getService(), token);
    }

    private void addPropsConf(CredentialsProperties props, Configuration destConf) {
        for (Map.Entry<String, String> entry : props.getProperties().entrySet()) {
            destConf.set(entry.getKey(), entry.getValue());
        }
    }

    private void injectConf(Configuration srcConf, Configuration destConf) {
        for (Map.Entry<String, String> entry : srcConf) {
            String name = entry.getKey();
            if (destConf.get(name) == null) {
                String value = entry.getValue();
                destConf.set(name, value);
            }
        }
    }
}
