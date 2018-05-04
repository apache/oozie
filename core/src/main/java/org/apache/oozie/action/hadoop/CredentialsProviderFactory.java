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
import java.util.HashMap;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.util.StringUtils;
import org.apache.oozie.util.XLog;

public class CredentialsProviderFactory {
    public static final String CRED_KEY = "oozie.credentials.credentialclasses";
    private static final XLog LOG = XLog.getLog(CredentialsProviderFactory.class);
    public static final String HDFS = "hdfs";
    public static final String YARN = "yarnRM";
    public static final String JHS = "jhs";
    private static CredentialsProviderFactory instance;
    private final Map<String, Class<? extends CredentialsProvider>> providerCache;

    @VisibleForTesting
    static void destroy() {
        instance = null;
    }

    public static CredentialsProviderFactory getInstance() throws Exception {
        if(instance == null) {
            instance = new CredentialsProviderFactory();
        }
        return instance;
    }

    private CredentialsProviderFactory() throws Exception {
        providerCache = new HashMap<>();
        for (String function : ConfigurationService.getStrings(CRED_KEY)) {
            function = StringUtils.trim(function);
            LOG.debug("Creating Credential class for : " + function);
            String[] str = function.split("=");
            if (str.length > 0) {
                String type = str[0];
                String classname = str[1];
                if (classname != null) {
                    LOG.debug("Creating Credential type : '{0}', class Name : '{1}'", type, classname);
                    Class<?> klass = null;
                    try {
                        klass = Thread.currentThread().getContextClassLoader().loadClass(classname);
                    }
                    catch (ClassNotFoundException ex) {
                        LOG.warn("Exception while loading the class '{0}'", classname, ex);
                        throw ex;
                    }
                    providerCache.put(type, (Class<CredentialsProvider>) klass);
                } else {
                    LOG.warn("Credential provider class is null for '{0}', skipping", type);
                }
            }
        }
        providerCache.put(HDFS, HDFSCredentials.class);
        providerCache.put(YARN, YarnRMCredentials.class);
        providerCache.put(JHS, JHSCredentials.class);
    }

    static Text getUniqueAlias(Token<?> token) {
        return new Text(String.format("%s_%s_%d", token.getKind().toString(),
                token.getService().toString(), System.currentTimeMillis()));
    }

    /**
     * Create Credential object
     *
     * @param type
     * @return Credential object
     * @throws Exception
     */
    public CredentialsProvider createCredentialsProvider(String type) throws Exception {
        Class<? extends CredentialsProvider> providerClass = providerCache.get(type);
        if(providerClass == null){
            return null;
        }
        return providerClass.newInstance();
    }

    /**
     * Relogs into Kerberos using the Keytab for the Oozie server user.  This should be called before attempting to get delegation
     * tokens via {@link CredentialsProvider} implementations to ensure that the Kerberos credentials are current and won't expire
     * too soon.
     *
     * @throws IOException
     */
    public static void ensureKerberosLogin() throws IOException {
        LOG.debug("About to relogin from keytab");
        UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
        LOG.debug("Relogin from keytab successful");
    }
}
