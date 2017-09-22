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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.util.XLog;

import java.io.IOException;
import java.net.URISyntaxException;

public class HadoopTokenHelper {
    /** The Kerberos principal for the resource manager.*/
    protected static final String RM_PRINCIPAL = "yarn.resourcemanager.principal";
    protected static final String HADOOP_YARN_RM = "yarn.resourcemanager.address";
    private XLog LOG = XLog.getLog(getClass());

    private String getServicePrincipal(final Configuration configuration) {
        return configuration.get(RM_PRINCIPAL);
    }

    String getServerPrincipal(final Configuration configuration) throws IOException {
        return getServerPrincipal(configuration, getServicePrincipal(configuration));
    }

    /**
     * Mimic {@link org.apache.hadoop.mapred.Master#getMasterPrincipal}, get Kerberos principal for use as delegation token renewer.
     *
     * @param configuration the {@link Configuration} containing the YARN RM address
     * @param servicePrincipal the configured service principal
     * @return the server principal originating from the host name and the service principal
     * @throws IOException when something goes wrong finding out the local address inside
     * {@link SecurityUtil#getServerPrincipal(String, String)}
     */
    private String getServerPrincipal(final Configuration configuration, final String servicePrincipal) throws IOException {
        Preconditions.checkNotNull(configuration, "configuration has to be filled");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(servicePrincipal), "servicePrincipal has to be filled");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(configuration.get(HADOOP_YARN_RM)),
                String.format("configuration entry %s has to be filled", HADOOP_YARN_RM));

        String serverPrincipal;
        final String target = configuration.get(HADOOP_YARN_RM);

        try {
            final String addr = NetUtils.createSocketAddr(target).getHostName();
            serverPrincipal = SecurityUtil.getServerPrincipal(servicePrincipal, addr);
            LOG.info("Delegation Token Renewer details: Principal={0},Target={1}", serverPrincipal, target);
        } catch (final IllegalArgumentException iae) {
            LOG.warn("An error happened while trying to get server principal. Getting it from service principal anyway.", iae);

            serverPrincipal = servicePrincipal.split("[/@]")[0];
            LOG.info("Delegation Token Renewer for {0} is {1}", target, serverPrincipal);
        }

        return serverPrincipal;
    }
}
