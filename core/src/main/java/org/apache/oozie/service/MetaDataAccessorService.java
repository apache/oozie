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
package org.apache.oozie.service;

import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hcatalog.api.HCatClient;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.XLog;

/**
 * This service provides a way of getting HCatClient instance
 */
public class MetaDataAccessorService implements Service {

    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    private static XLog log;

    @Override
    public void init(Services services) throws ServiceException {
        init(services.getConf());
    }

    private void init(Configuration conf) {
        log = XLog.getLog(getClass());
    }

    /**
     * Get an HCatClient object using doAs for end user
     *
     * @param server : server end point
     * @param user : end user id
     * @return : HCatClient
     * @throws Exception
     */
    public HCatClient getHCatClient(String server, String user) throws MetaDataAccessorException {
        final HiveConf conf = new HiveConf();
        updateConf(conf, server);
        HCatClient client = null;
        try {
            UserGroupInformation ugi = UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser());
            log.info("Create HCatClient for user [{0}] login_user [{1}] and server [{1}] ", user,
                    UserGroupInformation.getLoginUser(), server);
            client = ugi.doAs(new PrivilegedExceptionAction<HCatClient>() {
                public HCatClient run() throws Exception {
                    return HCatClient.create(conf);
                }
            });
        }
        catch (Exception e) {
            throw new MetaDataAccessorException(ErrorCode.E1504, e);
        }
        return client;
    }

    private void updateConf(Configuration conf, String server) {
        conf.set(HIVE_METASTORE_URIS, server);
        // TODO: add more conf?
    }

    @Override
    public void destroy() {
        // TODO Auto-generated method stub

    }

    @Override
    public Class<? extends Service> getInterface() {
        return MetaDataAccessorService.class;
    }

}
