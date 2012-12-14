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

import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.HashSet;
import java.util.Set;

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
    private Set<String> supportedSchemes;
    private UserGroupInformationService ugiService;

    @Override
    public void init(Services services) throws ServiceException {
        init(services.getConf());
    }

    private void init(Configuration conf) {
        log = XLog.getLog(getClass());
        supportedSchemes = new HashSet<String>();
        supportedSchemes.add("hcat");
        ugiService = Services.get().get(UserGroupInformationService.class);
    }

    public Set<String> getSupportedSchemes() {
        return supportedSchemes;
    }

    /**
     * Get an HCatClient object using doAs for end user
     *
     * @param uri : hcatalog access uri
     * @param user : end user id
     * @return : HCatClient
     * @throws Exception
     */
    public HCatClient getHCatClient(String uri, String user) throws MetaDataAccessorException {
        try {
            return getHCatClient(new URI(uri), new Configuration(), user);//TODO: Remove
        } catch (URISyntaxException e) {
            throw new MetaDataAccessorException(ErrorCode.E1504, e);
        }
    }

    /**
     * Get an HCatClient object using doAs for end user
     *
     * @param uri : hcatalog access uri
     * @param user : end user id
     * @return : HCatClient
     * @throws Exception
     */
    public HCatClient getHCatClient(URI uri, Configuration conf, String user) throws MetaDataAccessorException {
        final HiveConf hiveConf = new HiveConf(conf, this.getClass());
        String serverURI = getMetastoreConnectURI(uri);
        updateConf(conf, serverURI);
        HCatClient client = null;
        try {
            UserGroupInformation ugi = ugiService.getProxyUser(user);
            log.info("Create HCatClient for user [{0}] login_user [{1}] and server [{2}] ", user,
                    UserGroupInformation.getLoginUser(), uri);
            client = ugi.doAs(new PrivilegedExceptionAction<HCatClient>() {
                public HCatClient run() throws Exception {
                    return HCatClient.create(hiveConf);
                }
            });
        }
        catch (Exception e) {
            throw new MetaDataAccessorException(ErrorCode.E1504, e);
        }
        return client;
    }

    public String getMetastoreConnectURI(URI uri) {
        //Hardcoding hcat to thrift mapping till support for webhcat(templeton) is added
        String metastoreURI = "thrift://" + uri.getHost() + ":" + uri.getPort();
        return metastoreURI;
    }

    private void updateConf(Configuration conf, String serverURI) {
        conf.set(HIVE_METASTORE_URIS, serverURI);
    }

    @Override
    public void destroy() {

    }

    @Override
    public Class<? extends Service> getInterface() {
        return MetaDataAccessorService.class;
    }

}
