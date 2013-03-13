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
package org.apache.oozie.dependency;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hcatalog.api.ConnectionFailureException;
import org.apache.hcatalog.api.HCatClient;
import org.apache.hcatalog.api.HCatPartition;
import org.apache.hcatalog.common.HCatException;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.action.hadoop.HCatLauncherURIHandler;
import org.apache.oozie.action.hadoop.LauncherURIHandler;
import org.apache.oozie.dependency.hcat.HCatMessageHandler;
import org.apache.oozie.service.HCatAccessorException;
import org.apache.oozie.service.HCatAccessorService;
import org.apache.oozie.service.PartitionDependencyManagerService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.URIHandlerService;
import org.apache.oozie.util.HCatURI;
import org.apache.oozie.util.XLog;

public class HCatURIHandler implements URIHandler {

    private Set<String> supportedSchemes;
    private Map<String, DependencyType> dependencyTypes;
    private List<Class<?>> classesToShip;

    @Override
    public void init(Configuration conf) {
        dependencyTypes = new HashMap<String, DependencyType>();
        supportedSchemes = new HashSet<String>();
        String[] schemes = conf.getStrings(URIHandlerService.URI_HANDLER_SUPPORTED_SCHEMES_PREFIX
                + this.getClass().getSimpleName() + URIHandlerService.URI_HANDLER_SUPPORTED_SCHEMES_SUFFIX, "hcat");
        supportedSchemes.addAll(Arrays.asList(schemes));
        classesToShip = new HCatLauncherURIHandler().getClassesForLauncher();
    }

    @Override
    public Set<String> getSupportedSchemes() {
        return supportedSchemes;
    }

    @Override
    public Class<? extends LauncherURIHandler> getLauncherURIHandlerClass() {
        return HCatLauncherURIHandler.class;
    }

    @Override
    public List<Class<?>> getClassesForLauncher() {
        return classesToShip;
    }

    @Override
    public DependencyType getDependencyType(URI uri) throws URIHandlerException {
        DependencyType depType = DependencyType.PULL;
        // Not initializing in constructor as this will be part of oozie.services.ext
        // and will be initialized after URIHandlerService
        HCatAccessorService hcatService = Services.get().get(HCatAccessorService.class);
        if (hcatService != null) {
            depType = dependencyTypes.get(uri.getAuthority());
            if (depType == null) {
                depType = hcatService.isKnownPublisher(uri) ? DependencyType.PUSH : DependencyType.PULL;
                dependencyTypes.put(uri.getAuthority(), depType);
            }
        }
        return depType;
    }

    @Override
    public void registerForNotification(URI uri, Configuration conf, String user, String actionID)
            throws URIHandlerException {
        HCatURI hcatURI;
        try {
            hcatURI = new HCatURI(uri);
        }
        catch (URISyntaxException e) {
            throw new URIHandlerException(ErrorCode.E0906, uri, e);
        }
        HCatAccessorService hcatService = Services.get().get(HCatAccessorService.class);
        if (!hcatService.isRegisteredForNotification(hcatURI)) {
            HCatClient client = getHCatClient(uri, conf, user);
            try {
                String topic = client.getMessageBusTopicName(hcatURI.getDb(), hcatURI.getTable());
                if (topic == null) {
                    return;
                }
                hcatService.registerForNotification(hcatURI, topic, new HCatMessageHandler(uri.getAuthority()));
            }
            catch (HCatException e) {
                throw new HCatAccessorException(ErrorCode.E1501, e);
            }
            finally {
                closeQuietly(client, true);
            }
        }
        PartitionDependencyManagerService pdmService = Services.get().get(PartitionDependencyManagerService.class);
        pdmService.addMissingDependency(hcatURI, actionID);
    }

    @Override
    public boolean unregisterFromNotification(URI uri, String actionID) {
        HCatURI hcatURI;
        try {
            hcatURI = new HCatURI(uri);
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e); // Unexpected at this point
        }
        PartitionDependencyManagerService pdmService = Services.get().get(PartitionDependencyManagerService.class);
        return pdmService.removeMissingDependency(hcatURI, actionID);
    }

    @Override
    public Context getContext(URI uri, Configuration conf, String user) throws URIHandlerException {
        HCatClient client = getHCatClient(uri, conf, user);
        return new HCatContext(conf, user, client);
    }

    @Override
    public boolean exists(URI uri, Context context) throws URIHandlerException {
        HCatClient client = ((HCatContext) context).getHCatClient();
        return exists(uri, client, false);
    }

    @Override
    public boolean exists(URI uri, Configuration conf, String user) throws URIHandlerException {
        HCatClient client = getHCatClient(uri, conf, user);
        return exists(uri, client, true);
    }

    @Override
    public String getURIWithDoneFlag(String uri, String doneFlag) throws URIHandlerException {
        return uri;
    }

    @Override
    public void validate(String uri) throws URIHandlerException {
        try {
            new HCatURI(uri); // will fail if uri syntax is incorrect
        }
        catch (URISyntaxException e) {
            throw new URIHandlerException(ErrorCode.E0906, uri, e);
        }

    }

    @Override
    public void destroy() {

    }

    private HCatClient getHCatClient(URI uri, Configuration conf, String user) throws HCatAccessorException {
        final HiveConf hiveConf = new HiveConf(conf, this.getClass());
        String serverURI = getMetastoreConnectURI(uri);
        if (!serverURI.equals("")) {
            hiveConf.set("hive.metastore.local", "false");
        }
        hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, serverURI);
        try {
            XLog.getLog(HCatURIHandler.class).info(
                    "Creating HCatClient for user [{0}] login_user [{1}] and server [{2}] ", user,
                    UserGroupInformation.getLoginUser(), serverURI);

            // HiveMetastoreClient (hive 0.9) currently does not work if UGI has doAs
            // We are good to connect as the oozie user since listPartitions does not require
            // authorization
            /*
            UserGroupInformation ugi = ugiService.getProxyUser(user);
            return ugi.doAs(new PrivilegedExceptionAction<HCatClient>() {
                public HCatClient run() throws Exception {
                    return HCatClient.create(hiveConf);
                }
            });
            */

            return HCatClient.create(hiveConf);
        }
        catch (HCatException e) {
            throw new HCatAccessorException(ErrorCode.E1501, e);
        }
        catch (IOException e) {
            throw new HCatAccessorException(ErrorCode.E1501, e);
        }

    }

    private String getMetastoreConnectURI(URI uri) {
        String metastoreURI;
        // For unit tests
        if (uri.getAuthority().equals("unittest-local")) {
            metastoreURI = "";
        }
        else {
            // Hardcoding hcat to thrift mapping till support for webhcat(templeton)
            // is added
            metastoreURI = "thrift://" + uri.getAuthority();
        }
        return metastoreURI;
    }

    private boolean exists(URI uri, HCatClient client, boolean closeClient) throws HCatAccessorException {
        try {
            HCatURI hcatURI = new HCatURI(uri.toString());
            List<HCatPartition> partitions = client.getPartitions(hcatURI.getDb(), hcatURI.getTable(),
                    hcatURI.getPartitionMap());
            return (partitions != null && !partitions.isEmpty());
        }
        catch (ConnectionFailureException e) {
            throw new HCatAccessorException(ErrorCode.E1501, e);
        }
        catch (HCatException e) {
            throw new HCatAccessorException(ErrorCode.E0902, e);
        }
        catch (URISyntaxException e) {
            throw new HCatAccessorException(ErrorCode.E0902, e);
        }
        finally {
            closeQuietly(client, closeClient);
        }
    }

    private void closeQuietly(HCatClient client, boolean close) {
        if (close && client != null) {
            try {
                client.close();
            }
            catch (Exception ignore) {
                XLog.getLog(HCatURIHandler.class).warn("Error closing hcat client", ignore);
            }
        }
    }

    static class HCatContext extends Context {

        private HCatClient hcatClient;

        /**
         * Create a HCatContext that can be used to access a hcat URI
         *
         * @param conf Configuration to access the URI
         * @param user name of the user the URI should be accessed as
         * @param hcatClient HCatClient to talk to hcatalog server
         */
        public HCatContext(Configuration conf, String user, HCatClient hcatClient) {
            super(conf, user);
            this.hcatClient = hcatClient;
        }

        /**
         * Get the HCatClient to talk to hcatalog server
         *
         * @return HCatClient to talk to hcatalog server
         */
        public HCatClient getHCatClient() {
            return hcatClient;
        }

        @Override
        public void destroy() {
            try {
                hcatClient.close();
            }
            catch (Exception ignore) {
                XLog.getLog(HCatContext.class).warn("Error closing hcat client", ignore);
            }
        }

    }

}
