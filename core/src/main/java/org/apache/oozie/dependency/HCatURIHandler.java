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
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import org.apache.oozie.jms.HCatMessageHandler;
import org.apache.oozie.service.JMSAccessorService;
import org.apache.oozie.service.MetaDataAccessorException;
import org.apache.oozie.service.PartitionDependencyManagerService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.URIAccessorException;
import org.apache.oozie.service.URIHandlerService;
import org.apache.oozie.service.UserGroupInformationService;
import org.apache.oozie.util.HCatURI;
import org.apache.oozie.util.XLog;
import org.jdom.Element;

public class HCatURIHandler extends URIHandler {

    private static XLog LOG = XLog.getLog(HCatURIHandler.class);
    private boolean isFrontEnd;
    private Set<String> supportedSchemes;
    private UserGroupInformationService ugiService;
    private List<Class<?>> classesToShip;
    private Map<String, DependencyType> dependencyTypes;
    private Set<String> registeredTopicURIs;

    public HCatURIHandler() {
        this.classesToShip = new ArrayList<Class<?>>();
        classesToShip.add(HCatURIHandler.class);
        classesToShip.add(HCatURIContext.class);
        classesToShip.add(HCatURI.class);
        classesToShip.add(MetaDataAccessorException.class);
        classesToShip.add(UserGroupInformationService.class);
        classesToShip.add(JMSAccessorService.class);
        classesToShip.add(PartitionDependencyManagerService.class);
        classesToShip.add(HCatMessageHandler.class);
        classesToShip.add(DependencyType.class);
    }

    @Override
    public void init(Configuration conf, boolean isFrontEnd) {
        this.isFrontEnd = isFrontEnd;
        if (isFrontEnd) {
            ugiService = Services.get().get(UserGroupInformationService.class);
            dependencyTypes = new HashMap<String, DependencyType>();
            registeredTopicURIs = new HashSet<String>();
        }
        supportedSchemes = new HashSet<String>();
        String[] schemes = conf.getStrings(URIHandlerService.URI_HANDLER_SUPPORTED_SCHEMES_PREFIX
                + this.getClass().getSimpleName() + URIHandlerService.URI_HANDLER_SUPPORTED_SCHEMES_SUFFIX, "hcat");
        supportedSchemes.addAll(Arrays.asList(schemes));
    }

    @Override
    public Set<String> getSupportedSchemes() {
        return supportedSchemes;
    }

    public Collection<Class<?>> getClassesToShip() {
        return classesToShip;
    }

    @Override
    public DependencyType getDependencyType(URI uri) throws URIAccessorException {
        JMSAccessorService jmsService = Services.get().get(JMSAccessorService.class);
        if (jmsService == null) {
            return DependencyType.PULL;
        }
        DependencyType depType = dependencyTypes.get(uri.getAuthority());
        if (depType == null) {
             depType = jmsService.isKnownPublisher(uri) ? DependencyType.PUSH : DependencyType.PULL;
             dependencyTypes.put(uri.getAuthority(), depType);
        }
        return depType;
 }

    @Override
    public void registerForNotification(URI uri, Configuration conf, String user, String actionID) throws URIAccessorException {
        String uriString = uri.toString();
        String uriMinusPartition = uriString.substring(0, uriString.lastIndexOf("/"));
        HCatURI hcatURI;
        try {
            hcatURI = new HCatURI(uri);
        }
        catch (URISyntaxException e) {
            throw new URIAccessorException(ErrorCode.E0906, uri, e);
        }
        // Check cache to avoid a call to hcat server to get the topic
        if (!registeredTopicURIs.contains(uriMinusPartition)) {
            HCatClient client = getHCatClient(uri, conf, user);
            try {
                String topic = client.getMessageBusTopicName(hcatURI.getDb(), hcatURI.getTable());
                if (topic == null) {
                    return;
                }
                JMSAccessorService jmsService = Services.get().get(JMSAccessorService.class);
                jmsService.registerForNotification(uri, topic, new HCatMessageHandler(uri.getAuthority()));
                registeredTopicURIs.add(uriMinusPartition);
            }
            catch (HCatException e) {
                throw new MetaDataAccessorException(ErrorCode.E1504, e);
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
            throw new RuntimeException(e); //Unexpected at this point
        }
        PartitionDependencyManagerService pdmService = Services.get().get(PartitionDependencyManagerService.class);
        return pdmService.removeMissingDependency(hcatURI, actionID);
    }

    @Override
    public URIContext getURIContext(URI uri, Configuration conf, String user) throws URIAccessorException {
        HCatClient client = getHCatClient(uri, conf, user);
        return new HCatURIContext(conf, user, client);
    }

    @Override
    public boolean create(URI uri, Configuration conf, String user) throws URIAccessorException {
        throw new MetaDataAccessorException(ErrorCode.E0902, new UnsupportedOperationException(
                "Add partition not supported"));
    }

    @Override
    public boolean exists(URI uri, URIContext uriContext) throws URIAccessorException {
        HCatClient client = ((HCatURIContext) uriContext).getHCatClient();
        return exists(uri, client, false);
    }

    @Override
    public boolean exists(URI uri, Configuration conf, String user) throws URIAccessorException {
        HCatClient client = getHCatClient(uri, conf, user);
        return exists(uri, client, true);
    }

    @Override
    public boolean delete(URI uri, Configuration conf, String user) throws URIAccessorException {
        HCatClient hCatClient = getHCatClient(uri, conf, user);
        return delete(hCatClient, uri, true);
    }

    @Override
    public String getURIWithDoneFlag(String uri, Element doneFlagElement) throws URIAccessorException {
        return uri;
    }

    @Override
    public String getURIWithDoneFlag(String uri, String doneFlag) throws URIAccessorException {
        return uri;
    }

    @Override
    public void validate(String uri) throws URIAccessorException {
        try {
            new HCatURI(uri);  //will fail if uri syntax is incorrect
        }
        catch (URISyntaxException e) {
            throw new URIAccessorException(ErrorCode.E0906, uri, e);
        }

    }

    @Override
    public void destroy() {

    }

    private HCatClient getHCatClient(URI uri, Configuration conf, String user) throws MetaDataAccessorException {
        final HiveConf hiveConf = new HiveConf(conf, this.getClass());
        String serverURI = getMetastoreConnectURI(uri);
        if (!serverURI.equals("")) {
            hiveConf.set("hive.metastore.local", "false");
        }
        hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, serverURI);
        HCatClient client = null;
        try {
            LOG.info("Creating HCatClient for user [{0}] login_user [{1}] and server [{2}] ", user,
                    UserGroupInformation.getLoginUser(), serverURI);
            if (isFrontEnd) {
                if (user == null) {
                    throw new MetaDataAccessorException(ErrorCode.E0902,
                            "user has to be specified to access metastore server");
                }
                UserGroupInformation ugi = ugiService.getProxyUser(user);
                client = ugi.doAs(new PrivilegedExceptionAction<HCatClient>() {
                    public HCatClient run() throws Exception {
                        return HCatClient.create(hiveConf);
                    }
                });
            }
            else {
                if (user != null && !user.equals(UserGroupInformation.getLoginUser().getShortUserName())) {
                    throw new MetaDataAccessorException(ErrorCode.E0902,
                            "Cannot access metastore server as a different user in backend");
                }
                // Delegation token fetched from metastore has new Text() as service and HiveMetastoreClient
                // looks for the same if not overriden by hive.metastore.token.signature
                // We are good as long as HCatCredentialHelper does not change the service of the token.
                client = HCatClient.create(hiveConf);
            }
        }
        catch (HCatException e) {
            throw new MetaDataAccessorException(ErrorCode.E1504, e);
        }
        catch (IOException e) {
            throw new MetaDataAccessorException(ErrorCode.E1504, e);
        }
        catch (InterruptedException e) {
            throw new MetaDataAccessorException(ErrorCode.E1504, e);
        }

        return client;
    }

    private String getMetastoreConnectURI(URI uri) {
        // For unit tests
        if (uri.getAuthority().equals("unittest-local")) {
            return "";
        }
        // Hardcoding hcat to thrift mapping till support for webhcat(templeton)
        // is added
        String metastoreURI = "thrift://" + uri.getAuthority();
        return metastoreURI;
    }

    private boolean exists(URI uri, HCatClient client, boolean closeClient) throws MetaDataAccessorException {
        try {
            HCatURI hcatURI = new HCatURI(uri.toString());
            List<HCatPartition> partitions = client.getPartitions(hcatURI.getDb(), hcatURI.getTable(),
                    hcatURI.getPartitionMap());
            if (partitions == null || partitions.isEmpty()) {
                return false;
            }
            return true;
        }
        catch (ConnectionFailureException e) {
            throw new MetaDataAccessorException(ErrorCode.E1504, e);
        }
        catch (HCatException e) {
            throw new MetaDataAccessorException(ErrorCode.E0902, e);
        }
        catch (URISyntaxException e) {
            throw new MetaDataAccessorException(ErrorCode.E0902, e);
        }
        finally {
            closeQuietly(client, closeClient);
        }
    }

    private boolean delete(HCatClient client, URI uri, boolean closeClient) throws URIAccessorException {
        try {
            HCatURI hcatURI = new HCatURI(uri.toString());
            client.dropPartitions(hcatURI.getDb(), hcatURI.getTable(), hcatURI.getPartitionMap(), true);
            LOG.info("Dropped partitions for " + uri);
            return true;
        }
        catch (ConnectionFailureException e) {
            throw new MetaDataAccessorException(ErrorCode.E1504, e);
        }
        catch (HCatException e) {
            throw new MetaDataAccessorException(ErrorCode.E0902, e);
        }
        catch (URISyntaxException e) {
            throw new MetaDataAccessorException(ErrorCode.E0902, e);
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
            }
        }
    }

}
