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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hcatalog.api.ConnectionFailureException;
import org.apache.hcatalog.api.HCatClient;
import org.apache.hcatalog.api.HCatPartition;
import org.apache.hcatalog.api.HCatTable;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.service.JMSAccessorService;
import org.apache.oozie.service.MetaDataAccessorException;
import org.apache.oozie.service.MetaDataAccessorService;
import org.apache.oozie.service.MetadataServiceException;
import org.apache.oozie.service.PartitionDependencyManagerService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.URIAccessorException;
import org.apache.oozie.util.HCatURI;
import org.apache.oozie.util.XLog;
import org.jdom.Element;

public class HCatURIHandler extends URIHandler {

    private static XLog LOG = XLog.getLog(HCatURIHandler.class);
    private MetaDataAccessorService service;

    public HCatURIHandler() {
        service = Services.get().get(MetaDataAccessorService.class);
    }

    @Override
    public void init(Configuration conf) {

    }

    @Override
    public Set<String> getSupportedSchemes() {
        return service.getSupportedSchemes();
    }

    @Override
    public DependencyType getDependencyType(URI uri) throws URIAccessorException {
        JMSAccessorService service = Services.get().get(JMSAccessorService.class);
        return service.getOrCreateConnection(uri.getScheme() + "://" + uri.getAuthority()) ? DependencyType.PUSH
                : DependencyType.PULL;
 }

    @Override
    public void registerForNotification(URI uri, String actionID) throws URIAccessorException {
        // TODO Register to PDMService, if jms url is configured for the hcat server
    }

    @Override
    public URIContext getURIContext(URI uri, Configuration conf, String user) throws URIAccessorException {
        HCatClient client = service.getHCatClient(uri, conf, user);
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
        return exists(uri, client);
    }

    @Override
    public boolean exists(URI uri, Configuration conf, String user) throws URIAccessorException {
        HCatClient client = service.getHCatClient(uri, conf, user);
        return exists(uri, client);
    }

    @Override
    public boolean delete(URI uri, Configuration conf, String user) throws URIAccessorException {
        HCatClient hCatClient = service.getHCatClient(uri, conf, user);
        return delete(hCatClient, uri);
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
    public void validate (String uri) throws URIAccessorException{
        try {
            new HCatURI(uri);  //will fail if uri syntax is incorrect
        }
        catch (URISyntaxException e) {
            throw new URIAccessorException(ErrorCode.E1025, uri, e);
        }

    }

    @Override
    public void destroy() {

    }

    private boolean exists(URI uri, HCatClient client) throws MetaDataAccessorException {
        try {
            HCatURI hcatURI = new HCatURI(uri.toString());
            List<HCatPartition> partitions = client.listPartitionsByFilter(hcatURI.getDb(), hcatURI.getTable(),
                    hcatURI.toFilter());
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
    }

    private boolean delete(HCatClient client, URI uri) throws URIAccessorException {
        try {
            HCatURI hcatURI = new HCatURI(uri.toString());
            List<HCatPartition> partitions = client.listPartitionsByFilter(hcatURI.getDb(), hcatURI.getTable(),
                    hcatURI.toFilter());
            if (partitions == null || partitions.isEmpty()) {
                return false;
            }
            else {
                // Only works if all partitions match. HCat team working on adding a dropPartitions API.
                // client.dropPartition(hcatURI.getDb(), hcatURI.getTable(),hcatURI.getPartitionMap(), true);
                // Tried an alternate way. But another bug. table.getPartCols() is empty.
                // TODO: Change this code and enable tests after fix from hcat team.
                HCatTable table = client.getTable(hcatURI.getDb(), hcatURI.getTable());
                List<HCatFieldSchema> partCols = table.getPartCols();
                Map<String, String> partKeyVals = new HashMap<String, String>();
                for (HCatPartition partition : partitions) {
                    List<String> partVals = partition.getValues();
                    partKeyVals.clear();
                    for (int i = 0; i < partCols.size(); i++) {
                        partKeyVals.put(partCols.get(i).getName(), partVals.get(i));
                    }
                    client.dropPartition(hcatURI.getDb(), hcatURI.getTable(), partKeyVals, true);
                }
                LOG.info("Dropped partitions for " + uri);
                return true;
            }
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
    }

}
