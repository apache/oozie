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
package org.apache.oozie.hcat;

import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import org.apache.hcatalog.api.HCatClient;
import org.apache.hcatalog.api.HCatPartition;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.service.MetaDataAccessorException;
import org.apache.oozie.service.MetaDataAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.HCatURI;

/**
 * This class is a wrapper around the HCatalog client class
 */
public class MetaDataClientWrapper {

    /**
     * Query one partition.
     *
     * @param server : server end point
     * @param db : database name
     * @param table : table name
     * @param partition : Map of partition key-val
     * @param user :end user
     * @return : Partition Object
     * @throws MetaDataAccessorException
     */
    public HCatPartition getOnePartition(String server, String db, String table, Map<String, String> partition,
            String user) throws MetaDataAccessorException {
        HCatClient client = Services.get().get(MetaDataAccessorService.class).getHCatClient(server, user);
        HCatPartition hPartition;
        try {
            hPartition = client.getPartition(db, table, partition);
        }
        catch (Exception e) {
            throw new MetaDataAccessorException(ErrorCode.E1504, e);
        }
        return hPartition;
    }

    /**
     * Query one partition.
     *
     * @param hcatURI : hcat URI
     * @param user : end user
     * @return Partition Object
     * @throws MetaDataAccessorException
     */
    public HCatPartition getOnePartition(String hcatURI, String user) throws MetaDataAccessorException {
        HCatURI uri = null;
        try {
            uri = new HCatURI(hcatURI);
        }
        catch (URISyntaxException e) {
            throw new MetaDataAccessorException(ErrorCode.E1504, e);
        }
        return getOnePartition(uri.getServer(), uri.getDb(), uri.getTable(), uri.getPartitionMap(), user);
    }

    /**
     * Query a set of partitions using specific filter.
     *
     * @param server : server end point
     * @param db : database name
     * @param table : table name
     * @param filter : key=value strings like SQL where clause
     * @param user :end user
     * @return : List of Partition Object
     * @throws MetaDataAccessorException
     */
    public List<HCatPartition> getPartitionsByFilter(String server, String db, String table, String filter, String user)
            throws MetaDataAccessorException {
        HCatClient client = Services.get().get(MetaDataAccessorService.class).getHCatClient(server, user);
        List<HCatPartition> hPartitions;
        try {
            hPartitions = client.listPartitionsByFilter(db, table, filter);
        }
        catch (Exception e) {
            throw new MetaDataAccessorException(ErrorCode.E1504, e);
        }
        return hPartitions;
    }

    /**
     * Query a set of partitions using specific filter.
     *
     * @param hcatURI :HCat URI
     * @param filter :key=value strings like SQL where clause
     * @param user : end user
     * @return List of Partition Object
     * @throws MetaDataAccessorException
     */
    public List<HCatPartition> getPartitionsByFilter(String hcatURI, String filter, String user)
            throws MetaDataAccessorException {
        HCatURI uri;
        try {
            uri = new HCatURI(hcatURI);
        }
        catch (URISyntaxException e) {
            throw new MetaDataAccessorException(ErrorCode.E1504, e);
        }
        return getPartitionsByFilter(uri.getServer(), uri.getDb(), uri.getTable(), filter, user);
    }

    /**
     * Delete one partition.
     *
     * @param server : server end point
     * @param db : database name
     * @param table : table name
     * @param partition : Map of partition key-val
     * @param ifExists
     * @param user :end user
     * @return : Partition Object
     * @throws MetaDataAccessorException
     */
    public void dropOnePartition(String server, String db, String table, Map<String, String> partition,
            boolean ifExists, String user) throws MetaDataAccessorException {
        HCatClient client = Services.get().get(MetaDataAccessorService.class).getHCatClient(server, user);
        try {
            client.dropPartition(db, table, partition, ifExists);
        }
        catch (Exception e) {
            throw new MetaDataAccessorException(ErrorCode.E1504, e);
        }
        return;
    }

    /**
     * Delete one partition.
     *
     * @param hcatURI :HCat URI
     * @param ifExists
     * @param user : end user id
     * @throws MetaDataAccessorException
     */
    public void dropOnePartition(String hcatURI, boolean ifExists, String user) throws MetaDataAccessorException {
        HCatURI uri;
        try {
            uri = new HCatURI(hcatURI);
        }
        catch (Exception e) {
            throw new MetaDataAccessorException(ErrorCode.E1504, e);
        }
        dropOnePartition(uri.getServer(), uri.getDb(), uri.getTable(), uri.getPartitionMap(), ifExists, user);
        return;
    }

}
