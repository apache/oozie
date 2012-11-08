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

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.service.Service;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.PartitionWrapper;
import org.apache.oozie.util.PartitionsGroup;
import org.apache.oozie.util.WaitingActions;
import org.apache.oozie.util.HCatURI;
import org.apache.oozie.util.XLog;

/**
 * Module that functions like a caching service to maintain partition dependency
 * mappings
 */
public class PartitionDependencyManagerService implements Service {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "PartitionDependencyManagerService.";
    public static final String HCAT_DEFAULT_SERVER_NAME = "hcat.default.server.name";
    public static final String HCAT_DEFAULT_DB_NAME = "hcat.default.db.name";
    public static final String MAP_MAX_WEIGHTED_CAPACITY = CONF_PREFIX + "map.max.weighted.capacity";
    private static final int DEFAULT_MAP_MAX_WEIGHTED_CAPACITY = 10000;

    private static XLog log;

    /*
     * Top-level map key = concatenated identifier for hcatServer + hcatDB
     * value = table map (key = tableName string, value = PartitionsGroup
     */
    private Map<String, Map<String, PartitionsGroup>> hcatInstanceMap;

    /*
     * Map denoting actions and corresponding 'available' partitions
     * key = coordinator actionId, value = available partitions
     */
    private Map<String, List<PartitionWrapper>> availMap;

    private static int maxCapacity;

    @Override
    public void init(Services services) throws ServiceException {
        init(services.getConf());
    }

    public void init(Configuration conf) throws ServiceException {
        hcatInstanceMap = new ConcurrentHashMap<String, Map<String, PartitionsGroup>>();
        availMap = new ConcurrentHashMap<String, List<PartitionWrapper>>();
        maxCapacity = conf.getInt(MAP_MAX_WEIGHTED_CAPACITY, DEFAULT_MAP_MAX_WEIGHTED_CAPACITY);
        log = XLog.getLog(getClass());
    }

    @Override
    public void destroy() {
    }

    @Override
    public Class<? extends Service> getInterface() {
        return PartitionDependencyManagerService.class;
    }

    /**
     * getter for hcatInstanceMap
     */
    public Map<String, Map<String, PartitionsGroup>> getHCatMap() {
        return hcatInstanceMap;
    }

    /**
     * getter for availMap
     */
    public Map<String, List<PartitionWrapper>> getAvailableMap() {
        return availMap;
    }

    /**
     * Adding missing partition entry specified by PartitionWrapper object
     *
     * @param partition
     * @param actionId
     * @throws MetadataServiceException
     */
    @SuppressWarnings("unused")
    public void addMissingPartition(PartitionWrapper partition, String actionId) throws MetadataServiceException {
        String prefix = PartitionWrapper.makePrefix(partition.getServerName(), partition.getDbName());
        Map<String, PartitionsGroup> tablePartitionsMap;
        String tableName = partition.getTableName();
        PartitionsGroup missingPartitions = null;
        WaitingActions actionsList;
        try {
            if (hcatInstanceMap.containsKey(prefix)) {
                tablePartitionsMap = hcatInstanceMap.get(prefix);
                if (tablePartitionsMap.containsKey(tableName)) {
                    actionsList = _getActionsForPartition(tablePartitionsMap, tableName, missingPartitions, partition);
                    if(missingPartitions != null) {
                        if(actionsList != null) {
                            // partition exists, therefore append action
                            actionsList.addAndUpdate(actionId);
                        }
                        else {
                            // new partition entry and info
                            actionsList = new WaitingActions(actionId);
                            missingPartitions.addPartitionAndAction(partition, actionsList);
                        }
                    }
                    else {
                        log.warn("No partition entries for table [{0}]", tableName);
                    }
                }
                else { // new table entry
                    tablePartitionsMap = new ConcurrentHashMap<String, PartitionsGroup>();
                    _createPartitionMapForTable(tablePartitionsMap, tableName, partition, actionId);
                }
            }
            else { // new partition from different hcat server/db
                _addNewEntry(hcatInstanceMap, prefix, tableName, partition, actionId);
            }
        }
        catch (ClassCastException e) {
            throw new MetadataServiceException(ErrorCode.E1501, e.getCause());
        }
        catch (NullPointerException e) {
            throw new MetadataServiceException(ErrorCode.E1501, e.getCause());
        }
        catch (IllegalArgumentException e) {
            throw new MetadataServiceException(ErrorCode.E1501, e.getCause());
        }
    }

    /**
     * Add missing partition entry specified by HCat URI
     *
     * @param hcatURI
     * @param actionId
     * @throws MetadataServiceException
     */
    public void addMissingPartition(String hcatURI, String actionId) throws MetadataServiceException {
        HCatURI uri;
        try {
            uri = new HCatURI(hcatURI);
        }
        catch (URISyntaxException e) {
            throw new MetadataServiceException(ErrorCode.E1503, e.getMessage());
        }
        PartitionWrapper partition = new PartitionWrapper(uri.getServer(), uri.getDb(), uri.getTable(),
                uri.getPartitionMap());
        addMissingPartition(partition, actionId);
    }

    /**
     * Remove partition entry specified by PartitionWrapper object
     * and cascading delete indicator
     *
     * @param partition
     * @param cascade
     * @return true if partition was successfully removed
     * false otherwise
     */
    public boolean removePartition(PartitionWrapper partition, boolean cascade) {
        String prefix = PartitionWrapper.makePrefix(partition.getServerName(), partition.getDbName());
        if (hcatInstanceMap.containsKey(prefix)) {
            Map<String, PartitionsGroup> tableMap = hcatInstanceMap.get(prefix);
            String tableName = partition.getTableName();
            if (tableMap.containsKey(tableName)) {
                PartitionsGroup missingPartitions = tableMap.get(tableName);
                if (missingPartitions != null) {
                    missingPartitions.getPartitionsMap().remove(partition);
                    // cascading removal
                    if (cascade) {
                        if (missingPartitions.getPartitionsMap().size() == 0) {
                            tableMap.remove(tableName);
                            if (tableMap.size() == 0) {
                                hcatInstanceMap.remove(prefix);
                            }
                        }
                    }
                    return true;
                }
                else {
                    log.warn("No partition entries for table [{0}]", tableName);
                }
            }
            else {
                log.warn("HCat table [{0}] not found", tableName);
            }
        }
        else {
            log.warn("HCat instance entry [{0}] not found", prefix);
        }
        return false;
    }

    /**
     * Remove partition entry specified by HCat URI and
     * cascading delete indicator
     *
     * @param hcatURI
     * @param cascade
     * @return true if partition was successfully removed
     * false otherwise
     * @throws MetadataServiceException
     */
    public boolean removePartition(String hcatURI, boolean cascade) throws MetadataServiceException {
        HCatURI uri;
        try {
            uri = new HCatURI(hcatURI);
        }
        catch (URISyntaxException e) {
            throw new MetadataServiceException(ErrorCode.E1503, e.getMessage());
        }
        PartitionWrapper partition = new PartitionWrapper(uri.getServer(), uri.getDb(), uri.getTable(),
                uri.getPartitionMap());
        return removePartition(partition, cascade);
    }

    /**
     * Remove partition entry specified by HCat URI with
     * default cascade mode - TRUE
     *
     * @param hcatURI
     * @return true if partition was successfully removed
     * false otherwise
     * @throws MetadataServiceException
     */
    public boolean removePartition(String hcatURI) throws MetadataServiceException {
        HCatURI uri;
        try {
            uri = new HCatURI(hcatURI);
        }
        catch (URISyntaxException e) {
            throw new MetadataServiceException(ErrorCode.E1503, e.getMessage());
        }
        PartitionWrapper partition = new PartitionWrapper(uri.getServer(), uri.getDb(), uri.getTable(),
                uri.getPartitionMap());
        return removePartition(partition, true);
    }

    /**
     * Move partition entry specified by ParitionWrapper object
     * from 'missing' to 'available' map
     *
     * @param partition
     * @return true if partition was successfully moved to availableMap
     * false otherwise
     */
    public boolean partitionAvailable(PartitionWrapper partition) {
        String prefix = PartitionWrapper.makePrefix(partition.getServerName(), partition.getDbName());
        if (hcatInstanceMap.containsKey(prefix)) {
            Map<String, PartitionsGroup> tableMap = hcatInstanceMap.get(prefix);
            String tableName = partition.getTableName();
            PartitionsGroup missingPartitions = null;
            if (tableMap.containsKey(tableName)) {
                WaitingActions actions = _getActionsForPartition(tableMap, tableName, missingPartitions, partition);
                if(actions != null) {
                    List<String> actionsList = actions.getActions();
                    Iterator<String> it = actionsList.iterator();
                    while (it.hasNext()) { // add actions into separate entries
                        String actionId = it.next();
                        if (availMap.containsKey(actionId)) {
                            // actionId exists, so append partition
                            availMap.get(actionId).add(partition);
                        }
                        else { // new entry
                            availMap.put(actionId,
                                    new CopyOnWriteArrayList<PartitionWrapper>(Arrays.asList((partition))));
                        }
                    }
                    removePartition(partition, true);
                    return true;
                }
                else {
                    log.warn("HCat Partition [{0}] not found", partition.toString());
                }
            }
            else {
                log.warn("HCat table [{0}] not found", tableName);
            }
        }
        else {
            log.warn("HCat instance [{0}] not found", prefix);
        }
        return false;
    }

    /**
     * Move partition entry specified by HCat URI from 'missing' to
     * 'available' map
     *
     * @param hcatURI
     * @return true if partition was successfully moved to availableMap
     * false otherwise
     * @throws MetadataServiceException
     */
    public boolean partitionAvailable(String hcatURI) throws MetadataServiceException {
        HCatURI uri;
        try {
            uri = new HCatURI(hcatURI);
        }
        catch (URISyntaxException e) {
            throw new MetadataServiceException(ErrorCode.E1503, e.getMessage());
        }
        PartitionWrapper partition = new PartitionWrapper(uri.getServer(), uri.getDb(), uri.getTable(),
                uri.getPartitionMap());
        return partitionAvailable(partition);
    }

    private WaitingActions _getActionsForPartition(Map<String, PartitionsGroup> tableMap, String tableName,
            PartitionsGroup missingPartitions, PartitionWrapper partition) {
        WaitingActions actionsList = null;
        missingPartitions = tableMap.get(tableName);
        if (missingPartitions != null && missingPartitions.getPartitionsMap().containsKey(partition)) {
            actionsList = missingPartitions.getPartitionsMap().get(partition);
        }
        else {
            log.warn("HCat Partition [{0}] not found", partition.toString());
        }
        return actionsList;
    }

    private void _createPartitionMapForTable(Map<String, PartitionsGroup> tableMap, String tableName,
            PartitionWrapper partition, String actionId) {
        PartitionsGroup partitions = new PartitionsGroup(
                new ConcurrentLinkedHashMap.Builder<PartitionWrapper, WaitingActions>().maximumWeightedCapacity(
                        maxCapacity).build());
        tableMap.put(tableName, partitions);
        WaitingActions newActions = new WaitingActions(actionId);
        partitions.getPartitionsMap().put(partition, newActions);
    }

    private void _addNewEntry(Map<String, Map<String, PartitionsGroup>> instanceMap, String prefix, String tableName,
            PartitionWrapper partition, String actionId) {
        Map<String, PartitionsGroup> tableMap = new ConcurrentHashMap<String, PartitionsGroup>();
        instanceMap.put(prefix, tableMap);
        _createPartitionMapForTable(tableMap, tableName, partition, actionId);
    }

}
