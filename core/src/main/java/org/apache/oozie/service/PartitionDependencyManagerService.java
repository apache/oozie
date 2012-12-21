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

import javax.jms.JMSException;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.command.coord.CoordActionUpdatePushMissingDependency;
import org.apache.oozie.jms.HCatMessageHandler;
import org.apache.oozie.jms.MessageReceiver;
import org.apache.oozie.util.HCatURI;
import org.apache.oozie.util.PartitionWrapper;
import org.apache.oozie.util.PartitionsGroup;
import org.apache.oozie.util.WaitingActions;
import org.apache.oozie.util.XCallable;
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
     * Top-level map key = concatenated identifier for hcatServer + hcatDB value
     * = table map (key = tableName string, value = PartitionsGroup
     */
    private Map<String, Map<String, PartitionsGroup>> hcatInstanceMap;

    /*
     * Map denoting actions and corresponding 'available' partitions key =
     * coordinator actionId, value = available partitions
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
        log.debug("PartitionDependencyManagerService initialized");
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
     * Returns a list of partitions for an actionId. 'null' if there is nothing
     *
     * @param actionId
     * @return List of partitions
     */
    public List<PartitionWrapper> getAvailablePartitions(String actionId) {
        return availMap.get(actionId);
    }

    /**
     * Remove an action from missing partition map
     *
     * @param hcatURI
     * @param actionId
     * @return
     * @throws MetadataServiceException
     */
    public boolean removeActionFromMissingPartitions(String hcatURI, String actionId) throws MetadataServiceException {
        boolean ret = false;
        HCatURI uri;
        try {
            uri = new HCatURI(hcatURI);
        }
        catch (URISyntaxException e) {
            throw new MetadataServiceException(ErrorCode.E1025, e.getMessage());
        }
        PartitionWrapper partition = new PartitionWrapper(uri.getServer(), uri.getDb(), uri.getTable(),
                uri.getPartitionMap());
        List<String> actions = _getActionsForPartition(partition);
        if (actions != null && actions.size() != 0) {
            ret = actions.remove(actionId);
        }
        else {
            log.info("No waiting actions in the partition [{0}], no-ops", partition);
        }
        return ret;
    }

    /**
     * Adding missing partition entry specified by PartitionWrapper object
     *
     * @param partition
     * @param actionId
     * @throws MetadataServiceException
     */
    private void addMissingPartition(PartitionWrapper partition, String actionId) throws MetadataServiceException {
        String prefix = PartitionWrapper.makePrefix(partition.getServerName(), partition.getDbName());
        Map<String, PartitionsGroup> tablePartitionsMap;
        String tableName = partition.getTableName();
        try {
            if (hcatInstanceMap.containsKey(prefix)) {
                tablePartitionsMap = hcatInstanceMap.get(prefix);
                if (tablePartitionsMap.containsKey(tableName)) {
                    addPartitionEntry(tablePartitionsMap, tableName, partition, actionId);
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
        catch (Exception e) {
            throw new MetadataServiceException(ErrorCode.E1501, e.getCause());
        }
    }

    private void _registerMessageReceiver(PartitionWrapper partition, String serverEndPoint) throws MetadataServiceException {
        String topic = _getTopic(partition);
        try {
            MessageReceiver recvr = Services.get().get(JMSAccessorService.class).getTopicReceiver(serverEndPoint, topic);
            //Register new listener only if topic is new. Else do nothing
            if(recvr == null) {
                //Registering new receiver
                recvr = new MessageReceiver(new HCatMessageHandler());
                log.debug("Registering to listen on topic :" + topic + "for endpoint " + serverEndPoint);
                recvr.registerTopic(serverEndPoint, topic); //server-endpoint is obtained from partition
            }
        }
        catch (JMSException e) {
            throw new MetadataServiceException(ErrorCode.E1506, e.getCause());
        }
    }

    private void _deregisterMessageReceiver(PartitionWrapper part) throws MetadataServiceException {
        String topic = _getTopic(part);
        log.debug("Deregistering receiver for topic:[" + topic + "]");
        try {
            MessageReceiver recvr = Services.get().get(JMSAccessorService.class).getTopicReceiver(topic);
            recvr.unRegisterTopic(topic);
        }
        catch (JMSException e) {
            throw new MetadataServiceException(ErrorCode.E1506, e.getCause());
        }
    }

    private String _getTopic(PartitionWrapper partition) {
        // TODO: Get it from HCAT directly. HCat doesn't support it yet
        StringBuilder topic = new StringBuilder();
        topic.append("hcat.").append(partition.getDbName()).append(".").append(partition.getTableName());
        return topic.toString();
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
            throw new MetadataServiceException(ErrorCode.E1025, e.getMessage());
        }
        PartitionWrapper partition = new PartitionWrapper(uri.getServer(), uri.getDb(), uri.getTable(),
                uri.getPartitionMap());
        addMissingPartition(partition, actionId);
        _registerMessageReceiver(partition, uri.getServerEndPoint());
    }


    public void addMissingPartitions(String[] hcatURIs, String actionId) throws MetadataServiceException {
        for (String uri : hcatURIs) {
            if (uri != null && uri.length() > 0) {
                addMissingPartition(uri, actionId);
            }
        }
    }

    /** Remove available partitions for an action
     *
     * @param partitions
     * @param actionId
     * @return
     * @throws MetadataServiceException
     */
    public boolean removeAvailablePartitions(List<PartitionWrapper> partitions, String actionId)
            throws MetadataServiceException {
        List<PartitionWrapper> availList = null;
        if (!availMap.containsKey(actionId)) {
            return false;
        }
        else {
            availList = availMap.get(actionId);
        }
        if (!availList.removeAll(partitions)) {
            return false;
        }
        if (availList.isEmpty()) {
            availMap.remove(actionId);
        }
        return true;
    }


    /**
     * Remove partition entry specified by PartitionWrapper object and cascading
     * delete indicator
     *
     * @param partition
     * @param cascade
     * @return true if partition was successfully removed false otherwise
     * @throws MetadataServiceException
     */
    public boolean removePartition(PartitionWrapper partition, boolean cascade) throws MetadataServiceException {
        log.debug("Removing partition " + partition + " with  cascade :" + cascade);
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
                            // TODO - do unregistering in a synchronized way
                            //_deregisterMessageReceiver(partition);
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
     * Remove partition entry specified by HCat URI and cascading delete
     * indicator
     *
     * @param hcatURI
     * @param cascade
     * @return true if partition was successfully removed false otherwise
     * @throws MetadataServiceException
     */
    public boolean removePartition(String hcatURI, boolean cascade) throws MetadataServiceException {
        HCatURI uri;
        try {
            uri = new HCatURI(hcatURI);
        }
        catch (URISyntaxException e) {
            throw new MetadataServiceException(ErrorCode.E1025, e.getMessage());
        }
        PartitionWrapper partition = new PartitionWrapper(uri.getServer(), uri.getDb(), uri.getTable(),
                uri.getPartitionMap());
        return removePartition(partition, cascade);
    }

    /**
     * Remove partition entry specified by HCat URI with default cascade mode -
     * TRUE
     *
     * @param hcatURI
     * @return true if partition was successfully removed false otherwise
     * @throws MetadataServiceException
     */
    public boolean removePartition(String hcatURI) throws MetadataServiceException {
        HCatURI uri;
        try {
            uri = new HCatURI(hcatURI);
        }
        catch (URISyntaxException e) {
            throw new MetadataServiceException(ErrorCode.E1025, e.getMessage());
        }
        PartitionWrapper partition = new PartitionWrapper(uri.getServer(), uri.getDb(), uri.getTable(),
                uri.getPartitionMap());
        return removePartition(partition, true);
    }

    /**
     * Move partition entry specified by ParitionWrapper object from 'missing'
     * to 'available' map
     *
     * @param partition
     * @return true if partition was successfully moved to availableMap
     * false otherwise
     * @throws MetadataServiceException
     */
    public boolean partitionAvailable(PartitionWrapper partition) throws MetadataServiceException {
        log.debug("Making partition [{0}] available ", partition);
        List<String> actionsList = _getActionsForPartition(partition);
        if (actionsList != null) {
            log.debug("List of actions to be updated: " + actionsList);
            Iterator<String> it = actionsList.iterator();
            while (it.hasNext()) { // add actions into separate entries
                String actionId = it.next();
                if (availMap.containsKey(actionId)) {
                    // actionId exists, so append partition
                    availMap.get(actionId).add(partition);
                }
                else { // new entry
                    availMap.put(actionId, new CopyOnWriteArrayList<PartitionWrapper>(Arrays.asList((partition))));
                }
                queueCallable(new CoordActionUpdatePushMissingDependency(actionId));
            }
            removePartition(partition, true);
            return true;
        }
        else {
            log.warn("No coord actions waitings for HCat Partition [{0}]", partition.toString());
        }

        return false;
    }

    /**
     * Move partition entry specified by HCat URI from 'missing' to 'available'
     * map
     *
     * @param hcatURI
     * @return true if partition was successfully moved to availableMap false
     *         otherwise
     * @throws MetadataServiceException
     */
    public boolean partitionAvailable(String hcatURI) throws MetadataServiceException {
        HCatURI uri;
        try {
            uri = new HCatURI(hcatURI);
        }
        catch (URISyntaxException e) {
            throw new MetadataServiceException(ErrorCode.E1025, e.getMessage());
        }
        PartitionWrapper partition = new PartitionWrapper(uri.getServer(), uri.getDb(), uri.getTable(),
                uri.getPartitionMap());
        return partitionAvailable(partition);
    }

    /**
     * Determine if a partition entry exists in cache
     *
     * @param hcatURI
     * @return true if partition exists in map, false otherwise
     * @throws MetadataServiceException
     */
    public boolean containsPartition(String hcatURI) throws MetadataServiceException {
        HCatURI uri;
        try {
            uri = new HCatURI(hcatURI);
        }
        catch (URISyntaxException e) {
            throw new MetadataServiceException(ErrorCode.E1025, e.getMessage());
        }
        PartitionWrapper partition = new PartitionWrapper(uri.getServer(), uri.getDb(), uri.getTable(),
                uri.getPartitionMap());
        return containsPartition(partition);
    }


    /**
     * Determine if a partition entry exists in cache
     *
     * @param partition
     * @return true if partition exists in map, false otherwise
     */
    public boolean containsPartition(PartitionWrapper partition) {
        String prefix = PartitionWrapper.makePrefix(partition.getServerName(), partition.getDbName());
        String tableName = partition.getTableName();
        boolean result = false;
        if (hcatInstanceMap.containsKey(prefix)) {
            Map<String, PartitionsGroup> tableMap = hcatInstanceMap.get(prefix);
            if (tableMap != null && tableMap.containsKey(tableName)) {
                PartitionsGroup partitionsMap = tableMap.get(tableName);
                if (partitionsMap != null && partitionsMap.getPartitionsMap().containsKey(partition)) {
                    result = true;
                }
            }
        }
        return result;
    }

    private void addPartitionEntry(Map<String, PartitionsGroup> tableMap, String tableName, PartitionWrapper partition,
            String actionId) {
        WaitingActions actionsList = null;
        PartitionsGroup missingPartitions = tableMap.get(tableName);
        if (missingPartitions != null && missingPartitions.getPartitionsMap().containsKey(partition)) {
            actionsList = missingPartitions.getPartitionsMap().get(partition);
            if (actionsList != null) {
                // partition exists, therefore append action
                actionsList.addAndUpdate(actionId);
            }
        }
        else {
            if (missingPartitions != null) {
                // new partition entry and info
                actionsList = new WaitingActions(actionId);
                missingPartitions.addPartitionAndAction(partition, actionsList);
            }
            else {
                log.warn(" missingPartitions for table [{0}]  is NULL.", tableName);
            }
        }
    }

    private List<String> _getActionsForPartition(PartitionWrapper partition) {
        String prefix = PartitionWrapper.makePrefix(partition.getServerName(), partition.getDbName());
        if (hcatInstanceMap.containsKey(prefix)) {
            Map<String, PartitionsGroup> tableMap = hcatInstanceMap.get(prefix);
            String tableName = partition.getTableName();
            if (tableMap.containsKey(tableName)) {
                PartitionsGroup missingPartitions = tableMap.get(tableName);
                if (missingPartitions != null) {
                    if (missingPartitions.getPartitionsMap().containsKey(partition)) {
                        WaitingActions actions = missingPartitions.getPartitionsMap().get(partition);
                        if (actions != null) {
                            return actions.getActions();
                        }
                        else {
                            log.warn("No coord actions waitings for HCat Partition [{0}]", partition.toString());
                        }
                    }
                    else {
                        log.warn("HCat Partition [{0}] not found", partition.toString());
                    }
                }
                else {
                    log.warn("MissingPartitions not created in HCat table [{0}]", tableName);
                }
            }
            else {
                log.warn("HCat table [{0}] not found", tableName);
            }
        }
        else {
            log.warn("HCat instance [{0}] not found", prefix);
        }

        return null;
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
        log.debug("Adding a New Entry for [{0}] with partition [{1}]  Action Id [{2}]", prefix, partition, actionId);
        Map<String, PartitionsGroup> tableMap = new ConcurrentHashMap<String, PartitionsGroup>();
        instanceMap.put(prefix, tableMap);
        _createPartitionMapForTable(tableMap, tableName, partition, actionId);
    }

    private void queueCallable(XCallable<?> callable) {
        boolean ret = Services.get().get(CallableQueueService.class).queue(callable);
        if (ret == false) {
            XLog.getLog(getClass()).warn(
                    "Unable to queue the callable commands for PartitionDependencyManagerService. "
                            + "Most possibly command queue is full. Queue size is :"
                            + Services.get().get(CallableQueueService.class).queueSize());
        }
    }

}
