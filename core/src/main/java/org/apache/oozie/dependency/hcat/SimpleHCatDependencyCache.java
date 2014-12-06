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

package org.apache.oozie.dependency.hcat;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.service.HCatAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.HCatURI;
import org.apache.oozie.util.XLog;

public class SimpleHCatDependencyCache implements HCatDependencyCache {

    private static XLog LOG = XLog.getLog(SimpleHCatDependencyCache.class);
    private static String DELIMITER = ";";

    /**
     * Map of server;db;table - sorter partition key order (country;dt;state) - sorted partition
     * value (us;20120101;CA) - Collection of waiting actions (actionID and original hcat uri as
     * string).
     */
    private ConcurrentMap<String, ConcurrentMap<String, Map<String, Collection<WaitingAction>>>> missingDeps;

    /**
     * Map of actionIDs and collection of available URIs
     */
    private ConcurrentMap<String, Collection<String>> availableDeps;

    /**
     * Map of actionIDs and partitions for reverse-lookup in purging
     */
    private ConcurrentMap<String, ConcurrentMap<String, Collection<String>>> actionPartitionMap;

    @Override
    public void init(Configuration conf) {
        missingDeps = new ConcurrentHashMap<String, ConcurrentMap<String, Map<String, Collection<WaitingAction>>>>();
        availableDeps = new ConcurrentHashMap<String, Collection<String>>();
        actionPartitionMap = new ConcurrentHashMap<String, ConcurrentMap<String, Collection<String>>>();
    }

    @Override
    public void addMissingDependency(HCatURI hcatURI, String actionID) {
        String tableKey = hcatURI.getServer() + DELIMITER + hcatURI.getDb() + DELIMITER + hcatURI.getTable();
        SortedPKV sortedPKV = new SortedPKV(hcatURI.getPartitionMap());
        // Partition keys seperated by ;. For eg: date;country;state
        String partKey = sortedPKV.getPartKeys();
        // Partition values seperated by ;. For eg: 20120101;US;CA
        String partVal = sortedPKV.getPartVals();
        ConcurrentMap<String, Map<String, Collection<WaitingAction>>> partKeyPatterns = missingDeps.get(tableKey);
        if (partKeyPatterns == null) {
            partKeyPatterns = new ConcurrentHashMap<String, Map<String, Collection<WaitingAction>>>();
            ConcurrentMap<String, Map<String, Collection<WaitingAction>>> existingMap = missingDeps.putIfAbsent(
                    tableKey, partKeyPatterns);
            if (existingMap != null) {
                partKeyPatterns = existingMap;
            }
        }
        ConcurrentMap<String, Collection<String>> partitionMap = actionPartitionMap.get(actionID);
        if (partitionMap == null) {
            partitionMap = new ConcurrentHashMap<String, Collection<String>>();
            ConcurrentMap<String, Collection<String>> existingPartMap = actionPartitionMap.putIfAbsent(actionID,
                    partitionMap);
            if (existingPartMap != null) {
                partitionMap = existingPartMap;
            }
        }
        synchronized (partitionMap) {
            Collection<String> partKeys = partitionMap.get(tableKey);
            if (partKeys == null) {
                partKeys = new ArrayList<String>();
            }
            partKeys.add(partKey);
            partitionMap.put(tableKey, partKeys);
        }
        synchronized (partKeyPatterns) {
            missingDeps.put(tableKey, partKeyPatterns); // To handle race condition with removal of partKeyPatterns
            Map<String, Collection<WaitingAction>> partValues = partKeyPatterns.get(partKey);
            if (partValues == null) {
                partValues = new HashMap<String, Collection<WaitingAction>>();
                partKeyPatterns.put(partKey, partValues);
            }
            Collection<WaitingAction> waitingActions = partValues.get(partVal);
            if (waitingActions == null) {
                waitingActions = new HashSet<WaitingAction>();
                partValues.put(partVal, waitingActions);
            }
            waitingActions.add(new WaitingAction(actionID, hcatURI.toURIString()));
        }
    }

    @Override
    public boolean removeMissingDependency(HCatURI hcatURI, String actionID) {
        String tableKey = hcatURI.getServer() + DELIMITER + hcatURI.getDb() + DELIMITER + hcatURI.getTable();
        SortedPKV sortedPKV = new SortedPKV(hcatURI.getPartitionMap());
        String partKey = sortedPKV.getPartKeys();
        String partVal = sortedPKV.getPartVals();
        Map<String, Map<String, Collection<WaitingAction>>> partKeyPatterns = missingDeps.get(tableKey);
        if (partKeyPatterns == null) {
            LOG.warn("Remove missing dependency - Missing table entry - uri={0}, actionID={1}",
                    hcatURI.toURIString(), actionID);
            return false;
        }
        ConcurrentMap<String, Collection<String>> partitionMap = actionPartitionMap.get(actionID);
        if (partitionMap != null) {
            synchronized (partitionMap) {
                Collection<String> partKeys = partitionMap.get(tableKey);
                if (partKeys != null) {
                    partKeys.remove(partKey);
                }
                if (partKeys.size() == 0) {
                    partitionMap.remove(tableKey);
                }
                if (partitionMap.size() == 0) {
                    actionPartitionMap.remove(actionID);
                }
            }
        }

        synchronized(partKeyPatterns) {
            Map<String, Collection<WaitingAction>> partValues = partKeyPatterns.get(partKey);
            if (partValues == null) {
                LOG.warn("Remove missing dependency - Missing partition pattern - uri={0}, actionID={1}",
                        hcatURI.toURIString(), actionID);
                return false;
            }
            Collection<WaitingAction> waitingActions = partValues.get(partVal);
            if (waitingActions == null) {
                LOG.warn("Remove missing dependency - Missing partition value - uri={0}, actionID={1}",
                        hcatURI.toURIString(), actionID);
                return false;
            }
            boolean removed = waitingActions.remove(new WaitingAction(actionID, hcatURI.toURIString()));
            if (!removed) {
                LOG.warn("Remove missing dependency - Missing action ID - uri={0}, actionID={1}",
                        hcatURI.toURIString(), actionID);
            }
            if (waitingActions.isEmpty()) {
                partValues.remove(partVal);
                if (partValues.isEmpty()) {
                    partKeyPatterns.remove(partKey);
                }
                if (partKeyPatterns.isEmpty()) {
                    missingDeps.remove(tableKey);
                    // Close JMS session. Stop listening on topic
                    HCatAccessorService hcatService = Services.get().get(HCatAccessorService.class);
                    hcatService.unregisterFromNotification(hcatURI);
                }
            }
            return removed;
        }
    }

    @Override
    public Collection<String> getWaitingActions(HCatURI hcatURI) {
        String tableKey = hcatURI.getServer() + DELIMITER + hcatURI.getDb() + DELIMITER + hcatURI.getTable();
        SortedPKV sortedPKV = new SortedPKV(hcatURI.getPartitionMap());
        String partKey = sortedPKV.getPartKeys();
        String partVal = sortedPKV.getPartVals();
        Map<String, Map<String, Collection<WaitingAction>>> partKeyPatterns = missingDeps.get(tableKey);
        if (partKeyPatterns == null) {
            return null;
        }
        Map<String, Collection<WaitingAction>> partValues = partKeyPatterns.get(partKey);
        if (partValues == null) {
            return null;
        }
        Collection<WaitingAction> waitingActions = partValues.get(partVal);
        if (waitingActions == null) {
            return null;
        }
        Collection<String> actionIDs = new ArrayList<String>();
        String uriString = hcatURI.toURIString();
        for (WaitingAction action : waitingActions) {
            if (action.getDependencyURI().equals(uriString)) {
                actionIDs.add(action.getActionID());
            }
        }
        return actionIDs;
    }

    @Override
    public Collection<String> markDependencyAvailable(String server, String db, String table,
            Map<String, String> partitions) {
        String tableKey = server + DELIMITER + db + DELIMITER + table;
        Map<String, Map<String, Collection<WaitingAction>>> partKeyPatterns = missingDeps.get(tableKey);
        if (partKeyPatterns == null) {
            LOG.warn("Got partition available notification for " + tableKey
                    + ". Unexpected and should not be listening to topic. Unregistering topic");
            HCatAccessorService hcatService = Services.get().get(HCatAccessorService.class);
            hcatService.unregisterFromNotification(server, db, table);
            return null;
        }
        Collection<String> actionsWithAvailDep = new HashSet<String>();
        List<String> partKeysToRemove = new ArrayList<String>();
        StringBuilder partValSB = new StringBuilder();
        synchronized (partKeyPatterns) {
            // If partition patterns are date, date;country and date;country;state,
            // construct the partition values for each pattern from the available partitions map and
            // for the matching value in the dependency map, get the waiting actions.
            for (Entry<String, Map<String, Collection<WaitingAction>>> entry : partKeyPatterns.entrySet()) {
                String[] partKeys = entry.getKey().split(DELIMITER);
                partValSB.setLength(0);
                for (String key : partKeys) {
                    partValSB.append(partitions.get(key)).append(DELIMITER);
                }
                partValSB.setLength(partValSB.length() - 1);

                Map<String, Collection<WaitingAction>> partValues = entry.getValue();
                String partVal = partValSB.toString();
                Collection<WaitingAction> wActions = entry.getValue().get(partVal);
                if (wActions == null)
                    continue;
                for (WaitingAction wAction : wActions) {
                    String actionID = wAction.getActionID();
                    actionsWithAvailDep.add(actionID);
                    Collection<String> depURIs = availableDeps.get(actionID);
                    if (depURIs == null) {
                        depURIs = new ArrayList<String>();
                        Collection<String> existing = availableDeps.putIfAbsent(actionID, depURIs);
                        if (existing != null) {
                            depURIs = existing;
                        }
                    }
                    synchronized (depURIs) {
                        depURIs.add(wAction.getDependencyURI());
                        availableDeps.put(actionID, depURIs);
                    }
                }
                partValues.remove(partVal);
                if (partValues.isEmpty()) {
                    partKeysToRemove.add(entry.getKey());
                }
            }
            for (String partKey : partKeysToRemove) {
                partKeyPatterns.remove(partKey);
            }
            if (partKeyPatterns.isEmpty()) {
                missingDeps.remove(tableKey);
                // Close JMS session. Stop listening on topic
                HCatAccessorService hcatService = Services.get().get(HCatAccessorService.class);
                hcatService.unregisterFromNotification(server, db, table);
            }
        }
        return actionsWithAvailDep;
    }

    @Override
    public Collection<String> getAvailableDependencyURIs(String actionID) {
        Collection<String> available = availableDeps.get(actionID);
        if (available !=  null) {
            // Return a copy
            available = new ArrayList<String>(available);
        }
        return available;
    }

    @Override
    public boolean removeAvailableDependencyURIs(String actionID, Collection<String> dependencyURIs) {
        if (!availableDeps.containsKey(actionID)) {
            return false;
        }
        else {
            Collection<String> availList = availableDeps.get(actionID);
            if (!availList.removeAll(dependencyURIs)) {
                return false;
            }
            synchronized (availList) {
                if (availList.isEmpty()) {
                    availableDeps.remove(actionID);
                }
            }
        }
        return true;
    }

    @Override
    public void destroy() {
        missingDeps.clear();
        availableDeps.clear();
    }

    private static class SortedPKV {
        private StringBuilder partKeys;
        private StringBuilder partVals;

        public SortedPKV(Map<String, String> partitions) {
            this.partKeys = new StringBuilder();
            this.partVals = new StringBuilder();
            ArrayList<String> keys = new ArrayList<String>(partitions.keySet());
            Collections.sort(keys);
            for (String key : keys) {
                this.partKeys.append(key).append(DELIMITER);
                this.partVals.append(partitions.get(key)).append(DELIMITER);
            }
            this.partKeys.setLength(partKeys.length() - 1);
            this.partVals.setLength(partVals.length() - 1);
        }

        public String getPartKeys() {
            return partKeys.toString();
        }

        public String getPartVals() {
            return partVals.toString();
        }
    }

    private HCatURI removePartitions(String coordActionId, Collection<String> partKeys,
            Map<String, Map<String, Collection<WaitingAction>>> partKeyPatterns) {
        HCatURI hcatUri = null;
        for (String partKey : partKeys) {
            Map<String, Collection<WaitingAction>> partValues = partKeyPatterns.get(partKey);
            Iterator<String> partValItr = partValues.keySet().iterator();
            while (partValItr.hasNext()) {
                String partVal = partValItr.next();
                Collection<WaitingAction> waitingActions = partValues.get(partVal);
                if (waitingActions != null) {
                    Iterator<WaitingAction> waitItr = waitingActions.iterator();
                    while (waitItr.hasNext()) {
                        WaitingAction waction = waitItr.next();
                        if (coordActionId.contains(waction.getActionID())) {
                            waitItr.remove();
                            if (hcatUri == null) {
                                try {
                                    hcatUri = new HCatURI(waction.getDependencyURI());
                                }
                                catch (URISyntaxException e) {
                                    continue;
                                }
                            }
                        }
                    }
                }
                // delete partition value with no waiting actions
                if (waitingActions.size() == 0) {
                    partValItr.remove();
                }
            }
            if (partValues.size() == 0) {
                partKeyPatterns.remove(partKey);
            }
        }
        return hcatUri;
    }

    @Override
    public void removeNonWaitingCoordActions(Set<String> coordActions) {
        HCatAccessorService hcatService = Services.get().get(HCatAccessorService.class);
        for (String coordActionId : coordActions) {
            LOG.info("Removing non waiting coord action {0} from partition dependency map", coordActionId);
            synchronized (actionPartitionMap) {
                Map<String, Collection<String>> partitionMap = actionPartitionMap.get(coordActionId);
                if (partitionMap != null) {
                    Iterator<String> tableItr = partitionMap.keySet().iterator();
                    while (tableItr.hasNext()) {
                        String tableKey = tableItr.next();
                        HCatURI hcatUri = null;
                        Map<String, Map<String, Collection<WaitingAction>>> partKeyPatterns = missingDeps.get(tableKey);
                        if (partKeyPatterns != null) {
                            synchronized (partKeyPatterns) {
                                Collection<String> partKeys = partitionMap.get(tableKey);
                                if (partKeys != null) {
                                    hcatUri = removePartitions(coordActionId, partKeys, partKeyPatterns);
                                }
                            }
                            if (partKeyPatterns.size() == 0) {
                                tableItr.remove();
                                if (hcatUri != null) {
                                    // Close JMS session. Stop listening on topic
                                    hcatService.unregisterFromNotification(hcatUri);
                                }
                            }
                        }
                    }
                }
                actionPartitionMap.remove(coordActionId);
            }
        }
    }

    @Override
    public void removeCoordActionWithDependenciesAvailable(String coordAction) {
        actionPartitionMap.remove(coordAction);
    }
}
