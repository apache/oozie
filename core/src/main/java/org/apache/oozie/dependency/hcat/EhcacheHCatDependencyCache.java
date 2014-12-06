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
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheException;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.event.CacheEventListener;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.service.HCatAccessorService;
import org.apache.oozie.service.PartitionDependencyManagerService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.HCatURI;
import org.apache.oozie.util.XLog;

public class EhcacheHCatDependencyCache implements HCatDependencyCache, CacheEventListener {

    private static XLog LOG = XLog.getLog(EhcacheHCatDependencyCache.class);
    private static String TABLE_DELIMITER = "#";
    private static String PARTITION_DELIMITER = ";";

    public static String CONF_CACHE_NAME = PartitionDependencyManagerService.CONF_PREFIX + "cache.ehcache.name";

    private CacheManager cacheManager;

    /**
     * Map of server to EhCache which has key as db#table#pk1;pk2#val;val2 and value as WaitingActions (list of
     * WaitingAction) which is Serializable (for overflowToDisk)
     */
    private ConcurrentMap<String, Cache> missingDepsByServer;

    private CacheConfiguration cacheConfig;
    /**
     * Map of server#db#table - sorted part key pattern - count of different partition values (count
     * of elements in the cache) still missing for a partition key pattern. This count is used to
     * quickly determine if there are any more missing dependencies for a table. When the count
     * becomes 0, we unregister from notifications as there are no more missing dependencies for
     * that table.
     */
    private ConcurrentMap<String, ConcurrentMap<String, SettableInteger>> partKeyPatterns;
    /**
     * Map of actionIDs and collection of available URIs
     */
    private ConcurrentMap<String, Collection<String>> availableDeps;

    @Override
    public void init(Configuration conf) {
        String cacheName = conf.get(CONF_CACHE_NAME);
        URL cacheConfigURL;
        if (cacheName == null) {
            cacheConfigURL = this.getClass().getClassLoader().getResource("ehcache-default.xml");
            cacheName = "dependency-default";
        }
        else {
            cacheConfigURL = this.getClass().getClassLoader().getResource("ehcache.xml");
        }
        if (cacheConfigURL == null) {
            throw new IllegalStateException("ehcache.xml is not found in classpath");
        }
        cacheManager = CacheManager.newInstance(cacheConfigURL);
        final Cache specifiedCache = cacheManager.getCache(cacheName);
        if (specifiedCache == null) {
            throw new IllegalStateException("Cache " + cacheName + " configured in " + CONF_CACHE_NAME
                    + " is not found");
        }
        cacheConfig = specifiedCache.getCacheConfiguration();
        missingDepsByServer = new ConcurrentHashMap<String, Cache>();
        partKeyPatterns = new ConcurrentHashMap<String, ConcurrentMap<String, SettableInteger>>();
        availableDeps = new ConcurrentHashMap<String, Collection<String>>();
    }

    @Override
    public void addMissingDependency(HCatURI hcatURI, String actionID) {

        // Create cache for the server if we don't have one
        Cache missingCache = missingDepsByServer.get(hcatURI.getServer());
        if (missingCache == null) {
            CacheConfiguration clonedConfig = cacheConfig.clone();
            clonedConfig.setName(hcatURI.getServer());
            missingCache = new Cache(clonedConfig);
            Cache exists = missingDepsByServer.putIfAbsent(hcatURI.getServer(), missingCache);
            if (exists == null) {
                cacheManager.addCache(missingCache);
                missingCache.getCacheEventNotificationService().registerListener(this);
            }
            else {
                missingCache.dispose(); //discard
            }
        }

        // Add hcat uri into the missingCache
        SortedPKV sortedPKV = new SortedPKV(hcatURI.getPartitionMap());
        String partKeys = sortedPKV.getPartKeys();
        String missingKey = hcatURI.getDb() + TABLE_DELIMITER + hcatURI.getTable() + TABLE_DELIMITER
                + partKeys + TABLE_DELIMITER + sortedPKV.getPartVals();
        boolean newlyAdded = true;
        synchronized (missingCache) {
            Element element = missingCache.get(missingKey);
            if (element == null) {
                WaitingActions waitingActions = new WaitingActions();
                element = new Element(missingKey, waitingActions);
                Element exists = missingCache.putIfAbsent(element);
                if (exists != null) {
                    newlyAdded = false;
                    waitingActions = (WaitingActions) exists.getObjectValue();
                }
                waitingActions.add(new WaitingAction(actionID, hcatURI.toURIString()));
            }
            else {
                newlyAdded = false;
                WaitingActions waitingActions = (WaitingActions) element.getObjectValue();
                waitingActions.add(new WaitingAction(actionID, hcatURI.toURIString()));
            }
        }

        // Increment count for the partition key pattern
        if (newlyAdded) {
            String tableKey = hcatURI.getServer() + TABLE_DELIMITER + hcatURI.getDb() + TABLE_DELIMITER
                    + hcatURI.getTable();
            synchronized (partKeyPatterns) {
                ConcurrentMap<String, SettableInteger> patternCounts = partKeyPatterns.get(tableKey);
                if (patternCounts == null) {
                    patternCounts = new ConcurrentHashMap<String, SettableInteger>();
                    partKeyPatterns.put(tableKey, patternCounts);
                }
                SettableInteger count = patternCounts.get(partKeys);
                if (count == null) {
                    patternCounts.put(partKeys, new SettableInteger(1));
                }
                else {
                    count.increment();
                }
            }
        }
    }

    @Override
    public boolean removeMissingDependency(HCatURI hcatURI, String actionID) {

        Cache missingCache = missingDepsByServer.get(hcatURI.getServer());
        if (missingCache == null) {
            LOG.warn("Remove missing dependency - Missing cache entry for server - uri={0}, actionID={1}",
                    hcatURI.toURIString(), actionID);
            return false;
        }
        SortedPKV sortedPKV = new SortedPKV(hcatURI.getPartitionMap());
        String partKeys = sortedPKV.getPartKeys();
        String missingKey = hcatURI.getDb() + TABLE_DELIMITER + hcatURI.getTable() + TABLE_DELIMITER +
                partKeys + TABLE_DELIMITER + sortedPKV.getPartVals();
        boolean decrement = false;
        boolean removed = false;
        synchronized (missingCache) {
            Element element = missingCache.get(missingKey);
            if (element == null) {
                LOG.warn("Remove missing dependency - Missing cache entry - uri={0}, actionID={1}",
                        hcatURI.toURIString(), actionID);
                return false;
            }
            Collection<WaitingAction> waitingActions = ((WaitingActions) element.getObjectValue()).getWaitingActions();
            removed = waitingActions.remove(new WaitingAction(actionID, hcatURI.toURIString()));
            if (!removed) {
                LOG.warn("Remove missing dependency - Missing action ID - uri={0}, actionID={1}",
                        hcatURI.toURIString(), actionID);
            }
            if (waitingActions.isEmpty()) {
                missingCache.remove(missingKey);
                decrement = true;
            }
        }
        // Decrement partition key pattern count if the cache entry is removed
        if (decrement) {
            String tableKey = hcatURI.getServer() + TABLE_DELIMITER + hcatURI.getDb() + TABLE_DELIMITER
                    + hcatURI.getTable();
            decrementPartKeyPatternCount(tableKey, partKeys, hcatURI.toURIString());
        }
        return removed;
    }

    @Override
    public Collection<String> getWaitingActions(HCatURI hcatURI) {
        Collection<String> actionIDs = null;
        Cache missingCache = missingDepsByServer.get(hcatURI.getServer());
        if (missingCache != null) {
            SortedPKV sortedPKV = new SortedPKV(hcatURI.getPartitionMap());
            String missingKey = hcatURI.getDb() + TABLE_DELIMITER + hcatURI.getTable() + TABLE_DELIMITER
                    + sortedPKV.getPartKeys() + TABLE_DELIMITER + sortedPKV.getPartVals();
            Element element = missingCache.get(missingKey);
            if (element != null) {
                WaitingActions waitingActions = (WaitingActions) element.getObjectValue();
                actionIDs = new ArrayList<String>();
                String uriString = hcatURI.getURI().toString();
                for (WaitingAction action : waitingActions.getWaitingActions()) {
                    if (action.getDependencyURI().equals(uriString)) {
                        actionIDs.add(action.getActionID());
                    }
                }
            }
        }
        return actionIDs;
    }

    @Override
    public Collection<String> markDependencyAvailable(String server, String db, String table,
            Map<String, String> partitions) {
        String tableKey = server + TABLE_DELIMITER + db + TABLE_DELIMITER + table;
        synchronized (partKeyPatterns) {
            Map<String, SettableInteger> patternCounts = partKeyPatterns.get(tableKey);
            if (patternCounts == null) {
                LOG.warn("Got partition available notification for " + tableKey
                        + ". Unexpected as no matching partition keys. Unregistering topic");
                unregisterFromNotifications(server, db, table);
                return null;
            }
            Cache missingCache = missingDepsByServer.get(server);
            if (missingCache == null) {
                LOG.warn("Got partition available notification for " + tableKey
                        + ". Unexpected. Missing server entry in cache. Unregistering topic");
                partKeyPatterns.remove(tableKey);
                unregisterFromNotifications(server, db, table);
                return null;
            }
            Collection<String> actionsWithAvailDep = new HashSet<String>();
            StringBuilder partValSB = new StringBuilder();
            // If partition patterns are date, date;country and date;country;state,
            // construct the partition values for each pattern and for the matching value in the
            // missingCache, get the waiting actions and mark it as available.
            for (Entry<String, SettableInteger> entry : patternCounts.entrySet()) {
                String[] partKeys = entry.getKey().split(PARTITION_DELIMITER);
                partValSB.setLength(0);
                for (String key : partKeys) {
                    partValSB.append(partitions.get(key)).append(PARTITION_DELIMITER);
                }
                partValSB.setLength(partValSB.length() - 1);
                String missingKey = db + TABLE_DELIMITER + table + TABLE_DELIMITER + entry.getKey() + TABLE_DELIMITER
                        + partValSB.toString();
                boolean removed = false;
                Element element = null;
                synchronized (missingCache) {
                    element = missingCache.get(missingKey);
                    if (element != null) {
                        missingCache.remove(missingKey);
                        removed = true;
                    }
                }
                if (removed) {
                    decrementPartKeyPatternCount(tableKey, entry.getKey(), server + TABLE_DELIMITER + missingKey);
                    // Add the removed entry to available dependencies
                    Collection<WaitingAction> wActions = ((WaitingActions) element.getObjectValue())
                            .getWaitingActions();
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
                }
            }
            return actionsWithAvailDep;
        }
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
        availableDeps.clear();
        cacheManager.shutdown();
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException();
    }

    @Override
    public void dispose() {
    }

    @Override
    public void notifyElementExpired(Ehcache cache, Element element) {
        // Invoked when timeToIdleSeconds or timeToLiveSeconds is met
        String missingDepKey = (String) element.getObjectKey();
        LOG.info("Cache entry [{0}] of cache [{1}] expired", missingDepKey, cache.getName());
        onExpiryOrEviction(cache, element, missingDepKey);
    }

    @Override
    public void notifyElementPut(Ehcache arg0, Element arg1) throws CacheException {

    }

    @Override
    public void notifyElementRemoved(Ehcache arg0, Element arg1) throws CacheException {
    }

    @Override
    public void notifyElementUpdated(Ehcache arg0, Element arg1) throws CacheException {
    }

    @Override
    public void notifyRemoveAll(Ehcache arg0) {
    }

    @Override
    public void notifyElementEvicted(Ehcache cache, Element element) {
        // Invoked when maxElementsInMemory is met
        String missingDepKey = (String) element.getObjectKey();
        LOG.info("Cache entry [{0}] of cache [{1}] evicted", missingDepKey, cache.getName());
        onExpiryOrEviction(cache, element, missingDepKey);
    }

    private void onExpiryOrEviction(Ehcache cache, Element element, String missingDepKey) {
        int partValIndex = missingDepKey.lastIndexOf(TABLE_DELIMITER);
        int partKeyIndex = missingDepKey.lastIndexOf(TABLE_DELIMITER, partValIndex - 1);
        // server#db#table. Name of the cache is that of the server.
        String tableKey = cache.getName() + TABLE_DELIMITER + missingDepKey.substring(0, partKeyIndex);
        String partKeys = missingDepKey.substring(partKeyIndex + 1, partValIndex);
        decrementPartKeyPatternCount(tableKey, partKeys, missingDepKey);
    }

    /**
     * Decrement partition key pattern count, once a hcat URI is removed from the cache
     *
     * @param tableKey key identifying the table - server#db#table
     * @param partKeys partition key pattern
     * @param hcatURI URI with the partition key pattern
     */
    private void decrementPartKeyPatternCount(String tableKey, String partKeys, String hcatURI) {
        synchronized (partKeyPatterns) {
            Map<String, SettableInteger> patternCounts = partKeyPatterns.get(tableKey);
            if (patternCounts == null) {
                LOG.warn("Removed dependency - Missing cache entry - uri={0}. "
                        + "But no corresponding pattern key table entry", hcatURI);
            }
            else {
                SettableInteger count = patternCounts.get(partKeys);
                if (count == null) {
                    LOG.warn("Removed dependency - Missing cache entry - uri={0}. "
                            + "But no corresponding pattern key entry", hcatURI);
                }
                else {
                    count.decrement();
                    if (count.getValue() == 0) {
                        patternCounts.remove(partKeys);
                    }
                    if (patternCounts.isEmpty()) {
                        partKeyPatterns.remove(tableKey);
                        String[] tableDetails = tableKey.split(TABLE_DELIMITER);
                        unregisterFromNotifications(tableDetails[0], tableDetails[1], tableDetails[2]);
                    }
                }
            }
        }
    }

    private void unregisterFromNotifications(String server, String db, String table) {
        // Close JMS session. Stop listening on topic
        HCatAccessorService hcatService = Services.get().get(HCatAccessorService.class);
        hcatService.unregisterFromNotification(server, db, table);
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
                this.partKeys.append(key).append(PARTITION_DELIMITER);
                this.partVals.append(partitions.get(key)).append(PARTITION_DELIMITER);
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

    private static class SettableInteger {
        private int value;

        public SettableInteger(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public void increment() {
            value++;
        }

        public void decrement() {
            value--;
        }
    }

    @Override
    public void removeNonWaitingCoordActions(Set<String> staleActions) {
        Iterator<String> serverItr = missingDepsByServer.keySet().iterator();
        while (serverItr.hasNext()) {
            String server = serverItr.next();
            Cache missingCache = missingDepsByServer.get(server);
            if (missingCache == null) {
                continue;
            }
            synchronized (missingCache) {
                for (Object key : missingCache.getKeys()) {
                    Element element = missingCache.get(key);
                    if (element == null) {
                        continue;
                    }
                    Collection<WaitingAction> waitingActions = ((WaitingActions) element.getObjectValue())
                            .getWaitingActions();
                    Iterator<WaitingAction> wactionItr = waitingActions.iterator();
                    HCatURI hcatURI = null;
                    while(wactionItr.hasNext()) {
                        WaitingAction waction = wactionItr.next();
                        if(staleActions.contains(waction.getActionID())) {
                            try {
                                hcatURI = new HCatURI(waction.getDependencyURI());
                                wactionItr.remove();
                            }
                            catch (URISyntaxException e) {
                                continue;
                            }
                        }
                    }
                    if (waitingActions.isEmpty() && hcatURI != null) {
                        missingCache.remove(key);
                        // Decrement partition key pattern count if the cache entry is removed
                        SortedPKV sortedPKV = new SortedPKV(hcatURI.getPartitionMap());
                        String partKeys = sortedPKV.getPartKeys();
                        String tableKey = hcatURI.getServer() + TABLE_DELIMITER + hcatURI.getDb() + TABLE_DELIMITER
                                + hcatURI.getTable();
                        String hcatURIStr = hcatURI.toURIString();
                        decrementPartKeyPatternCount(tableKey, partKeys, hcatURIStr);
                    }
                }
            }
        }
    }

    @Override
    public void removeCoordActionWithDependenciesAvailable(String coordAction) {
        // to be implemented when reverse-lookup data structure for purging is added
    }

}
