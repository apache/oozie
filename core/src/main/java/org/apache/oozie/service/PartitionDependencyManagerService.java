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

import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.oozie.command.coord.CoordActionUpdatePushMissingDependency;
import org.apache.oozie.dependency.hcat.HCatDependencyCache;
import org.apache.oozie.dependency.hcat.SimpleHCatDependencyCache;
import org.apache.oozie.util.HCatURI;
import org.apache.oozie.util.XLog;

/**
 * Module that functions like a caching service to maintain partition dependency mappings
 */
public class PartitionDependencyManagerService implements Service {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "PartitionDependencyManagerService.";
    public static final String CACHE_MANAGER_IMPL = CONF_PREFIX + "cache.manager.impl";

    private static XLog LOG = XLog.getLog(PartitionDependencyManagerService.class);

    private HCatDependencyCache dependencyCache;

    @Override
    public void init(Services services) throws ServiceException {
        init(services.getConf());
    }

    private void init(Configuration conf) throws ServiceException {
        Class<?> defaultClass = conf.getClass(CACHE_MANAGER_IMPL, null);
        dependencyCache = (defaultClass == null) ? new SimpleHCatDependencyCache()
                : (HCatDependencyCache) ReflectionUtils.newInstance(defaultClass, null);
        dependencyCache.init(conf);
        LOG.info("PartitionDependencyManagerService initialized. Dependency cache is {0} ", dependencyCache.getClass()
                .getName());
    }

    @Override
    public void destroy() {
        dependencyCache.destroy();
    }

    @Override
    public Class<? extends Service> getInterface() {
        return PartitionDependencyManagerService.class;
    }

    /**
     * Add a missing partition dependency and the actionID waiting on it
     *
     * @param hcatURI dependency URI
     * @param actionID ID of action which is waiting for the dependency
     */
    public void addMissingDependency(HCatURI hcatURI, String actionID) {
        dependencyCache.addMissingDependency(hcatURI, actionID);
    }

    /**
     * Remove a missing partition dependency associated with a actionID
     *
     * @param hcatURI dependency URI
     * @param actionID ID of action which is waiting for the dependency
     * @return true if successful, else false
     */
    public boolean removeMissingDependency(HCatURI hcatURI, String actionID) {
        return dependencyCache.removeMissingDependency(hcatURI, actionID);
    }

    /**
     * Get the list of actionIDs waiting for a partition
     *
     * @param hcatURI dependency URI
     * @return list of actionIDs
     */
    public Collection<String> getWaitingActions(HCatURI hcatURI) {
        return dependencyCache.getWaitingActions(hcatURI);
    }

    /**
     * Mark a partition dependency as available
     *
     * @param server host:port of the server
     * @param db name of the database
     * @param table name of the table
     * @param partitions list of available partitions
     * @return list of actionIDs for which the dependency is now available
     */
    public void partitionAvailable(String server, String db, String table, Map<String, String> partitions) {
        Collection<String> actionsWithAvailableDep = dependencyCache.markDependencyAvailable(server, db, table,
                partitions);
        if (actionsWithAvailableDep != null) {
            for (String actionID : actionsWithAvailableDep) {
                boolean ret = Services.get().get(CallableQueueService.class)
                        .queue(new CoordActionUpdatePushMissingDependency(actionID), 100);
                if (ret == false) {
                    XLog.getLog(getClass()).warn(
                            "Unable to queue the callable commands for PartitionDependencyManagerService for actionID "
                                    + actionID + ".Most possibly command queue is full. Queue size is :"
                                    + Services.get().get(CallableQueueService.class).queueSize());
                }
            }
        }
    }

    /**
     * Get a list of available dependency URIs for a actionID
     *
     * @param actionID action id
     * @return list of available dependency URIs
     */
    public Collection<String> getAvailableDependencyURIs(String actionID) {
        return dependencyCache.getAvailableDependencyURIs(actionID);
    }

    /**
     * Remove the list of available dependency URIs for a actionID once the missing dependencies are processed.
     *
     * @param actionID action id
     * @param dependencyURIs set of dependency URIs
     * @return true if successful, else false
     */
    public boolean removeAvailableDependencyURIs(String actionID, Collection<String> dependencyURIs) {
        return dependencyCache.removeAvailableDependencyURIs(actionID, dependencyURIs);
    }

}
