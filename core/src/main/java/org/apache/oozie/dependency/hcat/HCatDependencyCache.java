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

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.util.HCatURI;

public interface HCatDependencyCache {

    /**
     * Initialize the cache with configuration
     *
     * @param conf configuration
     */
    public void init(Configuration conf);

    /**
     * Add a missing partition dependency and the actionID waiting on it
     *
     * @param hcatURI dependency URI
     * @param actionID ID of action which is waiting for the dependency
     */
    public void addMissingDependency(HCatURI hcatURI, String actionID);

    /**
     * Remove a missing partition dependency associated with a actionID
     *
     * @param hcatURI dependency URI
     * @param actionID ID of action which is waiting for the dependency
     * @return true if successful, else false
     */
    public boolean removeMissingDependency(HCatURI hcatURI, String actionID);

    /**
     * Get the list of actionIDs waiting for a partition
     *
     * @param hcatURI dependency URI
     * @return list of actionIDs
     */
    public Collection<String> getWaitingActions(HCatURI hcatURI);

    /**
     * Mark a partition dependency as available
     *
     * @param server host:port of the server
     * @param db name of the database
     * @param table name of the table
     * @param partitions list of available partitions
     * @return list of actionIDs for which the dependency is now available
     */
    public Collection<String> markDependencyAvailable(String server, String db, String table, Map<String, String> partitions);

    /**
     * Get a list of available dependency URIs for a actionID
     *
     * @param actionID action id
     * @return list of available dependency URIs
     */
    public Collection<String> getAvailableDependencyURIs(String actionID);

    /**
     * Remove the list of available dependency URIs for a actionID once the missing dependencies are processed.
     *
     * @param actionID action id
     * @param dependencyURIs set of dependency URIs
     * @return true if successful, else false
     */
    public boolean removeAvailableDependencyURIs(String actionID, Collection<String> dependencyURIs);

    /**
     * Destroy the cache
     */
    public void destroy();

    /**
     * Purge stale actions
     */
    public void removeNonWaitingCoordActions(Set<String> coordActions);

    /**
     * Remove coordAction when all dependencies met
     */
    public void removeCoordActionWithDependenciesAvailable(String coordAction);

}
