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

import org.apache.oozie.dependency.hcat.EhcacheHCatDependencyCache;

public class TestHAPartitionDependencyManagerEhCache extends TestHAPartitionDependencyManagerService {

    protected void setUp() throws Exception {
        super.setUp();
        services.getConf().set(PartitionDependencyManagerService.CACHE_MANAGER_IMPL,
                EhcacheHCatDependencyCache.class.getName());
        services.setService(ZKJobsConcurrencyService.class);
        PartitionDependencyManagerService pdms = services.get(PartitionDependencyManagerService.class);
        pdms.init(services);
    }

    @Override
    public void testDependencyCacheWithHA(){
    }

    @Override
    public void testPurgeMissingDependencies() throws Exception {
        PartitionDependencyManagerService pdms = services.get(PartitionDependencyManagerService.class);
        testPurgeMissingDependenciesForCache(pdms);
    }
}
