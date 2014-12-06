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

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.util.XConfiguration;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * The GroupsService class delegates to the Hadoop's <code>org.apache.hadoop.security.Groups</code>
 * to retrieve the groups a user belongs to.
 */
public class GroupsService implements Service {
    public static final String CONF_PREFIX = Service.CONF_PREFIX + "GroupsService.";

    private org.apache.hadoop.security.Groups hGroups;

    /**
     * Returns the service interface.
     * @return <code>GroupService</code>
     */
    @Override
    public Class<? extends Service> getInterface() {
        return GroupsService.class;
    }

    /**
     * Initializes the service.
     *
     * @param services services singleton initializing the service.
     */
    @Override
    public void init(Services services) {
        Configuration sConf = services.getConf();
        Configuration gConf = new XConfiguration();
        for (Map.Entry<String, String> entry : sConf) {
            String name = entry.getKey();
            if (name.startsWith(CONF_PREFIX)) {
                gConf.set(name.substring(CONF_PREFIX.length()), sConf.get(name));
            }
        }
        hGroups = new org.apache.hadoop.security.Groups(gConf);
    }

    /**
     * Destroys the service.
     */
    @Override
    public void destroy() {
    }

    /**
     * Returns the list of groups a user belongs to.
     *
     * @param user user name.
     * @return the groups the given user belongs to.
     * @throws IOException thrown if there was an error retrieving the groups of the user.
     */
    public List<String> getGroups(String user) throws IOException {
        return hGroups.getGroups(user);
    }

}
