/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.service;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.oozie.test.XTestCase;

import java.util.Arrays;
import java.util.List;

public class TestGroupsService extends XTestCase {

    public void testService() throws Exception {
        Services services = new Services();
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_CLASSES, StringUtils.join(",", Arrays.asList(GroupsService.class.getName())));
        services.init();
        try {
            GroupsService groups = services.get(GroupsService.class);
            Assert.assertNotNull(groups);
            List<String> g = groups.getGroups(System.getProperty("user.name"));
            Assert.assertNotSame(g.size(), 0);
        }
        finally {
            services.destroy();
        }
    }

    public void testInvalidGroupsMapping() throws Exception {
        Services services = new Services();
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_CLASSES, StringUtils.join(",", Arrays.asList(GroupsService.class.getName())));
        conf.set("oozie.service.GroupsService.hadoop.security.group.mapping", String.class.getName());
        try {
            services.init();
            fail();
        }
        catch (ServiceException ex) {
        }
        catch (Exception ex) {
            fail(ex.toString());
        }
    }

}

