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

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.oozie.test.XTestCase;

import java.security.AccessControlException;
import java.util.Arrays;
import java.util.List;

public class TestProxyUserService extends XTestCase {

    public void testService() throws Exception {
        Services services = new Services();
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_CLASSES, StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                                                                                    ProxyUserService.class.getName())));
        services.init();
        try {
            ProxyUserService proxyUser = services.get(ProxyUserService.class);
            Assert.assertNotNull(proxyUser);
        }
        finally {
            services.destroy();
        }
    }

    public void testWrongConfigGroups() throws Exception {
        Services services = new Services();
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_CLASSES, StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                                                                                    ProxyUserService.class.getName())));
        conf.set("oozie.service.ProxyUserService.proxyuser.foo.hosts", "*");
        try {
            services.init();
            fail();
        }
        catch (ServiceException ex) {
        }
        catch (Exception ex) {
            fail();
        }
        finally {
            services.destroy();
        }
    }

    public void testWrongHost() throws Exception {
        Services services = new Services();
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_CLASSES, StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                                                                                    ProxyUserService.class.getName())));
        conf.set("oozie.service.ProxyUserService.proxyuser.foo.hosts", "otherhost");
        conf.set("oozie.service.ProxyUserService.proxyuser.foo.groups", "*");
        try {
            services.init();
            fail();
        }
        catch (ServiceException ex) {
        }
        catch (Exception ex) {
            fail();
        }
        finally {
            services.destroy();
        }
    }

    public void testWrongConfigHosts() throws Exception {
        Services services = new Services();
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_CLASSES, StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                                                                                    ProxyUserService.class.getName())));
        conf.set("oozie.service.ProxyUserService.proxyuser.foo.groups", "*");
        try {
            services.init();
            fail();
        }
        catch (ServiceException ex) {
        }
        catch (Exception ex) {
            fail();
        }
        finally {
            services.destroy();
        }
    }

    public void testValidateAnyHostAnyUser() throws Exception {
        Services services = new Services();
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_CLASSES, StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                                                                                    ProxyUserService.class.getName())));
        conf.set("oozie.service.ProxyUserService.proxyuser.foo.hosts", "*");
        conf.set("oozie.service.ProxyUserService.proxyuser.foo.groups", "*");
        services.init();
        try {
            ProxyUserService proxyUser = services.get(ProxyUserService.class);
            Assert.assertNotNull(proxyUser);
            proxyUser.validate("foo", "localhost", "bar");
        }
        finally {
            services.destroy();
        }
    }

    public void testInvalidProxyUser() throws Exception {
        Services services = new Services();
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_CLASSES, StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                                                                                    ProxyUserService.class.getName())));
        conf.set("oozie.service.ProxyUserService.proxyuser.foo.hosts", "*");
        conf.set("oozie.service.ProxyUserService.proxyuser.foo.groups", "*");
        services.init();
        try {
            ProxyUserService proxyUser = services.get(ProxyUserService.class);
            Assert.assertNotNull(proxyUser);
            proxyUser.validate("bar", "localhost", "foo");
            fail();
        }
        catch (AccessControlException ex) {
        }
        catch (Exception ex) {
            fail(ex.toString());
        }
        finally {
            services.destroy();
        }
    }

    public void testValidateHost() throws Exception {
        Services services = new Services();
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_CLASSES, StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                                                                                    ProxyUserService.class.getName())));
        conf.set("oozie.service.ProxyUserService.proxyuser.foo.hosts", "localhost");
        conf.set("oozie.service.ProxyUserService.proxyuser.foo.groups", "*");
        services.init();
        try {
            ProxyUserService proxyUser = services.get(ProxyUserService.class);
            Assert.assertNotNull(proxyUser);
            proxyUser.validate("foo", "localhost", "bar");
        }
        finally {
            services.destroy();
        }
    }

    private String getGroup() throws Exception {
        Services services = new Services();
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_CLASSES, StringUtils.join(",", Arrays.asList(GroupsService.class.getName())));
        conf.set("server.services", StringUtils.join(",", Arrays.asList(GroupsService.class.getName())));
        services.init();
        GroupsService groups = services.get(GroupsService.class);
        List<String> g = groups.getGroups(System.getProperty("user.name"));
        services.destroy();
        return g.get(0);
    }

    public void testValidateGroup() throws Exception {
        Services services = new Services();
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_CLASSES, StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                                                                                    ProxyUserService.class.getName())));
        conf.set("oozie.service.ProxyUserService.proxyuser.foo.hosts", "*");
        conf.set("oozie.service.ProxyUserService.proxyuser.foo.groups", getGroup());
        services.init();
        try {
            ProxyUserService proxyUser = services.get(ProxyUserService.class);
            Assert.assertNotNull(proxyUser);
            proxyUser.validate("foo", "localhost", System.getProperty("user.name"));
        }
        finally {
            services.destroy();
        }
    }


    public void testUnknownHost() throws Exception {
        Services services = new Services();
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_CLASSES, StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                                                                                    ProxyUserService.class.getName())));
        conf.set("oozie.service.ProxyUserService.proxyuser.foo.hosts", "localhost");
        conf.set("oozie.service.ProxyUserService.proxyuser.foo.groups", "*");
        services.init();
        try {
            ProxyUserService proxyUser = services.get(ProxyUserService.class);
            Assert.assertNotNull(proxyUser);
            proxyUser.validate("foo", "unknownhost.bar.foo", "bar");
            fail();
        }
        catch (AccessControlException ex) {

        }
        catch (Exception ex) {
            fail(ex.toString());
        }
        finally {
            services.destroy();
        }
    }

    public void testInvalidHost() throws Exception {
        Services services = new Services();
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_CLASSES, StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                                                                                    ProxyUserService.class.getName())));
        conf.set("oozie.service.ProxyUserService.proxyuser.foo.hosts", "localhost");
        conf.set("oozie.service.ProxyUserService.proxyuser.foo.groups", "*");
        services.init();
        try {
            ProxyUserService proxyUser = services.get(ProxyUserService.class);
            Assert.assertNotNull(proxyUser);
            proxyUser.validate("foo", "www.example.com", "bar");
            fail();
        }
        catch (AccessControlException ex) {

        }
        catch (Exception ex) {
            fail(ex.toString());
        }
        finally {
            services.destroy();
        }
    }

    public void testInvalidGroup() throws Exception {
        Services services = new Services();
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_CLASSES, StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                                                                                    ProxyUserService.class.getName())));
        conf.set("oozie.service.ProxyUserService.proxyuser.foo.hosts", "localhost");
        conf.set("oozie.service.ProxyUserService.proxyuser.foo.groups", "nobody");
        services.init();
        try {
            ProxyUserService proxyUser = services.get(ProxyUserService.class);
            Assert.assertNotNull(proxyUser);
            proxyUser.validate("foo", "localhost", System.getProperty("user.name"));
            fail();
        }
        catch (AccessControlException ex) {

        }
        catch (Exception ex) {
            fail(ex.toString());
        }
        finally {
            services.destroy();
        }
    }

    public void testNullProxyUser() throws Exception {
        Services services = new Services();
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_CLASSES, StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                                                                                    ProxyUserService.class.getName())));
        services.init();
        try {
            ProxyUserService proxyUser = services.get(ProxyUserService.class);
            Assert.assertNotNull(proxyUser);
            proxyUser.validate(null, "localhost", "bar");
            fail();
        }
        catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().contains("oozie.service.ProxyUserService.proxyuser.#USER#.hosts"));
            assertTrue(ex.getMessage().contains("oozie.service.ProxyUserService.proxyuser.#USER#.groups"));
        }
        catch (Exception ex) {
            fail(ex.toString());
        }
        finally {
            services.destroy();
        }
    }

    public void testNullHost() throws Exception {
        Services services = new Services();
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_CLASSES, StringUtils.join(",", Arrays.asList(GroupsService.class.getName(),
                                                                                    ProxyUserService.class.getName())));
        services.init();
        try {
            ProxyUserService proxyUser = services.get(ProxyUserService.class);
            Assert.assertNotNull(proxyUser);
            proxyUser.validate("foo", null, "bar");
            fail();
        }
        catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().contains("oozie.service.ProxyUserService.proxyuser.foo.hosts"));
            assertTrue(ex.getMessage().contains("oozie.service.ProxyUserService.proxyuser.foo.groups"));
        }
        catch (Exception ex) {
            fail(ex.toString());
        }
        finally {
            services.destroy();
        }
    }
}

