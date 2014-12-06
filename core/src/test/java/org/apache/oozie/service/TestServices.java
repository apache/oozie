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

import org.apache.oozie.test.XTestCase;

import java.io.File;

public class TestServices extends XTestCase {

    public void testDefaultServices() throws Exception {
        setSystemProperty(ConfigurationService.OOZIE_CONFIG_FILE, "oozie-dummy.xml");
        setSystemProperty(Services.CONF_SERVICE_CLASSES, "");
        setSystemProperty(Services.CONF_SERVICE_EXT_CLASSES, "");
        Services services = new Services();
        services.init();
        try {
            assertNotNull(services.get(XLogService.class));
            assertNotNull(services.get(ConfigurationService.class));

            String shouldBe = "oozie-" + System.getProperty("user.name");
            assertTrue(shouldBe.startsWith(services.getSystemId()));
            assertNotNull(services.getRuntimeDir());
            assertTrue(new File(services.getRuntimeDir()).exists());
        }
        finally {
            services.destroy();
        }
    }

    public static class S1 implements Service {
        public static boolean INITED_S1 = false;

        @Override
        public void init(Services services) throws ServiceException {
            INITED_S1 = true;
        }

        @Override
        public void destroy() {
        }

        @Override
        public Class<? extends Service> getInterface() {
            return S1.class;
        }
    }

    public static class S2 implements Service {

        @Override
        public void init(Services services) throws ServiceException {
        }

        @Override
        public void destroy() {
        }

        @Override
        public Class<? extends Service> getInterface() {
            return S2.class;
        }
    }

    public static class S1Ext extends S1 {
        public static boolean INITED_S1EXT = false;

        @Override
        public void init(Services services) throws ServiceException {
            INITED_S1EXT = true;
        }
    }

    private static final String SERVICES = S1.class.getName() + "," + S2.class.getName();

    public void testServiceExtLoading() throws Exception {
        setSystemProperty(Services.CONF_SERVICE_CLASSES, SERVICES);
        Services services = new Services();
        services.init();
        try {
            assertEquals(S1.class,  services.get(S1.class).getClass());
            assertEquals(S2.class,  services.get(S2.class).getClass());
        }
        finally {
            services.destroy();
        }
    }

    private static final String SERVICES_EXT = S1Ext.class.getName();

    public void testServicesExtLoading() throws Exception {
        S1.INITED_S1 = false;
        S1Ext.INITED_S1EXT = false;
        setSystemProperty(Services.CONF_SERVICE_CLASSES, SERVICES);
        setSystemProperty(Services.CONF_SERVICE_EXT_CLASSES, SERVICES_EXT);
        Services services = new Services();
        services.init();
        try {
            assertEquals(S1Ext.class,  services.get(S1.class).getClass());
            assertEquals(S2.class,  services.get(S2.class).getClass());
            assertFalse(S1.INITED_S1);
            assertTrue(S1Ext.INITED_S1EXT);
        }
        finally {
            services.destroy();
        }
    }
}
