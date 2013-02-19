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

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.jms.JMSConnectionInfo;
import org.apache.oozie.test.XTestCase;
import org.junit.Test;

public class TestHCatAccessorService extends XTestCase {
    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = super.setupServicesForHCatalog();
        services.init();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    @Test
    public void testGetJMSConnectionInfoNoDefault() throws Exception {
        services.destroy();
        services = super.setupServicesForHCatalog();
        Configuration conf = services.getConf();
        String server2 = "hcat://${1}.${2}.server.com:8020=java.naming.factory.initial#Dummy.Factory;" +
                "java.naming.provider.url#tcp://broker.${2}:61616";
        String server3 = "hcat://xyz.corp.dummy.com=java.naming.factory.initial#Dummy.Factory;" +
                "java.naming.provider.url#tcp:localhost:61616";

        String jmsConnectionURL = server2 + "," + server3;
        conf.set(HCatAccessorService.JMS_CONNECTIONS_PROPERTIES, jmsConnectionURL);
        services.init();

        HCatAccessorService hcatService = services.get(HCatAccessorService.class);
        // No default JMS mapping
        JMSConnectionInfo connInfo = hcatService.getJMSConnectionInfo(new URI("http://unknown:9999/fs"));
        assertNull(connInfo);
        connInfo = hcatService.getJMSConnectionInfo(new URI("hcat://server1.colo1.server.com:8020/db/table/pk1=val1;pk2=val2"));
        assertEquals("java.naming.factory.initial#Dummy.Factory;java.naming.provider.url#tcp://broker.colo1:61616",
                connInfo.getJNDIPropertiesString());
        connInfo = hcatService.getJMSConnectionInfo(new URI("hcat://xyz.corp.dummy.com/db/table"));
        assertEquals("java.naming.factory.initial#Dummy.Factory;java.naming.provider.url#tcp:localhost:61616",
                connInfo.getJNDIPropertiesString());
    }

    @Test
    public void testGetJMSConnectionInfo() throws Exception {
        services.destroy();
        services = super.setupServicesForHCatalog();
        Configuration conf = services.getConf();
        String server1 = "default=java.naming.factory.initial#org.apache.activemq.jndi.ActiveMQInitialContextFactory;" +
                "java.naming.provider.url#vm://localhost?broker.persistent=false";
        String server2 = "hcat://${1}.${2}.server.com:8020=java.naming.factory.initial#Dummy.Factory;" +
                "java.naming.provider.url#tcp://broker.${2}:61616";
        String server3 = "hcat://xyz.corp.dummy.com=java.naming.factory.initial#Dummy.Factory;" +
                "java.naming.provider.url#tcp:localhost:61616";

        String jmsConnectionURL = server1 + "," + server2 + "," + server3;
        conf.set(HCatAccessorService.JMS_CONNECTIONS_PROPERTIES, jmsConnectionURL);
        services.init();

        HCatAccessorService hcatService = services.get(HCatAccessorService.class);
        JMSConnectionInfo connInfo = hcatService.getJMSConnectionInfo(new URI("hcat://hcatserver.blue.server.com:8020"));
        // rules will be applied
        assertEquals("java.naming.factory.initial#Dummy.Factory;java.naming.provider.url#tcp://broker.blue:61616",
                connInfo.getJNDIPropertiesString());

        connInfo = hcatService.getJMSConnectionInfo(new URI("http://unknown:9999/fs"));
        // will map to default
        assertEquals(
                "java.naming.factory.initial#org.apache.activemq.jndi.ActiveMQInitialContextFactory;" +
                "java.naming.provider.url#vm://localhost?broker.persistent=false",
                connInfo.getJNDIPropertiesString());

        connInfo = hcatService.getJMSConnectionInfo(new URI("hcat://xyz.corp.dummy.com"));
        assertEquals("java.naming.factory.initial#Dummy.Factory;java.naming.provider.url#tcp:localhost:61616",
                connInfo.getJNDIPropertiesString());
    }

}
