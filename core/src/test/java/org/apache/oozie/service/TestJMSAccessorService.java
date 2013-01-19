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
import java.util.Properties;

import javax.jms.ConnectionFactory;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.jms.DefaultConnectionContext;
import org.apache.oozie.jms.HCatMessageHandler;
import org.apache.oozie.jms.MessageReceiver;
import org.apache.oozie.jms.ConnectionContext;
import org.apache.oozie.test.XTestCase;
import org.junit.Test;

public class TestJMSAccessorService extends XTestCase {
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
    public void testService() {
        JMSAccessorService jmsService = services.get(JMSAccessorService.class);
        Assert.assertNotNull(jmsService);
    }

    @Test
    public void testConnection() throws Exception {
        JMSAccessorService jmsService = services.get(JMSAccessorService.class);
        // both servers should connect to default JMS server
        Properties props = jmsService.getJMSServerProps("blahblah");
        ConnectionContext ctxt1 = new DefaultConnectionContext();
        ConnectionFactory connFactory = ctxt1.createConnectionFactory(props);
        ctxt1.createConnection(connFactory);
        assertTrue(ctxt1.isConnectionInitialized());
        ctxt1.close();
        props = jmsService.getJMSServerProps(JMSAccessorService.DEFAULT_SERVER_ENDPOINT);
        ConnectionContext ctxt2 = new DefaultConnectionContext();
        connFactory = ctxt2.createConnectionFactory(props);
        ctxt2.createConnection(connFactory);
        assertTrue(ctxt2.isConnectionInitialized());
        ctxt2.close();

        assertNotNull(ctxt1);
        assertNotNull(ctxt2);
    }

    @Test
    public void testRegisterSingleConsumerPerTopic() {

        try {
            JMSAccessorService jmsService = services.get(JMSAccessorService.class);
            String server = "hcat.server.com:5080";
            String topic = "hcat.mydb.mytable";

            jmsService.registerForNotification(new URI("hcat://hcat.server.com:5080/mydb/mytable/mypart=10"), topic,
                    new HCatMessageHandler("hcat.server.com"));

            MessageReceiver receiver1 = jmsService.getMessageReceiver(server, topic);

            jmsService.registerForNotification(new URI("hcat://hcat.server.com:5080/mydb/mytable/mypart=11"), topic,
                    new HCatMessageHandler("hcat.server.com"));

            MessageReceiver receiver2 = jmsService.getMessageReceiver(server, topic);
            assertEquals(receiver1, receiver2);
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Exception encountered : " + e);
        }

    }

    @Test
    public void testUnRegisterTopic() {

        try {
            JMSAccessorService jmsService = services.get(JMSAccessorService.class);
            String server = "hcat.server.com:5080";
            String topic = "hcatalog.mydb.mytable";

            jmsService.registerForNotification(new URI("hcat://hcat.server.com:5080/mydb/mytable/mypart=10"), topic,
                    new HCatMessageHandler("hcat.server.com"));

            MessageReceiver receiver1 = jmsService.getMessageReceiver(server, topic);
            assertNotNull(receiver1);

            jmsService.unregisterFromNotification(server, topic);

            receiver1 = jmsService.getMessageReceiver(server, topic);
            assertEquals(null, receiver1);
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Exception encountered : " + e);
        }

    }

    @Test
    public void testGetJMSServerMappingNoDefault() throws ServiceException {
        services.destroy();
        services = super.setupServicesForHCatalog();
        Configuration conf = services.getConf();
        String server2 = "hcat://${1}.${2}.server.com:8020=java.naming.factory.initial#Dummy.Factory;java.naming.provider.url#tcp://broker.${2}:61616";
        String server3 = "hcat://xyz.corp.dummy.com=java.naming.factory.initial#Dummy.Factory;java.naming.provider.url#tcp:localhost:61616";

        String jmsConnectionURL = server2 + "," + server3;
        conf.set(JMSAccessorService.JMS_CONNECTIONS_PROPERTIES, jmsConnectionURL);
        services.init();

        JMSAccessorService jmsService = services.get(JMSAccessorService.class);
        // No default JMS mapping
        String jmsServerMapping = jmsService.getJMSServerMapping("UNKNOWN_SERVER");
        assertNull(jmsServerMapping);
    }

    @Test
    public void testGetJMSServerMapping() throws ServiceException{
        services.destroy();
        services = super.setupServicesForHCatalog();
        Configuration conf = services.getConf();
        String server1 = "default=java.naming.factory.initial#org.apache.activemq.jndi.ActiveMQInitialContextFactory;java.naming.provider.url#vm://localhost?broker.persistent=false";
        String server2 = "hcat://${1}.${2}.server.com:8020=java.naming.factory.initial#Dummy.Factory;java.naming.provider.url#tcp://broker.${2}:61616";
        String server3 = "hcat://xyz.corp.dummy.com=java.naming.factory.initial#Dummy.Factory;java.naming.provider.url#tcp:localhost:61616";

        String jmsConnectionURL = server1+","+server2+","+server3;
        conf.set(JMSAccessorService.JMS_CONNECTIONS_PROPERTIES, jmsConnectionURL);
        services.init();


        JMSAccessorService jmsService = services.get(JMSAccessorService.class);
        String jmsServerMapping = jmsService.getJMSServerMapping("hcat://hcatserver.blue.server.com:8020");
        // rules will be applied
        assertEquals("java.naming.factory.initial#Dummy.Factory;java.naming.provider.url#tcp://broker.blue:61616", jmsServerMapping);

        jmsServerMapping = jmsService.getJMSServerMapping("UNKNOWN_SERVER");
        // will map to default
        assertEquals("java.naming.factory.initial#org.apache.activemq.jndi.ActiveMQInitialContextFactory;java.naming.provider.url#vm://localhost?broker.persistent=false", jmsServerMapping);

        jmsServerMapping = jmsService.getJMSServerMapping("hcat://xyz.corp.dummy.com");
        assertEquals("java.naming.factory.initial#Dummy.Factory;java.naming.provider.url#tcp:localhost:61616", jmsServerMapping);
    }

    @Test
    public void testConnectionContext() throws ServiceException {
        try {
            services.destroy();
            services = super.setupServicesForHCatalog();
            Configuration conf = services.getConf();
            // set the connection factory name
            String jmsURL = "hcat://${1}.${2}.server.com:8020=java.naming.factory.initial#org.apache.activemq.jndi.ActiveMQInitialContextFactory;java.naming.provider.url#vm://localhost?broker.persistent=false;connectionFactoryNames#dynamicFactories/hcat.prod.${1}";
            conf.set(JMSAccessorService.JMS_CONNECTIONS_PROPERTIES, jmsURL);
            services.init();
            JMSAccessorService jmsService = services.get(JMSAccessorService.class);
            String jmsServerMapping = jmsService.getJMSServerMapping("hcat://hcatserver.blue.server.com:8020");
            assertEquals(
                    "java.naming.factory.initial#org.apache.activemq.jndi.ActiveMQInitialContextFactory;java.naming.provider.url#vm://localhost?broker.persistent=false;connectionFactoryNames#dynamicFactories/hcat.prod.hcatserver",
                  jmsServerMapping);

            ConnectionContext ctx1 = new DefaultConnectionContext();
            Properties props = jmsService.getJMSServerProps("hcat://hcatserver.blue.server.com:8020");
            ctx1.createConnectionFactory(props);
            assertEquals("dynamicFactories/hcat.prod.hcatserver", ctx1.getConnectionFactoryName());
        }
        catch (Exception e) {
            fail("Unexpected exception " + e);
        }
    }

}
