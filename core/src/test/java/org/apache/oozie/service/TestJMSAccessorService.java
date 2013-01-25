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
import java.util.Random;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Session;

import junit.framework.Assert;

import org.apache.activemq.broker.BrokerService;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.jms.DefaultConnectionContext;
import org.apache.oozie.jms.HCatMessageHandler;
import org.apache.oozie.jms.MessageReceiver;
import org.apache.oozie.jms.ConnectionContext;
import org.apache.oozie.test.XTestCase;
import org.junit.Test;

public class TestJMSAccessorService extends XTestCase {
    private Services services;
    private static Random random = new Random();

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
        Properties props = jmsService.getJMSPropsFromConf(jmsService.getJMSServerConnectString("blahblah"));
        ConnectionContext ctxt1 = new DefaultConnectionContext();
        ConnectionFactory connFactory = ctxt1.createConnectionFactory(props);
        ctxt1.createConnection(connFactory);
        assertTrue(ctxt1.isConnectionInitialized());
        ctxt1.close();
        props = jmsService.getJMSPropsFromConf(jmsService.getJMSServerConnectString(JMSAccessorService.DEFAULT_SERVER_ENDPOINT));
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
        String jmsServerMapping = jmsService.getJMSServerConnectString("UNKNOWN_SERVER");
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
        String jmsServerMapping = jmsService.getJMSServerConnectString("hcat://hcatserver.blue.server.com:8020");
        // rules will be applied
        assertEquals("java.naming.factory.initial#Dummy.Factory;java.naming.provider.url#tcp://broker.blue:61616", jmsServerMapping);

        jmsServerMapping = jmsService.getJMSServerConnectString("UNKNOWN_SERVER");
        // will map to default
        assertEquals("java.naming.factory.initial#org.apache.activemq.jndi.ActiveMQInitialContextFactory;java.naming.provider.url#vm://localhost?broker.persistent=false", jmsServerMapping);

        jmsServerMapping = jmsService.getJMSServerConnectString("hcat://xyz.corp.dummy.com");
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
            String jmsServerMapping = jmsService.getJMSServerConnectString("hcat://hcatserver.blue.server.com:8020");
            assertEquals(
                    "java.naming.factory.initial#org.apache.activemq.jndi.ActiveMQInitialContextFactory;java.naming.provider.url#vm://localhost?broker.persistent=false;connectionFactoryNames#dynamicFactories/hcat.prod.hcatserver",
                  jmsServerMapping);

            ConnectionContext ctx1 = new DefaultConnectionContext();
            Properties props = jmsService.getJMSPropsFromConf(jmsService
                    .getJMSServerConnectString("hcat://hcatserver.blue.server.com:8020"));
            ctx1.createConnectionFactory(props);
            assertEquals("dynamicFactories/hcat.prod.hcatserver", ctx1.getConnectionFactoryName());
        }
        catch (Exception e) {
            fail("Unexpected exception " + e);
        }
    }

    @Test
    public void testConnectionRetry() throws Exception {
        int randomPort = 30000 + random.nextInt(10000);
        String brokerURl = "tcp://localhost:" + randomPort;
        Configuration servicesConf = services.getConf();
        servicesConf.set(JMSAccessorService.CONF_RETRY_INITIAL_DELAY, "1");
        servicesConf.set(JMSAccessorService.CONF_RETRY_MAX_ATTEMPTS, "3");
        servicesConf.set(JMSAccessorService.JMS_CONNECTIONS_PROPERTIES,
                "default=java.naming.factory.initial#" + ActiveMQConnFactory + ";" +
                "java.naming.provider.url#" + brokerURl + ";" +
                "connectionFactoryNames#"+ "ConnectionFactory");
        JMSAccessorService jmsService = new JMSAccessorService();
        jmsService.init(services);
        String publisherAuthority = "hcat.server.com:5080";
        String topic = "topic.topic1";
        jmsService.registerForNotification(new URI("hcat://hcat.server.com:5080/db/table/pk1=val1"), topic,
                new HCatMessageHandler(publisherAuthority));
        assertFalse(jmsService.isListeningToTopic(publisherAuthority, topic));
        assertTrue(jmsService.isConnectionInRetryList(publisherAuthority));
        assertTrue(jmsService.isTopicInRetryList(publisherAuthority, topic));
        // Start the broker and check if listening to topic now
        BrokerService broker = new BrokerService();
        broker.addConnector(brokerURl);
        broker.start();
        Thread.sleep(1000);
        assertTrue(jmsService.isListeningToTopic(publisherAuthority, topic));
        assertFalse(jmsService.isConnectionInRetryList(publisherAuthority));
        assertFalse(jmsService.isTopicInRetryList(publisherAuthority, topic));
        broker.stop();
        jmsService.destroy();

    }

    @Test
    public void testConnectionRetryExceptionListener() throws Exception {
        int randomPort = 30000 + random.nextInt(10000);
        String brokerURL = "tcp://localhost:" + randomPort;
        String jmsConnnectString = "java.naming.factory.initial#" + ActiveMQConnFactory + ";" +
                "java.naming.provider.url#" + brokerURL + ";" +
                "connectionFactoryNames#"+ "ConnectionFactory";
        services.destroy();
        services = super.setupServicesForHCatalog();
        Configuration servicesConf = services.getConf();
        servicesConf.set(JMSAccessorService.CONF_RETRY_INITIAL_DELAY, "1");
        servicesConf.set(JMSAccessorService.CONF_RETRY_MAX_ATTEMPTS, "3");
        servicesConf.set(JMSAccessorService.JMS_CONNECTIONS_PROPERTIES,
                "default=" + jmsConnnectString);
        services.init();
        JMSAccessorService jmsService = Services.get().get(JMSAccessorService.class);
        String publisherAuthority = "hcat.server.com:5080";
        String topic = "topic.topic1";
        // Start the broker
        BrokerService broker = new BrokerService();
        broker.addConnector(brokerURL);
        broker.start();
        jmsService.registerForNotification(new URI("hcat://hcat.server.com:5080/db/table/pk1=val1"), topic,
                new HCatMessageHandler(publisherAuthority));
        assertTrue(jmsService.isListeningToTopic(publisherAuthority, topic));
        assertFalse(jmsService.isConnectionInRetryList(publisherAuthority));
        assertFalse(jmsService.isTopicInRetryList(publisherAuthority, topic));
        ConnectionContext connCtxt = jmsService.createConnectionContext(jmsConnnectString);
        broker.stop();
        try {
            connCtxt.createSession(Session.AUTO_ACKNOWLEDGE);
            fail("Exception expected");
        }
        catch (JMSException e) {
            Thread.sleep(100);
            assertFalse(jmsService.isListeningToTopic(publisherAuthority, topic));
            assertTrue(jmsService.isConnectionInRetryList(publisherAuthority));
            assertTrue(jmsService.isTopicInRetryList(publisherAuthority, topic));
        }
        broker = new BrokerService();
        broker.addConnector(brokerURL);
        broker.start();
        Thread.sleep(1000);
        assertTrue(jmsService.isListeningToTopic(publisherAuthority, topic));
        assertFalse(jmsService.isConnectionInRetryList(publisherAuthority));
        assertFalse(jmsService.isTopicInRetryList(publisherAuthority, topic));
        broker.stop();
        jmsService.destroy();

    }

    @Test
    public void testConnectionRetryMaxAttempt() throws Exception {
        String jmsConnnectString = "java.naming.factory.initial#" + ActiveMQConnFactory + ";"
                + "java.naming.provider.url#" + "tcp://localhost:12345;connectionFactoryNames#ConnectionFactory";
        Configuration servicesConf = services.getConf();
        servicesConf.set(JMSAccessorService.CONF_RETRY_INITIAL_DELAY, "1");
        servicesConf.set(JMSAccessorService.CONF_RETRY_MAX_ATTEMPTS, "1");
        servicesConf.set(JMSAccessorService.JMS_CONNECTIONS_PROPERTIES,
                "default=" + jmsConnnectString);
        JMSAccessorService jmsService = new JMSAccessorService();
        jmsService.init(services);
        String publisherAuthority = "hcat.server.com:5080";
        String topic = "topic.topic1";
        jmsService.registerForNotification(new URI("hcat://hcat.server.com:5080/db/table/pk1=val1"), topic,
                new HCatMessageHandler(publisherAuthority));
        assertTrue(jmsService.isConnectionInRetryList(publisherAuthority));
        assertTrue(jmsService.isTopicInRetryList(publisherAuthority, topic));
        assertFalse(jmsService.isListeningToTopic(publisherAuthority, topic));
        Thread.sleep(1100);
        // Should not retry again as max attempt is 1
        assertTrue(jmsService.isConnectionInRetryList(publisherAuthority));
        assertTrue(jmsService.isTopicInRetryList(publisherAuthority, topic));
        assertFalse(jmsService.isListeningToTopic(publisherAuthority, topic));
        assertEquals(1, jmsService.getNumConnectionAttempts("hcat.server.com:5080"));
        assertFalse(jmsService.retryConnection(jmsConnnectString));
        jmsService.destroy();
    }

}
