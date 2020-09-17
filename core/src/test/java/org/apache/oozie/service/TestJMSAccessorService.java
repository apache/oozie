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
import java.net.URISyntaxException;
import java.util.Random;

import javax.jms.Session;

import org.apache.activemq.broker.BrokerService;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.dependency.hcat.HCatMessageHandler;
import org.apache.oozie.jms.ConnectionContext;
import org.apache.oozie.jms.DefaultConnectionContext;
import org.apache.oozie.jms.JMSConnectionInfo;
import org.apache.oozie.jms.MessageReceiver;
import org.apache.oozie.test.XTestCase;
import org.junit.Test;

public class TestJMSAccessorService extends XTestCase {
    private Services services;
    private static Random random = new Random();
    private static final int JMS_TIMEOUT_MS = 5000;

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
    public void testConnection() throws Exception {
        HCatAccessorService hcatService = services.get(HCatAccessorService.class);
        JMSAccessorService jmsService = services.get(JMSAccessorService.class);
        // both servers should connect to default JMS server
        JMSConnectionInfo connInfo = hcatService.getJMSConnectionInfo(new URI("hcat://hcatserver.blue.server.com:8020"));
        ConnectionContext ctxt1 = jmsService.createConnectionContext(connInfo);
        assertTrue(ctxt1.isConnectionInitialized());
        JMSConnectionInfo connInfo1 = hcatService.getJMSConnectionInfo(new URI("http://unknown:80"));
        ConnectionContext ctxt2 = jmsService.createConnectionContext(connInfo1);
        assertTrue(ctxt2.isConnectionInitialized());
        assertEquals(ctxt1, ctxt2);
        ctxt1.close();
    }

    @Test
    public void testRegisterSingleConsumerPerTopic() throws URISyntaxException {
        HCatAccessorService hcatService = services.get(HCatAccessorService.class);
        JMSAccessorService jmsService = services.get(JMSAccessorService.class);
        String server = "hcat.server.com:5080";
        String topic = "hcat.mydb.mytable";

        JMSConnectionInfo connInfo = hcatService.getJMSConnectionInfo(new URI("hcat://hcat.server.com:8020"));
        jmsService.registerForNotification(connInfo, topic, new HCatMessageHandler(server));

        MessageReceiver receiver1 = jmsService.getMessageReceiver(connInfo, topic);
        jmsService.registerForNotification(connInfo, topic, new HCatMessageHandler(server));

        MessageReceiver receiver2 = jmsService.getMessageReceiver(connInfo, topic);
        assertEquals(receiver1, receiver2);
    }

    @Test
    public void testUnRegisterTopic() throws URISyntaxException {
        HCatAccessorService hcatService = services.get(HCatAccessorService.class);
        JMSAccessorService jmsService = services.get(JMSAccessorService.class);
        String server = "hcat.server.com:5080";
        String topic = "hcatalog.mydb.mytable";

        JMSConnectionInfo connInfo = hcatService.getJMSConnectionInfo(new URI("hcat://hcat.server.com:8020"));
        jmsService.registerForNotification(connInfo, topic, new HCatMessageHandler(server));

        MessageReceiver receiver1 = jmsService.getMessageReceiver(connInfo, topic);
        assertNotNull(receiver1);

        jmsService.unregisterFromNotification(connInfo, topic);

        receiver1 = jmsService.getMessageReceiver(connInfo, topic);
        assertEquals(null, receiver1);
    }

    @Test
    public void testConnectionContext() throws Exception {
        services.destroy();
        services = super.setupServicesForHCatalog();
        Configuration conf = services.getConf();
        // set the connection factory name
        String jmsURL = "hcat://${1}.${2}.server.com:8020=java.naming.factory.initial#" +
                "org.apache.activemq.jndi.ActiveMQInitialContextFactory" +
                ";java.naming.provider.url#vm://localhost?broker.persistent=false;" +
                "connectionFactoryNames#dynamicFactories/hcat.prod.${1}";
        conf.set(HCatAccessorService.JMS_CONNECTIONS_PROPERTIES, jmsURL);
        services.init();
        HCatAccessorService hcatService = services.get(HCatAccessorService.class);
        JMSConnectionInfo connInfo = hcatService.getJMSConnectionInfo(new URI("hcat://hcatserver.blue.server.com:8020"));
        assertEquals(
                "java.naming.factory.initial#org.apache.activemq.jndi.ActiveMQInitialContextFactory;java.naming.provider.url#" +
                        "vm://localhost?broker.persistent=false;connectionFactoryNames#dynamicFactories/hcat.prod.hcatserver",
                        connInfo.getJNDIPropertiesString());

        ConnectionContext ctx = new DefaultConnectionContext();
        ctx.createConnection(connInfo.getJNDIProperties());
        ctx.close();
    }

    @Test
    public void testConnectionRetry() throws Exception {
        services.destroy();
        services = super.setupServicesForHCatalog();
        int randomPort = 30000 + random.nextInt(10000);
        String brokerURl = "tcp://localhost:" + randomPort;
        Configuration servicesConf = services.getConf();
        servicesConf.set(JMSAccessorService.CONF_RETRY_INITIAL_DELAY, "1");
        servicesConf.set(JMSAccessorService.CONF_RETRY_MAX_ATTEMPTS, "3");
        servicesConf.set(HCatAccessorService.JMS_CONNECTIONS_PROPERTIES, "default=java.naming.factory.initial#"
                + ActiveMQConnFactory + ";" + "java.naming.provider.url#" + brokerURl + ";" + "connectionFactoryNames#"
                + "ConnectionFactory");
        services.init();
        HCatAccessorService hcatService = Services.get().get(HCatAccessorService.class);
        JMSAccessorService jmsService = Services.get().get(JMSAccessorService.class);

        String publisherAuthority = "hcat.server.com:5080";
        String topic = "topic.topic1";
        JMSConnectionInfo connInfo = hcatService.getJMSConnectionInfo(new URI("hcat://hcat.server.com:8020"));
        jmsService.registerForNotification(connInfo, topic, new HCatMessageHandler(publisherAuthority));
        assertFalse(jmsService.isListeningToTopic(connInfo, topic));
        assertTrue(jmsService.isConnectionInRetryList(connInfo));
        assertTrue(jmsService.isTopicInRetryList(connInfo, topic));
        // Start the broker and check if listening to topic now
        BrokerService broker = new BrokerService();
        try {
            broker.addConnector(brokerURl);
            broker.setDataDirectory(getTestCaseDir());
            broker.setUseJmx(false);
            broker.start();

            waitFor(JMS_TIMEOUT_MS, new Predicate() {
                @Override
                public boolean evaluate() throws Exception {
                    return jmsService.isListeningToTopic(connInfo, topic)
                            && !jmsService.isConnectionInRetryList(connInfo)
                            && !jmsService.isTopicInRetryList(connInfo, topic);
                }
            });
            assertTrue(jmsService.isListeningToTopic(connInfo, topic));
            assertFalse(jmsService.isConnectionInRetryList(connInfo));
            assertFalse(jmsService.isTopicInRetryList(connInfo, topic));
        } finally {
            broker.stop();
        }
    }

    @Test
    public void testConnectionRetryExceptionListener() throws Exception {
        BrokerService broker = null;
        try {
            services.destroy();
            services = super.setupServicesForHCatalog();
            int randomPort = 30000 + random.nextInt(10000);
            String brokerURL = "tcp://localhost:" + randomPort;
            String jndiPropertiesString = "java.naming.factory.initial#" + ActiveMQConnFactory + ";"
                    + "java.naming.provider.url#" + brokerURL + ";" + "connectionFactoryNames#" + "ConnectionFactory";
            Configuration servicesConf = services.getConf();
            servicesConf.set(JMSAccessorService.CONF_RETRY_INITIAL_DELAY, "1");
            servicesConf.set(JMSAccessorService.CONF_RETRY_MAX_ATTEMPTS, "3");
            servicesConf.set(HCatAccessorService.JMS_CONNECTIONS_PROPERTIES, "default=" + jndiPropertiesString);
            services.init();
            HCatAccessorService hcatService = Services.get().get(HCatAccessorService.class);
            JMSAccessorService jmsService = Services.get().get(JMSAccessorService.class);

            String publisherAuthority = "hcat.server.com:5080";
            String topic = "topic.topic1";
            // Start the broker
            broker = new BrokerService();
            broker.addConnector(brokerURL);
            broker.setDataDirectory(getTestCaseDir());
            broker.setUseJmx(false);
            broker.start();
            JMSConnectionInfo connInfo = hcatService.getJMSConnectionInfo(new URI("hcat://hcat.server.com:8020"));
            jmsService.registerForNotification(connInfo, topic, new HCatMessageHandler(publisherAuthority));
            assertTrue(jmsService.isListeningToTopic(connInfo, topic));
            assertFalse(jmsService.isConnectionInRetryList(connInfo));
            assertFalse(jmsService.isTopicInRetryList(connInfo, topic));
            ConnectionContext connCtxt = jmsService.createConnectionContext(connInfo);
            broker.stop();

            try {
                connCtxt.createSession(Session.AUTO_ACKNOWLEDGE);
                fail("Exception expected");
            }
            catch (Exception e) {
                waitFor(JMS_TIMEOUT_MS, new Predicate() {
                    @Override
                    public boolean evaluate() throws Exception {
                        return !jmsService.isListeningToTopic(connInfo, topic)
                                && jmsService.isConnectionInRetryList(connInfo)
                                && jmsService.isTopicInRetryList(connInfo, topic);
                    }
                });
                assertFalse(jmsService.isListeningToTopic(connInfo, topic));
                assertTrue(jmsService.isConnectionInRetryList(connInfo));
                assertTrue(jmsService.isTopicInRetryList(connInfo, topic));
            }
            broker = new BrokerService();

            broker.addConnector(brokerURL);
            broker.setDataDirectory(getTestCaseDir());
            broker.setUseJmx(false);
            broker.start();
            waitFor(JMS_TIMEOUT_MS, new Predicate() {
                @Override
                public boolean evaluate() throws Exception {
                    return jmsService.isListeningToTopic(connInfo, topic)
                            && !jmsService.isConnectionInRetryList(connInfo)
                            && !jmsService.isTopicInRetryList(connInfo, topic);
                }
            });
            assertTrue(jmsService.isListeningToTopic(connInfo, topic));
            assertFalse(jmsService.isConnectionInRetryList(connInfo));
            assertFalse(jmsService.isTopicInRetryList(connInfo, topic));

            broker.stop();
        } finally {
            if (broker != null) {
                broker.stop();
            }
        }
    }

    @Test
    public void testConnectionRetryMaxAttempt() throws Exception {
        services.destroy();
        services = super.setupServicesForHCatalog();
        String jndiPropertiesString = "java.naming.factory.initial#" + ActiveMQConnFactory + ";"
                + "java.naming.provider.url#" + "tcp://localhost:12345;connectionFactoryNames#ConnectionFactory";
        Configuration servicesConf = services.getConf();
        servicesConf.set(JMSAccessorService.CONF_RETRY_INITIAL_DELAY, "1");
        servicesConf.set(JMSAccessorService.CONF_RETRY_MAX_ATTEMPTS, "1");
        servicesConf.set(HCatAccessorService.JMS_CONNECTIONS_PROPERTIES, "default=" + jndiPropertiesString);
        services.init();
        HCatAccessorService hcatService = Services.get().get(HCatAccessorService.class);
        JMSAccessorService jmsService = Services.get().get(JMSAccessorService.class);

        String publisherAuthority = "hcat.server.com:5080";
        String topic = "topic.topic1";
        JMSConnectionInfo connInfo = hcatService.getJMSConnectionInfo(new URI("hcat://hcat.server.com:8020"));

        jmsService.registerForNotification(connInfo, topic, new HCatMessageHandler(publisherAuthority));
        assertTrue(jmsService.isConnectionInRetryList(connInfo));
        assertTrue(jmsService.isTopicInRetryList(connInfo, topic));
        assertFalse(jmsService.isListeningToTopic(connInfo, topic));
        waitFor(JMS_TIMEOUT_MS, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return jmsService.getNumConnectionAttempts(connInfo) == 1
                        && jmsService.isConnectionInRetryList(connInfo)
                        && jmsService.isTopicInRetryList(connInfo, topic)
                        && !jmsService.isListeningToTopic(connInfo, topic)
                        && !jmsService.retryConnection(connInfo);
            }
        });

        // Should not retry again as max attempt is 1
        assertTrue(jmsService.isConnectionInRetryList(connInfo));
        assertTrue(jmsService.isTopicInRetryList(connInfo, topic));
        assertFalse(jmsService.isListeningToTopic(connInfo, topic));
        assertFalse(jmsService.retryConnection(connInfo));
    }
}
