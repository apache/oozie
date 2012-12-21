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

import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.test.XTestCase;
import org.junit.Test;

public class TestJMSAccessorService extends XTestCase {
    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = super.setupServicesForHCatalog();
        setSystemProperty(PartitionDependencyManagerService.MAP_MAX_WEIGHTED_CAPACITY, "100");
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
        assertTrue(jmsService.getOrCreateConnection("blahblah"));
        assertTrue(jmsService.getOrCreateConnection(JMSAccessorService.DEFAULT_SERVER_ENDPOINT));
    }

    @Test
    public void testConsumer() throws Exception {
        JMSAccessorService jmsService = services.get(JMSAccessorService.class);
        jmsService.getOrCreateConnection(JMSAccessorService.DEFAULT_SERVER_ENDPOINT);
        MessageConsumer consumer = null;
        try {
            consumer = jmsService.getMessageConsumer(JMSAccessorService.DEFAULT_SERVER_ENDPOINT, "test-topic");
            assertTrue(consumer != null);
        }
        finally {
            if (consumer != null) {
                consumer.close();
                jmsService.removeSession(JMSAccessorService.DEFAULT_SERVER_ENDPOINT, "test-topic");
            }
        }
    }

    @Test
    public void testProducer() throws Exception {
        JMSAccessorService jmsService = services.get(JMSAccessorService.class);
        jmsService.getOrCreateConnection(JMSAccessorService.DEFAULT_SERVER_ENDPOINT);
        MessageProducer producer = null;
        try {
            producer = jmsService.getMessageProducer(JMSAccessorService.DEFAULT_SERVER_ENDPOINT, "test-topic");
            assertTrue(producer != null);
        }
        finally {
            if (producer != null) {
                jmsService.removeSession(JMSAccessorService.DEFAULT_SERVER_ENDPOINT, "test-topic");
            }
        }
    }

    @Test
    public void testSession() throws Exception {
        JMSAccessorService jmsService = services.get(JMSAccessorService.class);
        jmsService.getOrCreateConnection(JMSAccessorService.DEFAULT_SERVER_ENDPOINT);
        Session sess = null;
        try {
            sess = jmsService.getSession(JMSAccessorService.DEFAULT_SERVER_ENDPOINT, "test-topic");
            assertTrue(sess != null);
        }
        finally {
            if (sess != null) {
                jmsService.removeSession(JMSAccessorService.DEFAULT_SERVER_ENDPOINT, "test-topic");
            }
        }
    }

    @Test
    public void testSingleConsumerPerTopic() {

        try {
            JMSAccessorService jmsService = services.get(JMSAccessorService.class);
            String endPoint = "hcat://hcat.server.com:5080";
            jmsService.getOrCreateConnection(endPoint);
            // Add sample missing partitions belonging to same table
            String partition1 = "hcat://hcat.server.com:5080/mydb/mytable/mypart=10";
            String partition2 = "hcat://hcat.server.com:5080/mydb/mytable/mypart=20";

            //Expected topic name is thus:
            String topic = "hcat.mydb.mytable";

            PartitionDependencyManagerService pdms = services.get(PartitionDependencyManagerService.class);

            pdms.addMissingPartition(partition1, "action-1");
            pdms.addMissingPartition(partition2, "action-2");
            //this registers the message receiver


            assertNotNull(jmsService.getConnectionContext(endPoint));
            assertNotNull(jmsService.getSession(endPoint, topic));

            //check there is only *1* message consumer since topic-name for both actions was same
            assertEquals(1, jmsService.getConnectionContext(endPoint).getTopicReceiverMap().size());

            String partition3 = "hcat://hcat.server.com:5080/mydb/otherTable/mypart=abc";
            pdms.addMissingPartition(partition3, "action-3");
            // Now there should be one more consumer for the new topic
            assertNotNull(jmsService.getSession(endPoint, "hcat.mydb.otherTable"));
            assertEquals(2, jmsService.getConnectionContext(endPoint).getTopicReceiverMap().size());

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
        String jmsServerMapping = jmsService.getJMSServerMapping("hcat://axoniteblue-1.blue.server.com:8020");
        // rules will be applied
        assertEquals("java.naming.factory.initial#Dummy.Factory;java.naming.provider.url#tcp://broker.blue:61616", jmsServerMapping);

        jmsServerMapping = jmsService.getJMSServerMapping("UNKNOWN_SERVER");
        // will map to default
        assertEquals("java.naming.factory.initial#org.apache.activemq.jndi.ActiveMQInitialContextFactory;java.naming.provider.url#vm://localhost?broker.persistent=false", jmsServerMapping);

        jmsServerMapping = jmsService.getJMSServerMapping("hcat://xyz.corp.dummy.com");
        assertEquals("java.naming.factory.initial#Dummy.Factory;java.naming.provider.url#tcp:localhost:61616", jmsServerMapping);
    }
}
