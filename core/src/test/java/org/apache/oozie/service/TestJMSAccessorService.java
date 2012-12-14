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

import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.HCatURI;

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

    public void testService() {
        JMSAccessorService jmsService = services.get(JMSAccessorService.class);
        Assert.assertNotNull(jmsService);
    }

    public void testConsumer() throws Exception {
        JMSAccessorService jmsService = services.get(JMSAccessorService.class);
        MessageConsumer consumer = null;
        try {
            consumer = jmsService.getMessageConsumer(JMSAccessorService.DEFAULT_SERVER_ENDPOINT, "test-topic");
            assert (consumer != null);
        }
        finally {
            if (consumer != null) {
                consumer.close();
                jmsService.removeSession(JMSAccessorService.DEFAULT_SERVER_ENDPOINT, "test-topic");
            }
        }
    }

    public void testProducer() throws Exception {
        JMSAccessorService jmsService = services.get(JMSAccessorService.class);
        MessageProducer producer = null;
        try {
            producer = jmsService.getMessageProducer(JMSAccessorService.DEFAULT_SERVER_ENDPOINT, "test-topic");
            assert (producer != null);
        }
        finally {
            if (producer != null) {
                jmsService.removeSession(JMSAccessorService.DEFAULT_SERVER_ENDPOINT, "test-topic");
            }
        }
    }

    public void testSession() throws Exception {
        JMSAccessorService jmsService = services.get(JMSAccessorService.class);
        Session sess = null;
        try {
            sess = jmsService.getSession(JMSAccessorService.DEFAULT_SERVER_ENDPOINT, "test-topic");
            assert (sess != null);
        }
        finally {
            if (sess != null) {
                jmsService.removeSession(JMSAccessorService.DEFAULT_SERVER_ENDPOINT, "test-topic");
            }
        }
    }

    public void testConnection() {
        JMSAccessorService jmsService = services.get(JMSAccessorService.class);
        JMSAccessorService.ConnectionContext conCtx = jmsService
                .getConnectionContext(JMSAccessorService.DEFAULT_SERVER_ENDPOINT);
        assert (conCtx.getConnection() != null);
    }

    public void testSingleConsumerPerTopic() {

        try {
            // Add sample missing partitions belonging to same table
            String partition1 = "hcat://hcat.server.com:5080/" + HCatURI.DB_PREFIX + "/mydb/" + HCatURI.TABLE_PREFIX
                    + "/mytable/" + HCatURI.PARTITION_PREFIX + "/mypart=10";
            String partition2 = "hcat://hcat.server.com:5080/" + HCatURI.DB_PREFIX + "/mydb/" + HCatURI.TABLE_PREFIX
                    + "/mytable/" + HCatURI.PARTITION_PREFIX + "/mypart=20";

            //Expected topic name is thus:
            String topic = "hcat.mydb.mytable";

            PartitionDependencyManagerService pdms = services.get(PartitionDependencyManagerService.class);

            pdms.addMissingPartition(partition1, "action-1");
            pdms.addMissingPartition(partition2, "action-2");
            //this registers the message receiver

            JMSAccessorService jmsService = services.get(JMSAccessorService.class);
            String endPoint = JMSAccessorService.DEFAULT_SERVER_ENDPOINT;
            assertNotNull(jmsService.getConnectionContext(endPoint));
            assertNotNull(jmsService.getSession(endPoint, topic));

            //check there is only *1* message consumer since topic-name for both actions was same
            assertEquals(1, jmsService.getConnectionContext(endPoint).getTopicReceiverMap().size());

            String partition3 = "hcat://hcat.server.com:5080/" + HCatURI.DB_PREFIX + "/mydb/" + HCatURI.TABLE_PREFIX
                    + "/otherTable/" + HCatURI.PARTITION_PREFIX + "/mypart=abc";
            pdms.addMissingPartition(partition3, "action-3");
            // Now there should be one more consumer for the new topic
            assertNotNull(jmsService.getSession(endPoint, "hcat.mydb.otherTable"));
            assertEquals(2, jmsService.getConnectionContext(endPoint).getTopicReceiverMap().size());

        }
        catch (Exception e) {
            fail("Exception encountered : " + e);
        }

    }
}
