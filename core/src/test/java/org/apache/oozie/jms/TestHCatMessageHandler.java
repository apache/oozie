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


package org.apache.oozie.jms;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.messaging.HCatEventMessage;
import org.apache.hive.hcatalog.messaging.json.JSONAddPartitionMessage;
import org.apache.log4j.Appender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.WriterAppender;
import org.apache.oozie.dependency.hcat.HCatMessageHandler;
import org.apache.oozie.service.HCatAccessorService;
import org.apache.oozie.service.PartitionDependencyManagerService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.HCatURI;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the message handling specific to HCat partition messages updating the
 * missing dependency cache
 */
public class TestHCatMessageHandler extends XDataTestCase {

    private ConnectionFactory connFac;
    private Connection conn;
    private Session session;
    private Services services;

    @Before
    protected void setUp() throws Exception {
        super.setUp();
        services = super.setupServicesForHCatalog();
        services.init();
        connFac = new ActiveMQConnectionFactory(localActiveMQBroker);
        conn = connFac.createConnection();
        conn.start();
        session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @After
    protected void tearDown() throws Exception {
        services.destroy();
        session.close();
        conn.close();
        super.tearDown();
    }


    /**
     * Test that message is processed to update the dependency map and mark
     * partition as available
     */
    @Test
    public void testCacheUpdateByMessage() {

        try {

            String actionId1 = "1234465451";
            String actionId2 = "1234465452";
            String actionId3 = "1234465453";
            String actionId4 = "1234465454";

            // add partition as missing
            HCatURI dep1 = new HCatURI("hcat://hcat.server.com:5080/mydb/mytbl/dt=20120101;country=us");
            HCatURI dep2 = new HCatURI("hcat://hcat.server.com:5080/mydb/mytbl/country=us;dt=20120101");
            HCatURI dep3 = new HCatURI("hcat://hcat.server.com:5080/mydb/mytbl/dt=20120102;country=us");
            HCatURI dep4 = new HCatURI("hcat://hcat.server.com:5080/mydb/mytbl/dt=20120102;country=us;state=CA");
            PartitionDependencyManagerService pdms = Services.get().get(PartitionDependencyManagerService.class);
            pdms.addMissingDependency(dep1, actionId1);
            pdms.addMissingDependency(dep2, actionId2);
            pdms.addMissingDependency(dep3, actionId3);
            pdms.addMissingDependency(dep4, actionId4);
            assertTrue(pdms.getWaitingActions(dep1).contains(actionId1));
            assertTrue(pdms.getWaitingActions(dep2).contains(actionId2));
            assertTrue(pdms.getWaitingActions(dep3).contains(actionId3));
            assertTrue(pdms.getWaitingActions(dep4).contains(actionId4));

            // construct message
            List<Map<String, String>> partitionsList = new ArrayList<Map<String,String>>();
            partitionsList.add(getPartitionMap("dt=20120101;country=us;state=CA"));
            partitionsList.add(getPartitionMap("dt=20120101;country=us;state=NY"));
            JSONAddPartitionMessage jsonMsg = new JSONAddPartitionMessage("thrift://"+dep1.getServer(), "", dep1.getDb(),
                    dep1.getTable(), partitionsList, System.currentTimeMillis());
            Message msg = session.createTextMessage(jsonMsg.toString());
            msg.setStringProperty(HCatConstants.HCAT_EVENT, HCatEventMessage.EventType.ADD_PARTITION.toString());

            // test message processing
            HCatMessageHandler hcatHandler = new HCatMessageHandler("hcat.server.com:5080");
            hcatHandler.process(msg);
            assertNull(pdms.getWaitingActions(dep1));
            assertNull(pdms.getWaitingActions(dep2));
            assertTrue(pdms.getWaitingActions(dep3).contains(actionId3));
            assertTrue(pdms.getWaitingActions(dep4).contains(actionId4));

            //test message processing through JMS notification listener
            partitionsList.clear();
            partitionsList.add(getPartitionMap("dt=20120102;country=us;state=CA"));
            partitionsList.add(getPartitionMap("dt=20120102;country=us;state=NY"));
            jsonMsg = new JSONAddPartitionMessage("thrift://"+dep1.getServer(), "", dep1.getDb(),
                    dep1.getTable(), partitionsList, System.currentTimeMillis());
            msg = session.createTextMessage(jsonMsg.toString());
            msg.setStringProperty(HCatConstants.HCAT_EVENT, HCatEventMessage.EventType.ADD_PARTITION.toString());
            HCatAccessorService hcatService = Services.get().get(HCatAccessorService.class);
            hcatService.registerForNotification(dep1, "hcat.topic1", hcatHandler);
            Topic topic = session.createTopic("hcat.topic1");
            MessageProducer producer = session.createProducer(topic);
            producer.send(msg);
            Thread.sleep(500);

            assertNull(pdms.getWaitingActions(dep3));
            assertNull(pdms.getWaitingActions(dep4));
            assertTrue(pdms.getAvailableDependencyURIs(actionId1).contains(dep1.getURI().toString()));
            assertTrue(pdms.getAvailableDependencyURIs(actionId2).contains(dep2.getURI().toString()));
            assertTrue(pdms.getAvailableDependencyURIs(actionId3).contains(dep3.getURI().toString()));
            assertTrue(pdms.getAvailableDependencyURIs(actionId4).contains(dep4.getURI().toString()));

        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Exception: " + e.getMessage());
        }

    }

    /**
     * Test the other type of messages - DROP_PARTITION and DROP_TABLE are
     * handled with the correct log messages
     */
    public void testDropEventTypeMessage() {
        try{
            // Set the log4j appender for getting the statements logged by
            // HCatMessageHandler
            Logger logger = Logger.getLogger(HCatMessageHandler.class);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Layout layout = new SimpleLayout();
            Appender appender = new WriterAppender(layout, out);
            logger.addAppender(appender);

            Message msg = session.createTextMessage("{" +
                "\"server\" : \"thrift://localhost:1234\"," +
                "\"db\" : \"default\"," +
                "\"table\" : \"newTable\"," +
                "\"timestamp\" : \"123456\"," +
                "\"partitions\" : [{ \"dt\" : \"2012_01_01\", \"grid\" : \"AB\" }]" +
                "}");
            msg.setStringProperty(HCatConstants.HCAT_EVENT, HCatEventMessage.EventType.DROP_PARTITION.toString());

            HCatMessageHandler hcatHandler = new HCatMessageHandler("localhost");
            hcatHandler.process(msg);
            //check logs to see appropriate error message
            String logMsg = out.toString();
            assertTrue(logMsg.contains(HCatEventMessage.EventType.DROP_PARTITION.toString()));

            msg.setStringProperty(HCatConstants.HCAT_EVENT, HCatEventMessage.EventType.DROP_TABLE.toString());
            hcatHandler.process(msg);
            //check logs to see appropriate error message
            logMsg = out.toString();
            assertTrue(logMsg.contains(HCatEventMessage.EventType.DROP_TABLE.toString()));
        }
        catch (Exception e) {
            fail("Exception caused " + e.getMessage());
        }
    }

}
