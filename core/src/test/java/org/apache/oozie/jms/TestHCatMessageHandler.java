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
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.messaging.HCatEventMessage;
import org.apache.hcatalog.messaging.json.JSONAddPartitionMessage;
import org.apache.log4j.Appender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.WriterAppender;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.service.PartitionDependencyManagerService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.HCatURI;
import org.apache.oozie.util.PartitionWrapper;
import org.apache.oozie.util.PartitionsGroup;
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

    @Before
    protected void setUp() throws Exception {
        super.setUp();
        Services services = super.setupServicesForHCatalog();
        services.init();

        connFac = new ActiveMQConnectionFactory(localActiveMQBroker);
        conn = connFac.createConnection();
        session = conn.createSession(true, Session.SESSION_TRANSACTED);
    }

    @After
    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    /**
     * Test HCat message is received by the HCatMessageHandler implementation
     * Can be used with actual ActiveMQ installation
     */
    @Test
    public void testProcessHCatMessage() {
        String topicName = "hcat.default.mytbl"; // Assuming you gave TABLE as
                                                 // 'mytbl'
        try {
            MessageReceiver recvr = new MessageReceiver(new HCatMessageHandler());
            recvr.registerTopic(topicName);
            Thread.sleep(5000);
            recvr.unRegisterTopic(topicName);
        }
        catch (Exception e) {
            fail("Exception caused " + e.getMessage());
        }
    }

    /**
     * Generic test from ActiveMQ messaging system to see message is received
     * and has the expected type
     *
     * @throws Exception
     */
    @Test
    public void testMessage() throws Exception {
        String topicName = "hcat.default.mytbl"; // Assuming you gave TABLE as
                                                 // 'mytbl'
        MessageReceiver recvr = new MessageReceiver(new MessageHandler() {
            public void process(Message msg) {
                assertTrue(msg instanceof ActiveMQTextMessage);
            }
        });
        recvr.registerTopic(topicName);
        Thread.sleep(5000);
        recvr.unRegisterTopic(topicName);
    }

    /**
     * Test that message is processed to update the dependency map and mark
     * partition as available
     */
    @Test
    public void testCacheUpdateByMessage() {

        try {
            // Define partition dependency
            String stringDep = "hcat://hcat.yahoo.com:5080/database/mydb/table/mytbl/partition/datastamp=12,region=us";
            HCatURI dep = new HCatURI(stringDep);
            List<Map<String, String>> partitions = new ArrayList<Map<String, String>>(1);
            partitions.add(dep.getPartitionMap());

            // Add action to DB
            CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
            String jobId = job.getId();
            CoordinatorActionBean action = addRecordToCoordActionTable(jobId, 1, CoordinatorAction.Status.WAITING,
                    "coord-action-for-action-input-check.xml", 0);
            String actionId = action.getId();

            // add partition as missing
            PartitionDependencyManagerService pdms = Services.get().get(PartitionDependencyManagerService.class);
            pdms.addMissingPartition(new PartitionWrapper(dep), actionId);

            // construct message
            JSONAddPartitionMessage jsonMsg = new JSONAddPartitionMessage(dep.getServerEndPoint(), "", dep.getDb(),
                    dep.getTable(), partitions, System.currentTimeMillis());
            Message msg = session.createTextMessage(jsonMsg.toString());
            msg.setStringProperty(HCatConstants.HCAT_EVENT, HCatEventMessage.EventType.ADD_PARTITION.toString());

            // process message
            HCatMessageHandler hcatHandler = new HCatMessageHandler();
            hcatHandler.process(msg);

            //partition removed from missing cache - cascade ON
            assertFalse(pdms.getHCatMap()
                    .containsKey(PartitionWrapper.makePrefix(dep.getServerEndPoint(), dep.getDb())));

            // bunch of other partitions
            stringDep = "hcat://hcat.yahoo.com:5080/database/mydb/table/mytbl/partition/user=joe";
            dep = new HCatURI(stringDep);
            pdms.addMissingPartition(new PartitionWrapper(dep), actionId);
            stringDep = "hcat://hcat.yahoo.com:5080/database/mydb/table/mytbl/partition/part=fake";
            dep = new HCatURI(stringDep);
            partitions = new ArrayList<Map<String, String>>(1);
            partitions.add(dep.getPartitionMap());

            // negative test - message for partition that does not exist in
            // partition dependency cache
            jsonMsg = new JSONAddPartitionMessage(dep.getServerEndPoint(), "", dep.getDb(), dep.getTable(), partitions,
                    System.currentTimeMillis());
            msg = session.createTextMessage(jsonMsg.toString());
            msg.setStringProperty(HCatConstants.HCAT_EVENT, HCatEventMessage.EventType.ADD_PARTITION.toString());

            hcatHandler.process(msg);

            PartitionsGroup pg = pdms.getHCatMap().get(PartitionWrapper.makePrefix(dep.getServerEndPoint(), dep.getDb()))
                    .get(dep.getTable());
            assertFalse(pg.getPartitionsMap().containsKey(new PartitionWrapper(dep)));

        }
        catch (Exception e) {
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

            HCatMessageHandler hcatHandler = new HCatMessageHandler();
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
