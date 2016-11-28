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

import java.text.ParseException;
import java.util.Date;
import java.util.Random;

import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.broker.BrokerService;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.AppType;
import org.apache.oozie.client.event.JobEvent.EventStatus;
import org.apache.oozie.client.event.Event.MessageType;
import org.apache.oozie.client.event.jms.JMSMessagingUtils;
import org.apache.oozie.client.event.jms.JMSHeaderConstants;
import org.apache.oozie.client.event.message.CoordinatorActionMessage;
import org.apache.oozie.client.event.message.WorkflowJobMessage;
import org.apache.oozie.event.*;
import org.apache.oozie.jms.ConnectionContext;
import org.apache.oozie.jms.JMSConnectionInfo;
import org.apache.oozie.jms.JMSJobEventListener;
import org.apache.oozie.service.JMSAccessorService;
import org.apache.oozie.service.JMSTopicService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.DateUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestJMSJobEventListener extends XTestCase {
    private Services services;
    private Configuration conf;

    @Before
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        conf = services.getConf();
        conf.set(Services.CONF_SERVICE_EXT_CLASSES,
                JMSAccessorService.class.getName() + "," + JMSTopicService.class.getName());
        conf.set(JMSJobEventListener.JMS_CONNECTION_PROPERTIES, "java.naming.factory.initial#" + ActiveMQConnFactory
                + ";" + "java.naming.provider.url#" + localActiveMQBroker + ";connectionFactoryNames#"
                + "ConnectionFactory");
        services.init();
    }

    @After
    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    @Test
    public void testOnWorkflowJobStartedEvent() throws ParseException {
        JMSJobEventListener wfEventListener = new JMSJobEventListener();
        wfEventListener.init(conf);
        Date startDate = DateUtils.parseDateUTC("2012-07-22T00:00Z");
        WorkflowJobEvent wfe = new WorkflowJobEvent("wfId1", "caId1", WorkflowJob.Status.RUNNING, "user1",
                "wf-app-name1", startDate, null);

        ConnectionContext jmsContext = getConnectionContext();
        try {
            Session session = jmsContext.createSession(Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = jmsContext.createConsumer(session, wfEventListener.getTopic(wfe));
            wfEventListener.onWorkflowJobEvent(wfe);
            TextMessage message = (TextMessage) consumer.receive(5000);
            assertFalse(message.getText().contains("endTime"));
            WorkflowJobMessage wfStartMessage = JMSMessagingUtils.getEventMessage(message);
            assertEquals(WorkflowJob.Status.RUNNING, wfStartMessage.getStatus());
            assertEquals(startDate, wfStartMessage.getStartTime());
            assertEquals("wfId1", wfStartMessage.getId());
            assertEquals("caId1", wfStartMessage.getParentId());
            assertEquals(MessageType.JOB, wfStartMessage.getMessageType());
            assertEquals(AppType.WORKFLOW_JOB, wfStartMessage.getAppType());
            assertEquals(EventStatus.STARTED, wfStartMessage.getEventStatus());
            assertEquals("user1", wfStartMessage.getUser());
            assertEquals("wf-app-name1", wfStartMessage.getAppName());
            wfEventListener.destroy();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testOnWorkflowJobSuccessEvent() throws ParseException {
        JMSJobEventListener wfEventListener = new JMSJobEventListener();
        wfEventListener.init(conf);
        Date startDate = DateUtils.parseDateUTC("2012-07-22T00:00Z");
        Date endDate = new Date();
        WorkflowJobEvent wfe = new WorkflowJobEvent("wfId1", "caId1", WorkflowJob.Status.SUCCEEDED, "user1",
                "wf-app-name1", startDate, endDate);

        ConnectionContext jmsContext = getConnectionContext();
        try {
            Session session = jmsContext.createSession(Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = jmsContext.createConsumer(session, wfEventListener.getTopic(wfe));
            wfEventListener.onWorkflowJobEvent(wfe);
            TextMessage message = (TextMessage) consumer.receive(5000);
            WorkflowJobMessage wfSuccMessage = JMSMessagingUtils.getEventMessage(message);
            assertEquals(WorkflowJob.Status.SUCCEEDED, wfSuccMessage.getStatus());
            assertEquals(startDate, wfSuccMessage.getStartTime());
            assertEquals(endDate, wfSuccMessage.getEndTime());
            assertEquals("wfId1", wfSuccMessage.getId());
            assertEquals("caId1", wfSuccMessage.getParentId());
            assertEquals(MessageType.JOB, wfSuccMessage.getMessageType());
            assertEquals(AppType.WORKFLOW_JOB, wfSuccMessage.getAppType());
            assertEquals(EventStatus.SUCCESS, wfSuccMessage.getEventStatus());
            assertEquals("user1", wfSuccMessage.getUser());
            assertEquals("wf-app-name1", wfSuccMessage.getAppName());
            wfEventListener.destroy();

        }
        catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testOnWorkflowJobFailureEvent() throws ParseException {
        JMSJobEventListener wfEventListener = new JMSJobEventListener();
        wfEventListener.init(conf);
        Date startDate = DateUtils.parseDateUTC("2012-07-22T00:00Z");
        Date endDate = new Date();
        WorkflowJobEvent wfe = new WorkflowJobEvent("wfId1", "caId1", WorkflowJob.Status.FAILED, "user1",
                "wf-app-name1", startDate, endDate);
        wfe.setErrorCode("dummyErrorCode");
        wfe.setErrorMessage("dummyErrorMessage");
        ConnectionContext jmsContext = getConnectionContext();
        try {
            Session session = jmsContext.createSession(Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = jmsContext.createConsumer(session, wfEventListener.getTopic(wfe));
            wfEventListener.onWorkflowJobEvent(wfe);
            TextMessage message = (TextMessage) consumer.receive(5000);
            WorkflowJobMessage wfFailMessage = JMSMessagingUtils.getEventMessage(message);
            assertEquals(WorkflowJob.Status.FAILED, wfFailMessage.getStatus());
            assertEquals(startDate, wfFailMessage.getStartTime());
            assertEquals(endDate, wfFailMessage.getEndTime());
            assertEquals("wfId1", wfFailMessage.getId());
            assertEquals("caId1", wfFailMessage.getParentId());
            assertEquals(MessageType.JOB, wfFailMessage.getMessageType());
            assertEquals(AppType.WORKFLOW_JOB, wfFailMessage.getAppType());
            assertEquals(EventStatus.FAILURE, wfFailMessage.getEventStatus());
            assertEquals("user1", wfFailMessage.getUser());
            assertEquals("wf-app-name1", wfFailMessage.getAppName());
            assertEquals("dummyErrorCode", wfFailMessage.getErrorCode());
            assertEquals("dummyErrorMessage", wfFailMessage.getErrorMessage());
            wfEventListener.destroy();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testOnWorkflowJobSuspendEvent() throws ParseException {
        JMSJobEventListener wfEventListener = new JMSJobEventListener();
        wfEventListener.init(conf);
        Date startDate = DateUtils.parseDateUTC("2012-07-22T00:00Z");
        WorkflowJobEvent wfe = new WorkflowJobEvent("wfId1", "caId1", WorkflowJob.Status.SUSPENDED, "user1",
                "wf-app-name1", startDate, null);
        ConnectionContext jmsContext = getConnectionContext();
        try {
            Session session = jmsContext.createSession(Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = jmsContext.createConsumer(session, wfEventListener.getTopic(wfe));
            wfEventListener.onWorkflowJobEvent(wfe);
            TextMessage message = (TextMessage) consumer.receive(5000);
            assertFalse(message.getText().contains("endTime"));
            WorkflowJobMessage wfFailMessage = JMSMessagingUtils.getEventMessage(message);
            assertEquals(WorkflowJob.Status.SUSPENDED, wfFailMessage.getStatus());
            assertEquals(startDate, wfFailMessage.getStartTime());
            assertEquals("wfId1", wfFailMessage.getId());
            assertEquals("caId1", wfFailMessage.getParentId());
            assertEquals(MessageType.JOB, wfFailMessage.getMessageType());
            assertEquals(AppType.WORKFLOW_JOB, wfFailMessage.getAppType());
            assertEquals(EventStatus.SUSPEND, wfFailMessage.getEventStatus());
            assertEquals("user1", wfFailMessage.getUser());
            assertEquals("wf-app-name1", wfFailMessage.getAppName());
            assertNull(wfFailMessage.getErrorCode());
            assertNull(wfFailMessage.getErrorMessage());
            wfEventListener.destroy();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testWorkflowJobSelectors() throws ParseException {
        JMSJobEventListener wfEventListener = new JMSJobEventListener();
        wfEventListener.init(conf);
        WorkflowJobEvent wfe = new WorkflowJobEvent("wfId1", "caId1", WorkflowJob.Status.FAILED, "user_1",
                "wf-app-name1", new Date(), new Date());
        ConnectionContext jmsContext = getConnectionContext();
        try {
            Session session = jmsContext.createSession(Session.AUTO_ACKNOWLEDGE);
            String selector = JMSHeaderConstants.USER + "='user_1'";
            MessageConsumer consumer = jmsContext.createConsumer(session, wfEventListener.getTopic(wfe), selector);
            wfEventListener.onWorkflowJobEvent(wfe);
            TextMessage message = (TextMessage) consumer.receive(5000);
            WorkflowJobMessage wfFailMessage = JMSMessagingUtils.getEventMessage(message);
            Assert.assertEquals(WorkflowJob.Status.FAILED, wfFailMessage.getStatus());
            assertEquals("user_1", wfFailMessage.getUser());
            assertEquals(MessageType.JOB, wfFailMessage.getMessageType());
            wfEventListener.destroy();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testWorkflowJobSelectorsNegative() {
        JMSJobEventListener wfEventListener = new JMSJobEventListener();
        wfEventListener.init(conf);
        WorkflowJobEvent wfe = new WorkflowJobEvent("wfId1", "caId1", WorkflowJob.Status.FAILED, "user1",
                "wf-app-name1", new Date(), new Date());
        ConnectionContext jmsContext = getConnectionContext();
        try {
            Session session = jmsContext.createSession(Session.AUTO_ACKNOWLEDGE);
            // Pass a selector which wont match and assert for null message
            String selector = JMSHeaderConstants.USER + "='Non_matching_user'";
            MessageConsumer consumer = jmsContext.createConsumer(session, wfEventListener.getTopic(wfe), selector);
            wfEventListener.onWorkflowJobEvent(wfe);
            TextMessage message = (TextMessage) consumer.receive(5000);
            assertNull(message);
            wfEventListener.destroy();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testWorkflowJobSelectorsOr() {
        JMSJobEventListener wfEventListener = new JMSJobEventListener();
        wfEventListener.init(conf);
        WorkflowJobEvent wfe = new WorkflowJobEvent("wfId1", "caId1", WorkflowJob.Status.FAILED, "user1",
                "wf-app-name1", new Date(), new Date());
        ConnectionContext jmsContext = getConnectionContext();
        try {
            Session session = jmsContext.createSession(Session.AUTO_ACKNOWLEDGE);
            // Pass a selector using OR condition
            String selector = JMSHeaderConstants.USER + "='Non_matching_user' OR " + JMSHeaderConstants.APP_NAME
                    + "='wf-app-name1'";
            MessageConsumer consumer = jmsContext.createConsumer(session, wfEventListener.getTopic(wfe), selector);
            wfEventListener.onWorkflowJobEvent(wfe);
            TextMessage message = (TextMessage) consumer.receive(5000);
            WorkflowJobMessage wfFailMessage = JMSMessagingUtils.getEventMessage(message);
            Assert.assertEquals(WorkflowJob.Status.FAILED, wfFailMessage.getStatus());
            assertEquals("user1", wfFailMessage.getUser());
            assertEquals(MessageType.JOB, wfFailMessage.getMessageType());
            wfEventListener.destroy();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testWorkflowJobSelectorsAnd() {
        JMSJobEventListener wfEventListener = new JMSJobEventListener();
        wfEventListener.init(conf);
        WorkflowJobEvent wfe = new WorkflowJobEvent("wfId1", "caId1", WorkflowJob.Status.FAILED, "user1",
                "wf-app-name1", new Date(), new Date());
        ConnectionContext jmsContext = getConnectionContext();
        try {
            Session session = jmsContext.createSession(Session.AUTO_ACKNOWLEDGE);
            // Pass a selector using AND condition
            String selector = JMSHeaderConstants.EVENT_STATUS + "='FAILURE' AND " + JMSHeaderConstants.APP_TYPE
                    + "='WORKFLOW_JOB' AND " + JMSHeaderConstants.MESSAGE_TYPE + "='JOB'";
            MessageConsumer consumer = jmsContext.createConsumer(session, wfEventListener.getTopic(wfe), selector);
            wfEventListener.onWorkflowJobEvent(wfe);
            TextMessage message = (TextMessage) consumer.receive(5000);
            WorkflowJobMessage wfFailMessage = JMSMessagingUtils.getEventMessage(message);
            Assert.assertEquals(WorkflowJob.Status.FAILED, wfFailMessage.getStatus());
            assertEquals("user1", wfFailMessage.getUser());
            assertEquals(MessageType.JOB, wfFailMessage.getMessageType());
            wfEventListener.destroy();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testConnectionDrop() {
        Random random = new Random();
        try {
            services.destroy();
            services = new Services();
            Configuration conf = services.getConf();
            conf.set(Services.CONF_SERVICE_EXT_CLASSES, JMSAccessorService.class.getName() + ","
                    + JMSTopicService.class.getName());
            int randomPort = 30000 + random.nextInt(10000);
            String brokerURl = "tcp://localhost:" + randomPort;
            conf.set(JMSJobEventListener.JMS_CONNECTION_PROPERTIES, "java.naming.factory.initial#"
                    + ActiveMQConnFactory + ";" + "java.naming.provider.url#" + brokerURl + ";connectionFactoryNames#"
                    + "ConnectionFactory");
            services.init();
            JMSJobEventListener wfEventListener = new JMSJobEventListener();
            wfEventListener.init(conf);
            BrokerService broker = new BrokerService();
            broker.addConnector(brokerURl);
            broker.start();
            ConnectionContext jmsContext = getConnectionContext();
            assertNotNull(jmsContext);
            broker.stop();
            jmsContext = getConnectionContext();
            // Exception Listener should have removed the old conn context
            assertNull(jmsContext);
            broker = new BrokerService();
            broker.addConnector(brokerURl);
            broker.start();
            WorkflowJobEvent wfe = new WorkflowJobEvent("wfId1", "caId1", WorkflowJob.Status.FAILED, "user1",
                    "wf-app-name1", new Date(), new Date());

            jmsContext = getConnectionContext();
            Session session = jmsContext.createSession(Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = jmsContext.createConsumer(session, wfEventListener.getTopic(wfe));
            wfEventListener.onWorkflowJobEvent(wfe);
            TextMessage message = (TextMessage) consumer.receive(5000);
            assertNotNull(message);
            broker.stop();
            wfEventListener.destroy();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

    }

    @Test
    public void testOnCoordinatorActionWaitingEvent() throws ParseException {
        JMSJobEventListener wfEventListner = new JMSJobEventListener();
        wfEventListner.init(conf);
        Date startDate = DateUtils.parseDateUTC("2012-07-22T00:00Z");
        Date nominalTime = DateUtils.parseDateUTC("2011-07-11T00:00Z");
        CoordinatorActionEvent cae = new CoordinatorActionEvent("caId1", "caJobId1", CoordinatorAction.Status.WAITING,
                "user1", "wf-app-name1", nominalTime, startDate, "missingDep1");
        ConnectionContext jmsContext = getConnectionContext();
        try {
            Session session = jmsContext.createSession(Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = jmsContext.createConsumer(session, wfEventListner.getTopic(cae));
            wfEventListner.onCoordinatorActionEvent(cae);
            TextMessage message = (TextMessage) consumer.receive(5000);
            assertFalse(message.getText().contains("endTime"));
            assertFalse(message.getText().contains("errorCode"));
            assertFalse(message.getText().contains("errorMessage"));
            CoordinatorActionMessage coordActionWaitingMessage = JMSMessagingUtils
                    .getEventMessage(message);
            assertEquals(CoordinatorAction.Status.WAITING, coordActionWaitingMessage.getStatus());
            assertEquals(startDate, coordActionWaitingMessage.getStartTime());
            assertEquals(nominalTime, coordActionWaitingMessage.getNominalTime());
            assertEquals("caJobId1", coordActionWaitingMessage.getParentId());
            assertEquals("caId1", coordActionWaitingMessage.getId());
            assertEquals(MessageType.JOB, coordActionWaitingMessage.getMessageType());
            assertEquals(AppType.COORDINATOR_ACTION, coordActionWaitingMessage.getAppType());
            assertEquals(EventStatus.WAITING, coordActionWaitingMessage.getEventStatus());
            assertEquals("user1", coordActionWaitingMessage.getUser());
            assertEquals("wf-app-name1", coordActionWaitingMessage.getAppName());
            assertEquals("missingDep1", coordActionWaitingMessage.getMissingDependency());
        }
        catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testOnCoordinatorActionStartEvent() throws ParseException {
        JMSJobEventListener coordEventListener = new JMSJobEventListener();
        coordEventListener.init(conf);
        Date startDate = DateUtils.parseDateUTC("2012-07-22T00:00Z");
        Date nominalTime = DateUtils.parseDateUTC("2011-07-11T00:00Z");
        CoordinatorActionEvent cae = new CoordinatorActionEvent("caId1", "caJobId1", CoordinatorAction.Status.RUNNING,
                "user1", "wf-app-name1", nominalTime, startDate, null);
        ConnectionContext jmsContext = getConnectionContext();
        try {
            Session session = jmsContext.createSession(Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = jmsContext.createConsumer(session, coordEventListener.getTopic(cae));
            coordEventListener.onCoordinatorActionEvent(cae);
            TextMessage message = (TextMessage) consumer.receive(5000);
            assertFalse(message.getText().contains("endTime"));
            assertFalse(message.getText().contains("errorCode"));
            assertFalse(message.getText().contains("errorMessage"));
            assertFalse(message.getText().contains("missingDependency"));
            CoordinatorActionMessage coordActionStartMessage = JMSMessagingUtils
                    .getEventMessage(message);
            assertEquals(CoordinatorAction.Status.RUNNING, coordActionStartMessage.getStatus());
            assertEquals(startDate, coordActionStartMessage.getStartTime());
            assertEquals("caJobId1", coordActionStartMessage.getParentId());
            assertEquals("caId1", coordActionStartMessage.getId());
            assertEquals(MessageType.JOB, coordActionStartMessage.getMessageType());
            assertEquals(AppType.COORDINATOR_ACTION, coordActionStartMessage.getAppType());
            assertEquals(EventStatus.STARTED, coordActionStartMessage.getEventStatus());
            assertEquals("user1", coordActionStartMessage.getUser());
            assertEquals("wf-app-name1", coordActionStartMessage.getAppName());
        }
        catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testOnCoordinatorJobSuccessEvent() throws ParseException {
        JMSJobEventListener coordEventListener = new JMSJobEventListener();
        coordEventListener.init(conf);
        Date startDate = DateUtils.parseDateUTC("2012-07-22T00:00Z");
        Date nominalTime = DateUtils.parseDateUTC("2011-07-11T00:00Z");
        Date endDate = new Date();
        CoordinatorActionEvent cae = new CoordinatorActionEvent("caId1", "caJobId1",
                CoordinatorAction.Status.SUCCEEDED, "user1", "wf-app-name1", nominalTime, startDate, null);
        cae.setEndTime(endDate);
        ConnectionContext jmsContext = getConnectionContext();
        try {
            Session session = jmsContext.createSession(Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = jmsContext.createConsumer(session, coordEventListener.getTopic(cae));
            coordEventListener.onCoordinatorActionEvent(cae);
            TextMessage message = (TextMessage) consumer.receive(5000);
            assertFalse(message.getText().contains("errorCode"));
            assertFalse(message.getText().contains("errorMessage"));
            assertFalse(message.getText().contains("missingDependency"));
            CoordinatorActionMessage coordActionSuccessMessage = JMSMessagingUtils
                    .getEventMessage(message);
            assertEquals(CoordinatorAction.Status.SUCCEEDED, coordActionSuccessMessage.getStatus());
            assertEquals(startDate, coordActionSuccessMessage.getStartTime());
            assertEquals(endDate, coordActionSuccessMessage.getEndTime());
            assertEquals("caJobId1", coordActionSuccessMessage.getParentId());
            assertEquals("caId1", coordActionSuccessMessage.getId());
            assertEquals(MessageType.JOB, coordActionSuccessMessage.getMessageType());
            assertEquals(AppType.COORDINATOR_ACTION, coordActionSuccessMessage.getAppType());
            assertEquals(EventStatus.SUCCESS, coordActionSuccessMessage.getEventStatus());
            assertEquals("user1", coordActionSuccessMessage.getUser());
            assertEquals("wf-app-name1", coordActionSuccessMessage.getAppName());
        }
        catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testOnCoordinatorJobFailureEvent() throws ParseException {
        JMSJobEventListener coordEventListener = new JMSJobEventListener();
        coordEventListener.init(conf);
        Date startDate = DateUtils.parseDateUTC("2012-07-22T00:00Z");
        Date nominalTime = DateUtils.parseDateUTC("2011-07-11T00:00Z");
        Date endDate = new Date();
        CoordinatorActionEvent cae = new CoordinatorActionEvent("caId1", "caJobId1", CoordinatorAction.Status.FAILED,
                "user1", "wf-app-name1", nominalTime, startDate, null);
        cae.setEndTime(endDate);
        cae.setErrorCode("E0101");
        cae.setErrorMessage("dummyError");
        ConnectionContext jmsContext = getConnectionContext();
        try {
            Session session = jmsContext.createSession(Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = jmsContext.createConsumer(session, coordEventListener.getTopic(cae));
            coordEventListener.onCoordinatorActionEvent(cae);
            TextMessage message = (TextMessage) consumer.receive(5000);
            assertFalse(message.getText().contains("missingDependency"));
            CoordinatorActionMessage coordActionFailMessage = JMSMessagingUtils
                    .getEventMessage(message);
            assertEquals(CoordinatorAction.Status.FAILED, coordActionFailMessage.getStatus());
            assertEquals(startDate, coordActionFailMessage.getStartTime());
            assertEquals(endDate, coordActionFailMessage.getEndTime());
            assertEquals("caJobId1", coordActionFailMessage.getParentId());
            assertEquals("caId1", coordActionFailMessage.getId());
            assertEquals(MessageType.JOB, coordActionFailMessage.getMessageType());
            assertEquals(AppType.COORDINATOR_ACTION, coordActionFailMessage.getAppType());
            assertEquals(EventStatus.FAILURE, coordActionFailMessage.getEventStatus());
            assertEquals("user1", coordActionFailMessage.getUser());
            assertEquals("wf-app-name1", coordActionFailMessage.getAppName());
            assertEquals("E0101", coordActionFailMessage.getErrorCode());
            assertEquals("dummyError", coordActionFailMessage.getErrorMessage());
        }
        catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testCoordinatorActionSelectors() throws ParseException {
        JMSJobEventListener coordEventListener = new JMSJobEventListener();
        coordEventListener.init(conf);
        Date startDate = DateUtils.parseDateUTC("2012-07-22T00:00Z");
        Date nominalTime = DateUtils.parseDateUTC("2011-07-11T00:00Z");
        CoordinatorActionEvent cae = new CoordinatorActionEvent("caId1", "caJobId1", CoordinatorAction.Status.FAILED,
                "user1", "wf-app-name1", nominalTime, startDate, null);
        ConnectionContext jmsContext = getConnectionContext();
        try {
            Session session = jmsContext.createSession(Session.AUTO_ACKNOWLEDGE);
            String selector = JMSHeaderConstants.USER + "='user1'";
            MessageConsumer consumer = jmsContext.createConsumer(session, coordEventListener.getTopic(cae), selector);
            coordEventListener.onCoordinatorActionEvent(cae);
            TextMessage message = (TextMessage) consumer.receive(5000);
            CoordinatorActionMessage coordActionFailMessage = JMSMessagingUtils
                    .getEventMessage(message);
            Assert.assertEquals(CoordinatorAction.Status.FAILED, coordActionFailMessage.getStatus());
            assertEquals("user1", coordActionFailMessage.getUser());
            assertEquals(MessageType.JOB, coordActionFailMessage.getMessageType());
        }
        catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testCoordinatorActionSelectorsNegative() throws ParseException {
        JMSJobEventListener coordEventListener = new JMSJobEventListener();
        coordEventListener.init(conf);
        Date startDate = DateUtils.parseDateUTC("2012-07-22T00:00Z");
        Date nominalTime = DateUtils.parseDateUTC("2011-07-11T00:00Z");
        CoordinatorActionEvent cae = new CoordinatorActionEvent("caId1", "caJobId1", CoordinatorAction.Status.FAILED,
                "user1", "wf-app-name1", nominalTime, startDate, null);
        ConnectionContext jmsContext = getConnectionContext();
        try {
            Session session = jmsContext.createSession(Session.AUTO_ACKNOWLEDGE);
            // Pass a selector which wont match and assert for null message
            String selector = JMSHeaderConstants.USER + "='Non_matching_user'";
            MessageConsumer consumer = jmsContext.createConsumer(session, coordEventListener.getTopic(cae), selector);
            coordEventListener.onCoordinatorActionEvent(cae);
            TextMessage message = (TextMessage) consumer.receive(5000);
            assertNull(message);
        }
        catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    private ConnectionContext getConnectionContext() {
        Configuration conf = services.getConf();
        String jmsProps = conf.get(JMSJobEventListener.JMS_CONNECTION_PROPERTIES);
        JMSConnectionInfo connInfo = new JMSConnectionInfo(jmsProps);
        JMSAccessorService jmsService = Services.get().get(JMSAccessorService.class);
        ConnectionContext jmsContext = jmsService.createProducerConnectionContext(connInfo);
        return jmsContext;

    }

}
