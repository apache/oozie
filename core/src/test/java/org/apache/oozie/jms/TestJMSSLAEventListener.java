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

import java.util.Date;

import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.AppType;
import org.apache.oozie.client.event.Event.MessageType;
import org.apache.oozie.client.event.SLAEvent.EventStatus;
import org.apache.oozie.client.event.SLAEvent.SLAStatus;
import org.apache.oozie.client.event.jms.JMSHeaderConstants;
import org.apache.oozie.client.event.jms.JMSMessagingUtils;
import org.apache.oozie.client.event.message.SLAMessage;
import org.apache.oozie.service.JMSAccessorService;
import org.apache.oozie.service.JMSTopicService;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.SLACalcStatus;
import org.apache.oozie.sla.SLARegistrationBean;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.DateUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestJMSSLAEventListener extends XTestCase {

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
                + ";" + "java.naming.provider.url#" + localActiveMQBroker + ";" + "connectionFactoryNames#"
                + "ConnectionFactory");
        services.init();
    }

    @After
    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    private ConnectionContext getConnectionContext() {
        Configuration conf = services.getConf();
        String jmsProps = conf.get(JMSJobEventListener.JMS_CONNECTION_PROPERTIES);
        JMSConnectionInfo connInfo = new JMSConnectionInfo(jmsProps);
        JMSAccessorService jmsService = Services.get().get(JMSAccessorService.class);
        ConnectionContext jmsContext = jmsService.createConnectionContext(connInfo);
        return jmsContext;
    }

    @Test
    public void testOnSLAStartMissEvent() throws Exception {
        JMSSLAEventListener slaListener = new JMSSLAEventListener();
        slaListener.init(conf);
        String id = "0000000-000000000000001-oozie-wrkf-C@1";
        SLACalcStatus startMiss = _createSLACalcStatus(id);
        SLARegistrationBean startMissBean = startMiss.getSLARegistrationBean();
        Date startDate = DateUtils.parseDateUTC("2013-01-01T00:00Z");
        startMiss.setId(id);
        startMissBean.setParentId("0000000-000000000000001-oozie-wrkf-C");
        startMissBean.setAppName("Test-SLA-Start-Miss");
        startMissBean.setUser("dummyuser");
        startMissBean.setExpectedStart(startDate);
        startMissBean.setNotificationMsg("notification of start miss");
        startMissBean.setJobData("random job data");
        startMiss.setEventStatus(EventStatus.START_MISS);
        startMiss.setSLAStatus(SLAStatus.NOT_STARTED);
        startMissBean.setAppType(AppType.COORDINATOR_ACTION);
        startMiss.setActualStart(DateUtils.parseDateUTC("2013-01-01T01:00Z"));

        ConnectionContext jmsContext = getConnectionContext();

        Session session = jmsContext.createSession(Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = jmsContext.createConsumer(session, slaListener.getTopic(startMiss));
        slaListener.onStartMiss(startMiss);
        TextMessage message = (TextMessage) consumer.receive(5000);
        SLAMessage slaStartMissMsg = (SLAMessage) JMSMessagingUtils.getEventMessage(message);
        // check msg header
        assertEquals(EventStatus.START_MISS, slaStartMissMsg.getEventStatus());
        assertEquals(SLAStatus.NOT_STARTED, slaStartMissMsg.getSLAStatus());
        assertEquals(AppType.COORDINATOR_ACTION, slaStartMissMsg.getAppType());
        assertEquals(MessageType.SLA, slaStartMissMsg.getMessageType());
        assertEquals("Test-SLA-Start-Miss", slaStartMissMsg.getAppName());
        assertEquals("dummyuser", slaStartMissMsg.getUser());
        // check msg body
        assertEquals("0000000-000000000000001-oozie-wrkf-C@1", slaStartMissMsg.getId());
        assertEquals("0000000-000000000000001-oozie-wrkf-C", slaStartMissMsg.getParentId());
        assertEquals(startDate, slaStartMissMsg.getExpectedStartTime());
        assertEquals("notification of start miss", slaStartMissMsg.getNotificationMessage());
    }

    public void testOnSLAEndMissEvent() throws Exception {
        JMSSLAEventListener slaListener = new JMSSLAEventListener();
        slaListener.init(conf);
        String id = "0000000-000000000000001-oozie-wrkf-C@1";
        SLACalcStatus endMiss = _createSLACalcStatus(id);
        SLARegistrationBean endMissBean = endMiss.getSLARegistrationBean();
        Date expectedEndDate = DateUtils.parseDateUTC("2013-01-01T00:00Z");
        Date actualEndDate = DateUtils.parseDateUTC("2013-01-01T01:00Z");
        endMiss.setId(id);
        endMissBean.setParentId("0000000-000000000000001-oozie-wrkf-C");
        endMissBean.setAppName("Test-SLA-End-Miss");
        endMiss.setEventStatus(EventStatus.END_MISS);
        endMiss.setSLAStatus(SLAStatus.IN_PROCESS);
        endMissBean.setAppType(AppType.COORDINATOR_ACTION);
        endMissBean.setUser("dummyuser");
        endMissBean.setNotificationMsg("notification of end miss");
        endMissBean.setExpectedEnd(expectedEndDate);
        endMiss.setActualEnd(actualEndDate);

        ConnectionContext jmsContext = getConnectionContext();

        Session session = jmsContext.createSession(Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = jmsContext.createConsumer(session, slaListener.getTopic(endMiss));
        slaListener.onEndMiss(endMiss);
        TextMessage message = (TextMessage) consumer.receive(5000);
        SLAMessage slaEndMissMsg = (SLAMessage) JMSMessagingUtils.getEventMessage(message);
        // check msg header
        assertEquals(EventStatus.END_MISS, slaEndMissMsg.getEventStatus());
        assertEquals(SLAStatus.IN_PROCESS, slaEndMissMsg.getSLAStatus());
        assertEquals(AppType.COORDINATOR_ACTION, slaEndMissMsg.getAppType());
        assertEquals(MessageType.SLA, slaEndMissMsg.getMessageType());
        assertEquals("Test-SLA-End-Miss", slaEndMissMsg.getAppName());
        assertEquals("dummyuser", slaEndMissMsg.getUser());
        // check msg body
        assertEquals("0000000-000000000000001-oozie-wrkf-C@1", slaEndMissMsg.getId());
        assertEquals("0000000-000000000000001-oozie-wrkf-C", slaEndMissMsg.getParentId());
        assertEquals(expectedEndDate, slaEndMissMsg.getExpectedEndTime());
        assertEquals(actualEndDate, slaEndMissMsg.getActualEndTime());
        assertEquals("notification of end miss", slaEndMissMsg.getNotificationMessage());
    }

    public void testOnSLADurationMissEvent() throws Exception {
        JMSSLAEventListener slaListener = new JMSSLAEventListener();
        slaListener.init(conf);
        String id = "0000000-000000000000001-oozie-wrkf-C@1";
        SLACalcStatus durationMiss = _createSLACalcStatus(id);
        SLARegistrationBean durationMissBean = durationMiss.getSLARegistrationBean();
        Date expectedStartDate = DateUtils.parseDateUTC("2013-01-01T00:00Z");
        Date actualStartDate = DateUtils.parseDateUTC("2013-01-01T01:00Z");
        Date expectedEndDate = DateUtils.parseDateUTC("2013-01-01T12:00Z");
        Date actualEndDate = DateUtils.parseDateUTC("2013-01-01T14:00Z");
        long expectedDuration = ( expectedEndDate.getTime() - actualStartDate.getTime() ) / (1000 * 60);
        durationMiss.setId(id);
        durationMissBean.setParentId("0000000-000000000000001-oozie-wrkf-C");
        durationMissBean.setAppName("Test-SLA-Duration-Miss");
        durationMiss.setEventStatus(EventStatus.DURATION_MISS);
        durationMiss.setSLAStatus(SLAStatus.IN_PROCESS);
        durationMissBean.setAppType(AppType.COORDINATOR_ACTION);
        durationMissBean.setUser("dummyuser");
        durationMissBean.setNotificationMsg("notification of duration miss");
        durationMissBean.setExpectedStart(expectedStartDate);
        durationMiss.setActualStart(actualStartDate);
        durationMissBean.setExpectedEnd(expectedEndDate);
        durationMiss.setActualEnd(actualEndDate);
        durationMissBean.setExpectedDuration(expectedDuration);
        long actualDuration = actualEndDate.getTime() - actualStartDate.getTime();
        durationMiss.setActualDuration(actualDuration);

        ConnectionContext jmsContext = getConnectionContext();
        Session session = jmsContext.createSession(Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = jmsContext.createConsumer(session, slaListener.getTopic(durationMiss));
        slaListener.onDurationMiss(durationMiss);
        TextMessage message = (TextMessage) consumer.receive(5000);
        SLAMessage durationMissMsg = (SLAMessage) JMSMessagingUtils.getEventMessage(message);
        // check msg header
        assertEquals(EventStatus.DURATION_MISS, durationMissMsg.getEventStatus());
        assertEquals(SLAStatus.IN_PROCESS, durationMissMsg.getSLAStatus());
        assertEquals(AppType.COORDINATOR_ACTION, durationMissMsg.getAppType());
        assertEquals(MessageType.SLA, durationMissMsg.getMessageType());
        assertEquals("Test-SLA-Duration-Miss", durationMissMsg.getAppName());
        assertEquals("dummyuser", durationMissMsg.getUser());
        // check msg body
        assertEquals("0000000-000000000000001-oozie-wrkf-C@1", durationMissMsg.getId());
        assertEquals("0000000-000000000000001-oozie-wrkf-C", durationMissMsg.getParentId());
        assertEquals(expectedStartDate, durationMissMsg.getExpectedStartTime());
        assertEquals(actualStartDate, durationMissMsg.getActualStartTime());
        assertEquals(expectedEndDate, durationMissMsg.getExpectedEndTime());
        assertEquals(actualEndDate, durationMissMsg.getActualEndTime());
        assertEquals(expectedDuration, durationMissMsg.getExpectedDuration());
        assertEquals(actualDuration, durationMissMsg.getActualDuration());
        assertEquals("notification of duration miss", durationMissMsg.getNotificationMessage());
    }

    @Test
    public void testSLAJobSelectors() throws Exception {
        JMSSLAEventListener slaListener = new JMSSLAEventListener();
        slaListener.init(conf);
        String id = "0000000-000000000000001-oozie-wrkf-C@1";
        SLACalcStatus startMiss = _createSLACalcStatus(id);
        SLARegistrationBean startMissBean = startMiss.getSLARegistrationBean();
        startMiss.setId(id);
        startMissBean.setAppName("Test-SLA-Start-Miss");
        startMissBean.setAppType(AppType.COORDINATOR_ACTION);
        startMissBean.setUser("dummyuser");
        startMiss.setEventStatus(EventStatus.START_MISS);
        startMiss.setSLAStatus(SLAStatus.NOT_STARTED);
        startMiss.setMsgType(MessageType.SLA);

        ConnectionContext jmsContext = getConnectionContext();
        Session session = jmsContext.createSession(Session.AUTO_ACKNOWLEDGE);
        String selector = JMSHeaderConstants.EVENT_STATUS + "='START_MISS'";
        MessageConsumer consumer = jmsContext.createConsumer(session, slaListener.getTopic(startMiss), selector);
        slaListener.onStartMiss(startMiss);
        TextMessage message = (TextMessage) consumer.receive(5000);
        System.out.println("\n Text " + message.getText());
        SLAMessage startMissMsg = (SLAMessage) JMSMessagingUtils.getEventMessage(message);
        assertEquals("dummyuser", startMissMsg.getUser());
        assertEquals(EventStatus.START_MISS, startMissMsg.getEventStatus());
        assertEquals(MessageType.SLA, startMissMsg.getMessageType());
    }

    @Test
    public void testSLAJobSelectorsNegative() throws Exception {
        JMSSLAEventListener slaListener = new JMSSLAEventListener();
        slaListener.init(conf);
        String id = "0000000-000000000000001-oozie-wrkf-C@1";
        SLACalcStatus startMiss = _createSLACalcStatus(id);
        SLARegistrationBean startMissBean = startMiss.getSLARegistrationBean();

        startMiss.setId(id);
        startMissBean.setAppName("Test-SLA-Start-Miss");
        startMissBean.setAppType(AppType.COORDINATOR_ACTION);
        startMissBean.setUser("dummyuser");
        startMiss.setEventStatus(EventStatus.START_MISS);
        startMiss.setSLAStatus(SLAStatus.NOT_STARTED);
        startMiss.setMsgType(MessageType.SLA);

        ConnectionContext jmsContext = getConnectionContext();

        Session session = jmsContext.createSession(Session.AUTO_ACKNOWLEDGE);
        // Pass a selector which does not match and assert for null message
        String selector = JMSHeaderConstants.EVENT_STATUS + "='SLA_END_MISS'";
        MessageConsumer consumer = jmsContext.createConsumer(session, slaListener.getTopic(startMiss), selector);
        slaListener.onStartMiss(startMiss);
        TextMessage message = (TextMessage) consumer.receive(5000);
        assertNull(message);
    }

    @Test
    public void testOnSLAStartMetEvent() throws Exception {
        JMSSLAEventListener slaListener = new JMSSLAEventListener();
        slaListener.init(conf);
        String id = "0000000-000000000000001-oozie-wrkf-C@1";
        SLACalcStatus startMet = _createSLACalcStatus(id);
        SLARegistrationBean startMetBean = startMet.getSLARegistrationBean();
        Date expectedStartDate = DateUtils.parseDateUTC("2013-01-01T10:00Z");
        Date actualStartDate = DateUtils.parseDateUTC("2013-01-01T09:55Z");
        startMetBean.setAppName("Test-SLA-Start-Met");
        startMet.setEventStatus(EventStatus.START_MET);
        startMet.setSLAStatus(SLAStatus.IN_PROCESS);
        startMetBean.setAppType(AppType.COORDINATOR_ACTION);
        startMet.setId(id);
        startMetBean.setParentId("0000000-000000000000001-oozie-wrkf-C");
        startMetBean.setUser("dummyuser");
        startMetBean.setNotificationMsg("notification of start miss");
        startMetBean.setJobData("random job data");
        startMetBean.setExpectedStart(expectedStartDate);
        startMet.setActualStart(actualStartDate);

        ConnectionContext jmsContext = getConnectionContext();
        Session session = jmsContext.createSession(Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = jmsContext.createConsumer(session, slaListener.getTopic(startMet));
        slaListener.onStartMet(startMet);
        TextMessage message = (TextMessage) consumer.receive(5000);
        SLAMessage slaStartMissMsg = (SLAMessage) JMSMessagingUtils.getEventMessage(message);
        // check msg header
        assertEquals(EventStatus.START_MET, slaStartMissMsg.getEventStatus());
        assertEquals(SLAStatus.IN_PROCESS, slaStartMissMsg.getSLAStatus());
        assertEquals(AppType.COORDINATOR_ACTION, slaStartMissMsg.getAppType());
        assertEquals(MessageType.SLA, slaStartMissMsg.getMessageType());
        assertEquals("Test-SLA-Start-Met", slaStartMissMsg.getAppName());
        assertEquals("dummyuser", slaStartMissMsg.getUser());
        // check msg body
        assertEquals("0000000-000000000000001-oozie-wrkf-C@1", slaStartMissMsg.getId());
        assertEquals("0000000-000000000000001-oozie-wrkf-C", slaStartMissMsg.getParentId());
        assertEquals(expectedStartDate, slaStartMissMsg.getExpectedStartTime());
        assertEquals(actualStartDate, slaStartMissMsg.getActualStartTime());
    }

    public void testOnSLAEndMetEvent() throws Exception {
        JMSSLAEventListener slaListener = new JMSSLAEventListener();
        slaListener.init(conf);
        String id = "0000000-000000000000001-oozie-wrkf-C@1";
        SLACalcStatus endMet = _createSLACalcStatus(id);
        SLARegistrationBean endMetBean = endMet.getSLARegistrationBean();
        Date expectedEndDate = DateUtils.parseDateUTC("2013-01-01T12:00Z");
        Date actualEndDate = DateUtils.parseDateUTC("2013-01-01T11:00Z");
        endMet.setId(id);
        endMetBean.setParentId("0000000-000000000000001-oozie-wrkf-C");
        endMetBean.setAppName("Test-SLA-End-Met");
        endMet.setEventStatus(EventStatus.END_MET);
        endMet.setSLAStatus(SLAStatus.MET);
        endMetBean.setAppType(AppType.COORDINATOR_ACTION);
        endMetBean.setUser("dummyuser");
        endMetBean.setNotificationMsg("notification of end met");
        endMetBean.setExpectedEnd(expectedEndDate);
        endMet.setActualEnd(actualEndDate);

        ConnectionContext jmsContext = getConnectionContext();

        Session session = jmsContext.createSession(Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = jmsContext.createConsumer(session, slaListener.getTopic(endMet));
        slaListener.onEndMet(endMet);
        TextMessage message = (TextMessage) consumer.receive(5000);
        SLAMessage slaEndMissMsg = (SLAMessage) JMSMessagingUtils.getEventMessage(message);
        // check msg header
        assertEquals(EventStatus.END_MET, slaEndMissMsg.getEventStatus());
        assertEquals(SLAStatus.MET, slaEndMissMsg.getSLAStatus());
        assertEquals(AppType.COORDINATOR_ACTION, slaEndMissMsg.getAppType());
        assertEquals(MessageType.SLA, slaEndMissMsg.getMessageType());
        assertEquals("Test-SLA-End-Met", slaEndMissMsg.getAppName());
        assertEquals("dummyuser", slaEndMissMsg.getUser());
        // check msg body
        assertEquals("0000000-000000000000001-oozie-wrkf-C@1", slaEndMissMsg.getId());
        assertEquals("0000000-000000000000001-oozie-wrkf-C", slaEndMissMsg.getParentId());
        assertEquals(expectedEndDate, slaEndMissMsg.getExpectedEndTime());
        assertEquals(actualEndDate, slaEndMissMsg.getActualEndTime());
        assertEquals("notification of end met", slaEndMissMsg.getNotificationMessage());
    }

    public void testOnSLADurationMetEvent() throws Exception {
        JMSSLAEventListener slaListener = new JMSSLAEventListener();
        slaListener.init(conf);
        String id = "0000000-000000000000001-oozie-wrkf-C@1";
        SLACalcStatus durationMet = _createSLACalcStatus(id);
        SLARegistrationBean durationMetBean = durationMet.getSLARegistrationBean();

        Date expectedStartDate = DateUtils.parseDateUTC("2013-01-01T00:00Z");
        Date actualStartDate = DateUtils.parseDateUTC("2013-01-01T01:00Z");
        Date expectedEndDate = DateUtils.parseDateUTC("2013-01-01T12:00Z");
        Date actualEndDate = DateUtils.parseDateUTC("2013-01-01T14:00Z");
        long expectedDuration = ( expectedEndDate.getTime() - actualStartDate.getTime() ) / (1000 * 60);
        durationMet.setId(id);
        durationMetBean.setParentId("0000000-000000000000001-oozie-wrkf-C");
        durationMetBean.setAppName("Test-SLA-Duration-Met");
        durationMet.setEventStatus(EventStatus.DURATION_MET);
        durationMet.setSLAStatus(SLAStatus.MET);
        durationMetBean.setAppType(AppType.COORDINATOR_ACTION);
        durationMetBean.setUser("dummyuser");
        durationMetBean.setNotificationMsg("notification of duration met");
        durationMetBean.setExpectedStart(expectedStartDate);
        durationMet.setActualStart(actualStartDate);
        durationMetBean.setExpectedEnd(expectedEndDate);
        durationMet.setActualEnd(actualEndDate);
        durationMetBean.setExpectedDuration(expectedDuration);
        long actualDuration = actualEndDate.getTime() - actualStartDate.getTime();
        durationMet.setActualDuration(actualDuration);

        ConnectionContext jmsContext = getConnectionContext();

        Session session = jmsContext.createSession(Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = jmsContext.createConsumer(session, slaListener.getTopic(durationMet));
        slaListener.onDurationMet(durationMet);
        TextMessage message = (TextMessage) consumer.receive(5000);
        SLAMessage durationMissMsg = (SLAMessage) JMSMessagingUtils.getEventMessage(message);
        // check msg header
        assertEquals(EventStatus.DURATION_MET, durationMissMsg.getEventStatus());
        assertEquals(SLAStatus.MET, durationMissMsg.getSLAStatus());
        assertEquals(AppType.COORDINATOR_ACTION, durationMissMsg.getAppType());
        assertEquals(MessageType.SLA, durationMissMsg.getMessageType());
        assertEquals("Test-SLA-Duration-Met", durationMissMsg.getAppName());
        assertEquals("dummyuser", durationMissMsg.getUser());
        // check msg body
        assertEquals("0000000-000000000000001-oozie-wrkf-C@1", durationMissMsg.getId());
        assertEquals("0000000-000000000000001-oozie-wrkf-C", durationMissMsg.getParentId());
        assertEquals(expectedStartDate, durationMissMsg.getExpectedStartTime());
        assertEquals(actualStartDate, durationMissMsg.getActualStartTime());
        assertEquals(expectedEndDate, durationMissMsg.getExpectedEndTime());
        assertEquals(actualEndDate, durationMissMsg.getActualEndTime());
        assertEquals(expectedDuration, durationMissMsg.getExpectedDuration());
        assertEquals(actualDuration, durationMissMsg.getActualDuration());
        assertEquals("notification of duration met", durationMissMsg.getNotificationMessage());
    }
    
    private SLACalcStatus _createSLACalcStatus(String actionId) {
        SLARegistrationBean reg = new SLARegistrationBean();
        reg.setId(actionId);
        reg.setAppType(AppType.COORDINATOR_ACTION);
        return new SLACalcStatus(reg);
    }
}
