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


package org.apache.oozie.sla;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.AppType;
import org.apache.oozie.action.email.EmailActionExecutor;
import org.apache.oozie.client.event.JobEvent;
import org.apache.oozie.client.event.SLAEvent;
import org.apache.oozie.client.event.SLAEvent.EventStatus;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.SLACalcStatus;
import org.apache.oozie.sla.SLARegistrationBean;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.DateUtils;
import org.junit.After;
import org.junit.Before;

import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.ServerSetup;

import javax.mail.Message.RecipientType;
import javax.mail.internet.MimeMessage;

import org.apache.oozie.sla.listener.SLAEmailEventListener;
import org.apache.oozie.sla.listener.SLAEmailEventListener.EmailField;
import org.apache.oozie.sla.service.SLAService;

public class TestSLAEmailEventListener extends XTestCase {

    private Services services;
    private static final int SMTP_TEST_PORT = 3025;
    private GreenMail greenMail;
    private SLAEmailEventListener slaEmailListener;
    private Configuration conf = null;

    @Before
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        conf = services.getConf();
        conf.set(EmailActionExecutor.EMAIL_SMTP_HOST, "localhost");
        conf.set(EmailActionExecutor.EMAIL_SMTP_PORT, String.valueOf(SMTP_TEST_PORT));
        conf.set(EmailActionExecutor.EMAIL_SMTP_AUTH, "false");
        conf.set(EmailActionExecutor.EMAIL_SMTP_USER, "");
        conf.set(EmailActionExecutor.EMAIL_SMTP_PASS, "");
        conf.set(EmailActionExecutor.EMAIL_SMTP_FROM, "oozie@localhost");
        conf.set(SLAEmailEventListener.BLACKLIST_CACHE_TIMEOUT, "1");
        conf.set(SLAEmailEventListener.BLACKLIST_FAIL_COUNT, "2");
        conf.set(SLAService.CONF_ALERT_EVENTS, SLAEvent.EventStatus.START_MISS.name() + ","
                + SLAEvent.EventStatus.END_MISS + "," + SLAEvent.EventStatus.DURATION_MISS);

        greenMail = new GreenMail(new ServerSetup(SMTP_TEST_PORT, null, "smtp"));
        greenMail.start();
        services.init();
        slaEmailListener = new SLAEmailEventListener();
        slaEmailListener.init(conf);
    }

    @After
    @Override
    protected void tearDown() throws Exception {
        if (null != greenMail) {
            greenMail.stop();
        }
        services.destroy();
        super.tearDown();
    }

    public void testOnStartMiss() throws Exception {
        String id = "0000000-000000000000001-oozie-wrkf-C@1";
        SLACalcStatus event = _createSLACalcStatus(id);
        SLARegistrationBean eventBean = event.getSLARegistrationBean();
        Date startDate = DateUtils.parseDateUTC("2013-01-01T00:00Z");
        Date actualstartDate = DateUtils.parseDateUTC("2013-01-01T01:00Z");
        event.setEventStatus(EventStatus.START_MISS);
        event.setJobStatus(JobEvent.EventStatus.STARTED.toString());
        event.setId(id);
        eventBean.setParentId("0000000-000000000000001-oozie-wrkf-C");
        eventBean.setAppName("Test-SLA-Start-Miss");
        eventBean.setUser("dummyuser");
        eventBean.setNominalTime(startDate);
        eventBean.setExpectedStart(startDate);
        eventBean.setNotificationMsg("Notification of Missing Expected Start Time");
        eventBean.setAlertContact("alert-receiver@oozie.com");
        event.setActualStart(actualstartDate);
        eventBean.setAppType(AppType.COORDINATOR_ACTION);

        slaEmailListener.onStartMiss(event);

        MimeMessage[] msgs = greenMail.getReceivedMessages();
        MimeMessage msg = msgs[0];
        // check message header
        assertEquals(msg.getFrom()[0].toString(), "oozie@localhost");
        assertEquals(msg.getRecipients(RecipientType.TO)[0].toString(), "alert-receiver@oozie.com");
        assertEquals(msg.getSubject(), "OOZIE - SLA " + EventStatus.START_MISS
                + " (AppName=Test-SLA-Start-Miss, JobID=0000000-000000000000001-oozie-wrkf-C@1)");
        // check message body
        String msgBody = msg.getContent().toString();
        String headerSep = SLAEmailEventListener.EMAIL_BODY_HEADER_SEPARATER;
        String sep = SLAEmailEventListener.EMAIL_BODY_FIELD_SEPARATER;
        String indent = SLAEmailEventListener.EMAIL_BODY_FIELD_INDENT;
        assertTrue(msgBody.indexOf("Status" + headerSep) > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.EVENT_STATUS.toString() + sep
                + EventStatus.START_MISS.toString()) > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.JOB_STATUS.toString() + sep
                + JobEvent.EventStatus.STARTED.toString()) > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.NOTIFICATION_MESSAGE.toString() + sep
                + "Notification of Missing Expected Start Time") > -1);
        assertTrue(msgBody.indexOf("Job Details" + headerSep) > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.APP_TYPE.toString() + sep + AppType.COORDINATOR_ACTION) > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.APP_NAME.toString() + sep + "Test-SLA-Start-Miss") > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.USER.toString() + sep + "dummyuser") > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.JOBID.toString() + sep
                + "0000000-000000000000001-oozie-wrkf-C@1") > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.PARENT_JOBID.toString() + sep
                + "0000000-000000000000001-oozie-wrkf-C") > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.JOB_URL.toString() + sep
                + conf.get(SLAEmailEventListener.OOZIE_BASE_URL) + "/?job=" + "0000000-000000000000001-oozie-wrkf-C@1") > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.PARENT_JOB_URL.toString() + sep
                + conf.get(SLAEmailEventListener.OOZIE_BASE_URL) + "/?job=" + "0000000-000000000000001-oozie-wrkf-C") > -1);
        assertTrue(msgBody.indexOf("SLA Details" + headerSep) > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.NOMINAL_TIME.toString() + sep + startDate) > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.EXPECTED_START_TIME.toString() + sep + startDate) > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.ACTUAL_START_TIME.toString() + sep + actualstartDate) > -1);
    }

    public void testOnEndMiss() throws Exception {
        String id = "0000000-000000000000001-oozie-wrkf-C@1";
        SLACalcStatus event = _createSLACalcStatus(id);
        SLARegistrationBean eventBean = event.getSLARegistrationBean();
        Date expectedStartDate = DateUtils.parseDateUTC("2013-01-01T00:00Z");
        Date actualStartDate = DateUtils.parseDateUTC("2013-01-01T01:00Z");
        Date expectedEndDate = DateUtils.parseDateUTC("2013-01-01T12:00Z");
        Date actualEndDate = DateUtils.parseDateUTC("2013-01-01T13:00Z");
        event.setId(id);
        eventBean.setParentId("0000000-000000000000001-oozie-wrkf-C");
        event.setEventStatus(EventStatus.END_MISS);
        event.setJobStatus(JobEvent.EventStatus.SUCCESS.toString());
        eventBean.setAppName("Test-SLA-End-Miss");
        eventBean.setUser("dummyuser");
        eventBean.setNominalTime(expectedStartDate);
        eventBean.setExpectedStart(expectedStartDate);
        eventBean.setExpectedEnd(expectedEndDate);
        eventBean.setNotificationMsg("notification of end miss");
        eventBean.setAlertContact("alert-receiver-endmiss@oozie.com");
        eventBean.setAppType(AppType.COORDINATOR_ACTION);
        eventBean.setExpectedStart(expectedStartDate);
        eventBean.setExpectedEnd(expectedEndDate);
        event.setActualStart(actualStartDate);
        event.setActualEnd(actualEndDate);

        slaEmailListener.onEndMiss(event);

        MimeMessage[] msgs = greenMail.getReceivedMessages();
        MimeMessage msg = msgs[0];
        // check message header
        assertEquals(msg.getFrom()[0].toString(), "oozie@localhost");
        assertEquals(msg.getRecipients(RecipientType.TO)[0].toString(), "alert-receiver-endmiss@oozie.com");
        assertEquals(msg.getSubject(), "OOZIE - SLA " + EventStatus.END_MISS
                + " (AppName=Test-SLA-End-Miss, JobID=0000000-000000000000001-oozie-wrkf-C@1)");
        // check message body
        String msgBody = msg.getContent().toString();
        String headerSep = SLAEmailEventListener.EMAIL_BODY_HEADER_SEPARATER;
        String sep = SLAEmailEventListener.EMAIL_BODY_FIELD_SEPARATER;
        String indent = SLAEmailEventListener.EMAIL_BODY_FIELD_INDENT;
        assertTrue(msgBody.indexOf("Status" + headerSep) > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.EVENT_STATUS.toString() + sep + EventStatus.END_MISS.toString()) > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.JOB_STATUS.toString() + sep
                + JobEvent.EventStatus.SUCCESS.toString()) > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.NOTIFICATION_MESSAGE.toString() + sep
                + "notification of end miss") > -1);

        assertTrue(msgBody.indexOf("Job Details" + headerSep) > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.APP_TYPE.toString() + sep + AppType.COORDINATOR_ACTION) > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.APP_NAME.toString() + sep + "Test-SLA-End-Miss") > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.USER.toString() + sep + "dummyuser") > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.JOBID.toString() + sep
                + "0000000-000000000000001-oozie-wrkf-C@1") > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.PARENT_JOBID.toString() + sep
                + "0000000-000000000000001-oozie-wrkf-C") > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.JOB_URL.toString() + sep
                + conf.get(SLAEmailEventListener.OOZIE_BASE_URL) + "/?job=" + "0000000-000000000000001-oozie-wrkf-C@1") > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.PARENT_JOB_URL.toString() + sep
                + conf.get(SLAEmailEventListener.OOZIE_BASE_URL) + "/?job=" + "0000000-000000000000001-oozie-wrkf-C") > -1);

        assertTrue(msgBody.indexOf("SLA Details" + headerSep) > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.NOMINAL_TIME.toString() + sep + expectedStartDate) > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.EXPECTED_START_TIME.toString() + sep + expectedStartDate) > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.ACTUAL_START_TIME.toString() + sep + actualStartDate) > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.EXPECTED_END_TIME.toString() + sep + expectedEndDate) > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.ACTUAL_END_TIME.toString() + sep + actualEndDate) > -1);

    }

    public void testOnDurationMiss() throws Exception {
        String id = "0000000-000000000000001-oozie-wrkf-C@1";
        SLACalcStatus event = _createSLACalcStatus(id);
        SLARegistrationBean eventBean = event.getSLARegistrationBean();
        Date expectedStartDate = DateUtils.parseDateUTC("2013-01-01T00:00Z");
        Date actualStartDate = DateUtils.parseDateUTC("2013-01-01T00:10Z");
        Date expectedEndDate = DateUtils.parseDateUTC("2013-01-01T00:20Z");
        Date actualEndDate = DateUtils.parseDateUTC("2013-01-01T00:40Z");
        long expectedDuration = expectedEndDate.getTime() - expectedStartDate.getTime();
        long actualDuration = actualEndDate.getTime() - actualStartDate.getTime();
        long expectedDurationInMins = expectedDuration / 60000;
        long actualDurationInMins = actualDuration / 60000;
        event.setId(id);
        eventBean.setParentId("0000000-000000000000001-oozie-wrkf-C");
        event.setEventStatus(EventStatus.DURATION_MISS);
        event.setJobStatus(JobEvent.EventStatus.SUCCESS.toString());
        eventBean.setAppName("Test-SLA-Duration-Miss");
        eventBean.setUser("dummyuser");
        eventBean.setNominalTime(expectedStartDate);
        eventBean.setExpectedStart(expectedStartDate);
        eventBean.setExpectedEnd(expectedEndDate);
        eventBean.setNotificationMsg("notification of duration miss");
        eventBean.setAlertContact("alert-receiver-durationmiss@oozie.com");
        eventBean.setAppType(AppType.COORDINATOR_ACTION);
        eventBean.setExpectedStart(expectedStartDate);
        eventBean.setExpectedEnd(expectedEndDate);
        event.setActualStart(actualStartDate);
        event.setActualEnd(actualEndDate);
        eventBean.setExpectedDuration(expectedDuration);
        event.setActualDuration(actualDuration);

        slaEmailListener.onEndMiss(event);

        MimeMessage[] msgs = greenMail.getReceivedMessages();
        MimeMessage msg = msgs[0];
        // check message header
        assertEquals(msg.getFrom()[0].toString(), "oozie@localhost");
        assertEquals(msg.getRecipients(RecipientType.TO)[0].toString(), "alert-receiver-durationmiss@oozie.com");
        assertEquals(msg.getSubject(), "OOZIE - SLA " + EventStatus.DURATION_MISS
                + " (AppName=Test-SLA-Duration-Miss, JobID=0000000-000000000000001-oozie-wrkf-C@1)");
        // check message body
        String msgBody = msg.getContent().toString();
        String headerSep = SLAEmailEventListener.EMAIL_BODY_HEADER_SEPARATER;
        String sep = SLAEmailEventListener.EMAIL_BODY_FIELD_SEPARATER;
        String indent = SLAEmailEventListener.EMAIL_BODY_FIELD_INDENT;
        assertTrue(msgBody.indexOf("Status" + headerSep) > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.EVENT_STATUS.toString() + sep
                + EventStatus.DURATION_MISS.toString()) > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.JOB_STATUS.toString() + sep
                + JobEvent.EventStatus.SUCCESS.toString()) > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.NOTIFICATION_MESSAGE.toString() + sep
                + "notification of duration miss") > -1);

        assertTrue(msgBody.indexOf("Job Details" + headerSep) > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.APP_TYPE.toString() + sep + AppType.COORDINATOR_ACTION) > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.APP_NAME.toString() + sep + "Test-SLA-Duration-Miss") > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.USER.toString() + sep + "dummyuser") > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.JOBID.toString() + sep
                + "0000000-000000000000001-oozie-wrkf-C@1") > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.PARENT_JOBID.toString() + sep
                + "0000000-000000000000001-oozie-wrkf-C") > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.JOB_URL.toString() + sep
                + conf.get(SLAEmailEventListener.OOZIE_BASE_URL) + "/?job=" + "0000000-000000000000001-oozie-wrkf-C@1") > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.PARENT_JOB_URL.toString() + sep
                + conf.get(SLAEmailEventListener.OOZIE_BASE_URL) + "/?job=" + "0000000-000000000000001-oozie-wrkf-C") > -1);

        assertTrue(msgBody.indexOf("SLA Details" + headerSep) > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.NOMINAL_TIME.toString() + sep + expectedStartDate) > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.EXPECTED_START_TIME.toString() + sep + expectedStartDate) > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.ACTUAL_START_TIME.toString() + sep + actualStartDate) > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.EXPECTED_END_TIME.toString() + sep + expectedEndDate) > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.ACTUAL_END_TIME.toString() + sep + actualEndDate) > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.EXPECTED_DURATION.toString() + sep + expectedDurationInMins) > -1);
        assertTrue(msgBody.indexOf(indent + EmailField.ACTUAL_DURATION.toString() + sep + actualDurationInMins) > -1);
    }

    public void testUserAlertEventSetting() throws Exception {
        String id = "0000000-000000000000001-oozie-wrkf-C@1";
        SLACalcStatus event = _createSLACalcStatus(id);
        SLARegistrationBean eventBean = event.getSLARegistrationBean();
        // user choose only END MISS, thus, START_MISS email should not be sent
        eventBean.setAlertEvents(EventStatus.END_MISS.name());

        Date startDate = DateUtils.parseDateUTC("2013-01-01T00:00Z");
        Date actualstartDate = DateUtils.parseDateUTC("2013-01-01T01:00Z");
        event.setEventStatus(EventStatus.START_MISS);
        event.setId(id);
        eventBean.setAppName("Test-SLA-Start-Miss");
        eventBean.setUser("dummyuser");
        eventBean.setNominalTime(startDate);
        eventBean.setExpectedStart(startDate);
        eventBean.setAlertContact("alert-receiver@oozie.com");
        event.setActualStart(actualstartDate);
        eventBean.setAppType(AppType.COORDINATOR_ACTION);

        slaEmailListener.onStartMiss(event);

        // START_MISS should not be sent
        MimeMessage[] msgs = greenMail.getReceivedMessages();
        assertEquals(msgs.length, 0);

        // DURATION_MISS should not be sent
        event.setEventStatus(EventStatus.DURATION_MISS);
        slaEmailListener.onDurationMiss(event);
        msgs = greenMail.getReceivedMessages();
        assertEquals(msgs.length, 0);

        // END_MISS should be sent
        event.setEventStatus(EventStatus.END_MISS);
        slaEmailListener.onEndMiss(event);
        msgs = greenMail.getReceivedMessages();
        assertNotNull(msgs[0]);
    }

    public void testInvalidDestAddress() throws Exception {
        String id = "0000000-000000000000001-oozie-wrkf-C@1";
        SLACalcStatus event = _createSLACalcStatus(id);
        SLARegistrationBean eventBean = event.getSLARegistrationBean();
        Date startDate = DateUtils.parseDateUTC("2013-01-01T00:00Z");
        // set invalid address as alert contact
        eventBean.setAlertContact("invalidAddress");
        event.setEventStatus(EventStatus.START_MISS);
        event.setId(id);
        eventBean.setAppType(AppType.COORDINATOR_ACTION);
        eventBean.setAppName("Test-SLA-Start-Miss");
        eventBean.setUser("dummyuser");
        eventBean.setExpectedStart(startDate);
        eventBean.setNotificationMsg("notification of start miss");
        eventBean.setAppType(AppType.COORDINATOR_ACTION);
        event.setActualStart(DateUtils.parseDateUTC("2013-01-01T01:00Z"));

        slaEmailListener.onStartMiss(event);

        MimeMessage[] msgs = greenMail.getReceivedMessages();
        assertEquals(msgs.length, 0);
    }

    public void testNoDestAddress() throws Exception {
        String id = "0000000-000000000000001-oozie-wrkf-C@1";
        SLACalcStatus event = _createSLACalcStatus(id);
        SLARegistrationBean eventBean = event.getSLARegistrationBean();
        Date startDate = DateUtils.parseDateUTC("2013-01-01T00:00Z");
        // set empty address as alert contact
        eventBean.setAlertContact("");
        event.setEventStatus(EventStatus.START_MISS);
        event.setId(id);
        eventBean.setAppType(AppType.COORDINATOR_ACTION);
        eventBean.setAppName("Test-SLA-Start-Miss");
        eventBean.setUser("dummyuser");
        eventBean.setExpectedStart(startDate);
        eventBean.setNotificationMsg("notification of start miss");
        eventBean.setAppType(AppType.COORDINATOR_ACTION);
        event.setActualStart(DateUtils.parseDateUTC("2013-01-01T01:00Z"));

        slaEmailListener.onStartMiss(event);

        MimeMessage[] msgs = greenMail.getReceivedMessages();
        assertEquals(msgs.length, 0);
    }

    public void testMultipleDestAddress() throws Exception {
        String id = "0000000-000000000000001-oozie-wrkf-C@1";
        SLACalcStatus event = _createSLACalcStatus(id);
        SLARegistrationBean eventBean = event.getSLARegistrationBean();
        Date startDate = DateUtils.parseDateUTC("2013-01-01T00:00Z");
        // set multiple addresses as alert contact
        eventBean.setAlertContact("alert-receiver1@oozie.com, alert-receiver2@oozie.com");
        event.setEventStatus(EventStatus.START_MISS);
        event.setId(id);
        eventBean.setAppType(AppType.COORDINATOR_ACTION);
        eventBean.setAppName("Test-SLA-Start-Miss");
        eventBean.setUser("dummyuser");
        eventBean.setExpectedStart(startDate);
        eventBean.setNotificationMsg("notification of start miss");
        eventBean.setAppType(AppType.COORDINATOR_ACTION);
        event.setActualStart(DateUtils.parseDateUTC("2013-01-01T01:00Z"));

        slaEmailListener.onStartMiss(event);

        MimeMessage[] msgs = greenMail.getReceivedMessages();
        MimeMessage msg = msgs[0];
        assertEquals(msg.getFrom()[0].toString(), "oozie@localhost");
        assertEquals(msg.getRecipients(RecipientType.TO)[0].toString(), "alert-receiver1@oozie.com");
        assertEquals(msg.getRecipients(RecipientType.TO)[1].toString(), "alert-receiver2@oozie.com");
    }

    public void testBlackList() throws Exception {
        String blackListedEmail = "alert-receiver@oozie.com";
        // add email to blacklist
        slaEmailListener.addBlackList(blackListedEmail);
        String id = "0000000-000000000000001-oozie-wrkf-C@1";
        SLACalcStatus event = _createSLACalcStatus(id);
        SLARegistrationBean eventBean = event.getSLARegistrationBean();
        event.setEventStatus(EventStatus.START_MISS);
        eventBean.setAlertContact(blackListedEmail);
        eventBean.setAppType(AppType.COORDINATOR_ACTION);
        eventBean.setAppName("Test-SLA-Start-Miss");
        eventBean.setUser("dummyuser");
        eventBean.setAlertContact("alert-receiver@oozie.com");
        event.setActualStart(DateUtils.parseDateUTC("2013-01-01T01:00Z"));

        // blacklist blocks email from being sent out
        slaEmailListener.onStartMiss(event);
        MimeMessage[] msgs = greenMail.getReceivedMessages();
        assertEquals(msgs.length, 0);

        // wait 1.5 sec (cache timeout set to 1sec in test's setup)
        Thread.sleep(1500);

        // cache is evicted
        slaEmailListener.onStartMiss(event);
        msgs = greenMail.getReceivedMessages();
        assertEquals(msgs.length, 1);
    }

    private SLACalcStatus _createSLACalcStatus(String actionId) {
        SLARegistrationBean reg = new SLARegistrationBean();
        reg.setId(actionId);
        reg.setAppType(AppType.COORDINATOR_ACTION);
        return new SLACalcStatus(reg);
    }
}
