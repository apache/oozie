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

package org.apache.oozie;

import java.util.Date;

import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;

@Deprecated
public class TestSLAEventBean extends XTestCase {

    private static final Date ee = new Date(1L);
    private static final Date es = new Date(2L);
    private static final Date st = new Date(3L);

    private void set(SLAEventBean bean) {
        bean.setAlertContact("ac");
        bean.setAlertFrequency("af");
        bean.setAlertPercentage("ap");
        bean.setAppName("an");
        bean.setAppType(org.apache.oozie.client.SLAEvent.SlaAppType.WORKFLOW_ACTION);
        bean.setAppTypeStr("WORKFLOW_ACTION");
        bean.setDevContact("dc");
        bean.setEvent_id(1L);
        bean.setEventType("et");
        bean.setExpectedEnd(ee);
        bean.setExpectedStart(es);
        bean.setGroupName("gn");
        bean.setJobData("jd");
        bean.setJobStatus(org.apache.oozie.client.SLAEvent.Status.STARTED);
        bean.setJobStatusStr("STARTED");
        bean.setNotificationMsg("nm");
        bean.setParentClientId("pci");
        bean.setParentSlaId("psi");
        bean.setQaContact("qc");
        bean.setSeContact("sc");
        bean.setSlaId("si");
        bean.setStatusTimestamp(st);
        bean.setUpstreamApps("ua");
        bean.setUser("u");
    }

    public void testSetGet() {
        final SLAEventBean bean = new SLAEventBean();

        set(bean);

        assertEquals("ac", bean.getAlertContact());
        assertEquals("af", bean.getAlertFrequency());
        assertEquals("ap", bean.getAlertPercentage());
        assertEquals("an", bean.getAppName());
        assertTrue(org.apache.oozie.client.SLAEvent.SlaAppType.WORKFLOW_ACTION
            == bean.getAppType());
        assertEquals("WORKFLOW_ACTION", bean.getAppTypeStr());
        assertEquals("dc", bean.getDevContact());
        assertEquals(1L, bean.getEvent_id());
        assertEquals("et", bean.getEventType());
        assertEquals("gn", bean.getGroupName());
        assertEquals("jd", bean.getJobData());
        assertEquals("STARTED", bean.getJobStatusStr());
        assertEquals("nm", bean.getNotificationMsg());
        assertEquals("pci", bean.getParentClientId());
        assertEquals("psi", bean.getParentSlaId());
        assertEquals("qc", bean.getQaContact());
        assertEquals("sc", bean.getSeContact());
        assertEquals("si", bean.getSlaId());
        assertEquals("ua", bean.getUpstreamApps());
        assertEquals("u", bean.getUser());

        assertEquals(ee, bean.getExpectedEnd());
        assertEquals(es, bean.getExpectedStart());
        assertEquals(st, bean.getStatusTimestamp());

        assertEquals(DateUtils.convertDateToTimestamp(st), bean.getStatusTimestampTS());
        assertEquals(DateUtils.convertDateToTimestamp(ee), bean.getExpectedEndTS());
        assertEquals(DateUtils.convertDateToTimestamp(es), bean.getExpectedStartTS());
    }

    public void testToXmlStatusEvent() {
        final SLAEventBean bean = new SLAEventBean();
        set(bean);
        Element el = bean.toXml();
        String actualXml = XmlUtils.prettyPrint(el).toString();
        assertEquals("<event>\r\n" + "  <sequence-id>1</sequence-id>\r\n" + "  <status>\r\n" + "    <sla-id>si</sla-id>\r\n"
                + "    <status-timestamp>1970-01-01T00:00Z</status-timestamp>\r\n" + "    <job-status>STARTED</job-status>\r\n"
                + "    <job-data>jd</job-data>\r\n" + "    <user>u</user>\r\n" + "    <group>gn</group>\r\n"
                + "    <app-name>an</app-name>\r\n" + "  </status>\r\n" + "</event>", actualXml);
    }

    public void testToXmlRegistrationEvent() {
        final SLAEventBean bean = new SLAEventBean();
        set(bean);
        // Set "CREATED" status to get the event of registration kind:
        bean.setJobStatus(org.apache.oozie.client.SLAEvent.Status.CREATED);
        Element el = bean.toXml();
        String actualXml = XmlUtils.prettyPrint(el).toString();
        System.out.println(actualXml);
        assertEquals("<event>\r\n" + "  <sequence-id>1</sequence-id>\r\n" + "  <registration>\r\n" + "    <sla-id>si</sla-id>\r\n"
                + "    <app-type>WORKFLOW_ACTION</app-type>\r\n" + "    <app-name>an</app-name>\r\n" + "    <user>u</user>\r\n"
                + "    <group>gn</group>\r\n" + "    <parent-sla-id>psi</parent-sla-id>\r\n"
                + "    <expected-start>1970-01-01T00:00Z</expected-start>\r\n"
                + "    <expected-end>1970-01-01T00:00Z</expected-end>\r\n"
                + "    <status-timestamp>1970-01-01T00:00Z</status-timestamp>\r\n"
                + "    <notification-msg>nm</notification-msg>\r\n" + "    <alert-contact>ac</alert-contact>\r\n"
                + "    <dev-contact>dc</dev-contact>\r\n" + "    <qa-contact>qc</qa-contact>\r\n"
                + "    <se-contact>sc</se-contact>\r\n" + "    <alert-percentage>ap</alert-percentage>\r\n"
                + "    <alert-frequency>af</alert-frequency>\r\n" + "    <upstream-apps>ua</upstream-apps>\r\n"
                + "    <job-status>CREATED</job-status>\r\n" + "    <job-data>jd</job-data>\r\n" + "  </registration>\r\n"
                + "</event>", actualXml);
    }

}
