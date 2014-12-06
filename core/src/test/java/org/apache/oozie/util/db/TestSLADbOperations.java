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

package org.apache.oozie.util.db;

import java.util.Date;
import org.apache.oozie.SLAEventBean;
import org.apache.oozie.client.SLAEvent;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;

public class TestSLADbOperations extends XTestCase {

    @SuppressWarnings("deprecation")
    public void testCreateSlaRegistrationEventMinReqFields() throws Exception {
        Date nomDate = DateUtils.parseDateOozieTZ("2014-01-01T01:01Z");
        String slaXML = " <sla:info xmlns:sla='uri:oozie:sla:0.2'>"
                + " <sla:nominal-time>" + DateUtils.formatDateOozieTZ(nomDate) + "</sla:nominal-time>"
                + " <sla:should-end>5</sla:should-end>"
                + "</sla:info>";
        Element eSla = XmlUtils.parseXml(slaXML);

        SLAEventBean regEvent =
            SLADbOperations.createSlaRegistrationEvent(eSla, null, "id1", SLAEvent.SlaAppType.WORKFLOW_JOB, "user1", "group1");
        assertEquals(SLAEvent.SlaAppType.WORKFLOW_JOB, regEvent.getAppType());
        assertEquals(new Date(nomDate.getTime() + 5 * 60 * 1000), regEvent.getExpectedEnd());
        assertEquals("group1", regEvent.getGroupName());
        assertEquals("id1", regEvent.getSlaId());
        assertEquals("user1", regEvent.getUser());

        regEvent =
            SLADbOperations.createSlaRegistrationEvent(eSla, "id1", SLAEvent.SlaAppType.WORKFLOW_JOB, "user1", "group1", null);
        assertEquals(SLAEvent.SlaAppType.WORKFLOW_JOB, regEvent.getAppType());
        assertEquals(new Date(nomDate.getTime() + 5 * 60 * 1000), regEvent.getExpectedEnd());
        assertEquals("group1", regEvent.getGroupName());
        assertEquals("id1", regEvent.getSlaId());
        assertEquals("user1", regEvent.getUser());
    }
}
