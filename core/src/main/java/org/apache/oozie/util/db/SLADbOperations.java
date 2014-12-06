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

import org.apache.oozie.ErrorCode;
import org.apache.oozie.SLAEventBean;
import org.apache.oozie.client.SLAEvent.SlaAppType;
import org.apache.oozie.client.SLAEvent.Status;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.service.Services;
import org.apache.oozie.store.Store;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;

@Deprecated
public class SLADbOperations {
    public static final String CLIENT_ID_TAG = "oozie:sla:client-id";

    public static SLAEventBean createSlaRegistrationEvent(Element eSla, Store store, String slaId, SlaAppType appType, String user,
            String groupName) throws Exception {
        if (eSla == null) {
            return null;
        }
        SLAEventBean sla = new SLAEventBean();
        sla.setAppName(getTagElement(eSla, "app-name"));
        sla.setParentClientId(getTagElement(eSla, "parent-child-id"));
        sla.setParentSlaId(getTagElement(eSla, "parent-sla-id"));
        String strNominalTime = getTagElement(eSla, "nominal-time");
        if (strNominalTime == null || strNominalTime.length() == 0) {
            throw new RuntimeException("Nominal time is required"); // TODO:
            // change to
            // CommandException
        }
        Date nominalTime = DateUtils.parseDateOozieTZ(strNominalTime);
        // Setting expected start time
        String strRelExpectedStart = getTagElement(eSla, "should-start");
        if (strRelExpectedStart != null && strRelExpectedStart.length() > 0) {
            int relExpectedStart = Integer.parseInt(strRelExpectedStart);
            if (relExpectedStart < 0) {
                sla.setExpectedStart(null);
            }
            else {
                Date expectedStart = new Date(nominalTime.getTime() + relExpectedStart * 60 * 1000);
                sla.setExpectedStart(expectedStart);
            }
        } else {
            sla.setExpectedStart(null);
        }

        // Setting expected end time
        String strRelExpectedEnd = getTagElement(eSla, "should-end");
        if (strRelExpectedEnd == null || strRelExpectedEnd.length() == 0) {
            throw new RuntimeException("should-end can't be empty");
        }
        int relExpectedEnd = Integer.parseInt(strRelExpectedEnd);
        if (relExpectedEnd < 0) {
            sla.setExpectedEnd(null);
        }
        else {
            Date expectedEnd = new Date(nominalTime.getTime() + relExpectedEnd * 60 * 1000);
            sla.setExpectedEnd(expectedEnd);
        }

        sla.setNotificationMsg(getTagElement(eSla, "notification-msg"));
        sla.setAlertContact(getTagElement(eSla, "alert-contact"));
        sla.setDevContact(getTagElement(eSla, "dev-contact"));
        sla.setQaContact(getTagElement(eSla, "qa-contact"));
        sla.setSeContact(getTagElement(eSla, "se-contact"));
        sla.setAlertFrequency(getTagElement(eSla, "alert-frequency"));
        sla.setAlertPercentage(getTagElement(eSla, "alert-percentage"));

        sla.setUpstreamApps(getTagElement(eSla, "upstream-apps"));

        // Oozie defined

        sla.setSlaId(slaId);
        sla.setAppType(appType);
        sla.setUser(user);
        sla.setGroupName(groupName);
        sla.setJobStatus(Status.CREATED);
        sla.setStatusTimestamp(new Date());

        return sla;
    }

    public static SLAEventBean createSlaRegistrationEvent(Element eSla,
                                                 String slaId, SlaAppType appType, String user, String groupName, XLog log)
            throws Exception {
        if (eSla == null) {
            return null;
        }
        SLAEventBean sla = new SLAEventBean();
        sla.setAppName(getTagElement(eSla, "app-name"));
        sla.setParentClientId(getTagElement(eSla, "parent-child-id"));
        sla.setParentSlaId(getTagElement(eSla, "parent-sla-id"));
        String strNominalTime = getTagElement(eSla, "nominal-time");
        if (strNominalTime == null || strNominalTime.length() == 0) {
            throw new RuntimeException("Nominal time is required"); // TODO:
            // change to
            // CommandException
        }
        Date nominalTime = DateUtils.parseDateOozieTZ(strNominalTime);
        // Setting expected start time
        String strRelExpectedStart = getTagElement(eSla, "should-start");
        if (strRelExpectedStart != null && strRelExpectedStart.length() > 0) {
            int relExpectedStart = Integer.parseInt(strRelExpectedStart);
            if (relExpectedStart < 0) {
                sla.setExpectedStart(null);
            }
            else {
                Date expectedStart = new Date(nominalTime.getTime() + relExpectedStart * 60 * 1000);
                sla.setExpectedStart(expectedStart);
            }
        } else {
            sla.setExpectedStart(null);
        }

        // Setting expected end time
        String strRelExpectedEnd = getTagElement(eSla, "should-end");
        if (strRelExpectedEnd == null || strRelExpectedEnd.length() == 0) {
            throw new RuntimeException("should-end can't be empty");
        }
        int relExpectedEnd = Integer.parseInt(strRelExpectedEnd);
        if (relExpectedEnd < 0) {
            sla.setExpectedEnd(null);
        }
        else {
            Date expectedEnd = new Date(nominalTime.getTime() + relExpectedEnd
                    * 60 * 1000);
            sla.setExpectedEnd(expectedEnd);
        }

        sla.setNotificationMsg(getTagElement(eSla, "notification-msg"));
        sla.setAlertContact(getTagElement(eSla, "alert-contact"));
        sla.setDevContact(getTagElement(eSla, "dev-contact"));
        sla.setQaContact(getTagElement(eSla, "qa-contact"));
        sla.setSeContact(getTagElement(eSla, "se-contact"));
        sla.setAlertFrequency(getTagElement(eSla, "alert-frequency"));
        sla.setAlertPercentage(getTagElement(eSla, "alert-percentage"));

        sla.setUpstreamApps(getTagElement(eSla, "upstream-apps"));

        // Oozie defined

        sla.setSlaId(slaId);
        sla.setAppType(appType);
        sla.setUser(user);
        sla.setGroupName(groupName);
        sla.setJobStatus(Status.CREATED);
        sla.setStatusTimestamp(new Date());

        return sla;
    }

    public static SLAEventBean createSlaStatusEvent(String id, Status status, SlaAppType appType, String appName,
            XLog log) throws Exception {
        SLAEventBean sla = new SLAEventBean();
        sla.setSlaId(id);
        sla.setJobStatus(status);
        sla.setAppType(appType);
        sla.setAppName(appName);
        sla.setStatusTimestamp(new Date());
        return sla;
    }

    public static SLAEventBean createStatusEvent(String slaXml, String id, Status stat, SlaAppType appType, XLog log)
            throws CommandException {
        if (slaXml == null || slaXml.length() == 0) {
            return null;
        }
        try {
            Element eSla = XmlUtils.parseXml(slaXml);
            Element eAppName = eSla.getChild("app-name", eSla.getNamespace());
            //stop-gap null handling till deprecated class is removed
            String appNameStr = eAppName != null ? eAppName.getText() : null;
            return createSlaStatusEvent(id, stat, appType, appNameStr, log);
        }
        catch (Exception e) {
            throw new CommandException(ErrorCode.E1007, " id " + id, e.getMessage(), e);
        }
    }

    public static String getClientId() {
        Services services = Services.get();
        if (services == null) {
            throw new RuntimeException("Services is not initialized");
        }
        String clientId = services.getConf().get(CLIENT_ID_TAG,
                                                 "oozie-default-instance"); // TODO" remove default
        if (clientId == null) {
            throw new RuntimeException(
                    "No SLA_CLIENT_ID defined in oozie-site.xml with property name "
                            + CLIENT_ID_TAG);
        }
        return clientId;
    }

    private static String getTagElement(Element elem, String tagName) {
        if (elem != null
                && elem.getChild(tagName, elem.getNamespace("sla")) != null) {
            return elem.getChild(tagName, elem.getNamespace("sla")).getText()
                    .trim();
        }
        else {
            return null;
        }
    }

}
