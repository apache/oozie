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

import java.text.ParseException;
import java.util.Date;

import org.apache.oozie.AppType;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.event.SLAEvent.EventStatus;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.SLARegistrationQueryExecutor;
import org.apache.oozie.executor.jpa.SLARegistrationQueryExecutor.SLARegQuery;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.service.SLAService;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;

public class SLAOperations {

    public static final String NOMINAL_TIME = "nominal-time";
    public static final String SHOULD_START = "should-start";
    public static final String SHOULD_END = "should-end";
    public static final String MAX_DURATION = "max-duration";
    public static final String ALERT_EVENTS = "alert-events";
    public static final String ALL_VALUE = "ALL";


    static public XLog LOG = XLog.getLog(SLAOperations.class);


    public static SLARegistrationBean createSlaRegistrationEvent(Element eSla, String jobId, String parentId,
            AppType appType, String user, String appName, XLog log, boolean rerun, boolean disableAlert)
            throws CommandException {
        if (eSla == null || !SLAService.isEnabled()) {
            log.debug("Not registering SLA for job [{0}]. Sla-Xml null OR SLAService not enabled", jobId);
            return null;
        }
        SLARegistrationBean sla = new SLARegistrationBean();

        // Setting nominal time
        String strNominalTime = getTagElement(eSla, NOMINAL_TIME);
        Date nominalTime = setNominalTime(strNominalTime, sla);

        // Setting expected start time
        String strExpectedStart = getTagElement(eSla, SHOULD_START);
        setExpectedStart(strExpectedStart, nominalTime, sla);

        // Setting expected end time
        String strExpectedEnd = getTagElement(eSla, SHOULD_END);
        setExpectedEnd(strExpectedEnd, nominalTime, sla);

        // Setting expected duration in milliseconds
        String expectedDurationStr = getTagElement(eSla, MAX_DURATION);
        setExpectedDuration(expectedDurationStr, sla);

        // Parse desired alert-types i.e. start-miss, end-miss, start-met etc..
        String alertEvents = getTagElement(eSla, ALERT_EVENTS);
        if (alertEvents != null) {
            String events[] = alertEvents.split(",");
            StringBuilder alertsStr = new StringBuilder();
            for (int i = 0; i < events.length; i++) {
                String event = events[i].trim().toUpperCase();
                try {
                    EventStatus.valueOf(event);
                }
                catch (IllegalArgumentException iae) {
                    XLog.getLog(SLAService.class).warn(
                            "Invalid value: [" + event + "]" + " for SLA Alert-event. Should be one of "
                                    + EventStatus.values() + ". Setting it to default [" + EventStatus.END_MISS.name()
                                    + "]");
                    event = EventStatus.END_MISS.name();
                }
                alertsStr.append(event).append(",");
            }
            sla.setAlertEvents(alertsStr.toString().substring(0, alertsStr.lastIndexOf(",")));
        }

        // Other sla config
        sla.setNotificationMsg(getTagElement(eSla, "notification-msg"));
        sla.setAlertContact(getTagElement(eSla, "alert-contact"));
        sla.setUpstreamApps(getTagElement(eSla, "upstream-apps"));

        //disable Alert flag in slaConfig
        if (disableAlert) {
            sla.addToSLAConfigMap(OozieClient.SLA_DISABLE_ALERT, Boolean.toString(disableAlert));
        }
        // Oozie defined
        sla.setId(jobId);
        sla.setAppType(appType);
        sla.setAppName(appName);
        sla.setUser(user);
        sla.setParentId(parentId);

        SLAService slaService = Services.get().get(SLAService.class);
        try {
            if (!rerun) {
                slaService.addRegistrationEvent(sla);
            }
            else {
                slaService.updateRegistrationEvent(sla);
            }
        }
        catch (ServiceException e) {
            throw new CommandException(ErrorCode.E1007, " id " + jobId, e.getMessage(), e);
        }

        log.debug("Job [{0}] reg for SLA. Size of Sla Xml = [{1}]", jobId, XmlUtils.prettyPrint(eSla).toString().length());
        return sla;
    }

    public static Date setNominalTime(String strNominalTime, SLARegistrationBean sla) throws CommandException {
        if (strNominalTime == null || strNominalTime.length() == 0) {
            return sla.getNominalTime();
        }
        Date nominalTime;
        try {
            nominalTime = DateUtils.parseDateOozieTZ(strNominalTime);
            sla.setNominalTime(nominalTime);
        }
        catch (ParseException pex) {
            throw new CommandException(ErrorCode.E0302, strNominalTime, pex);
        }
        return nominalTime;
    }

    public static void setExpectedStart(String strExpectedStart, Date nominalTime, SLARegistrationBean sla)
            throws CommandException {
        if (strExpectedStart != null) {
            float expectedStart = Float.parseFloat(strExpectedStart);
            if (expectedStart < 0) {
                throw new CommandException(ErrorCode.E0302, strExpectedStart, "for SLA Expected start time");
            }
            else {
                Date expectedStartTime = new Date(nominalTime.getTime() + (long) (expectedStart * 60 * 1000));
                sla.setExpectedStart(expectedStartTime);
                LOG.debug("Setting expected start to " + expectedStartTime + " for job " + sla.getId());
            }
        }
    }

    public static void setExpectedEnd(String strExpectedEnd, Date nominalTime, SLARegistrationBean sla)
            throws CommandException {
        if (strExpectedEnd != null) {
            float expectedEnd = Float.parseFloat(strExpectedEnd);
            if (expectedEnd < 0) {
                throw new CommandException(ErrorCode.E0302, strExpectedEnd, "for SLA Expected end time");
            }
            else {
                Date expectedEndTime = new Date(nominalTime.getTime() + (long) (expectedEnd * 60 * 1000));
                sla.setExpectedEnd(expectedEndTime);
                LOG.debug("Setting expected end to " + expectedEndTime + " for job " + sla.getId());

            }
        }
    }

    public static void setExpectedDuration(String expectedDurationStr, SLARegistrationBean sla) {
        if (expectedDurationStr != null && expectedDurationStr.length() > 0) {
            float expectedDuration = Float.parseFloat(expectedDurationStr);
            if (expectedDuration > 0) {
                long duration = (long) (expectedDuration * 60 * 1000);
                LOG.debug("Setting expected duration to " + duration + " for job " + sla.getId());
                sla.setExpectedDuration(duration);
            }
        }
        else if (sla.getExpectedStart() != null) {
            long duration = sla.getExpectedEnd().getTime() - sla.getExpectedStart().getTime();
            LOG.debug("Setting expected duration to " + duration + " for job " + sla.getId());
            sla.setExpectedDuration(sla.getExpectedEnd().getTime() - sla.getExpectedStart().getTime());
        }
    }

    /**
     * Retrieve registration event
     * @param jobId the jobId
     * @throws CommandException
     * @throws JPAExecutorException
     */
    public static void updateRegistrationEvent(String jobId) throws CommandException, JPAExecutorException {
        SLAService slaService = Services.get().get(SLAService.class);
        try {
            SLARegistrationBean reg = SLARegistrationQueryExecutor.getInstance().get(SLARegQuery.GET_SLA_REG_ALL, jobId);
            if (reg != null) { //handle coord rerun with different config without sla
                slaService.updateRegistrationEvent(reg);
            }
        }
        catch (ServiceException e) {
            throw new CommandException(ErrorCode.E1007, " id " + jobId, e.getMessage(), e);
        }

    }

    /*
     * parentId null
     */
    public static SLARegistrationBean createSlaRegistrationEvent(Element eSla, String jobId, AppType appType,
            String user, String appName, XLog log) throws CommandException {
        return createSlaRegistrationEvent(eSla, jobId, null, appType, user, appName, log, false);
    }

    /*
     * appName null
     */
    public static SLARegistrationBean createSlaRegistrationEvent(Element eSla, String jobId, String parentId,
            AppType appType, String user, XLog log) throws CommandException {
        return createSlaRegistrationEvent(eSla, jobId, parentId, appType, user, null, log, false);
    }

    /*
     * parentId + appName null
     */
    public static SLARegistrationBean createSlaRegistrationEvent(Element eSla, String jobId, AppType appType,
            String user, XLog log) throws CommandException {
        return createSlaRegistrationEvent(eSla, jobId, null, appType, user, null, log, false);
    }

    /*
     * default disableAlert flag
     */
    public static SLARegistrationBean createSlaRegistrationEvent(Element eSla, String jobId, String parentId,
            AppType appType, String user, String appName, XLog log, boolean rerun) throws CommandException {
        return createSlaRegistrationEvent(eSla, jobId, null, appType, user, appName, log, rerun, false);
    }

    public static String getTagElement(Element elem, String tagName) {
        if (elem != null && elem.getChild(tagName, elem.getNamespace("sla")) != null) {
            return elem.getChild(tagName, elem.getNamespace("sla")).getText().trim();
        }
        else {
            return null;
        }
    }

}
