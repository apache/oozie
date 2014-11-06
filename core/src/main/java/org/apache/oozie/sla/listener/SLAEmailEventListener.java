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


package org.apache.oozie.sla.listener;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.NoSuchProviderException;
import javax.mail.SendFailedException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMessage.RecipientType;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.action.email.EmailActionExecutor;
import org.apache.oozie.action.email.EmailActionExecutor.JavaMailAuthenticator;
import org.apache.oozie.client.event.SLAEvent;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.sla.listener.SLAEventListener;
import org.apache.oozie.sla.service.SLAService;
import org.apache.oozie.util.XLog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class SLAEmailEventListener extends SLAEventListener {

    public static final String SMTP_CONNECTION_TIMEOUT = EmailActionExecutor.CONF_PREFIX + "smtp.connectiontimeout";
    public static final String SMTP_TIMEOUT = EmailActionExecutor.CONF_PREFIX + "smtp.timeout";
    public static final String BLACKLIST_CACHE_TIMEOUT = EmailActionExecutor.CONF_PREFIX + "blacklist.cachetimeout";
    public static final String BLACKLIST_FAIL_COUNT = EmailActionExecutor.CONF_PREFIX + "blacklist.failcount";
    public static final String OOZIE_BASE_URL = "oozie.base.url";
    private Session session;
    private String oozieBaseUrl;
    private InternetAddress fromAddr;
    private String ADDRESS_SEPARATOR = ",";
    private LoadingCache<String, AtomicInteger> blackList;
    private int blacklistFailCount;
    private final String BLACKLIST_CACHE_TIMEOUT_DEFAULT = "1800"; // in sec. default to 30 min
    private final String BLACKLIST_FAIL_COUNT_DEFAULT = "2"; // stop sending when fail count equals or exceeds
    private final String SMTP_HOST_DEFAULT = "localhost";
    private final String SMTP_PORT_DEFAULT = "25";
    private final boolean SMTP_AUTH_DEFAULT = false;
    private final String SMTP_SOURCE_DEFAULT = "oozie@localhost";
    private final String SMTP_CONNECTION_TIMEOUT_DEFAULT = "5000";
    private final String SMTP_TIMEOUT_DEFAULT = "5000";
    private static XLog LOG = XLog.getLog(SLAEmailEventListener.class);
    private Set<SLAEvent.EventStatus> alertEvents;
    public static String EMAIL_BODY_FIELD_SEPARATER = " - ";
    public static String EMAIL_BODY_FIELD_INDENT = "  ";
    public static String EMAIL_BODY_HEADER_SEPARATER = ":";

    public enum EmailField {
        EVENT_STATUS("SLA Status"), APP_TYPE("App Type"), APP_NAME("App Name"), USER("User"), JOBID("Job ID"), PARENT_JOBID(
                "Parent Job ID"), JOB_URL("Job URL"), PARENT_JOB_URL("Parent Job URL"), NOMINAL_TIME("Nominal Time"),
                EXPECTED_START_TIME("Expected Start Time"), ACTUAL_START_TIME("Actual Start Time"),
                EXPECTED_END_TIME("Expected End Time"), ACTUAL_END_TIME("Actual End Time"), EXPECTED_DURATION("Expected Duration (in mins)"),
                ACTUAL_DURATION("Actual Duration (in mins)"), NOTIFICATION_MESSAGE("Notification Message"), UPSTREAM_APPS("Upstream Apps"),
                JOB_STATUS("Job Status");
        private String name;

        private EmailField(String name) {
            this.name = name;
        }

        public String toString() {
            return name;
        }
    };

    @Override
    public void init(Configuration conf) throws Exception {

        oozieBaseUrl = ConfigurationService.get(conf, OOZIE_BASE_URL);
        // Get SMTP properties from the configuration used in Email Action
        String smtpHost = conf.get(EmailActionExecutor.EMAIL_SMTP_HOST, SMTP_HOST_DEFAULT);
        String smtpPort = conf.get(EmailActionExecutor.EMAIL_SMTP_PORT, SMTP_PORT_DEFAULT);
        Boolean smtpAuth = conf.getBoolean(EmailActionExecutor.EMAIL_SMTP_AUTH, SMTP_AUTH_DEFAULT);
        String smtpUser = conf.get(EmailActionExecutor.EMAIL_SMTP_USER, "");
        String smtpPassword = conf.get(EmailActionExecutor.EMAIL_SMTP_PASS, "");
        String smtpConnectTimeout = conf.get(SMTP_CONNECTION_TIMEOUT, SMTP_CONNECTION_TIMEOUT_DEFAULT);
        String smtpTimeout = conf.get(SMTP_TIMEOUT, SMTP_TIMEOUT_DEFAULT);

        int blacklistTimeOut = Integer.valueOf(conf.get(BLACKLIST_CACHE_TIMEOUT, BLACKLIST_CACHE_TIMEOUT_DEFAULT));
        blacklistFailCount = Integer.valueOf(conf.get(BLACKLIST_FAIL_COUNT, BLACKLIST_FAIL_COUNT_DEFAULT));

        // blacklist email addresses causing SendFailedException with cache timeout
        blackList = CacheBuilder.newBuilder()
                .expireAfterWrite(blacklistTimeOut, TimeUnit.SECONDS)
                .build(new CacheLoader<String, AtomicInteger>() {
                    public AtomicInteger load(String key) throws Exception {
                        return new AtomicInteger();
                    }
                });

        // Set SMTP properties
        Properties properties = new Properties();
        properties.setProperty("mail.smtp.host", smtpHost);
        properties.setProperty("mail.smtp.port", smtpPort);
        properties.setProperty("mail.smtp.auth", smtpAuth.toString());
        properties.setProperty("mail.smtp.connectiontimeout", smtpConnectTimeout);
        properties.setProperty("mail.smtp.timeout", smtpTimeout);

        try {
            fromAddr = new InternetAddress(conf.get("oozie.email.from.address", SMTP_SOURCE_DEFAULT));
        }
        catch (AddressException ae) {
            LOG.error("Bad Source Address specified in oozie.email.from.address", ae);
            throw ae;
        }

        if (!smtpAuth) {
            session = Session.getInstance(properties);
        }
        else {
            session = Session.getInstance(properties, new JavaMailAuthenticator(smtpUser, smtpPassword));
        }

        alertEvents = new HashSet<SLAEvent.EventStatus>();
        String alertEventsStr = ConfigurationService.get(conf, SLAService.CONF_ALERT_EVENTS);
        if (alertEventsStr != null) {
            String[] alertEvt = alertEventsStr.split(",", -1);
            for (String evt : alertEvt) {
                alertEvents.add(SLAEvent.EventStatus.valueOf(evt));
            }
        }
    }

    @Override
    public void destroy() {
    }

    private void sendSLAEmail(SLAEvent event) throws Exception {
        // If no address is provided, the user did not want to send an email so simply log it and do nothing
        if (event.getAlertContact() == null || event.getAlertContact().trim().length() == 0) {
            LOG.info("No destination address provided; an SLA alert email will not be sent");
        } else {
            // Create and send an email
            Message message = new MimeMessage(session);
            setMessageHeader(message, event);
            setMessageBody(message, event);
            sendEmail(message);
        }
    }

    @Override
    public void onStartMiss(SLAEvent event) {
        boolean flag = false;
        if (event.getAlertEvents() == null) {
            flag = alertEvents.contains(SLAEvent.EventStatus.START_MISS);
        }
        else if (event.getAlertEvents().contains(SLAEvent.EventStatus.START_MISS.name())) {
            flag = true;
        }

        if (flag) {
            try {
                sendSLAEmail(event);
            }
            catch (Exception e) {
                LOG.error("Failed to send StartMiss alert email", e);
            }
        }
    }

    @Override
    public void onEndMiss(SLAEvent event) {
        boolean flag = false;
        if (event.getAlertEvents() == null) {
            flag = alertEvents.contains(SLAEvent.EventStatus.END_MISS);
        }
        else if (event.getAlertEvents().contains(SLAEvent.EventStatus.END_MISS.name())) {
            flag = true;
        }

        if (flag) {
            try {
                sendSLAEmail(event);
            }
            catch (Exception e) {
                LOG.error("Failed to send EndMiss alert email", e);
            }
        }
    }

    @Override
    public void onDurationMiss(SLAEvent event) {
        boolean flag = false;
        if (event.getAlertEvents() == null) {
            flag = alertEvents.contains(SLAEvent.EventStatus.DURATION_MISS);
        }
        else if (event.getAlertEvents().contains(SLAEvent.EventStatus.DURATION_MISS.name())) {
            flag = true;
        }

        if (flag) {
            try {
                sendSLAEmail(event);
            }
            catch (Exception e) {
                LOG.error("Failed to send DurationMiss alert email", e);
            }
        }
    }

    private Address[] parseAddress(String str) {
        Address[] addrs = null;
        List<InternetAddress> addrList = new ArrayList<InternetAddress>();
        String[] emails = str.split(ADDRESS_SEPARATOR, -1);

        for (String email : emails) {
            boolean isBlackListed = false;
            AtomicInteger val = blackList.getIfPresent(email);
            if(val != null){
                isBlackListed = ( val.get() >= blacklistFailCount );
            }
            if (!isBlackListed) {
                try {
                    // turn on strict syntax check by setting 2nd argument true
                    addrList.add(new InternetAddress(email, true));
                }
                catch (AddressException ae) {
                    // simply skip bad address but do not throw exception
                    LOG.error("Skipping bad destination address: " + email, ae);
                }
            }
        }

        if (addrList.size() > 0)
            addrs = (Address[]) addrList.toArray(new InternetAddress[addrList.size()]);

        return addrs;
    }

    private void setMessageHeader(Message msg, SLAEvent event) throws MessagingException {
        Address[] from = new InternetAddress[] { fromAddr };
        Address[] to;
        StringBuilder subject = new StringBuilder();

        to = parseAddress(event.getAlertContact());
        if (to == null) {
            LOG.error("Destination address is null or invalid, stop sending SLA alert email");
            throw new IllegalArgumentException("Destination address is not specified properly");
        }
        subject.append("OOZIE - SLA ");
        subject.append(event.getEventStatus().name());
        subject.append(" (AppName=");
        subject.append(event.getAppName());
        subject.append(", JobID=");
        subject.append(event.getId());
        subject.append(")");

        try {
            msg.addFrom(from);
            msg.addRecipients(RecipientType.TO, to);
            msg.setSubject(subject.toString());
        }
        catch (MessagingException me) {
            LOG.error("Message Exception in setting message header of SLA alert email", me);
            throw me;
        }
    }

    private void setMessageBody(Message msg, SLAEvent event) throws MessagingException {
        StringBuilder body = new StringBuilder();
        printHeading(body, "Status");
        printField(body, EmailField.EVENT_STATUS.toString(), event.getEventStatus());
        printField(body, EmailField.JOB_STATUS.toString(), event.getJobStatus());
        printField(body, EmailField.NOTIFICATION_MESSAGE.toString(), event.getNotificationMsg());

        printHeading(body, "Job Details");
        printField(body, EmailField.APP_NAME.toString(), event.getAppName());
        printField(body, EmailField.APP_TYPE.toString(), event.getAppType());
        printField(body, EmailField.USER.toString(), event.getUser());
        printField(body, EmailField.JOBID.toString(), event.getId());
        printField(body, EmailField.JOB_URL.toString(), getJobLink(event.getId()));
        printField(body, EmailField.PARENT_JOBID.toString(), event.getParentId() != null ? event.getParentId() : "N/A");
        printField(body, EmailField.PARENT_JOB_URL.toString(),
                event.getParentId() != null ? getJobLink(event.getParentId()) : "N/A");
        printField(body, EmailField.UPSTREAM_APPS.toString(), event.getUpstreamApps());

        printHeading(body, "SLA Details");
        printField(body, EmailField.NOMINAL_TIME.toString(), event.getNominalTime());
        printField(body, EmailField.EXPECTED_START_TIME.toString(), event.getExpectedStart());
        printField(body, EmailField.ACTUAL_START_TIME.toString(), event.getActualStart());
        printField(body, EmailField.EXPECTED_END_TIME.toString(), event.getExpectedEnd());
        printField(body, EmailField.ACTUAL_END_TIME.toString(), event.getActualEnd());
        printField(body, EmailField.EXPECTED_DURATION.toString(), getDurationInMins(event.getExpectedDuration()));
        printField(body, EmailField.ACTUAL_DURATION.toString(), getDurationInMins(event.getActualDuration()));

        try {
            msg.setText(body.toString());
        }
        catch (MessagingException me) {
            LOG.error("Message Exception in setting message body of SLA alert email", me);
            throw me;
        }
    }

    private long getDurationInMins(long duration) {
        if (duration < 0) {
            return duration;
        }
        return duration / 60000; //Convert millis to minutes
    }

    private String getJobLink(String jobId) {
        StringBuffer url = new StringBuffer();
        String param = "/?job=";
        url.append(oozieBaseUrl);
        url.append(param);
        url.append(jobId);
        return url.toString();
    }

    private void printField(StringBuilder st, String name, Object value) {
        String lineFeed = "\n";
        if (value != null) {
            st.append(EMAIL_BODY_FIELD_INDENT);
            st.append(name);
            st.append(EMAIL_BODY_FIELD_SEPARATER);
            st.append(value);
            st.append(lineFeed);
        }
    }

    private void printHeading(StringBuilder st, String header) {
        st.append(header);
        st.append(EMAIL_BODY_HEADER_SEPARATER);
        st.append("\n");
    }

    private void sendEmail(Message message) throws MessagingException {
        try {
            Transport.send(message);
        }
        catch (NoSuchProviderException se) {
            LOG.error("Could not find an SMTP transport provider to email", se);
            throw se;
        }
        catch (MessagingException me) {
            LOG.error("Message Exception in transporting SLA alert email", me);
            if (me instanceof SendFailedException) {
                Address[] invalidAddrs = ((SendFailedException) me).getInvalidAddresses();
                if (invalidAddrs != null && invalidAddrs.length > 0) {
                    for (Address addr : invalidAddrs) {
                        try {
                            // 'get' method loads key into cache when it doesn't exist
                            AtomicInteger val = blackList.get(addr.toString());
                            val.incrementAndGet();
                        }
                        catch (Exception e) {
                            LOG.debug("blacklist loading throwed exception");
                        }
                    }
                }
            }
            throw me;
        }
    }

    @VisibleForTesting
    public void addBlackList(String email) throws Exception {
        // this is for testing
        if(email == null || email.equals("")){
            return;
        }
        AtomicInteger val = blackList.get(email);
        val.set(blacklistFailCount);
    }

    @Override
    public void onStartMet(SLAEvent work) {
    }

    @Override
    public void onEndMet(SLAEvent work) {
    }

    @Override
    public void onDurationMet(SLAEvent work) {
    }

}
