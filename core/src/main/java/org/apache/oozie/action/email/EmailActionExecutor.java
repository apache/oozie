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

package org.apache.oozie.action.email;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.Message.RecipientType;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.NoSuchProviderException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.action.ActionExecutorException.ErrorType;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom2.Element;
import org.jdom2.Namespace;

/**
 * Email action executor. It takes to, cc, bcc addresses along with a subject and body and sends
 * out an email.
 */
public class EmailActionExecutor extends ActionExecutor {

    public static final String CONF_PREFIX = "oozie.email.";
    public static final String EMAIL_SMTP_HOST = CONF_PREFIX + "smtp.host";
    public static final String EMAIL_SMTP_SOCKET = CONF_PREFIX + "smtp.socketFactory.class";
    public static final String EMAIL_SMTP_SOCKET_PORT = CONF_PREFIX + "smtp.socketFactory.port";
    public static final String EMAIL_SMTP_SOCKET_FALLBACK = CONF_PREFIX + "smtp.socketFactory.fallback";
    public static final String EMAIL_SMTP_PORT = CONF_PREFIX + "smtp.port";
    public static final String EMAIL_SMTP_AUTH = CONF_PREFIX + "smtp.auth";
    public static final String EMAIL_SMTP_USER = CONF_PREFIX + "smtp.username";
    public static final String EMAIL_SMTP_PASS = CONF_PREFIX + "smtp.password";
    public static final String EMAIL_SMTP_FROM = CONF_PREFIX + "from.address";
    public static final String EMAIL_SMTP_STARTTLS = CONF_PREFIX + "smtp.starttls.enable";
    public static final String EMAIL_SMTP_SOCKET_TIMEOUT_MS = CONF_PREFIX + "smtp.socket.timeout.ms";
    public static final String EMAIL_ATTACHMENT_ENABLED = CONF_PREFIX + "attachment.enabled";

    private final static String TO = "to";
    private final static String CC = "cc";
    private final static String BCC = "bcc";
    private final static String SUB = "subject";
    private final static String BOD = "body";
    private final static String ATTACHMENT = "attachment";
    private final static String COMMA = ",";
    private final static String CONTENT_TYPE = "content_type";

    private final static String DEFAULT_CONTENT_TYPE = "text/plain";
    private final XLog LOG = XLog.getLog(getClass());
    public static final String EMAIL_ATTACHMENT_ERROR_MSG =
            "\n Note: This email is missing configured email attachments "
            + "as sending attachments in email action is disabled in the Oozie server. "
            + "It could be for security compliance with data protection or other reasons";

    public EmailActionExecutor() {
        super("email");
    }

    @Override
    public void initActionType() {
        super.initActionType();
    }

    @Override
    public void start(Context context, WorkflowAction action) throws ActionExecutorException {
        LOG.info("Starting action");
        try {
            context.setStartData("-", "-", "-");
            Element actionXml = XmlUtils.parseXml(action.getConf());
            validateAndMail(context, actionXml);
            context.setExecutionData("OK", null);
        }
        catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    protected void validateAndMail(Context context, Element element) throws ActionExecutorException {
        // The XSD does the min/max occurrence validation for us.
        Namespace ns = element.getNamespace();
        String tos[] = new String[0];
        String ccs[] = new String[0];
        String bccs[] ;
        String subject = "";
        String body = "";
        String attachments[] = new String[0];
        String contentType;
        Element child = null;

        // <to> - One ought to exist.
        String text = element.getChildTextTrim(TO, ns);
        if (text.isEmpty()) {
            throw new ActionExecutorException(ErrorType.ERROR, "EM001", "No recipients were specified in the to-address field.");
        }
        tos = text.split(COMMA);

        // <cc> - Optional, but only one ought to exist.
        try {
            ccs = element.getChildTextTrim(CC, ns).split(COMMA);
        } catch (Exception e) {
            // It is alright for cc to be given empty or not be present.
            ccs = new String[0];
        }

        // <bcc> - Optional, but only one ought to exist.
        try {
            bccs = element.getChildTextTrim(BCC, ns).split(COMMA);
        } catch (Exception e) {
            // It is alright for bcc to be given empty or not be present.
            bccs = new String[0];
        }
        // <subject> - One ought to exist.
        subject = element.getChildTextTrim(SUB, ns);

        // <body> - One ought to exist.
        body = element.getChildTextTrim(BOD, ns);

        // <attachment> - Optional
        String attachment = element.getChildTextTrim(ATTACHMENT, ns);
        if(attachment != null) {
            attachments = attachment.split(COMMA);
        }

        contentType = element.getChildTextTrim(CONTENT_TYPE, ns);
        if (contentType == null || contentType.isEmpty()) {
            contentType = DEFAULT_CONTENT_TYPE;
        }

        // All good - lets try to mail!
        email(tos, ccs, bccs, subject, body, attachments, contentType, context.getWorkflow().getUser());
    }

    public void email(String[] to, String[] cc, String subject, String body, String[] attachments,
                      String contentType, String user) throws ActionExecutorException {
        email(to, cc, new String[0], subject, body, attachments, contentType, user);
    }

    public void email(String[] to, String[] cc, String[] bcc, String subject, String body, String[] attachments,
                      String contentType, String user) throws ActionExecutorException {
        // Get mailing server details.
        String smtpHost = ConfigurationService.get(EMAIL_SMTP_HOST);
        Integer smtpPortInt = ConfigurationService.getInt(EMAIL_SMTP_PORT);
        Integer smtpSocketPortInt = ConfigurationService.getInt(EMAIL_SMTP_SOCKET_PORT);
        Boolean smtpAuthBool = ConfigurationService.getBoolean(EMAIL_SMTP_AUTH);
        String smtpUser = ConfigurationService.get(EMAIL_SMTP_USER);
        String smtpPassword = ConfigurationService.getPassword(EMAIL_SMTP_PASS, "");
        Boolean smtpStarttlsBool = ConfigurationService.getBoolean(EMAIL_SMTP_STARTTLS);
        String smtpSocketClass = ConfigurationService.get(EMAIL_SMTP_SOCKET);
        Boolean smtpSocketFallbackBool = ConfigurationService.getBoolean(EMAIL_SMTP_SOCKET_FALLBACK);
        String fromAddr = ConfigurationService.get(EMAIL_SMTP_FROM);
        Integer timeoutMillisInt = ConfigurationService.getInt(EMAIL_SMTP_SOCKET_TIMEOUT_MS);

        Properties properties = new Properties();
        properties.setProperty("mail.smtp.host", smtpHost);
        properties.setProperty("mail.smtp.port", smtpPortInt.toString());
        properties.setProperty("mail.smtp.auth", smtpAuthBool.toString());
        properties.setProperty("mail.smtp.starttls.enable", smtpStarttlsBool.toString());
        properties.setProperty("mail.smtp.socketFactory.class", smtpSocketClass);
        properties.setProperty("mail.smtp.socketFactory.port", smtpSocketPortInt.toString());
        properties.setProperty("mail.smtp.socketFactory.fallback", smtpSocketFallbackBool.toString());

        // Apply sensible timeouts, as defaults are infinite. See https://s.apache.org/javax-mail-timeouts
        properties.setProperty("mail.smtp.connectiontimeout", timeoutMillisInt.toString());
        properties.setProperty("mail.smtp.timeout", timeoutMillisInt.toString());
        properties.setProperty("mail.smtp.writetimeout", timeoutMillisInt.toString());

        Session session;
        // Do not use default instance (i.e. Session.getDefaultInstance)
        // (cause it may lead to issues when used second time).
        if (!smtpAuthBool) {
            session = Session.getInstance(properties);
        } else {
            session = Session.getInstance(properties, new JavaMailAuthenticator(smtpUser, smtpPassword));
        }

        Message message = new MimeMessage(session);
        InternetAddress from;
        List<InternetAddress> toAddrs = new ArrayList<InternetAddress>(to.length);
        List<InternetAddress> ccAddrs = new ArrayList<InternetAddress>(cc.length);
        List<InternetAddress> bccAddrs = new ArrayList<InternetAddress>(bcc.length);

        try {
            from = new InternetAddress(fromAddr);
            message.setFrom(from);
        } catch (AddressException e) {
            throw new ActionExecutorException(ErrorType.ERROR, "EM002",
                    "Bad from address specified in ${oozie.email.from.address}.", e);
        } catch (MessagingException e) {
            throw new ActionExecutorException(ErrorType.ERROR, "EM003",
                    "Error setting a from address in the message.", e);
        }

        try {
            // Add all <to>
            for (String toStr : to) {
                toAddrs.add(new InternetAddress(toStr.trim()));
            }
            message.addRecipients(RecipientType.TO, toAddrs.toArray(new InternetAddress[0]));

            // Add all <cc>
            for (String ccStr : cc) {
                ccAddrs.add(new InternetAddress(ccStr.trim()));
            }
            message.addRecipients(RecipientType.CC, ccAddrs.toArray(new InternetAddress[0]));

            // Add all <bcc>
            for (String bccStr : bcc) {
                bccAddrs.add(new InternetAddress(bccStr.trim()));
            }
            message.addRecipients(RecipientType.BCC, bccAddrs.toArray(new InternetAddress[0]));

            // Set subject
            message.setSubject(subject);

            // when there is attachment
            if (attachments != null && attachments.length > 0 && ConfigurationService.getBoolean(EMAIL_ATTACHMENT_ENABLED)) {
                Multipart multipart = new MimeMultipart();

                // Set body text/content
                MimeBodyPart bodyPart = new MimeBodyPart();
                bodyPart.setContent(body, contentType);
                multipart.addBodyPart(bodyPart);

                for (String attachment : attachments) {
                    URI attachUri = new URI(attachment);
                    if (attachUri.getScheme() != null && attachUri.getScheme().equals("file")) {
                        throw new ActionExecutorException(ErrorType.ERROR, "EM008",
                                "Encountered an error when attaching a file. A local file cannot be attached:"
                                        + attachment);
                    }
                    MimeBodyPart messageBodyPart = new MimeBodyPart();
                    DataSource source = new URIDataSource(attachUri, user);
                    messageBodyPart.setDataHandler(new DataHandler(source));
                    messageBodyPart.setFileName(new File(attachment).getName());
                    multipart.addBodyPart(messageBodyPart);
                }
                message.setContent(multipart);
            }
            else {
                if (attachments != null && attachments.length > 0 && !ConfigurationService.getBoolean(EMAIL_ATTACHMENT_ENABLED)) {
                    body = body + EMAIL_ATTACHMENT_ERROR_MSG;
                }
                message.setContent(body, contentType);
            }
            message.saveChanges();
        }
        catch (AddressException e) {
            throw new ActionExecutorException(ErrorType.ERROR, "EM004", "Bad address format in <to> or <cc> or <bcc>.", e);
        }
        catch (MessagingException e) {
            throw new ActionExecutorException(ErrorType.ERROR, "EM005", "An error occurred while adding recipients.", e);
        }
        catch (URISyntaxException e) {
            throw new ActionExecutorException(ErrorType.ERROR, "EM008", "Encountered an error when attaching a file", e);
        }
        catch (HadoopAccessorException e) {
            throw new ActionExecutorException(ErrorType.ERROR, "EM008", "Encountered an error when attaching a file", e);
        }

        try {
            // Send over SMTP Transport
            // (Session+Message has adequate details.)
            Transport.send(message);
        } catch (NoSuchProviderException e) {
            throw new ActionExecutorException(ErrorType.ERROR, "EM006",
                    "Could not find an SMTP transport provider to email.", e);
        } catch (MessagingException e) {
            throw new ActionExecutorException(ErrorType.ERROR, "EM007",
                    "Encountered an error while sending the email message over SMTP.", e);
        }
        LOG.info("Email sent to [{0}]", to);
    }

    @Override
    public void end(Context context, WorkflowAction action) throws ActionExecutorException {
        String externalStatus = action.getExternalStatus();
        WorkflowAction.Status status = externalStatus.equals("OK") ? WorkflowAction.Status.OK :
                                       WorkflowAction.Status.ERROR;
        context.setEndData(status, getActionSignal(status));
        LOG.info("Action ended with external status [{0}]", action.getExternalStatus());
    }

    @Override
    public void check(Context context, WorkflowAction action)
            throws ActionExecutorException {

    }

    @Override
    public void kill(Context context, WorkflowAction action)
            throws ActionExecutorException {

    }

    @Override
    public boolean isCompleted(String externalStatus) {
        return true;
    }

    public static class JavaMailAuthenticator extends Authenticator {

        String user;
        String password;

        public JavaMailAuthenticator(String user, String password) {
            this.user = user;
            this.password = password;
        }

        @Override
        protected PasswordAuthentication getPasswordAuthentication() {
           return new PasswordAuthentication(user, password);
        }
    }

    class URIDataSource implements DataSource{

        HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
        FileSystem fs;
        URI uri;
        public URIDataSource(URI uri, String user) throws HadoopAccessorException {
            this.uri = uri;
            Configuration fsConf = has.createConfiguration(uri.getAuthority());
            fs = has.createFileSystem(user, uri, fsConf);
        }

        @Override
        public InputStream getInputStream() throws IOException {
            return fs.open(new Path(uri));
        }

        @Override
        public OutputStream getOutputStream() throws IOException {
            return fs.create(new Path(uri));
        }

        @Override
        public String getContentType() {
            return "application/octet-stream";
        }

        @Override
        public String getName() {
            return uri.getPath();
        }
    }
}
