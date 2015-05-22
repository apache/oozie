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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.io.Writer;

import javax.mail.BodyPart;
import javax.mail.Multipart;
import javax.mail.internet.MimeMessage;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.action.hadoop.ActionExecutorTestCase;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.JDOMException;

import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.GreenMailUtil;

public class TestEmailActionExecutor extends ActionExecutorTestCase {

    // GreenMail helps unit test with functional in-memory mail servers.
    GreenMail server;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        server = new GreenMail();
        server.start();
    }

    private Context createNormalContext(String actionXml) throws Exception {
        EmailActionExecutor ae = new EmailActionExecutor();

        Services.get().getConf().setInt("oozie.email.smtp.port", server.getSmtp().getPort());
        // Use default host 'localhost'. Hence, do not set the smtp host.
        // Services.get().getConf().set("oozie.email.smtp.host", "localhost");
        // Use default from address, 'oozie@localhost'.
        // Hence, do not set the from address conf.
        // Services.get().getConf().set("oozie.email.from.address", "oozie@localhost");

        // Disable auth tests by default.
        Services.get().getConf().setBoolean("oozie.email.smtp.auth", false);
        Services.get().getConf().set("oozie.email.smtp.username", "");
        Services.get().getConf().set("oozie.email.smtp.password", "");

        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());


        WorkflowJobBean wf = createBaseWorkflow(protoConf, "email-action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());
        action.setConf(actionXml);

        return new Context(wf, action);
    }

    private Context createAuthContext(String actionXml) throws Exception {
        Context ctx = createNormalContext(actionXml);

        // Override and enable auth.
        Services.get().getConf().setBoolean("oozie.email.smtp.auth", true);
        Services.get().getConf().set("oozie.email.smtp.username", "oozie@localhost");
        Services.get().getConf().set("oozie.email.smtp.password", "oozie");
        return ctx;
    }

    private Element prepareEmailElement(Boolean ccs) throws JDOMException {
        StringBuilder elem = new StringBuilder();
        elem.append("<email xmlns=\"uri:oozie:email-action:0.1\">");
        elem.append("<to>    abc@oozie.com, def@oozie.com    </to>");
        if (ccs) {
            elem.append("<cc>ghi@oozie.com,jkl@oozie.com</cc>");
        }
        elem.append("<subject>sub</subject>");
        elem.append("<body>bod</body>");
        elem.append("</email>");
        return XmlUtils.parseXml(elem.toString());
    }

    private Element prepareBadElement(String elem) throws JDOMException {
        Element good = prepareEmailElement(true);
        good.getChild("email").addContent(new Element(elem));
        return good;
    }

    public void testSetupMethods() {
        EmailActionExecutor email = new EmailActionExecutor();
        assertEquals("email", email.getType());
    }

    public void testDoNormalEmail() throws Exception {
        EmailActionExecutor email = new EmailActionExecutor();
        email.validateAndMail(createNormalContext("email-action"), prepareEmailElement(false));
        assertEquals("bod", GreenMailUtil.getBody(server.getReceivedMessages()[0]));
    }

    public void testDoAuthEmail() throws Exception {
        EmailActionExecutor email = new EmailActionExecutor();
        email.validateAndMail(createAuthContext("email-action"), prepareEmailElement(true));
        assertEquals("bod", GreenMailUtil.getBody(server.getReceivedMessages()[0]));
    }

    public void testValidation() throws Exception {
        EmailActionExecutor email = new EmailActionExecutor();

        Context ctx = createNormalContext("email-action");

        // Multiple <to>s
        try {
            email.validateAndMail(ctx, prepareBadElement("to"));
            fail();
        } catch (Exception e) {
            // Validation succeeded.
        }

        // Multiple <cc>s
        try {
            email.validateAndMail(ctx, prepareBadElement("cc"));
            fail();
        } catch (Exception e) {
            // Validation succeeded.
        }

        // Multiple <subject>s
        try {
            email.validateAndMail(ctx, prepareBadElement("subject"));
            fail();
        } catch (Exception e) {
            // Validation succeeded.
        }

        // Multiple <body>s
        try {
            email.validateAndMail(ctx, prepareBadElement("body"));
            fail();
        } catch (Exception e) {
            // Validation succeeded.
        }
    }

    public void testContentTypeDefault() throws Exception {
        EmailActionExecutor email = new EmailActionExecutor();
        email.validateAndMail(createAuthContext("email-action"), prepareEmailElement(true));
        assertEquals("bod", GreenMailUtil.getBody(server.getReceivedMessages()[0]));
        assertTrue(server.getReceivedMessages()[0].getContentType().contains("text/plain"));
    }

    public void testContentType() throws Exception {
        StringBuilder elem = new StringBuilder();
        elem.append("<email xmlns=\"uri:oozie:email-action:0.2\">");
        elem.append("<to>purushah@yahoo-inc.com</to>");
        elem.append("<subject>sub</subject>");
        elem.append("<content_type>text/html</content_type>");
        elem.append("<body>&lt;body&gt; This is a test mail &lt;/body&gt;</body>");
        elem.append("</email>");
        EmailActionExecutor emailContnetType = new EmailActionExecutor();
        emailContnetType.validateAndMail(createAuthContext("email-action"), XmlUtils.parseXml(elem.toString()));
        assertEquals("<body> This is a test mail </body>", GreenMailUtil.getBody(server.getReceivedMessages()[0]));
        assertTrue(server.getReceivedMessages()[0].getContentType().contains("text/html"));
    }

    public void testLocalFileAttachmentError() throws Exception {
        File attachFile1 = new File(getTestCaseDir() + File.separator + "attachment1.txt");
        String content1 = "this is attachment content in file1";
        File attachFile2 = new File(getTestCaseDir() + File.separator + "attachment2.txt");
        String content2 = "this is attachment content in file2";
        BufferedWriter output = new BufferedWriter(new FileWriter(attachFile1));
        output.write(content1);
        output.close();
        output = new BufferedWriter(new FileWriter(attachFile2));
        output.write(content2);
        output.close();
        StringBuilder tag = new StringBuilder();
        tag.append("file://").append(attachFile1.getAbsolutePath()).append(",file://")
                .append(attachFile2.getAbsolutePath());
        // local file not attached to email (for security reason)
        try{
            assertAttachment(tag.toString(), 0, content1, content2);
            fail();
        }catch (ActionExecutorException e){
            assertEquals("EM008", e.getErrorCode());
        }
    }

    public void testHDFSFileAttachment() throws Exception {
        String file1 = "file1";
        Path path1 = new Path(getFsTestCaseDir(), file1);
        String content1 = "this is attachment content in file1";
        String file2 = "file2";
        Path path2 = new Path(getFsTestCaseDir(), file2);
        String content2 = "this is attachment content in file2";
        FileSystem fs = getFileSystem();
        Writer writer = new OutputStreamWriter(fs.create(path1, true));
        writer.write(content1);
        writer.close();
        writer = new OutputStreamWriter(fs.create(path2, true));
        writer.write(content2);
        writer.close();
        StringBuilder tag = new StringBuilder();
        tag.append(path1.toString()).append(",").append(path2.toString());
        assertAttachment(tag.toString(), 2, content1, content2);

        //test case when email attachment support set to false
        ConfigurationService.setBoolean(EmailActionExecutor.EMAIL_ATTACHMENT_ENABLED, false);
        sendAndReceiveEmail(tag.toString());
        MimeMessage retMeg = server.getReceivedMessages()[1];
        String msgBody = GreenMailUtil.getBody(retMeg);
        assertEquals(msgBody.indexOf("This is a test mail"), 0);
        assertNotSame(msgBody.indexOf(EmailActionExecutor.EMAIL_ATTACHMENT_ERROR_MSG),-1);
        // email content is not multi-part since not attaching files
        assertFalse(retMeg.getContent() instanceof Multipart);
        assertTrue(retMeg.getContentType().contains("text/plain"));
    }

    private void assertAttachment(String attachtag, int attachCount, String content1, String content2) throws Exception {
        sendAndReceiveEmail(attachtag);
        MimeMessage retMeg = server.getReceivedMessages()[0];
        Multipart retParts = (Multipart) (retMeg.getContent());
        int numAttach = 0;
        for (int i = 0; i < retParts.getCount(); i++) {
            BodyPart bp = retParts.getBodyPart(i);
            String disp = bp.getDisposition();
            String retValue = IOUtils.toString(bp.getInputStream());
            if (disp != null && (disp.equals(BodyPart.ATTACHMENT))) {
                assertTrue(retValue.equals(content1) || retValue.equals(content2));
                numAttach++;
            }
            else {
                assertEquals("This is a test mail", retValue);
            }
        }
        assertEquals(attachCount, numAttach);
    }

    private void sendAndReceiveEmail(String attachtag) throws Exception {
        StringBuilder elem = new StringBuilder();
        elem.append("<email xmlns=\"uri:oozie:email-action:0.2\">");
        elem.append("<to>oozie@yahoo-inc.com</to>");
        elem.append("<subject>sub</subject>");
        elem.append("<body>This is a test mail</body>");
        elem.append("<attachment>").append(attachtag).append("</attachment>");
        elem.append("</email>");
        EmailActionExecutor emailExecutor = new EmailActionExecutor();
        emailExecutor.validateAndMail(createAuthContext("email-action"), XmlUtils.parseXml(elem.toString()));
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        server.stop();
    }
}
