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

import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.hadoop.ActionExecutorTestCase;
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

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        server.stop();
    }
}
