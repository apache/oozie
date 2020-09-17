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

package org.apache.oozie.command.wf;

import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.NotificationXCommand;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.EmbeddedServletContainer;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.workflow.WorkflowInstance;
import org.junit.Assert;
import org.mockito.Mockito;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class TestWorkflowNotificationXCommand extends XTestCase {
    private EmbeddedServletContainer container;
    private CallbackServlet callbackServlet;

    @SuppressWarnings("serial")
    public static class CallbackServlet extends HttpServlet {
        String jobID = null;
        String nodeName = null;
        String status = null;
        String parentID = null;
        final ReentrantLock lock = new ReentrantLock();
        final Condition updated = lock.newCondition();
        boolean requestProcessed = false;

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            jobID = req.getParameter("jobId");
            nodeName = req.getParameter("nodeName");
            status = req.getParameter("status");
            parentID = req.getParameter("parentId");
            resp.setStatus(HttpServletResponse.SC_OK);

            lock.lock();
            try {
                requestProcessed = true;
                updated.signalAll();
            } finally {
                lock.unlock();
            }
        }

        public void waitUntilRequestProcessed() throws InterruptedException {
            lock.lock();
            try {
                while (!requestProcessed) {
                    updated.await(10, TimeUnit.SECONDS);
                }
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        setSystemProperty(NotificationXCommand.NOTIFICATION_URL_CONNECTION_TIMEOUT_KEY, "50");
        Services services = new Services();
        services.init();
        container = new EmbeddedServletContainer("blah");
        container.addServletEndpoint("/hang/*", HangServlet.class);
        callbackServlet = new CallbackServlet();
        container.addServletEndpoint("/callback/*", callbackServlet);
        container.start();
    }

    @Override
    public void tearDown() throws Exception {
        try {
            container.stop();
        }
        catch (Exception ex) {
        }
        try {
            Services.get().destroy();
        }
        catch (Exception ex) {
        }
        super.tearDown();
    }

    public void testWFNotificationTimeout() throws Exception {
        XConfiguration conf = new XConfiguration();
        conf.set(OozieClient.WORKFLOW_NOTIFICATION_URL, container.getServletURL("/hang/*"));
        WorkflowInstance wfi = Mockito.mock(WorkflowInstance.class);
        Mockito.when(wfi.getConf()).thenReturn(conf);
        WorkflowJobBean workflow = Mockito.mock(WorkflowJobBean.class);
        Mockito.when(workflow.getId()).thenReturn("1");
        Mockito.when(workflow.getStatus()).thenReturn(WorkflowJob.Status.SUCCEEDED);
        Mockito.when(workflow.getWorkflowInstance()).thenReturn(wfi);
        WorkflowNotificationXCommand command = new WorkflowNotificationXCommand(workflow);
        command.setRetry(3);
        long start = System.currentTimeMillis();
        command.call();
        long end = System.currentTimeMillis();
        Assert.assertTrue(end - start >= 50);
        Assert.assertTrue(end - start < 10000);
    }

    public void testWFNotification() throws Exception {
        String notificationUrl = "/callback/wf?jobId=$jobId&parentId=$parentId";
        _testNotificationParentId(notificationUrl, "1", null, "");

        notificationUrl = "/callback/wf?jobId=$jobId";
        _testNotificationParentId(notificationUrl, "1", null, null);

        notificationUrl = "/callback/wf?jobId=$jobId&parentId=$parentId";
        _testNotificationParentId(notificationUrl, "1", "0000000-111111-oozie-XXX-C@1", "0000000-111111-oozie-XXX-C@1");

        notificationUrl = "/callback/wf?jobId=$jobId";
        _testNotificationParentId(notificationUrl, "1", "0000000-111111-oozie-XXX-C@1", null);

    }

    private void _testNotificationParentId(String notificationUrl, String jobId, String parentId, String expectedParentId)
            throws Exception{
        XConfiguration conf = new XConfiguration();
        conf.set(OozieClient.WORKFLOW_NOTIFICATION_URL, container.getServletURL(notificationUrl));
        WorkflowInstance wfi = Mockito.mock(WorkflowInstance.class);
        Mockito.when(wfi.getConf()).thenReturn(conf);
        WorkflowJobBean workflow = Mockito.mock(WorkflowJobBean.class);
        Mockito.when(workflow.getId()).thenReturn(jobId);
        Mockito.when(workflow.getStatus()).thenReturn(WorkflowJob.Status.SUCCEEDED);
        Mockito.when(workflow.getParentId()).thenReturn(parentId);
        Mockito.when(workflow.getWorkflowInstance()).thenReturn(wfi);
        WorkflowNotificationXCommand command = new WorkflowNotificationXCommand(workflow);
        command.setRetry(3);
        command.call();
        callbackServlet.waitUntilRequestProcessed();

        Assert.assertEquals(jobId, callbackServlet.jobID);
        Assert.assertEquals(expectedParentId, callbackServlet.parentID);
    }
}
