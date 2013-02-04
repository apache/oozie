package org.apache.oozie.command.wf;

import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.EmbeddedServletContainer;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.workflow.WorkflowInstance;
import org.junit.Assert;
import org.mockito.Mockito;

public class TestNotificationXCommand extends XTestCase {
    private EmbeddedServletContainer container;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        setSystemProperty(NotificationXCommand.NOTIFICATION_URL_CONNECTION_TIMEOUT_KEY, "50");
        Services services = new Services();
        services.init();
        container = new EmbeddedServletContainer("blah");
        container.addServletEndpoint("/hang/*", HangServlet.class);
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
        NotificationXCommand command = new NotificationXCommand(workflow);
        command.retries = 3;
        long start = System.currentTimeMillis();
        command.call();
        long end = System.currentTimeMillis();
        Assert.assertTrue(end - start >= 50);
        Assert.assertTrue(end - start <= 100);
    }
}
