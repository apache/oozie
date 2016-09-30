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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.ForTestingActionExecutor;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionsGetForJobJPAExecutor;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.ExtendedCallableQueueService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.LiteWorkflowStoreService;
import org.apache.oozie.service.SchemaService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.XConfiguration;

public class TestActionUserRetry extends XDataTestCase {
    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        setSystemProperty(SchemaService.WF_CONF_EXT_SCHEMAS, "wf-ext-schema.xsd");
        setSystemProperty(LiteWorkflowStoreService.CONF_USER_RETRY_ERROR_CODE_EXT, ForTestingActionExecutor.TEST_ERROR);
        setSystemProperty(Services.CONF_SERVICE_EXT_CLASSES, ExtendedCallableQueueService.class.getName());
        services = new Services();
        services.init();
        services.get(ActionService.class).registerAndInitExecutor(ForTestingActionExecutor.class);
        ConfigurationService.setBoolean(SignalXCommand.FORK_PARALLEL_JOBSUBMISSION, true);
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testUserRetry() throws JPAExecutorException, IOException, CommandException{
        Configuration conf = new XConfiguration();
        String workflowUri = getTestCaseFileUri("workflow.xml");

        //@formatter:off
        String appXml = "<workflow-app xmlns=\"uri:oozie:workflow:0.3\" name=\"wf-fork\">"
                + "<start to=\"fork1\"/>"
                + "<fork name=\"fork1\">"
                + "<path start=\"action1\"/>"
                + "<path start=\"action2\"/>"
                + "</fork>"
                +"<action name=\"action1\" retry-max=\"2\" retry-interval=\"0\">"
                + "<test xmlns=\"uri:test\">"
                +    "<signal-value>${wf:conf('signal-value')}</signal-value>"
                +    "<external-status>${wf:conf('external-status')}</external-status> "
                +    "<error>${wf:conf('error')}</error>"
                +    "<avoid-set-execution-data>${wf:conf('avoid-set-execution-data')}</avoid-set-execution-data>"
                +    "<avoid-set-end-data>${wf:conf('avoid-set-end-data')}</avoid-set-end-data>"
                +    "<running-mode>${wf:conf('running-mode')}</running-mode>"
                + "</test>"
                + "<ok to=\"join1\"/>"
                + "<error to=\"kill\"/>"
                + "</action>"
                + "<action name=\"action2\">"
                + "<fs></fs><ok to=\"join1\"/>"
                + "<error to=\"kill\"/>"
                + "</action>"
                + "<join name=\"join1\" to=\"end\"/>"
                + "<kill name=\"kill\"><message>killed</message></kill>"
                + "<end name=\"end\"/>"
                + "</workflow-app>";
           //@Formatter:on
        writeToFile(appXml, workflowUri);
        conf.set(OozieClient.APP_PATH, workflowUri);
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("error", "start.error");
        conf.set("external-status", "error");
        conf.set("signal-value", "based_on_action_status");

        SubmitXCommand sc = new SubmitXCommand(conf);
        final String jobId = sc.call();
        new StartXCommand(jobId).call();
        final WorkflowActionsGetForJobJPAExecutor actionsGetExecutor = new WorkflowActionsGetForJobJPAExecutor(jobId);
        final JPAService jpaService = Services.get().get(JPAService.class);

        waitFor(20 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                List<WorkflowActionBean> actions = jpaService.execute(actionsGetExecutor);
                WorkflowActionBean action = null;
                for (WorkflowActionBean bean : actions) {
                    if (bean.getType().equals("test")) {
                        action = bean;
                        break;
                    }
                }
                return (action != null && action.getUserRetryCount() == 2);
            }
        });

        List<WorkflowActionBean> actions = jpaService.execute(actionsGetExecutor);
        WorkflowActionBean action = null;
        for (WorkflowActionBean bean : actions) {
            if (bean.getType().equals("test")) {
                action = bean;
                break;
            }
        }
        assertNotNull(action);
        assertEquals(2, action.getUserRetryCount());
    }

    public void testUserRetryPolicy() throws JPAExecutorException, IOException, CommandException {
        Configuration conf = new XConfiguration();
        String workflowUri = getTestCaseFileUri("workflow.xml");

        //@formatter:off
        String appXml = "<workflow-app xmlns=\"uri:oozie:workflow:0.5\" name=\"wf-fork\">" + "<start to=\"fork1\"/>"
                + "<fork name=\"fork1\">" + "<path start=\"action1\"/>" + "<path start=\"action2\"/>" + "</fork>"
                + "<action name=\"action1\" retry-max=\"2\" retry-interval=\"1\" retry-policy=\"exponential\">"
                + "<test xmlns=\"uri:test\">"
                + "<signal-value>${wf:conf('signal-value')}</signal-value>"
                + "<external-status>${wf:conf('external-status')}</external-status> "
                + "<error>${wf:conf('error')}</error>"
                + "<avoid-set-execution-data>${wf:conf('avoid-set-execution-data')}</avoid-set-execution-data>"
                + "<avoid-set-end-data>${wf:conf('avoid-set-end-data')}</avoid-set-end-data>"
                + "<running-mode>${wf:conf('running-mode')}</running-mode>" + "</test>" + "<ok to=\"join1\"/>"
                + "<error to=\"kill\"/>" + "</action>" + "<action name=\"action2\">" + "<fs></fs><ok to=\"join1\"/>"
                + "<error to=\"kill\"/>" + "</action>" + "<join name=\"join1\" to=\"end\"/>"
                + "<kill name=\"kill\"><message>killed</message></kill>" + "<end name=\"end\"/>" + "</workflow-app>";
        // @Formatter:on
        writeToFile(appXml, workflowUri);
        conf.set(OozieClient.APP_PATH, workflowUri);
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("error", "start.error");
        conf.set("external-status", "error");
        conf.set("signal-value", "based_on_action_status");

        SubmitXCommand sc = new SubmitXCommand(conf);
        final String jobId = sc.call();
        new StartXCommand(jobId).call();
        final WorkflowActionsGetForJobJPAExecutor actionsGetExecutor = new WorkflowActionsGetForJobJPAExecutor(jobId);
        final JPAService jpaService = Services.get().get(JPAService.class);
        // set a timeout for exponential retry of action with respect to given
        // retry-interval and retry-max.
        // If retry-interval is 1 then, for first retry, delay will be 1 min,
        // for second retry it will be 2 min, 4, 8, 16 & so on.
        int timeout = (1 + 2) * 60 * 1000;
        waitFor(timeout, new Predicate() {
            public boolean evaluate() throws Exception {
                List<WorkflowActionBean> actions = jpaService.execute(actionsGetExecutor);
                WorkflowActionBean action = null;
                for (WorkflowActionBean bean : actions) {
                    if (bean.getType().equals("test")) {
                        action = bean;
                        break;
                    }
                }
                return (action != null && action.getUserRetryCount() == 2);
            }
        });

        List<WorkflowActionBean> actions = jpaService.execute(actionsGetExecutor);
        WorkflowActionBean action = null;
        for (WorkflowActionBean bean : actions) {
            if (bean.getType().equals("test")) {
                action = bean;
                break;
            }
        }
        assertNotNull(action);
        assertEquals(2, action.getUserRetryCount());
    }

    private void writeToFile(String appXml, String appPath) throws IOException {
        File wf = new File(URI.create(appPath));
        PrintWriter out = null;
        try {
            out = new PrintWriter(new FileWriter(wf));
            out.println(appXml);
        }
        catch (IOException iex) {
            throw iex;
        }
        finally {
            if (out != null) {
                out.close();
            }
        }
    }
}
