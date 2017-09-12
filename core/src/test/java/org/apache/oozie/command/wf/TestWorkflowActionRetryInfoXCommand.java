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
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.ForTestingActionExecutor;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.JsonUtils;
import org.apache.oozie.executor.jpa.WorkflowActionsGetForJobJPAExecutor;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.service.ExtendedCallableQueueService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.LiteWorkflowStoreService;
import org.apache.oozie.service.SchemaService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.XConfiguration;
import org.joda.time.Interval;

public class TestWorkflowActionRetryInfoXCommand extends XDataTestCase {
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
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testRetryConsoleUrl() throws Exception {
        //@formatter:off
        String wfXml = "<workflow-app xmlns=\"uri:oozie:workflow:0.3\" name=\"wf-fork\">"
                + "<start to=\"action1\"/>"
                +"<action name=\"action1\" retry-max=\"2\" retry-interval=\"0\">"
                + "<test xmlns=\"uri:test\">"
                +    "<signal-value>${wf:conf('signal-value')}</signal-value>"
                +    "<external-status>${wf:conf('external-status')}</external-status> "
                +    "<external-childIds>${wf:conf('external-status')}</external-childIds> "
                +    "<error>${wf:conf('error')}</error>"
                +    "<avoid-set-execution-data>${wf:conf('avoid-set-execution-data')}</avoid-set-execution-data>"
                +    "<avoid-set-end-data>${wf:conf('avoid-set-end-data')}</avoid-set-end-data>"
                +    "<running-mode>async-error</running-mode>"
                + "</test>"
                + "<ok to=\"end\"/>"
                + "<error to=\"kill\"/>"
                + "</action>"
                + "<kill name=\"kill\"><message>killed</message></kill>"
                + "<end name=\"end\"/>"
                + "</workflow-app>";
        //@Formatter:on
        validateRetryConsoleUrl(wfXml);
    }

    public void testRetryConsoleUrlForked() throws Exception {
        //@formatter:off
        String wfXml = "<workflow-app xmlns=\"uri:oozie:workflow:0.3\" name=\"wf-fork\">"
                + "<start to=\"fork1\"/>"
                + "<fork name=\"fork1\">"
                +    "<path start=\"action1\"/>"
                +    "<path start=\"action2\"/>"
                + "</fork>"
                +"<action name=\"action1\" retry-max=\"2\" retry-interval=\"0\">"
                + "<test xmlns=\"uri:test\">"
                +    "<signal-value>${wf:conf('signal-value')}</signal-value>"
                +    "<external-status>${wf:conf('external-status')}</external-status> "
                +    "<external-childIds>${wf:conf('external-status')}</external-childIds> "
                +    "<error>${wf:conf('error')}</error>"
                +    "<avoid-set-execution-data>${wf:conf('avoid-set-execution-data')}</avoid-set-execution-data>"
                +    "<avoid-set-end-data>${wf:conf('avoid-set-end-data')}</avoid-set-end-data>"
                +    "<running-mode>async-error</running-mode>"
                + "</test>"
                + "<ok to=\"join\"/>"
                + "<error to=\"kill\"/>"
                + "</action>"
                +"<action name=\"action2\" retry-max=\"2\" retry-interval=\"0\">"
                + "<test xmlns=\"uri:test\">"
                +    "<signal-value>${wf:conf('signal-value')}</signal-value>"
                +    "<external-status>${wf:conf('external-status')}</external-status> "
                +    "<external-childIds>${wf:conf('external-status')}</external-childIds> "
                +    "<error>${wf:conf('error')}</error>"
                +    "<avoid-set-execution-data>${wf:conf('avoid-set-execution-data')}</avoid-set-execution-data>"
                +    "<avoid-set-end-data>${wf:conf('avoid-set-end-data')}</avoid-set-end-data>"
                +    "<running-mode>async-error</running-mode>"
                + "</test>"
                + "<ok to=\"join\"/>"
                + "<error to=\"kill\"/>"
                + "</action>"
                + "<join name=\"join\" to=\"end\"/>"
                + "<kill name=\"kill\"><message>killed</message></kill>"
                + "<end name=\"end\"/>"
                + "</workflow-app>";
        //@Formatter:on
        validateRetryConsoleUrl(wfXml);
    }

    public void validateRetryConsoleUrl(String wfXml) throws Exception {
        Configuration conf = new XConfiguration();
        File workflowUri = new File(getTestCaseDir(), "workflow.xml");
        writeToFile(wfXml, workflowUri);
        conf.set(OozieClient.APP_PATH, workflowUri.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("external-status", "error");
        conf.set("signal-value", "based_on_action_status");
        conf.set("external-childIds", "1");

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
        WorkflowActionBean action1 = null;
        WorkflowActionBean action2 = null;
        for (WorkflowActionBean bean : actions) {
            if (bean.getType().equals("test") && bean.getName().equals("action1")) {
                action1 = bean;
            }
            else if (bean.getType().equals("test") && bean.getName().equals("action2")) {
                action2 = bean;
            }
        }

        WorkflowActionRetryInfoXCommand command = new WorkflowActionRetryInfoXCommand(action1.getId());
        List<Map<String, String>> retriesList = command.call();
        assertEquals(2, retriesList.size());
        assertEquals(2, action1.getUserRetryCount());

        assertEquals(retriesList.get(0).get(JsonTags.ACTION_ATTEMPT), "1");
        assertEquals(retriesList.get(0).get(JsonTags.WORKFLOW_ACTION_START_TIME),
                JsonUtils.formatDateRfc822(action1.getStartTime()));

        assertNotNull(retriesList.get(0).get(JsonTags.WORKFLOW_ACTION_CONSOLE_URL));
        assertNotNull(retriesList.get(0).get(JsonTags.WORKFLOW_ACTION_EXTERNAL_CHILD_IDS));

        assertNotNull(retriesList.get(1).get(JsonTags.WORKFLOW_ACTION_CONSOLE_URL));
        assertNotNull(retriesList.get(1).get(JsonTags.WORKFLOW_ACTION_EXTERNAL_CHILD_IDS));

        final Date actionEndTime = action2 == null ? action1.getEndTime() : action2.getEndTime();
        final Date secondRetryEndTime = JsonUtils.parseDateRfc822(retriesList.get(1).get(JsonTags.WORKFLOW_ACTION_END_TIME));

        assertTrue("action end time should be within ten seconds of second retry end time",
                new Interval(secondRetryEndTime.getTime(), secondRetryEndTime.getTime() + 10_000)
                        .contains(actionEndTime.getTime()));
    }
}
