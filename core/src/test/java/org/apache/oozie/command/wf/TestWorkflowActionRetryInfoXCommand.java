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
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.ForTestingActionExecutor;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.JsonUtils;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.WorkflowActionsGetForJobJPAExecutor;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.service.ExtendedCallableQueueService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.LiteWorkflowStoreService;
import org.apache.oozie.service.SchemaService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.XConfiguration;

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

    private void validateRetryConsoleUrl(String wfXml) throws Exception {
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
                for (WorkflowActionBean bean : actions) {
                    if (bean.getType().equals("test") && bean.getUserRetryCount() == 2) {
                        return true;
                    }
                }
                return false;
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

        List<Map<String, String>> retries1List = getRetryList(action1);
        int retries1Num = retries1List == null ? 0 : retries1List.size();

        List<Map<String, String>> retries2List = getRetryList(action2);
        int retries2Num = retries2List == null ? 0 : retries2List.size();

        assertTrue( String.format("Invalid number of retries %d and %d", retries1Num, retries2Num),
                retries1Num == 2 || retries2Num == 2);

        if (retries1List != null) {
            assertEquals("Invalid action attempt", retries1List.get(0).get(JsonTags.ACTION_ATTEMPT), "1");
            assertEquals("Invalid workflow action start time", retries1List.get(0).get(JsonTags.WORKFLOW_ACTION_START_TIME),
                    JsonUtils.formatDateRfc822(action1.getStartTime()));
            for (Map<String,String> retry : retries1List) {
                assertNotNull("Workflow action console URL should not be null", retry.get(JsonTags.WORKFLOW_ACTION_CONSOLE_URL));
                assertNotNull("Missing external child ids", retry.get(JsonTags.WORKFLOW_ACTION_EXTERNAL_CHILD_IDS));
            }
            if (retries1List.size() >= 2) {
                final Date action1EndTime = action1.getEndTime();
                final Date secondRetry1EndTime = JsonUtils.parseDateRfc822(retries1List.get(1).get(
                        JsonTags.WORKFLOW_ACTION_END_TIME));
                assertTrue("action end time should be within ten seconds of second retry end time",
                        endTimeIsWithinExpectedTime(action1EndTime.toInstant(), secondRetry1EndTime.toInstant().plusSeconds(10)));
            }
        }

        if (retries2List != null) {
            assertEquals("Invalid action attempt", retries2List.get(0).get(JsonTags.ACTION_ATTEMPT), "1");
            assertEquals("Invalid workflow action start time", retries2List.get(0).get(JsonTags.WORKFLOW_ACTION_START_TIME),
                    JsonUtils.formatDateRfc822(action2.getStartTime()));
            if (retries2List.size() >= 2) {
                final Date action2EndTime = action2.getEndTime();
                final Date secondRetry2EndTime = JsonUtils.parseDateRfc822(retries2List.get(1).get(
                        JsonTags.WORKFLOW_ACTION_END_TIME));
                assertTrue("action end time should be within ten seconds of second retry end time",
                        endTimeIsWithinExpectedTime(action2EndTime.toInstant(), secondRetry2EndTime.toInstant().plusSeconds(10)));
            }
        }
    }

    private List<Map<String, String>> getRetryList(WorkflowActionBean action) throws CommandException {
        if (action == null) {
            return null;
        }
        WorkflowActionRetryInfoXCommand command = new WorkflowActionRetryInfoXCommand(action.getId());
        return command.call();
    }

    private boolean endTimeIsWithinExpectedTime(Instant endTime, Instant expectedTime) {
        return ChronoUnit.MILLIS.between(endTime, expectedTime) >= 0;
    }
}
