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

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.NotificationXCommand;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;

public class WorkflowNotificationXCommand extends NotificationXCommand {

    private static final String STATUS_PATTERN = "\\$status";
    private static final String JOB_ID_PATTERN = "\\$jobId";
    private static final String NODE_NAME_PATTERN = "\\$nodeName";

    public WorkflowNotificationXCommand(WorkflowJobBean workflow) {
        super("job.notification", "job.notification", 0);
        ParamChecker.notNull(workflow, "workflow");
        jobId = workflow.getId();
        url = workflow.getWorkflowInstance().getConf().get(OozieClient.WORKFLOW_NOTIFICATION_URL);
        if (url != null) {
            url = url.replaceAll(JOB_ID_PATTERN, workflow.getId());
            url = url.replaceAll(STATUS_PATTERN, workflow.getStatus().toString());
            proxyConf = workflow.getWorkflowInstance().getConf()
                    .get(OozieClient.WORKFLOW_NOTIFICATION_PROXY, ConfigurationService.get(NOTIFICATION_PROXY_KEY));
            LOG.debug("Proxy :" + proxyConf);
        }
    }

    public WorkflowNotificationXCommand(WorkflowJobBean workflow, WorkflowActionBean action) {
        super("action.notification", "job.notification", 0);
        ParamChecker.notNull(workflow, "workflow");
        ParamChecker.notNull(action, "action");
        jobId = action.getId();
        url = workflow.getWorkflowInstance().getConf().get(OozieClient.ACTION_NOTIFICATION_URL);
        if (url != null) {
            url = url.replaceAll(JOB_ID_PATTERN, workflow.getId());
            url = url.replaceAll(NODE_NAME_PATTERN, action.getName());
            if (action.isComplete()) {
                url = url.replaceAll(STATUS_PATTERN, "T:" + action.getTransition());
            }
            else {
                url = url.replaceAll(STATUS_PATTERN, "S:" + action.getStatus().toString());
            }
            proxyConf = workflow.getWorkflowInstance().getConf()
                    .get(OozieClient.WORKFLOW_NOTIFICATION_PROXY, ConfigurationService.get(NOTIFICATION_PROXY_KEY));
            LOG.debug("Proxy :" + proxyConf);
        }
    }

    @Override
    protected void loadState() throws CommandException {
        LogUtils.setLogInfo(jobId);
    }
}
