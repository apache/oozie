/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.command.wf;

import java.util.List;
import java.util.Map;

import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.WorkflowsInfo;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.util.XLog;

/**
 * Command for loading the Workflows according to the given filter information
 */
public class JobsCommand extends WorkflowCommand<WorkflowsInfo> {
    private Map<String, List<String>> filter;
    private int start;
    private int len;

    /**
     * Constructor taking the filter information
     *
     * @param filter Can be name, status, user, group and combination of these
     * @param start starting from this index in the list of workflows matching the filter are returned
     * @param length number of workflows to be returned from the list of workflows matching the filter and starting from
     * index "start".
     */
    public JobsCommand(Map<String, List<String>> filter, int start, int length) {
        super("job.info", "job.info", 0, XLog.OPS, true);
        this.filter = filter;
        this.start = start;
        this.len = length;
    }

    @Override
    protected WorkflowsInfo call(WorkflowStore store) throws StoreException {
        WorkflowsInfo workflowsInfo = store.getWorkflowsInfo(filter, start, len);
        for (WorkflowJobBean workflow : workflowsInfo.getWorkflows()) {
            workflow.setConsoleUrl(JobCommand.getJobConsoleUrl(workflow.getId()));
        }
        return workflowsInfo;
    }

}
