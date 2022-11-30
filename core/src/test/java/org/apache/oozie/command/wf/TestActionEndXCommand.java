/*
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

import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.control.ForkActionExecutor;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.XCommand;
import org.apache.oozie.service.InstrumentationService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.workflow.WorkflowInstance;

public class TestActionEndXCommand extends XDataTestCase {

    /**
     * Test : verify that ActionEndXCommand failed the precondition verification phase when job != RUNNING and
     * there is an other action which status' is FAILED.
     *
     * @throws Exception
     */
    public void testParalellyFailedActionInJobContainingFork() throws Exception {
        Instrumentation inst = Services.get().get(InstrumentationService.class).get();

        WorkflowJobBean job = this.addRecordToWfJobTable(WorkflowJob.Status.FAILED, WorkflowInstance.Status.FAILED);

        WorkflowActionBean forkAction = this.addRecordToWfActionTableWithType(job.getId(), "forkAction",
                WorkflowAction.Status.OK, ForkActionExecutor.TYPE);

        WorkflowActionBean failedAction = this.addRecordToWfActionTable(job.getId(), "failedAction",
                WorkflowAction.Status.FAILED);

        WorkflowActionBean greenAction = this.addRecordToWfActionTable(job.getId(), "greenAction",
                WorkflowAction.Status.DONE, "/", true);

        ActionEndXCommand checkCmd = new ActionEndXCommand(greenAction.getId(), greenAction.getType());

        checkCmd.call();

        try {
            // this is supposed to throw NullPointerException
            inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(checkCmd.getName() + ".preconditionfailed").getValue();
            fail("A NullPointerException should have been thrown");
        } catch (NullPointerException expect) {
            // we should get here
        }
    }
}
