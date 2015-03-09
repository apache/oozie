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

import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.XCommand;
import org.apache.oozie.executor.jpa.WorkflowActionQueryExecutor;
import org.apache.oozie.service.InstrumentationService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.workflow.WorkflowInstance;

public class TestCompletedActionXCommand extends XDataTestCase {
    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testEarlyCallbackTimeout() throws Exception {
        final Instrumentation inst = Services.get().get(InstrumentationService.class).get();

        WorkflowJobBean job = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        WorkflowActionBean action = addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.PREP);
        final CompletedActionXCommand cmd = new CompletedActionXCommand(action.getId(), "SUCCEEDED", null);

        long xexceptionCount;
        try {
            xexceptionCount = inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(cmd.getName()
                    + ".xexceptions").getValue();
        } catch (NullPointerException npe){
            //counter might be null
            xexceptionCount = 0L;
        }
        assertEquals(0L, xexceptionCount);

        long executionsCount;
        try {
            executionsCount = inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(cmd.getName()
                    + ".executions").getValue();
        } catch (NullPointerException npe){
            //counter might be null
            executionsCount = 0L;
        }
        assertEquals(0L, executionsCount);

        long executionCount;
        try {
            executionCount = inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(cmd.getName()
                    + ".execution").getValue();
        } catch (NullPointerException npe){
            //counter might be null
            executionCount = 0L;
        }
        assertEquals(0L, executionCount);

        cmd.call();
        int timeout = 10000 * 5 * 2;
        waitFor(timeout, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                long xexceptionCount;
                try {
                    xexceptionCount = inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(cmd.getName()
                            + ".xexceptions").getValue();
                } catch(NullPointerException npe) {
                    //counter might be null
                    xexceptionCount = 0L;
                }
                return (xexceptionCount == 1L);
            }
        });
        executionsCount = inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(cmd.getName()
                + ".executions").getValue();
        assertEquals(6L, executionsCount);
        try {
            executionCount = inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(cmd.getName()
                    + ".execution").getValue();
        } catch (NullPointerException npe){
            //counter might be null
            executionCount = 0L;
        }
        assertEquals(0L, executionCount);
        xexceptionCount = inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(cmd.getName()
                + ".xexceptions").getValue();
        assertEquals(1L, xexceptionCount);
    }

    public void testEarlyCallbackTransitionToRunning() throws Exception {
        final Instrumentation inst = Services.get().get(InstrumentationService.class).get();

        WorkflowJobBean job = addRecordToWfJobTable(WorkflowJob.Status.RUNNING, WorkflowInstance.Status.RUNNING);
        final WorkflowActionBean action = addRecordToWfActionTable(job.getId(), "1", WorkflowAction.Status.PREP);
        final CompletedActionXCommand cmd = new CompletedActionXCommand(action.getId(), "SUCCEEDED", null);

        long xexceptionCount;
        try {
            xexceptionCount = inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(cmd.getName()
                    + ".xexceptions").getValue();
        } catch (NullPointerException npe){
            //counter might be null
            xexceptionCount = 0L;
        }
        assertEquals(0L, xexceptionCount);

        long executionsCount;
        try {
            executionsCount = inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(cmd.getName()
                    + ".executions").getValue();
        } catch (NullPointerException npe){
            //counter might be null
            executionsCount = 0L;
        }
        assertEquals(0L, executionsCount);

        long checkXCommandExecutionsCount;
        try {
            checkXCommandExecutionsCount = inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(
                    "action.check.executions").getValue();
        } catch (NullPointerException npe){
            //counter might be null
            checkXCommandExecutionsCount = 0L;
        }
        assertEquals(0L, checkXCommandExecutionsCount);

        cmd.call();
        int timeout = 100000 * 5 * 2;
        waitFor(timeout, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                long executionsCount;
                try {
                    executionsCount = inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(cmd.getName()
                            + ".executions").getValue();
                } catch (NullPointerException npe){
                    //counter might be null
                    executionsCount = 0L;
                }
                if (executionsCount == 3 && !action.getStatus().equals(WorkflowAction.Status.RUNNING)) {
                    // Transition the action to RUNNING
                    action.setStatus(WorkflowAction.Status.RUNNING);
                    WorkflowActionQueryExecutor.getInstance().executeUpdate(
                            WorkflowActionQueryExecutor.WorkflowActionQuery.UPDATE_ACTION, action);
                }
                long checkXCommandExecutionsCount;
                try {
                    checkXCommandExecutionsCount = inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(
                            "action.check.executions").getValue();
                } catch (NullPointerException npe){
                    //counter might be null
                    checkXCommandExecutionsCount = 0L;
                }
                return (checkXCommandExecutionsCount == 1L);
            }
        });
        executionsCount = inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(cmd.getName()
                + ".executions").getValue();
        assertTrue("expected a value greater than 3L, but found " + executionsCount, executionsCount >= 3L);
        checkXCommandExecutionsCount = inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(
                    "action.check.executions").getValue();
        assertEquals(1L, checkXCommandExecutionsCount);
        try {
            xexceptionCount = inst.getCounters().get(XCommand.INSTRUMENTATION_GROUP).get(cmd.getName()
                    + ".xexceptions").getValue();
        } catch (NullPointerException npe){
            //counter might be null
            xexceptionCount = 0L;
        }
        assertEquals(0L, xexceptionCount);
    }
}
