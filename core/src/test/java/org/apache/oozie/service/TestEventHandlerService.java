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
package org.apache.oozie.service;

import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.event.BundleJobEvent;
import org.apache.oozie.event.CoordinatorActionEvent;
import org.apache.oozie.event.CoordinatorJobEvent;
import org.apache.oozie.event.MemoryEventQueue;
import org.apache.oozie.event.WorkflowActionEvent;
import org.apache.oozie.event.WorkflowJobEvent;
import org.apache.oozie.event.listener.JobEventListener;
import org.apache.oozie.test.XDataTestCase;

public class TestEventHandlerService extends XDataTestCase {

    StringBuilder output = new StringBuilder();

    protected void setUp() throws Exception {
        super.setUp();
        Services services = new Services();
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_EXT_CLASSES, "org.apache.oozie.service.EventHandlerService");
        services.init();
    }

    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    public void testService() throws Exception {
        EventHandlerService ehs = _testEventHandlerService();
        // check default initializations
        assertTrue(ehs.getEventQueue() instanceof MemoryEventQueue);
        Set<String> jobtypes = ehs.getAppTypes();
        assertTrue(jobtypes.contains("workflow_job"));
        assertTrue(jobtypes.contains("coordinator_action"));
    }

    public void testEventListener() throws Exception {
        EventHandlerService ehs = _testEventHandlerService();
        ehs.addEventListener(new DummyJobEventListener());

        /*
         * Workflow Job events
         */
        WorkflowJobEvent event = new WorkflowJobEvent("jobid", "parentid", WorkflowJob.Status.RUNNING, getTestUser(),
                "myapp", null, null);
        ehs.queueEvent(event);
        ehs.new EventWorker().run();
        assertTrue(output.toString().contains("Workflow Job STARTED"));
        output.setLength(0);

        event.setStatus(WorkflowJob.Status.SUSPENDED);
        ehs.queueEvent(event);
        ehs.new EventWorker().run();
        assertTrue(output.toString().contains("Workflow Job SUSPEND"));
        output.setLength(0);

        event.setStatus(WorkflowJob.Status.SUCCEEDED);
        ehs.queueEvent(event);
        ehs.new EventWorker().run();
        assertTrue(output.toString().contains("Workflow Job SUCCESS"));
        output.setLength(0);

        event.setStatus(WorkflowJob.Status.KILLED);
        ehs.queueEvent(event);
        ehs.new EventWorker().run();
        assertTrue(output.toString().contains("Workflow Job FAILURE"));
        output.setLength(0);

        /*
         * Coordinator Action events
         */
        CoordinatorActionEvent event2 = new CoordinatorActionEvent("jobid", "parentid",
                CoordinatorAction.Status.WAITING, getTestUser(), "myapp", null, null, null);
        ehs.queueEvent(event2);
        ehs.new EventWorker().run();
        assertTrue(output.toString().contains("Coord Action WAITING"));
        output.setLength(0);

        event2.setStatus(CoordinatorAction.Status.RUNNING);
        ehs.queueEvent(event2);
        ehs.new EventWorker().run();
        assertTrue(output.toString().contains("Coord Action START"));
        output.setLength(0);

        event2.setStatus(CoordinatorAction.Status.SUSPENDED);
        ehs.queueEvent(event2);
        ehs.new EventWorker().run();
        assertTrue(output.toString().contains("Coord Action SUSPEND"));
        output.setLength(0);

        event2.setStatus(CoordinatorAction.Status.SUCCEEDED);
        ehs.queueEvent(event2);
        ehs.new EventWorker().run();
        assertTrue(output.toString().contains("Coord Action SUCCESS"));
        output.setLength(0);

        event2.setStatus(CoordinatorAction.Status.TIMEDOUT);
        ehs.queueEvent(event2);
        ehs.new EventWorker().run();
        assertTrue(output.toString().contains("Coord Action FAILURE"));
        output.setLength(0);

        event2.setStatus(CoordinatorAction.Status.KILLED);
        ehs.queueEvent(event2);
        ehs.new EventWorker().run();
        assertTrue(output.toString().contains("Coord Action FAILURE"));
        output.setLength(0);
    }

    private EventHandlerService _testEventHandlerService() throws Exception {
        Services services = Services.get();
        EventHandlerService ehs = services.get(EventHandlerService.class);
        assertNotNull(ehs);
        return ehs;
    }

    class DummyJobEventListener extends JobEventListener {

        @Override
        public void onWorkflowJobStart(WorkflowJobEvent wje) {
            if (wje != null) {
                output.append("Dummy Workflow Job STARTED");
            }
        }

        @Override
        public void onWorkflowJobSuccess(WorkflowJobEvent wje) {
            if (wje != null) {
                output.append("Dummy Workflow Job SUCCESS");
            }
        }

        @Override
        public void onWorkflowJobFailure(WorkflowJobEvent wje) {
            if (wje != null) {
                output.append("Dummy Workflow Job FAILURE");
            }
        }

        @Override
        public void onWorkflowJobSuspend(WorkflowJobEvent wje) {
            if (wje != null) {
                output.append("Dummy Workflow Job SUSPEND");
            }
        }

        @Override
        public void onWorkflowActionStart(WorkflowActionEvent wae) {
            if (wae != null) {
                output.append("Dummy Workflow Action START");
            }
        }

        @Override
        public void onWorkflowActionSuccess(WorkflowActionEvent wae) {
            if (wae != null) {
                output.append("Dummy Workflow Action SUCCESS");
            }
        }

        @Override
        public void onWorkflowActionFailure(WorkflowActionEvent wae) {
            if (wae != null) {
                output.append("Dummy Workflow Action FAILURE");
            }
        }

        @Override
        public void onWorkflowActionSuspend(WorkflowActionEvent wae) {
            if (wae != null) {
                output.append("Dummy Workflow Action SUSPEND");
            }
        }

        @Override
        public void onCoordinatorJobStart(CoordinatorJobEvent cje) {
            if (cje != null) {
                output.append("Dummy Coord Job START");
            }
        }

        @Override
        public void onCoordinatorJobSuccess(CoordinatorJobEvent cje) {
            if (cje != null) {
                output.append("Dummy Coord Job SUCCESS");
            }
        }

        @Override
        public void onCoordinatorJobFailure(CoordinatorJobEvent cje) {
            if (cje != null) {
                output.append("Dummy Coord Job FAILURE");
            }
        }

        @Override
        public void onCoordinatorJobSuspend(CoordinatorJobEvent cje) {
            if (cje != null) {
                output.append("Dummy Coord Job SUSPEND");
            }
        }

        @Override
        public void onCoordinatorActionWaiting(CoordinatorActionEvent cae) {
            if (cae != null) {
                output.append("Dummy Coord Action WAITING");
            }
        }

        @Override
        public void onCoordinatorActionStart(CoordinatorActionEvent cae) {
            if (cae != null) {
                output.append("Dummy Coord Action START");
            }
        }

        @Override
        public void onCoordinatorActionSuccess(CoordinatorActionEvent cae) {
            if (cae != null) {
                output.append("Dummy Coord Action SUCCESS");
            }
        }

        @Override
        public void onCoordinatorActionFailure(CoordinatorActionEvent cae) {
            if (cae != null) {
                output.append("Dummy Coord Action FAILURE");
            }
        }

        @Override
        public void onCoordinatorActionSuspend(CoordinatorActionEvent cae) {
            if (cae != null) {
                output.append("Dummy Coord Action SUSPEND");
            }
        }

        @Override
        public void onBundleJobStart(BundleJobEvent bje) {
            if (bje != null) {
                output.append("Dummy Bundle Job START");
            }
        }

        @Override
        public void onBundleJobSuccess(BundleJobEvent bje) {
            if (bje != null) {
                output.append("Dummy Bundle Job SUCCESS");
            }
        }

        @Override
        public void onBundleJobFailure(BundleJobEvent bje) {
            if (bje != null) {
                output.append("Dummy Bundle Job FAILURE");
            }
        }

        @Override
        public void onBundleJobSuspend(BundleJobEvent bje) {
            if (bje != null) {
                output.append("Dummy Bundle Job SUSPEND");
            }
        }

        @Override
        public void init() {
        }

        @Override
        public void destroy() {
        }

    }

}
