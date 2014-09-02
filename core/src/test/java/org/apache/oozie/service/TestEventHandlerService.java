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

import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.event.BundleJobEvent;
import org.apache.oozie.event.CoordinatorActionEvent;
import org.apache.oozie.event.CoordinatorJobEvent;
import org.apache.oozie.event.MemoryEventQueue;
import org.apache.oozie.event.WorkflowActionEvent;
import org.apache.oozie.event.WorkflowJobEvent;
import org.apache.oozie.event.listener.JobEventListener;
import org.apache.oozie.test.XDataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestEventHandlerService extends XDataTestCase {

    static StringBuilder output = new StringBuilder();

    @Before
    protected void setUp() throws Exception {
        super.setUp();
        Services services = new Services();
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_EXT_CLASSES, "org.apache.oozie.service.EventHandlerService");
        conf.setClass(EventHandlerService.CONF_LISTENERS, DummyJobEventListener.class, JobEventListener.class);
        services.init();
    }

    @After
    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    @Test
    public void testService() throws Exception {
        EventHandlerService ehs = _testEventHandlerService();
        // check default initializations
        assertTrue(ehs.getEventQueue() instanceof MemoryEventQueue);
        Set<String> jobtypes = ehs.getAppTypes();
        assertTrue(jobtypes.contains("workflow_job"));
        assertTrue(jobtypes.contains("coordinator_action"));

        Services services = Services.get();
        services.destroy();
        services = new Services();
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_EXT_CLASSES, "");
        services.init();
        assertFalse(EventHandlerService.isEnabled());
    }

    @Test
    public void testEventListener() throws Exception {
        EventHandlerService ehs = _testEventHandlerService();
        /*
         * Workflow Job events
         */
        WorkflowJobEvent event = new WorkflowJobEvent("jobid", "parentid", WorkflowJob.Status.RUNNING, getTestUser(),
                "myapp", null, null);
        ehs.queueEvent(event);
        ehs.new EventWorker().run();
        assertTrue(output.toString().contains("Workflow Job event STARTED"));
        output.setLength(0);

        event.setStatus(WorkflowJob.Status.SUSPENDED);
        ehs.queueEvent(event);
        ehs.new EventWorker().run();
        assertTrue(output.toString().contains("Workflow Job event SUSPEND"));
        output.setLength(0);

        event.setStatus(WorkflowJob.Status.SUCCEEDED);
        ehs.queueEvent(event);
        ehs.new EventWorker().run();
        assertTrue(output.toString().contains("Workflow Job event SUCCESS"));
        output.setLength(0);

        event.setStatus(WorkflowJob.Status.KILLED);
        ehs.queueEvent(event);
        ehs.new EventWorker().run();
        assertTrue(output.toString().contains("Workflow Job event FAILURE"));
        output.setLength(0);

        /*
         * Coordinator Action events
         */
        CoordinatorActionEvent event2 = new CoordinatorActionEvent("parentid@1", "parentid",
                CoordinatorAction.Status.WAITING, getTestUser(), "myapp", null, null, null);
        ehs.queueEvent(event2);
        ehs.new EventWorker().run();
        assertTrue(output.toString().contains("Coord Action event WAITING"));
        output.setLength(0);

        event2.setStatus(CoordinatorAction.Status.RUNNING);
        ehs.queueEvent(event2);
        ehs.new EventWorker().run();
        assertTrue(output.toString().contains("Coord Action event STARTED"));
        output.setLength(0);

        event2.setStatus(CoordinatorAction.Status.SUSPENDED);
        ehs.queueEvent(event2);
        ehs.new EventWorker().run();
        assertTrue(output.toString().contains("Coord Action event SUSPEND"));
        output.setLength(0);

        event2.setStatus(CoordinatorAction.Status.SUCCEEDED);
        ehs.queueEvent(event2);
        ehs.new EventWorker().run();
        assertTrue(output.toString().contains("Coord Action event SUCCESS"));
        output.setLength(0);

        event2.setStatus(CoordinatorAction.Status.TIMEDOUT);
        ehs.queueEvent(event2);
        ehs.new EventWorker().run();
        assertTrue(output.toString().contains("Coord Action event FAILURE"));
        output.setLength(0);

        event2.setStatus(CoordinatorAction.Status.KILLED);
        ehs.queueEvent(event2);
        ehs.new EventWorker().run();
        assertTrue(output.toString().contains("Coord Action event FAILURE"));
        output.setLength(0);

        /*
         * Workflow Action events
         */
        WorkflowActionEvent event3 = new WorkflowActionEvent("parentid@wfaction", "parentid",
                WorkflowAction.Status.RUNNING, getTestUser(), "myapp", null, null);
        ehs.queueEvent(event3);
        ehs.new EventWorker().run();
        assertTrue(output.toString().contains("Workflow Action event STARTED"));
        output.setLength(0);

        event3.setStatus(WorkflowAction.Status.START_MANUAL);
        ehs.queueEvent(event3);
        ehs.new EventWorker().run();
        assertTrue(output.toString().contains("Workflow Action event SUSPEND"));
        output.setLength(0);

        event3.setStatus(WorkflowAction.Status.OK);
        ehs.queueEvent(event3);
        ehs.new EventWorker().run();
        assertTrue(output.toString().contains("Workflow Action event SUCCESS"));
        output.setLength(0);

        event3.setStatus(WorkflowAction.Status.ERROR);
        ehs.queueEvent(event3);
        ehs.new EventWorker().run();
        assertTrue(output.toString().contains("Workflow Action event FAILURE"));
        output.setLength(0);

        event3.setStatus(WorkflowAction.Status.KILLED);
        ehs.queueEvent(event3);
        ehs.new EventWorker().run();
        assertTrue(output.toString().contains("Workflow Action event FAILURE"));
        output.setLength(0);

        event3.setStatus(WorkflowAction.Status.FAILED);
        ehs.queueEvent(event3);
        ehs.new EventWorker().run();
        assertTrue(output.toString().contains("Workflow Action event FAILURE"));
        output.setLength(0);

    }

    @Test
    public void testEventLogging() throws Exception {
        EventHandlerService ehs = _testEventHandlerService();
        // job event
        WorkflowJobEvent event = new WorkflowJobEvent("jobid", "parentid", WorkflowJob.Status.RUNNING, getTestUser(),
                "myapp", null, null);

        TestLogAppender appender = null;
        Logger logger = null;
        try {
            appender = getTestLogAppender();
            logger = Logger.getLogger(EventHandlerService.class);
            logger.addAppender(appender);
            logger.setLevel(Level.DEBUG);
            ehs.queueEvent(event);

            List<LoggingEvent> log = appender.getLog();
            LoggingEvent logEntry = log.get(0);
            assertEquals(Level.DEBUG, logEntry.getLevel());
            assertTrue(logEntry.getMessage().toString().contains("APP[myapp] JOB[jobid] ACTION[-] Queueing event : ID: jobid"));
            assertEquals("org.apache.oozie.service.EventHandlerService", logEntry.getLoggerName());

            ehs.new EventWorker().run();

            log = appender.getLog();
            logEntry = log.get(1);
            assertEquals(Level.DEBUG, logEntry.getLevel());
            assertTrue(logEntry.getMessage().toString().contains("APP[myapp] JOB[jobid] ACTION[-] Processing event : ID: jobid"));

            // action event
            CoordinatorActionEvent event2 = new CoordinatorActionEvent("jobid2@1", "parentid",
                    CoordinatorAction.Status.WAITING, getTestUser(), "myapp", null, null, null);
            ehs.queueEvent(event2);

            log = appender.getLog();
            logEntry = log.get(2);
            assertTrue(logEntry.getMessage().toString().contains("APP[myapp] JOB[jobid2] ACTION[jobid2@1] Queueing event : ID: jobid2@1"));

            WorkflowJobEvent event3 = new WorkflowJobEvent("jobid-other", "parentid", WorkflowJob.Status.RUNNING, getTestUser(),
                    "myapp-other", null, null);
            ehs.queueEvent(event3);

            log = appender.getLog();
            logEntry = log.get(3);
            assertTrue(logEntry.getMessage().toString()
                    .contains("APP[myapp-other] JOB[jobid-other] ACTION[-] Queueing event : ID: jobid-other"));
        }
        finally {
            logger.removeAppender(appender);
        }
    }

    private EventHandlerService _testEventHandlerService() throws Exception {
        Services services = Services.get();
        EventHandlerService ehs = services.get(EventHandlerService.class);
        assertNotNull(ehs);
        assertTrue(EventHandlerService.isEnabled());
        return ehs;
    }

    static class DummyJobEventListener extends JobEventListener {

        @Override
        public void onWorkflowJobEvent(WorkflowJobEvent wje) {
            if (wje != null) {
                output.append("Dummy Workflow Job event " + wje.getEventStatus());
            }
        }

        @Override
        public void onWorkflowActionEvent(WorkflowActionEvent wae) {
            if (wae != null) {
                output.append("Dummy Workflow Action event "+ wae.getEventStatus());
            }
        }

        @Override
        public void onCoordinatorJobEvent(CoordinatorJobEvent cje) {
            if (cje != null) {
                output.append("Dummy Coord Job event " + cje.getEventStatus());
            }
        }

        @Override
        public void onCoordinatorActionEvent(CoordinatorActionEvent cae) {
            if (cae != null) {
                output.append("Dummy Coord Action event " + cae.getEventStatus());
            }
        }

        @Override
        public void onBundleJobEvent(BundleJobEvent bje) {
            if (bje != null) {
                output.append("Dummy Bundle Job event " + bje.getEventStatus());
            }
        }

        @Override
        public void init(Configuration conf) {
        }

        @Override
        public void destroy() {
        }

    }

}
