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
package org.apache.oozie.sla;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.AppType;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.event.JobEvent.EventStatus;
import org.apache.oozie.client.event.SLAEvent;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.listener.SLAEventListener;
import org.apache.oozie.sla.service.SLAService;
import org.apache.oozie.test.XDataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSLAService extends XDataTestCase {

    static StringBuilder output = new StringBuilder();

    @Override
    @Before
    protected void setUp() throws Exception {
        super.setUp();
        Services services = new Services();
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_EXT_CLASSES, "org.apache.oozie.service.EventHandlerService,"
                + "org.apache.oozie.sla.service.SLAService");
        conf.setClass(EventHandlerService.CONF_LISTENERS, DummySLAEventListener.class, SLAEventListener.class);
        services.init();
        cleanUpDBTables();
    }

    @Override
    @After
    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    @Test
    public void testBasicService() throws Exception {
        Services services = Services.get();
        SLAService slas = services.get(SLAService.class);
        assertNotNull(slas);
        assertTrue(SLAService.isEnabled());

        services.destroy();
        services = new Services();
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_EXT_CLASSES, "");
        services.init();
        assertFalse(SLAService.isEnabled());
    }

    @Test
    public void testUpdateSLA() throws Exception {
        SLAService slas = Services.get().get(SLAService.class);
        assertNotNull(slas);
        assertTrue(SLAService.isEnabled());

        EventHandlerService ehs = Services.get().get(EventHandlerService.class);
        // test start-miss
        SLARegistrationBean sla1 = _createSLARegistration("job-1", AppType.WORKFLOW_JOB);
        sla1.setExpectedStart(new Date(System.currentTimeMillis() - 1 * 1 * 3600 * 1000)); //1 hour back
        sla1.setExpectedEnd(new Date(System.currentTimeMillis() + 1 * 1 * 3600 * 1000)); //1 hour ahead
        slas.addRegistrationEvent(sla1);
        assertEquals(1, slas.getSLACalculator().size());
        slas.runSLAWorker();
        ehs.new EventWorker().run();
        assertTrue(output.toString().contains("Sla START - MISS!!!"));
        output.setLength(0);

        // test different jobs and events start-met and end-miss
        sla1 = _createSLARegistration("job-2", AppType.WORKFLOW_JOB);
        sla1.setExpectedStart(new Date(System.currentTimeMillis() + 1 * 3600 * 1000)); //1 hour ahead
        sla1.setExpectedEnd(new Date(System.currentTimeMillis() + 2 * 3600 * 1000)); //2 hours ahead
        slas.addRegistrationEvent(sla1);
        slas.addStatusEvent(sla1.getJobId(), WorkflowJob.Status.RUNNING.name(), EventStatus.STARTED, new Date(),
                new Date());
        SLARegistrationBean sla2 = _createSLARegistration("job-3", AppType.COORDINATOR_JOB);
        sla2.setExpectedStart(new Date(System.currentTimeMillis() + 1 * 3600 * 1000)); //1 hour ahead only for testing
        sla2.setExpectedEnd(new Date(System.currentTimeMillis() - 2 * 3600 * 1000)); //2 hours back
        slas.addRegistrationEvent(sla2);
        assertEquals(3, slas.getSLACalculator().size());
        slas.addStatusEvent(sla2.getJobId(), CoordinatorJob.Status.SUCCEEDED.name(), EventStatus.SUCCESS, new Date(),
                new Date());
        slas.runSLAWorker();
        ehs.new EventWorker().run();
        assertTrue(output.toString().contains(sla1.getJobId() + " Sla START - MET!!!"));
        assertTrue(output.toString().contains(sla2.getJobId() + " Sla END - MISS!!!"));
        output.setLength(0);

        // test same job multiple events (start-miss, end-miss) through regular check
        sla2 = _createSLARegistration("job-4", AppType.WORKFLOW_JOB);
        sla2.setExpectedStart(new Date(System.currentTimeMillis() - 2 * 3600 * 1000)); //2 hours back
        sla2.setExpectedEnd(new Date(System.currentTimeMillis() - 1 * 3600 * 1000)); //1 hour back
        slas.addRegistrationEvent(sla2);
        assertEquals(3, slas.getSLACalculator().size()); // remains same as before since
                                                         // sla-END stage job is removed from map
        slas.runSLAWorker();
        assertEquals(2, ehs.getEventQueue().size());
        ehs.new EventWorker().run();
        System.out.println(output);
        assertTrue(output.toString().contains(sla2.getJobId() + " Sla START - MISS!!!"));
        assertTrue(output.toString().contains(sla2.getJobId() + " Sla END - MISS!!!"));
        output.setLength(0);

        // test same job multiple events (start-met, end-met) through job status event
        sla1 = _createSLARegistration("action-1", AppType.COORDINATOR_ACTION);
        sla1.setExpectedStart(new Date(System.currentTimeMillis() + 1 * 3600 * 1000)); //1 hour ahead
        sla1.setExpectedEnd(new Date(System.currentTimeMillis() + 2 * 3600 * 1000)); //2 hours ahead
        slas.addRegistrationEvent(sla1);
        assertEquals(3, slas.getSLACalculator().size());
        slas.addStatusEvent(sla1.getJobId(), CoordinatorAction.Status.RUNNING.name(), EventStatus.STARTED, new Date(),
                new Date());
        slas.addStatusEvent(sla1.getJobId(), CoordinatorAction.Status.SUCCEEDED.name(), EventStatus.SUCCESS,
                new Date(), new Date());
        slas.runSLAWorker();
        assertEquals(3, ehs.getEventQueue().size());
        ehs.new EventWorker().run();
        System.out.println(output);
        assertTrue(output.toString().contains(sla1.getJobId() + " Sla START - MET!!!"));
        assertTrue(output.toString().contains(sla1.getJobId() + " Sla END - MET!!!"));
    }

    private SLARegistrationBean _createSLARegistration(String jobId, AppType appType) {
        SLARegistrationBean bean = new SLARegistrationBean();
        bean.setJobId(jobId);
        bean.setAppType(appType);
        return bean;
    }

    public static class DummySLAEventListener extends SLAEventListener {

        @Override
        public void onStartMet(SLAEvent sla) {
            output.append(sla.getJobId() + " Sla START - MET!!!");
        }

        @Override
        public void onStartMiss(SLAEvent sla) {
            output.append(sla.getJobId() + " Sla START - MISS!!!");
        }

        @Override
        public void onEndMet(SLAEvent sla) {
            output.append(sla.getJobId() + " Sla END - MET!!!");
        }

        @Override
        public void onEndMiss(SLAEvent sla) {
            output.append(sla.getJobId() + " Sla END - MISS!!!");
        }

        @Override
        public void onDurationMet(SLAEvent sla) {
            output.append(sla.getJobId() + " Sla DURATION - MET!!!");
        }

        @Override
        public void onDurationMiss(SLAEvent sla) {
            output.append(sla.getJobId() + " Sla DURATION - MISS!!!");
        }

        @Override
        public void init(Configuration conf) {
        }

        @Override
        public void destroy() {
        }

    }

}
