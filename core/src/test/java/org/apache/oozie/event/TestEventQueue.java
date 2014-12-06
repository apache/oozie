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

package org.apache.oozie.event;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.service.JMSAccessorService;
import org.apache.oozie.service.JMSTopicService;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.service.SLAService;
import org.apache.oozie.test.XDataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test case to check correct functioning of MemoryEventQueue
 */
public class TestEventQueue extends XDataTestCase {

    private Services services;

    @Before
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_EXT_CLASSES,
                JMSAccessorService.class.getName() + "," + JMSTopicService.class.getName() + ","
                        + EventHandlerService.class.getName() + "," + SLAService.class.getName());
        conf.setInt(EventHandlerService.CONF_BATCH_SIZE, 3);
        conf.set(EventHandlerService.CONF_LISTENERS, ""); // this unit test is meant to
                                                          // target queue operations only
        services.init();
    }

    @After
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    @Test
    public void testMemoryEventQueueBasic() throws Exception {
        EventHandlerService ehs = Services.get().get(EventHandlerService.class);
        assertNotNull(ehs);
        EventQueue eventQ = ehs.getEventQueue();
        assertNotNull(eventQ);
        assertTrue(eventQ instanceof MemoryEventQueue); //default
    }

    @Test
    public void testQueueOperations() throws Exception {
        EventHandlerService ehs = Services.get().get(EventHandlerService.class);
        EventQueue eventQ = ehs.getEventQueue();
        assertEquals(eventQ.size(), 0);
        assertEquals(eventQ.getBatchSize(), 3);

        // create some events to enqueue
        WorkflowJobEvent wfEvent = new WorkflowJobEvent("1234-W", "1234-C", WorkflowJob.Status.RUNNING, getTestUser(),
                "myapp", null, null);
        for (int i = 0; i < 10; i++) {
            ehs.queueEvent(wfEvent);
        }
        assertEquals(eventQ.size(), 10);

        // test single threads polling from queue
        int numThreads = 1;
        Thread[] thread = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            thread[i] = new Thread(ehs.new EventWorker());
            thread[i].run();
        }
        assertEquals(eventQ.size(), 7); // n(events) - n(batch) i.e.
                                                  // 10-3 = 7

        // restore events count to 10
        for (int i = 0; i < 3; i++) {
            ehs.queueEvent(wfEvent);
        }
        assertEquals(eventQ.size(), 10);
        // test two threads polling concurrently from queue
        numThreads = 2;
        thread = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            thread[i] = new Thread(ehs.new EventWorker());
            thread[i].run();
        }
        assertEquals(eventQ.size(), 4); // n(events) - n(batch)*n(threads)
                                                  // i.e. 10 - 3*2 = 4

        // enqueue events again
        for (int i = 0; i < 6; i++) {
            ehs.queueEvent(wfEvent);
        }
        assertEquals(eventQ.size(), 10);
        // test the 2 threads draining repeatedly (mimicking SchedulerService)
        // from queue
        int repetition = 3;
        int r = 0;
        while (r < repetition) {
            if (eventQ.isEmpty()) {
                break;
            }
            for (int i = 0; i < numThreads; i++) {
                thread[i].run();
            }
            r++;
        }
        assertEquals(eventQ.size(), 0);
    }

}
