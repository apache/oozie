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
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

/**
 * Test case to check correct functioning of MemoryEventQueue
 */
public class TestEventQueue extends XDataTestCase {

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

    public void testMemoryEventQueueBasic() throws Exception {
        EventHandlerService ehs = Services.get().get(EventHandlerService.class);
        assertNotNull(ehs);
        EventQueue eventQ = ehs.getEventQueue();
        assertNotNull(eventQ);
        assertTrue(eventQ instanceof MemoryEventQueue); //default
    }

    public void testQueueOperations() throws Exception {
        Services services = Services.get();
        Configuration conf = services.getConf();

        // set smaller batch size for the events queue
        conf.setInt(EventHandlerService.CONF_BATCH_SIZE, 3);
        EventHandlerService ehs = Services.get().get(EventHandlerService.class);
        ehs.init(services);
        EventQueue eventQ = ehs.getEventQueue();
        assertEquals(eventQ.getCurrentSize(), 0);
        assertEquals(eventQ.getBatchSize(), 3);

        // create some events to enqueue
        WorkflowJobEvent wfEvent = new WorkflowJobEvent("1234-W", "1234-C", WorkflowJob.Status.RUNNING, getTestUser(),
                "myapp", null, null);
        for (int i = 0; i < 10; i++)
            ehs.queueEvent(wfEvent);
        assertEquals(eventQ.getCurrentSize(), 10);

        // test single threads polling from queue
        int numThreads = 1;
        Thread[] thread = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            thread[i] = new Thread(ehs.new EventWorker());
            thread[i].run();
        }
        assertEquals(eventQ.getCurrentSize(), 7); // n(events) - n(batch) i.e.
                                                  // 10-3 = 7

        // restore events count to 10
        for (int i = 0; i < 3; i++)
            ehs.queueEvent(wfEvent);
        assertEquals(eventQ.getCurrentSize(), 10);
        // test two threads polling concurrently from queue
        numThreads = 2;
        thread = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            thread[i] = new Thread(ehs.new EventWorker());
            thread[i].run();
        }
        assertEquals(eventQ.getCurrentSize(), 4); // n(events) - n(batch)*n(threads)
                                                  // i.e. 10 - 3*2 = 4

        // enqueue events again
        for (int i = 0; i < 6; i++)
            ehs.queueEvent(wfEvent);
        assertEquals(eventQ.getCurrentSize(), 10);
        // test the 2 threads draining repeatedly (mimicking SchedulerService)
        // from queue
        int repetition = 3;
        int r = 0;
        while (r < repetition) {
            if (eventQ.isEmpty())
                break;
            for (int i = 0; i < numThreads; i++) {
                thread[i].run();
            }
            r++;
        }
        assertEquals(eventQ.getCurrentSize(), 0);
    }

}
