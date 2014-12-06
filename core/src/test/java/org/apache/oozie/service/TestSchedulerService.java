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

import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.util.concurrent.atomic.AtomicInteger;

public class TestSchedulerService extends XTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        new Services().init();
    }

    @Override
    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    public void testInstrumentation() throws Exception {
        assertNotNull(Services.get().get(SchedulerService.class));
        SchedulerService ss = Services.get().get(SchedulerService.class);
        final AtomicInteger counter = new AtomicInteger();
        ss.schedule(new Runnable() {
            public void run() {
                counter.incrementAndGet();
            }
        }, 0, 1, SchedulerService.Unit.SEC);

        waitFor(2 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return false;
            }
        });
        assertTrue(counter.get() > 1);
    }

}
