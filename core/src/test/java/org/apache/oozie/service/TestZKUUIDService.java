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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.oozie.BulkResponseInfo;
import org.apache.oozie.BundleEngine;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.client.Job;
import org.apache.oozie.executor.jpa.BulkJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.UUIDService.ApplicationType;
import org.apache.oozie.test.ZKXTestCase;
import org.apache.oozie.util.ZKUtils;

public class TestZKUUIDService extends ZKXTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testRegisterUnregister() throws Exception {
        assertEquals(0, ZKUtils.getUsers().size());
        ZKUUIDService zkUUUIDService = new ZKUUIDService();
        try {
            zkUUUIDService.init(Services.get());
            assertEquals(1, ZKUtils.getUsers().size());
            assertEquals(zkUUUIDService, ZKUtils.getUsers().iterator().next());
            zkUUUIDService.destroy();
            assertEquals(0, ZKUtils.getUsers().size());
        }
        finally {
            zkUUUIDService.destroy();
        }
    }

    public void testIDGeneration() throws Exception {
        ZKUUIDService uuid = new ZKUUIDService();
        try {

            setSystemProperty(UUIDService.CONF_GENERATOR, "counter");
            uuid.init(Services.get());
            String id = uuid.generateId(ApplicationType.WORKFLOW);
            assertTrue(id.startsWith("0000000-"));
            for (int i = 0; i < 1000; i++) {
                id = uuid.generateId(ApplicationType.WORKFLOW);
            }
            assertTrue(id.startsWith("0001000-"));
        }
        finally {
            uuid.destroy();
        }
    }

    public void testMultipleIDGeneration() throws Exception {
        ZKUUIDService uuid1 = new ZKUUIDService();
        ZKUUIDService uuid2 = new ZKUUIDService();

        try {
            setSystemProperty(UUIDService.CONF_GENERATOR, "counter");
            uuid1.init(Services.get());
            uuid2.init(Services.get());
            for (int i = 0; i < 1000; i += 2) {
                String id1 = uuid1.generateId(ApplicationType.WORKFLOW);
                String id2 = uuid2.generateId(ApplicationType.WORKFLOW);
                assertEquals(Integer.parseInt(id1.substring(0, 7)), i);
                assertEquals(Integer.parseInt(id2.substring(0, 7)), i + 1);
            }
        }
        finally {
            uuid1.destroy();
            uuid2.destroy();
        }

    }

    public void testMultipleIDGeneration_withMultiThread() throws Exception {
        final List<Boolean> result = new ArrayList<Boolean>(10000);
        final ZKUUIDService uuid1 = new ZKUUIDService();
        final ZKUUIDService uuid2 = new ZKUUIDService();
        setSystemProperty(UUIDService.CONF_GENERATOR, "counter");
        uuid1.init(Services.get());
        uuid2.init(Services.get());

        try {
            Thread t1 = new Thread() {
                public void run() {
                    for (int i = 0; i < 5000; i++) {
                        String id = uuid1.generateId(ApplicationType.WORKFLOW);
                        result.add(Integer.parseInt(id.substring(0, 7)), true);
                    }
                }
            };
            Thread t2 = new Thread() {
                public void run() {
                    for (int i = 0; i < 5000; i++) {
                        String id = uuid2.generateId(ApplicationType.WORKFLOW);
                        result.add(Integer.parseInt(id.substring(0, 7)), true);
                    }
                }
            };
            t1.start();
            t2.start();
            t1.join();
            t2.join();
            for (int i = 0; i < 10000; i++) {
                assertTrue(result.get(i));
            }
        }
        finally {
            uuid1.destroy();
            uuid2.destroy();
        }
    }

    public void testResetSequence() throws Exception {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyMMddHHmmssSSS");
        Services service = Services.get();
        service.setService(ZKLocksService.class);
        ZKUUIDService uuid = new ZKUUIDService();
        try {
            setSystemProperty(UUIDService.CONF_GENERATOR, "counter");
            uuid.setMaxSequence(900);
            uuid.init(service);
            String id = uuid.generateId(ApplicationType.WORKFLOW);
            Date d = dateFormat.parse(id.split("-")[1]);
            assertTrue(id.startsWith("0000000-"));
            for (int i = 0; i < 1000; i++) {
                id = uuid.generateId(ApplicationType.WORKFLOW);
            }
            assertTrue(id.startsWith("0000100-"));
            Date newDate = dateFormat.parse(id.split("-")[1]);
            assertTrue(newDate.after(d));
        }
        finally {
            uuid.destroy();
        }
    }

    public void testResetSequence_withMultiThread() throws Exception {
        Services service = Services.get();
        service.setService(ZKLocksService.class);

        final AtomicInteger result[] = new AtomicInteger[5000];
        final ZKUUIDService uuid1 = new ZKUUIDService();
        final ZKUUIDService uuid2 = new ZKUUIDService();
        setSystemProperty(UUIDService.CONF_GENERATOR, "counter");
        uuid1.init(service);
        uuid2.init(service);
        uuid1.setMaxSequence(5000);
        uuid2.setMaxSequence(5000);

        for (int i = 0; i < 5000; i++) {
            result[i]=new AtomicInteger(0);
        }

        try {
            Thread t1 = new Thread() {
                public void run() {
                    for (int i = 0; i < 5000; i++) {
                        String id = uuid1.generateId(ApplicationType.WORKFLOW);
                        int index = Integer.parseInt(id.substring(0, 7));
                        result[index].incrementAndGet();
                    }
                }
            };
            Thread t2 = new Thread() {
                public void run() {
                    for (int i = 0; i < 5000; i++) {
                        String id = uuid2.generateId(ApplicationType.WORKFLOW);
                        int index = Integer.parseInt(id.substring(0, 7));
                        result[index].incrementAndGet();
                    }
                }
            };
            t1.start();
            t2.start();
            t1.join();
            t2.join();
            for (int i = 0; i < 5000; i++) {
                assertEquals(result[i].get(), 2);
            }
        }
        finally {
            uuid1.destroy();
            uuid2.destroy();
        }
    }

    public void testBulkJobForZKUUIDService() throws Exception {
        Services service = Services.get();
        ZKUUIDService uuid = new ZKUUIDService();
        try {
            setSystemProperty(UUIDService.CONF_GENERATOR, "counter");
            uuid.init(service);
            String bundleId = uuid.generateId(ApplicationType.BUNDLE);
            BundleJobBean bundle = createBundleJob(bundleId, Job.Status.SUCCEEDED, false);
            JPAService jpaService = Services.get().get(JPAService.class);
            BundleJobInsertJPAExecutor bundleInsertjpa = new BundleJobInsertJPAExecutor(bundle);
            jpaService.execute(bundleInsertjpa);
            addCoordForBulkMonitor(bundleId);
            String request = "bundle=" + bundleId;
            BulkJPAExecutor bulkjpa = new BulkJPAExecutor(BundleEngine.parseBulkFilter(request), 1, 1);
            try {
                BulkResponseInfo response = jpaService.execute(bulkjpa);
                assertEquals(response.getResponses().get(0).getBundle().getId(), bundleId);
            }
            catch (JPAExecutorException jex) {
                fail(); // should not throw exception as this case is now supported
            }
        }
        finally {
            uuid.destroy();
        }
    }

    public void testFallback() throws Exception {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyMMddHHmmssSSS");

        Services service = Services.get();
        ZKUUIDServiceWithException uuid = new ZKUUIDServiceWithException();
        try {
            setSystemProperty(UUIDService.CONF_GENERATOR, "counter");
            uuid.init(service);
            String id = uuid.generateId(ApplicationType.BUNDLE);
            assertTrue(id.startsWith("0000000-"));
            id = uuid.generateId(ApplicationType.BUNDLE);
            assertTrue(id.startsWith("0000001-"));

            id = uuid.generateId(ApplicationType.BUNDLE);
            assertTrue(id.startsWith("0000002-"));

            id = uuid.generateId(ApplicationType.BUNDLE);
            assertTrue(id.startsWith("0000003-"));

            id = uuid.generateId(ApplicationType.BUNDLE);
            assertTrue(id.startsWith("0000004-"));
            uuid.setThrowException();

            Date beforeDate=new Date();
            Thread.sleep(2000);
            id = uuid.generateId(ApplicationType.BUNDLE);
            assertTrue(id.startsWith("0000000-"));
            assertTrue(dateFormat.parse(id.split("-")[1]).after(beforeDate));

            beforeDate=new Date();
            Thread.sleep(2000);
            id = uuid.generateId(ApplicationType.BUNDLE);
            assertTrue(id.startsWith("0000001-"));
            assertTrue(dateFormat.parse(id.split("-")[1]).after(beforeDate));

            uuid.resetThrowException();
            beforeDate=new Date();
            Thread.sleep(2000);
            id = uuid.generateId(ApplicationType.BUNDLE);
            assertTrue(id.startsWith("0000005-"));
            assertTrue(dateFormat.parse(id.split("-")[1]).before(beforeDate));


        }
        finally {
            uuid.destroy();
        }
    }

}

class ZKUUIDServiceWithException extends ZKUUIDService {
    boolean throwEx = false;

    public ZKUUIDServiceWithException() {

    }

    public void setThrowException() {
        throwEx = true;
    }

    public void resetThrowException() {
        throwEx = false;
    }

    @Override
    protected long getZKSequence() throws Exception {
        if (throwEx) {
            throw new Exception("Can't generate UUID");
        }
        return super.getZKSequence();
    }

}