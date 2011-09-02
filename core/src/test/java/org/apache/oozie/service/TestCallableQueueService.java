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
package org.apache.oozie.service;

import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.XCallable;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class TestCallableQueueService extends XTestCase {
    static AtomicLong EXEC_ORDER = new AtomicLong();

    public class MyCallable implements XCallable<Void> {
        String type;
        int priority;
        long executed = 0;
        int wait;
        long order;

        public MyCallable() {
            this(0, 0);
        }

        public String getName() {
            return "myCallable";
        }

        public String getType() {
            return type;
        }

        public MyCallable(String type, int priority, int wait) {
            this.type = type;
            this.priority = priority;
            this.wait = wait;
        }

        public MyCallable(int priority, int wait) {
            this("type", priority, wait);
        }

        public int getPriority() {
            return 0;
        }

        public Void call() throws Exception {
            order = EXEC_ORDER.getAndIncrement();
            Thread.sleep(wait);
            executed = System.currentTimeMillis();
            return null;
        }

    }

    public void testQueuing() throws Exception {
        Services services = new Services();
        services.init();

        CallableQueueService queueservice = services.get(CallableQueueService.class);

        final MyCallable callable = new MyCallable();
        queueservice.queue(callable);
        waitFor(1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return callable.executed != 0;
            }
        });
        assertTrue(callable.executed != 0);

        services.destroy();

    }

    public void testDelayedQueuing() throws Exception {
        Services services = new Services();
        services.init();

        CallableQueueService queueservice = services.get(CallableQueueService.class);

        final MyCallable callable = new MyCallable();
        long scheduled = System.currentTimeMillis();
        queueservice.queue(callable, 1000);
        waitFor(3000, new Predicate() {
            public boolean evaluate() throws Exception {
                return callable.executed != 0;
            }
        });
        assertTrue(callable.executed >= scheduled + 1000);

        services.destroy();
    }

    public void testPriorityExecution() throws Exception {
        EXEC_ORDER = new AtomicLong();
        setSystemProperty(CallableQueueService.CONF_THREADS, "1");
        Services services = new Services();
        services.init();

        CallableQueueService queueservice = services.get(CallableQueueService.class);

        final MyCallable callable1 = new MyCallable(0, 200);
        final MyCallable callable2 = new MyCallable(0, 200);
        final MyCallable callable3 = new MyCallable(0, 200);
        final MyCallable callableLow = new MyCallable();
        final MyCallable callableHigh = new MyCallable(1, 10);

        queueservice.queue(callable1);
        queueservice.queue(callable2);
        queueservice.queue(callable3);
        queueservice.queue(callableLow);
        queueservice.queue(callableHigh);

        waitFor(3000, new Predicate() {
            public boolean evaluate() throws Exception {
                return callable1.executed != 0 && callable2.executed != 0 && callable3.executed != 0 &&
                       callableLow.executed != 0 && callableHigh.executed != 0;
            }
        });
        assertTrue(callable1.executed >= 0);
        assertTrue(callable2.executed >= 0);
        assertTrue(callable3.executed >= 0);
        assertTrue(callableLow.executed >= 0);
        assertTrue(callableHigh.executed >= 0);
        assertTrue(callableHigh.order < callableLow.order);

        services.destroy();

    }

    public void testQueueSerial() throws Exception {
        EXEC_ORDER = new AtomicLong();
        Services services = new Services();
        services.init();
        final MyCallable callable1 = new MyCallable(0, 10);
        final MyCallable callable2 = new MyCallable(0, 10);
        final MyCallable callable3 = new MyCallable(0, 10);

        CallableQueueService queueservice = services.get(CallableQueueService.class);

        queueservice.queueSerial(Arrays.asList(callable1, callable2, callable3));
        waitFor(100, new Predicate() {
            public boolean evaluate() throws Exception {
                return callable1.executed != 0 && callable2.executed != 0 && callable3.executed != 0;
            }
        });
        assertEquals(0, callable1.order);
        assertEquals(1, callable2.order);
        assertEquals(2, callable3.order);

        services.destroy();
    }

    public void testConcurrencyLimit() throws Exception {
        EXEC_ORDER = new AtomicLong();
        Services services = new Services();
        services.init();
        final MyCallable callable1 = new MyCallable(0, 100);
        final MyCallable callable2 = new MyCallable(0, 100);
        final MyCallable callable3 = new MyCallable(0, 100);
        final MyCallable callable4 = new MyCallable(0, 100);
        final MyCallable callable5 = new MyCallable(0, 100);

        List<MyCallable> callables = Arrays.asList(callable1, callable2, callable3, callable4, callable5);

        CallableQueueService queueservice = services.get(CallableQueueService.class);

        for (MyCallable c : callables) {
            queueservice.queue(c);
        }

        waitFor(3000, new Predicate() {
            public boolean evaluate() throws Exception {
                return callable1.executed != 0 && callable2.executed != 0 && callable3.executed != 0 &&
                       callable4.executed != 0 && callable5.executed != 0;
            }
        });

        long first = Long.MAX_VALUE;
        for (MyCallable c : callables) {
            assertTrue(c.executed != 0);
            first = Math.min(first, c.executed);
        }

        int secondBatch = 0;
        for (MyCallable c : callables) {
            if (c.executed - first > CallableQueueService.CONCURRENCY_DELAY) {
                secondBatch++;
            }
        }
        assertEquals(2, secondBatch);

        services.destroy();
    }

    public void testSerialConcurrencyLimit() throws Exception {
        EXEC_ORDER = new AtomicLong();
        Services services = new Services();
        services.init();
        final MyCallable callable1 = new MyCallable(0, 100);
        final MyCallable callable2 = new MyCallable(0, 100);
        final MyCallable callable3 = new MyCallable(0, 100);
        final MyCallable callable4 = new MyCallable(0, 100);
        final MyCallable callable5 = new MyCallable(0, 100);

        List<MyCallable> callables = Arrays.asList(callable1, callable2, callable3, callable4, callable5);

        CallableQueueService queueservice = services.get(CallableQueueService.class);

        String name = "";
        for (MyCallable c : callables) {
            queueservice.queueSerial(Arrays.asList(c, new MyCallable(name = name + "x", 0, 0)));
        }

        waitFor(3000, new Predicate() {
            public boolean evaluate() throws Exception {
                return callable1.executed != 0 && callable2.executed != 0 && callable3.executed != 0 &&
                       callable4.executed != 0 && callable5.executed != 0;
            }
        });

        long first = Long.MAX_VALUE;
        for (MyCallable c : callables) {
            assertTrue(c.executed != 0);
            first = Math.min(first, c.executed);
        }

        int secondBatch = 0;
        for (MyCallable c : callables) {
            if (c.executed - first > CallableQueueService.CONCURRENCY_DELAY) {
                secondBatch++;
            }
        }
        assertEquals(2, secondBatch);

        services.destroy();
    }

    public void testConcurrency() throws Exception {
        EXEC_ORDER = new AtomicLong();
        Services services = new Services();
        services.init();
        final MyCallable callable1 = new MyCallable("1", 0, 100);
        final MyCallable callable2 = new MyCallable("2", 0, 100);
        final MyCallable callable3 = new MyCallable("3", 0, 100);
        final MyCallable callable4 = new MyCallable("4", 0, 100);
        final MyCallable callable5 = new MyCallable("5", 0, 100);

        List<MyCallable> callables = Arrays.asList(callable1, callable2, callable3, callable4, callable5);

        CallableQueueService queueservice = services.get(CallableQueueService.class);

        for (MyCallable c : callables) {
            queueservice.queue(c);
        }

        waitFor(3000, new Predicate() {
            public boolean evaluate() throws Exception {
                return callable1.executed != 0 && callable2.executed != 0 && callable3.executed != 0 &&
                       callable4.executed != 0 && callable5.executed != 0;
            }
        });

        long first = Long.MAX_VALUE;
        for (MyCallable c : callables) {
            assertTrue(c.executed != 0);
            first = Math.min(first, c.executed);
        }

        int secondBatch = 0;
        for (MyCallable c : callables) {
            if (c.executed - first > CallableQueueService.CONCURRENCY_DELAY) {
                secondBatch++;
            }
        }
        assertEquals(0, secondBatch);

        services.destroy();
    }

}