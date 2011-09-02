/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.service;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.XCallable;

public class TestCallableQueueService extends XTestCase {
    static AtomicLong EXEC_ORDER = new AtomicLong();

    public class MyCallable implements XCallable<Void> {
        String type;
        int priority;
        long executed = 0;
        int wait;
        long order;
        long created = System.currentTimeMillis();
        private String name = "myCallable";
        private String key = null;

        public MyCallable() {
            this(0, 0);
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getType() {
            return type;
        }

        public MyCallable(String type, int priority, int wait) {
            this.type = type;
            this.priority = priority;
            this.wait = wait;
            this.key = name + "_" + UUID.randomUUID();
        }

        public MyCallable(String key, String type, int priority, int wait) {
            this.type = type;
            this.priority = priority;
            this.wait = wait;
            this.key = key;
        }

        public MyCallable(int priority, int wait) {
            this("type", priority, wait);
        }

        @Override
        public int getPriority() {
            return this.priority;
        }

        @Override
        public long getCreatedTime() {
            return created;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Type:").append(getType());
            sb.append(",Priority:").append(getPriority());
            return sb.toString();
        }

        public Void call() throws Exception {
            order = EXEC_ORDER.getAndIncrement();
            Thread.sleep(wait);
            executed = System.currentTimeMillis();
            return null;
        }

        @Override
        public String getKey() {
            return this.key;
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

    public static class CLCallable implements XCallable<Void> {

        @Override
        public String getName() {
            return "name";
        }

        @Override
        public int getPriority() {
            return 0;
        }

        @Override
        public String getType() {
            return "type";
        }
        
		@Override
		public String getKey() {
			return "name" + "_" + UUID.randomUUID();
		}

        @Override
        public long getCreatedTime() {
            return 0;
        }

        @Override
        public Void call() throws Exception {
            incr();
            Thread.sleep(100);
            decr();
            return null;
        }

        private static int counter;
        private static int max;

        private synchronized void incr() {
            counter++;
            max = Math.max(max, counter);
        }

        private synchronized void decr() {
            counter--;
        }

        public static int getConcurrency() {
            return max;
        }

        public static void resetConcurrency() {
            max = 0;
        }

    }

    public void testConcurrencyLimit() throws Exception {
        Services services = new Services();
        services.init();

        CLCallable.resetConcurrency();
        final CallableQueueService queueservice = services.get(CallableQueueService.class);

        for (int i = 0; i < 10; i++) {
            queueservice.queue(new CLCallable(), 10);
        }

        waitFor(2000, new Predicate() {
            public boolean evaluate() throws Exception {
                return queueservice.queueSize() == 0;
            }
        });

        System.out.println("Callable Queue Size :" + queueservice.queueSize());
        System.out.println("CLCallable Concurrency :" + CLCallable.getConcurrency());

        assertTrue(CLCallable.getConcurrency() <= 3);

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

        String type = "";
        for (MyCallable c : callables) {
            queueservice.queueSerial(Arrays.asList(c, new MyCallable(type = type + "x", 0, 0)));
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

    public void testQueueUniquenessWithSameKey() throws Exception {
        EXEC_ORDER = new AtomicLong();
        Services services = new Services();
        services.init();
        final MyCallable callable1 = new MyCallable("key", "1", 0, 100);
        final MyCallable callable2 = new MyCallable("key", "2", 0, 100);
        final MyCallable callable3 = new MyCallable("key", "3", 0, 100);

        List<MyCallable> callables = Arrays.asList(callable1, callable2, callable3);

        CallableQueueService queueservice = services.get(CallableQueueService.class);

        for (MyCallable c : callables) {
            queueservice.queue(c);
        }

        waitFor(200, new Predicate() {
            public boolean evaluate() throws Exception {
                return callable1.executed != 0 && callable2.executed == 0 && callable3.executed == 0;
            }
        });

        assertTrue(callable1.executed != 0);
        assertTrue(callable2.executed == 0);
        assertTrue(callable3.executed == 0);

        services.destroy();
    }

    public void testQueueUniquenessWithSameKeyInComposite() throws Exception {
        EXEC_ORDER = new AtomicLong();
        Services services = new Services();
        services.init();
        final MyCallable callable1 = new MyCallable("key", "1", 0, 100);
        final MyCallable callable2 = new MyCallable("key", "2", 0, 100);
        final MyCallable callable3 = new MyCallable("key", "3", 0, 100);

        List<MyCallable> callables = Arrays.asList(callable1, callable2, callable3);

        CallableQueueService queueservice = services.get(CallableQueueService.class);

        String type = "";
        for (MyCallable c : callables) {
            queueservice.queueSerial(Arrays.asList(c, new MyCallable(type = type + "x", 0, 0)));
        }

        waitFor(200, new Predicate() {
            public boolean evaluate() throws Exception {
                return callable1.executed != 0 && callable2.executed == 0 && callable3.executed == 0;
            }
        });

        assertTrue(callable1.executed != 0);
        assertTrue(callable2.executed == 0);
        assertTrue(callable3.executed == 0);

        services.destroy();
    }

    public void testQueueUniquenessWithSameKeyInOneComposite() throws Exception {
        EXEC_ORDER = new AtomicLong();
        Services services = new Services();
        services.init();
        final MyCallable callable1 = new MyCallable("key", "1", 0, 100);
        final MyCallable callable2 = new MyCallable("key", "2", 0, 100);
        final MyCallable callable3 = new MyCallable("key", "3", 0, 100);

        CallableQueueService queueservice = services.get(CallableQueueService.class);

        queueservice.queueSerial(Arrays.asList(callable1, callable2, callable3));

        waitFor(200, new Predicate() {
            public boolean evaluate() throws Exception {
                return callable1.executed != 0 && callable2.executed == 0 && callable3.executed == 0;
            }
        });

        assertTrue(callable1.executed != 0);
        assertTrue(callable2.executed == 0);
        assertTrue(callable3.executed == 0);

        services.destroy();
    }

    public void testQueueUniquenessWithDiffKey() throws Exception {
        EXEC_ORDER = new AtomicLong();
        Services services = new Services();
        services.init();
        final MyCallable callable1 = new MyCallable("key1", "1", 0, 100);
        final MyCallable callable2 = new MyCallable("key2", "2", 0, 100);
        final MyCallable callable3 = new MyCallable("key3", "3", 0, 100);

        List<MyCallable> callables = Arrays.asList(callable1, callable2, callable3);

        CallableQueueService queueservice = services.get(CallableQueueService.class);

        for (MyCallable c : callables) {
            queueservice.queue(c);
        }

        waitFor(200, new Predicate() {
            public boolean evaluate() throws Exception {
                return callable1.executed != 0 && callable2.executed != 0 && callable3.executed != 0;
            }
        });

        assertTrue(callable1.executed != 0);
        assertTrue(callable2.executed != 0);
        assertTrue(callable3.executed != 0);

        services.destroy();
    }

    public void testQueueUniquenessWithDiffKeyInComposite() throws Exception {
        EXEC_ORDER = new AtomicLong();
        Services services = new Services();
        services.init();
        final MyCallable callable1 = new MyCallable("key1", "1", 0, 100);
        final MyCallable callable2 = new MyCallable("key2", "2", 0, 100);
        final MyCallable callable3 = new MyCallable("key3", "3", 0, 100);

        List<MyCallable> callables = Arrays.asList(callable1, callable2, callable3);

        CallableQueueService queueservice = services.get(CallableQueueService.class);

        String type = "";
        for (MyCallable c : callables) {
            queueservice.queueSerial(Arrays.asList(c, new MyCallable(type = type + "x", 0, 0)));
        }

        waitFor(200, new Predicate() {
            public boolean evaluate() throws Exception {
                return callable1.executed != 0 && callable2.executed != 0 && callable3.executed != 0;
            }
        });

        assertTrue(callable1.executed != 0);
        assertTrue(callable2.executed != 0);
        assertTrue(callable3.executed != 0);

        services.destroy();
    }

    public void testQueueUniquenessWithDiffKeyInOneComposite() throws Exception {
        EXEC_ORDER = new AtomicLong();
        Services services = new Services();
        services.init();
        final MyCallable callable1 = new MyCallable("key1", "1", 0, 100);
        final MyCallable callable2 = new MyCallable("key2", "2", 0, 100);
        final MyCallable callable3 = new MyCallable("key3", "3", 0, 100);

        CallableQueueService queueservice = services.get(CallableQueueService.class);

        queueservice.queueSerial(Arrays.asList(callable1, callable2, callable3));

        waitFor(200, new Predicate() {
            public boolean evaluate() throws Exception {
                return callable1.executed != 0 && callable2.executed != 0 && callable3.executed != 0;
            }
        });

        assertTrue(callable1.executed != 0);
        assertTrue(callable2.executed != 0);
        assertTrue(callable3.executed != 0);

        services.destroy();
    }

}
