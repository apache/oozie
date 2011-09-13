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
package org.apache.oozie.util;

import junit.framework.TestCase;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.oozie.util.PriorityDelayQueue.QueueElement;

public class TestPriorityDelayQueue extends TestCase {

    public static class DPriorityDelayQueue<E> extends PriorityDelayQueue<E> {

        //for tests debugging purposes
        public DPriorityDelayQueue(int priorities, long maxWait, TimeUnit unit, int maxSize) {
            super(priorities, maxWait, unit, maxSize);
        }

        protected void debug(String template, Object... args) {
            System.out.println(MessageFormat.format(template, args));
        }

    }

    public void testQueueElement() throws Exception {
        Object obj = new Object();

        try {
            new PriorityDelayQueue.QueueElement<Object>(null);
            fail();
        }
        catch (IllegalArgumentException ex) {
        }

        try {
            new PriorityDelayQueue.QueueElement<Object>(null, 0, 0, TimeUnit.MILLISECONDS);
            fail();
        }
        catch (IllegalArgumentException ex) {
        }

        try {
            new PriorityDelayQueue.QueueElement<Object>(obj, -1, 0, TimeUnit.MILLISECONDS);
            fail();
        }
        catch (IllegalArgumentException ex) {
        }

        try {
            new PriorityDelayQueue.QueueElement<Object>(obj, 0, -1, TimeUnit.MILLISECONDS);
            fail();
        }
        catch (IllegalArgumentException ex) {
        }

        PriorityDelayQueue.QueueElement<Object> e1 = new PriorityDelayQueue.QueueElement<Object>(obj);
        assertEquals(obj, e1.getElement());
        assertEquals(0, e1.getPriority());
        assertTrue(e1.getDelay(TimeUnit.MILLISECONDS) <= 0);

        e1 = new PriorityDelayQueue.QueueElement<Object>(obj, 1, 200, TimeUnit.MILLISECONDS);
        assertEquals(obj, e1.getElement());
        assertEquals(1, e1.getPriority());
        assertTrue(e1.getDelay(TimeUnit.MILLISECONDS) <= 200);
        assertTrue(e1.getDelay(TimeUnit.MILLISECONDS) >= 100);
        Thread.sleep(300);
        assertTrue(e1.getDelay(TimeUnit.MILLISECONDS) <= 0);

        PriorityDelayQueue.QueueElement<Object> e2 = new PriorityDelayQueue.QueueElement<Object>(obj);

        assertTrue(e1.compareTo(e2) < 0);
    }

    public void testQueueConstructor() throws Exception {
        try {
            new PriorityDelayQueue<Integer>(0, 1000, TimeUnit.MILLISECONDS, -1);
            fail();
        }
        catch (IllegalArgumentException ex) {
        }

        try {
            new PriorityDelayQueue<Integer>(1, 1000, TimeUnit.MILLISECONDS, 0);
            fail();
        }
        catch (IllegalArgumentException ex) {
        }

        try {
            new PriorityDelayQueue<Integer>(1, 1000, TimeUnit.MILLISECONDS, -2);
            fail();
        }
        catch (IllegalArgumentException ex) {
        }

        try {
            new PriorityDelayQueue<Integer>(1, 0, TimeUnit.MILLISECONDS, 0);
            fail();
        }
        catch (IllegalArgumentException ex) {
        }
    }

    public void testBoundUnboundQueueSize() {
        PriorityDelayQueue<Integer> q = new PriorityDelayQueue<Integer>(1, 1000, TimeUnit.MILLISECONDS, -1);
        assertEquals(1, q.getPriorities());
        assertEquals(-1, q.getMaxSize());
        assertEquals(1000, q.getMaxWait(TimeUnit.MILLISECONDS));
        assertEquals(0, q.size());
        assertTrue(q.offer(new PriorityDelayQueue.QueueElement<Integer>(1)));
        assertEquals(1, q.size());
        assertTrue(q.offer(new PriorityDelayQueue.QueueElement<Integer>(1)));
        assertEquals(2, q.size());
        assertTrue(q.offer(new PriorityDelayQueue.QueueElement<Integer>(1)));
        assertEquals(3, q.size());

        q = new PriorityDelayQueue<Integer>(1, 1000, TimeUnit.MILLISECONDS, 1);
        assertEquals(1, q.getMaxSize());
        assertEquals(0, q.size());
        assertTrue(q.offer(new PriorityDelayQueue.QueueElement<Integer>(1)));
        assertEquals(1, q.size());
        assertFalse(q.offer(new PriorityDelayQueue.QueueElement<Integer>(1)));
        assertEquals(1, q.size());
        assertNotNull(q.poll());
        assertEquals(0, q.size());
        assertTrue(q.offer(new PriorityDelayQueue.QueueElement<Integer>(1)));
        assertEquals(1, q.size());
    }

    public void testPoll() throws Exception {
        PriorityDelayQueue<Integer> q = new PriorityDelayQueue<Integer>(3, 500, TimeUnit.MILLISECONDS, -1);

        //test immediate offer polling

        q.offer(new PriorityDelayQueue.QueueElement<Integer>(1));
        assertEquals((Integer) 1, q.poll().getElement());
        assertEquals(0, q.size());

        //test delayed offer polling

        q.offer(new PriorityDelayQueue.QueueElement<Integer>(2, 0, 10, TimeUnit.MILLISECONDS));
        assertNull(q.poll());

        Thread.sleep(11);

        assertEquals((Integer) 2, q.poll().getElement());
        assertEquals(0, q.size());

        //test different priorities immediate offer polling

        q.offer(new PriorityDelayQueue.QueueElement<Integer>(10, 0, 0, TimeUnit.MILLISECONDS));
        q.offer(new PriorityDelayQueue.QueueElement<Integer>(30, 2, 0, TimeUnit.MILLISECONDS));
        q.offer(new PriorityDelayQueue.QueueElement<Integer>(20, 1, 0, TimeUnit.MILLISECONDS));

        assertEquals((Integer) 30, q.poll().getElement());
        assertEquals((Integer) 20, q.poll().getElement());
        assertEquals((Integer) 10, q.poll().getElement());
        assertEquals(0, q.size());

        //test different priorities equal delayed offer polling

        q.offer(new PriorityDelayQueue.QueueElement<Integer>(10, 0, 10, TimeUnit.MILLISECONDS));
        q.offer(new PriorityDelayQueue.QueueElement<Integer>(30, 2, 10, TimeUnit.MILLISECONDS));
        q.offer(new PriorityDelayQueue.QueueElement<Integer>(20, 1, 10, TimeUnit.MILLISECONDS));

        Thread.sleep(11);

        List<Integer> list = new ArrayList<Integer>();
        while (list.size() != 3) {
            QueueElement<Integer> e = q.poll();
            if (e != null) {
                list.add(e.getElement());
            }
        }
        assertEquals((Integer) 30, list.get(0));
        assertEquals((Integer) 20, list.get(1));
        assertEquals((Integer) 10, list.get(2));
        assertEquals(0, q.size());

        //test different priorities different delayed offer polling after delay

        q.offer(new PriorityDelayQueue.QueueElement<Integer>(10, 0, 10, TimeUnit.MILLISECONDS));
        q.offer(new PriorityDelayQueue.QueueElement<Integer>(30, 2, 20, TimeUnit.MILLISECONDS));
        q.offer(new PriorityDelayQueue.QueueElement<Integer>(20, 1, 0, TimeUnit.MILLISECONDS));

        Thread.sleep(21);

        list = new ArrayList<Integer>();
        while (list.size() != 3) {
            QueueElement<Integer> e = q.poll();
            if (e != null) {
                list.add(e.getElement());
            }
        }
        assertEquals((Integer) 30, list.get(0));
        assertEquals((Integer) 20, list.get(1));
        assertEquals((Integer) 10, list.get(2));
        assertEquals(0, q.size());

        //test different priorities different delayed offer polling within delay

        long start = System.currentTimeMillis();
        q.offer(new PriorityDelayQueue.QueueElement<Integer>(10, 0, 100, TimeUnit.MILLISECONDS));
        q.offer(new PriorityDelayQueue.QueueElement<Integer>(30, 2, 200, TimeUnit.MILLISECONDS));
        q.offer(new PriorityDelayQueue.QueueElement<Integer>(20, 1, 0, TimeUnit.MILLISECONDS));

        assertEquals((Integer) 20, q.poll().getElement());
        long delay = System.currentTimeMillis() - start;
        Thread.sleep(101 - delay);
        assertEquals((Integer) 10, q.poll().getElement());

        start = System.currentTimeMillis();
        delay = System.currentTimeMillis() - start;

        Thread.sleep(101 - delay);
        assertEquals((Integer) 30, q.poll().getElement());

        assertEquals(0, q.size());
    }

    public void testPeek() throws Exception {
        PriorityDelayQueue<Integer> q = new PriorityDelayQueue<Integer>(3, 500, TimeUnit.MILLISECONDS, -1);

        //test immediate offer peeking

        q.offer(new PriorityDelayQueue.QueueElement<Integer>(1));
        assertEquals((Integer) 1, q.peek().getElement());
        q.poll();
        assertEquals(0, q.size());

        //test delay offer peeking

        q.offer(new PriorityDelayQueue.QueueElement<Integer>(1, 1, 10, TimeUnit.MILLISECONDS));
        assertEquals((Integer) 1, q.peek().getElement());
        Thread.sleep(11);
        assertNotNull(q.poll());
        assertEquals(0, q.size());

        //test different priorities immediate offer peeking

        q.offer(new PriorityDelayQueue.QueueElement<Integer>(10, 0, 0, TimeUnit.MILLISECONDS));
        q.offer(new PriorityDelayQueue.QueueElement<Integer>(30, 2, 0, TimeUnit.MILLISECONDS));
        q.offer(new PriorityDelayQueue.QueueElement<Integer>(20, 1, 0, TimeUnit.MILLISECONDS));

        assertEquals((Integer) 30, q.peek().getElement());
        assertNotNull(q.poll());
        assertEquals((Integer) 20, q.peek().getElement());
        assertNotNull(q.poll());
        assertEquals((Integer) 10, q.peek().getElement());
        assertNotNull(q.poll());
        assertEquals(0, q.size());

        //test different priorities delayed offer peeking

        q.offer(new PriorityDelayQueue.QueueElement<Integer>(30, 2, 200, TimeUnit.MILLISECONDS));
        q.offer(new PriorityDelayQueue.QueueElement<Integer>(10, 0, 100, TimeUnit.MILLISECONDS));
        q.offer(new PriorityDelayQueue.QueueElement<Integer>(20, 1, 150, TimeUnit.MILLISECONDS));

        assertEquals((Integer) 10, q.peek().getElement());
        Thread.sleep(100);
        assertNotNull(q.poll());
        assertEquals((Integer) 20, q.peek().getElement());
        Thread.sleep(50);
        assertNotNull(q.poll());
        assertEquals((Integer) 30, q.peek().getElement());
        Thread.sleep(50);
        assertNotNull(q.poll());
        assertEquals(0, q.size());
    }

    public void testAntiStarvation() throws Exception {
        PriorityDelayQueue<Integer> q = new PriorityDelayQueue<Integer>(3, 500, TimeUnit.MILLISECONDS, -1);
        q.offer(new PriorityDelayQueue.QueueElement<Integer>(1));
        q.peek();
        assertEquals(1, q.sizes()[0]);
        Thread.sleep(600);
        q.peek();
        assertEquals(1, q.sizes()[1]);
        Thread.sleep(600);
        q.peek();
        assertEquals(1, q.sizes()[2]);
    }

    public void testConcurrency() throws Exception {
        final int threads = 5;
        final AtomicInteger counter = new AtomicInteger(threads);
        final int priorities = 5;
        final PriorityDelayQueue<String> queue =
                new PriorityDelayQueue<String>(priorities, 100, TimeUnit.MILLISECONDS, -1);

        for (int i = 0; i < threads; i++) {
            final int count = i;
            new Thread(new Runnable() {
                public void run() {
                    for (int j = 0; j < 10; j++) {
                        String msg = count + " - " + j;
                        try {
                            queue.offer(new PriorityDelayQueue.QueueElement<String>(msg,
                                        (int) (Math.random() * priorities),
                                        (int) (Math.random() * 500), TimeUnit.MILLISECONDS));
                            Thread.sleep((int) (Math.random() * 50));
                        }
                        catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                        counter.decrementAndGet();
                    }
                }
            }).start();
        }

        while (counter.get() > 0) {
            while (queue.poll() != null) {
            }
            Thread.sleep(10);
        }
        while (queue.size() > 0) {
            while (queue.poll() != null) {
            }
            Thread.sleep(10);
        }
    }

    public void testIterator() throws Exception {
        PriorityDelayQueue<Integer> q = new PriorityDelayQueue<Integer>(3, 500, TimeUnit.MILLISECONDS, -1);

        q.offer(new PriorityDelayQueue.QueueElement<Integer>(1, 1, 10, TimeUnit.MILLISECONDS));
        q.offer(new PriorityDelayQueue.QueueElement<Integer>(10, 0, 0, TimeUnit.MILLISECONDS));
        q.offer(new PriorityDelayQueue.QueueElement<Integer>(30, 2, 0, TimeUnit.MILLISECONDS));
        q.offer(new PriorityDelayQueue.QueueElement<Integer>(20, 1, 0, TimeUnit.MILLISECONDS));

        Iterator<PriorityDelayQueue.QueueElement<Integer>> it = q.iterator();
        assertTrue(it.hasNext());

        int size = 0;
        while (it.hasNext()) {
            it.next();
            size++;
        }
        assertEquals(4, size);

        assertNotNull(q.poll());
        assertNotNull(q.poll());

        q.offer(new PriorityDelayQueue.QueueElement<Integer>(40, 0, 0, TimeUnit.MILLISECONDS));
        q.offer(new PriorityDelayQueue.QueueElement<Integer>(50, 2, 0, TimeUnit.MILLISECONDS));
        q.offer(new PriorityDelayQueue.QueueElement<Integer>(60, 1, 0, TimeUnit.MILLISECONDS));

        assertEquals(5, q.size());
        assertNotNull(q.poll());

        it = q.iterator();
        Thread.sleep(50);
        size = 0;
        while (it.hasNext()) {
            it.next();
            size++;
        }
        assertEquals(4, size);
    }

    public void testClear() {
        PriorityDelayQueue<Integer> q = new PriorityDelayQueue<Integer>(3, 500, TimeUnit.MILLISECONDS, -1);
        q.offer(new PriorityDelayQueue.QueueElement<Integer>(1, 1, 10, TimeUnit.MILLISECONDS));
        q.offer(new PriorityDelayQueue.QueueElement<Integer>(10, 0, 0, TimeUnit.MILLISECONDS));
        assertEquals(2, q.size());
        q.clear();
        assertEquals(0, q.size());
    }

}
