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

package org.apache.oozie.lock;

import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.XLog;

public class TestMemoryLocks extends XTestCase {
    private XLog log = XLog.getLog(getClass());

    private MemoryLocks locks;

    protected void setUp() throws Exception {
        super.setUp();
        locks = new MemoryLocks();
    }

    protected void tearDown() throws Exception {
        locks = null;
        super.tearDown();
    }

    public abstract class Locker implements Runnable {
        protected String name;
        private String nameIndex;
        private StringBuffer sb;
        protected long timeout;


        public Locker(String name, int nameIndex, long timeout, StringBuffer buffer) {
            this.name = name;
            this.nameIndex = name + ":" + nameIndex;
            this.sb = buffer;
            this.timeout = timeout;
        }

        public void run() {
            try {
                log.info("Getting lock [{0}]", nameIndex);
                MemoryLocks.MemoryLockToken token = getLock();
                if (token != null) {
                    log.info("Got lock [{0}]", nameIndex);
                    sb.append(nameIndex + "-L ");
                    synchronized (this) {
                        wait();
                    }
                    sb.append(nameIndex + "-U ");
                    token.release();
                    log.info("Release lock [{0}]", nameIndex);
                }
                else {
                    sb.append(nameIndex + "-N ");
                    log.info("Did not get lock [{0}]", nameIndex);
                }
            }
            catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        public void finish() {
            synchronized (this) {
                notify();
            }
        }

        protected abstract MemoryLocks.MemoryLockToken getLock() throws InterruptedException;


    }

    public class ReadLocker extends Locker {

        public ReadLocker(String name, int nameIndex, long timeout, StringBuffer buffer) {
            super(name, nameIndex, timeout, buffer);
        }

        protected MemoryLocks.MemoryLockToken getLock() throws InterruptedException {
            return locks.getReadLock(name, timeout);
        }
    }

    public class WriteLocker extends Locker {

        public WriteLocker(String name, int nameIndex, long timeout, StringBuffer buffer) {
            super(name, nameIndex, timeout, buffer);
        }

        protected MemoryLocks.MemoryLockToken getLock() throws InterruptedException {
            return locks.getWriteLock(name, timeout);
        }
    }

    public void testWaitWriteLock() throws Exception {
        StringBuffer sb = new StringBuffer("");
        Locker l1 = new WriteLocker("a", 1, -1, sb);
        Locker l2 = new WriteLocker("a", 2, -1, sb);

        new Thread(l1).start();
        Thread.sleep(500);
        new Thread(l2).start();
        Thread.sleep(500);
        l1.finish();
        Thread.sleep(500);
        l2.finish();
        Thread.sleep(500);
        assertEquals("a:1-L a:1-U a:2-L a:2-U", sb.toString().trim());
    }

    public void testNoWaitWriteLock() throws Exception {
        StringBuffer sb = new StringBuffer("");
        Locker l1 = new WriteLocker("a", 1, 0, sb);
        Locker l2 = new WriteLocker("a", 2, 0, sb);

        new Thread(l1).start();
        Thread.sleep(500);
        new Thread(l2).start();
        Thread.sleep(500);
        l1.finish();
        Thread.sleep(500);
        l2.finish();
        Thread.sleep(500);
        assertEquals("a:1-L a:2-N a:1-U", sb.toString().trim());
    }

    public void testTimeoutWaitingWriteLock() throws Exception {
        StringBuffer sb = new StringBuffer("");
        Locker l1 = new WriteLocker("a", 1, 0, sb);
        Locker l2 = new WriteLocker("a", 2, 1000, sb);

        new Thread(l1).start();
        Thread.sleep(500);
        new Thread(l2).start();
        Thread.sleep(500);
        l1.finish();
        Thread.sleep(500);
        l2.finish();
        Thread.sleep(500);
        assertEquals("a:1-L a:1-U a:2-L a:2-U", sb.toString().trim());
    }

    public void testTimeoutTimingOutWriteLock() throws Exception {
        StringBuffer sb = new StringBuffer("");
        Locker l1 = new WriteLocker("a", 1, 0, sb);
        Locker l2 = new WriteLocker("a", 2, 50, sb);

        new Thread(l1).start();
        Thread.sleep(500);
        new Thread(l2).start();
        Thread.sleep(500);
        l1.finish();
        Thread.sleep(500);
        l2.finish();
        Thread.sleep(500);
        assertEquals("a:1-L a:2-N a:1-U", sb.toString().trim());
    }

    public void testReadLock() throws Exception {
        StringBuffer sb = new StringBuffer("");
        Locker l1 = new ReadLocker("a", 1, -1, sb);
        Locker l2 = new ReadLocker("a", 2, -1, sb);

        new Thread(l1).start();
        Thread.sleep(500);
        new Thread(l2).start();
        Thread.sleep(500);
        l1.finish();
        Thread.sleep(500);
        l2.finish();
        Thread.sleep(500);
        assertEquals("a:1-L a:2-L a:1-U a:2-U", sb.toString().trim());
    }

    public void testReadWriteLock() throws Exception {
        StringBuffer sb = new StringBuffer("");
        Locker l1 = new ReadLocker("a", 1, -1, sb);
        Locker l2 = new WriteLocker("a", 2, -1, sb);

        new Thread(l1).start();
        Thread.sleep(500);
        new Thread(l2).start();
        Thread.sleep(500);
        l1.finish();
        Thread.sleep(500);
        l2.finish();
        Thread.sleep(500);
        assertEquals("a:1-L a:1-U a:2-L a:2-U", sb.toString().trim());
    }

    public void testWriteReadLock() throws Exception {
        StringBuffer sb = new StringBuffer("");
        Locker l1 = new WriteLocker("a", 1, -1, sb);
        Locker l2 = new ReadLocker("a", 2, -1, sb);

        new Thread(l1).start();
        Thread.sleep(500);
        new Thread(l2).start();
        Thread.sleep(500);
        l1.finish();
        Thread.sleep(500);
        l2.finish();
        Thread.sleep(500);
        assertEquals("a:1-L a:1-U a:2-L a:2-U", sb.toString().trim());
    }

}
