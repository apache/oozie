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

import java.util.UUID;

import org.apache.oozie.service.MemoryLocksService;
import org.apache.oozie.service.MemoryLocksService.Type;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.LockerCoordinator;
import org.apache.oozie.util.Locker;
import org.apache.oozie.util.XLog;

public class TestMemoryLocks extends XTestCase {
    private XLog log = XLog.getLog(getClass());
    public static final int DEFAULT_LOCK_TIMEOUT = 5 * 1000;

    private MemoryLocks locks;

    protected void setUp() throws Exception {
        super.setUp();
        locks = new MemoryLocks();
    }

    protected void tearDown() throws Exception {
        locks = null;
        super.tearDown();
    }


    public class ReadLocker extends Locker {

        public ReadLocker(String name, int nameIndex, long timeout, StringBuffer buffer) {
            super(name, nameIndex, timeout, buffer);
        }

        @Override
        protected MemoryLocks.MemoryLockToken getLock() throws InterruptedException {
            return locks.getLock(name, Type.READ, timeout);
        }
    }

    public class WriteLocker extends Locker {

        public WriteLocker(String name, int nameIndex, long timeout, StringBuffer buffer) {
            super(name, nameIndex, timeout, buffer);
        }

        @Override
        protected MemoryLocks.MemoryLockToken getLock() throws InterruptedException {
            return locks.getLock(name, Type.WRITE, timeout);
        }
    }

    public void testWaitWriteLock() throws Exception {
        StringBuffer sb = new StringBuffer("");
        Locker l1 = new WriteLocker("a", 1, -1, sb);
        Locker l2 = new WriteLocker("a", 2, -1, sb);

        new Thread(l1).start();
        l1.awaitLockAcquire();

        new Thread(l2).start();
        l2.awaitStart();

        l1.proceed();
        l2.proceed();

        l1.awaitTermination();
        l2.awaitTermination();

        assertEquals("a:1-L a:1-U a:2-L a:2-U", sb.toString().trim());
    }

    public void testNoWaitWriteLock() throws Exception {
        StringBuffer sb = new StringBuffer("");
        Locker l1 = new WriteLocker("a", 1, 0, sb);
        Locker l2 = new WriteLocker("a", 2, 0, sb);

        new Thread(l1).start();
        l1.awaitLockAcquire();

        new Thread(l2).start();
        l2.awaitStart();

        l2.proceed();
        l2.awaitTermination();

        l1.proceed();
        l1.awaitTermination();

        assertEquals("a:1-L a:2-N a:1-U", sb.toString().trim());
    }

    public void testTimeoutWaitingWriteLock() throws Exception {
        StringBuffer sb = new StringBuffer("");
        Locker l1 = new WriteLocker("a", 1, 0, sb);
        Locker l2 = new WriteLocker("a", 2, 10000, sb);

        new Thread(l1).start();
        l1.awaitLockAcquire();

        new Thread(l2).start();
        l2.awaitStart();

        l1.proceed();
        l1.awaitTermination();

        l2.proceed();
        l2.awaitTermination();

        assertEquals("a:1-L a:1-U a:2-L a:2-U", sb.toString().trim());
    }

    public void testTimeoutTimingOutWriteLock() throws Exception {
        StringBuffer sb = new StringBuffer("");
        Locker l1 = new WriteLocker("a", 1, 0, sb);
        Locker l2 = new WriteLocker("a", 2, 50, sb);

        new Thread(l1).start();
        l1.awaitLockAcquire();

        new Thread(l2).start();
        l2.awaitStart();

        l2.proceed();
        l2.awaitTermination();  // L2 will time out after 50ms

        l1.proceed();
        l1.awaitTermination();

        assertEquals("a:1-L a:2-N a:1-U", sb.toString().trim());
    }

    public void testReadLock() throws Exception {
        StringBuffer sb = new StringBuffer("");
        Locker l1 = new ReadLocker("a", 1, -1, sb);
        Locker l2 = new ReadLocker("a", 2, -1, sb);

        new Thread(l1).start();
        l1.awaitLockAcquire();  // L1 is holding a readlock

        new Thread(l2).start();
        l2.awaitLockAcquire();  // both L1 & L2 are holding a readlock

        l1.proceed();
        l1.awaitTermination();

        l2.proceed();
        l2.awaitTermination();

        assertEquals("a:1-L a:2-L a:1-U a:2-U", sb.toString().trim());
    }

    public void testReadWriteLock() throws Exception {
        StringBuffer sb = new StringBuffer("");
        Locker l1 = new ReadLocker("a", 1, -1, sb);
        Locker l2 = new WriteLocker("a", 2, -1, sb);

        new Thread(l1).start();
        l1.awaitLockAcquire();

        new Thread(l2).start();
        l2.awaitStart();

        l1.proceed();
        l1.awaitTermination();

        l2.proceed();
        l2.awaitTermination();

        assertEquals("a:1-L a:1-U a:2-L a:2-U", sb.toString().trim());
    }

    public void testWriteReadLock() throws Exception {
        StringBuffer sb = new StringBuffer("");
        Locker l1 = new WriteLocker("a", 1, -1, sb);
        Locker l2 = new ReadLocker("a", 2, -1, sb);

        new Thread(l1).start();
        l1.awaitLockAcquire();

        new Thread(l2).start();
        l2.awaitStart();

        l1.proceed();
        l1.awaitTermination();

        l2.proceed();
        l2.awaitTermination();

        assertEquals("a:1-L a:1-U a:2-L a:2-U", sb.toString().trim());
    }

    public class SameThreadWriteLocker implements Runnable {
        protected String name;
        private String nameIndex;
        private StringBuffer sb;
        protected long timeout;
        private final LockerCoordinator coordinator = new LockerCoordinator();

        public SameThreadWriteLocker(String name, int nameIndex, long timeout, StringBuffer buffer) {
            this.name = name;
            this.nameIndex = name + ":" + nameIndex;
            this.sb = buffer;
            this.timeout = timeout;
        }

        public void run() {
            try {
                coordinator.startDone();
                log.info("Getting lock [{0}]", nameIndex);
                MemoryLocks.MemoryLockToken token = getLock();
                MemoryLocks.MemoryLockToken token2 = getLock();

                if (token != null) {
                    log.info("Got lock [{0}]", nameIndex);
                    sb.append(nameIndex + "-L1 ");
                    if (token2 != null) {
                        sb.append(nameIndex + "-L2 ");
                    }
                    sb.append(nameIndex + "-U1 ");

                    coordinator.lockAcquireDone();
                    coordinator.awaitContinueSignal();

                    token.release();
                    sb.append(nameIndex + "-U2 ");
                    token2.release();
                    log.info("Release lock [{0}]", nameIndex);
                }
                else {
                    coordinator.awaitContinueSignal();
                    sb.append(nameIndex + "-N ");
                    log.info("Did not get lock [{0}]", nameIndex);
                }
                coordinator.terminated();
            }
            catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        public void awaitLockAcquire() throws InterruptedException {
            coordinator.awaitLockAcquire();
        }

        public void awaitStart() throws InterruptedException {
            coordinator.awaitStart();
        }

        public void proceed() {
            coordinator.signalLockerContinue();
        }

        public void awaitTermination() throws InterruptedException {
            coordinator.awaitTermination();
        }

        protected MemoryLocks.MemoryLockToken getLock() throws InterruptedException {
            return locks.getLock(name, Type.WRITE, timeout);
        }
    }

    public void testWriteLockSameThreadNoWait() throws Exception {
        StringBuffer sb = new StringBuffer("");
        SameThreadWriteLocker l1 = new SameThreadWriteLocker("a", 1, 0, sb);
        Locker l2 = new WriteLocker("a", 2, 0, sb);

        new Thread(l1).start();
        l1.awaitLockAcquire();

        new Thread(l2).start();
        l1.awaitStart();

        l2.proceed();
        l2.awaitTermination();

        l1.proceed();
        l1.awaitTermination();

        assertEquals("a:1-L1 a:1-L2 a:1-U1 a:2-N a:1-U2", sb.toString().trim());
    }

    public void testWriteLockSameThreadWait() throws Exception {
        StringBuffer sb = new StringBuffer("");
        SameThreadWriteLocker l1 = new SameThreadWriteLocker("a", 1, 0, sb);
        Locker l2 = new WriteLocker("a", 2, 10000, sb);

        new Thread(l1).start();
        l1.awaitLockAcquire();

        new Thread(l2).start();
        l1.awaitStart();

        l1.proceed();
        l1.awaitTermination();

        l2.proceed();
        l2.awaitTermination();

        assertEquals("a:1-L1 a:1-L2 a:1-U1 a:1-U2 a:2-L a:2-U", sb.toString().trim());
    }

    public void testLockReentrant() throws ServiceException, InterruptedException {
        final String path = UUID.randomUUID().toString();
        MemoryLocksService lockService = new MemoryLocksService();
        try {
            lockService.init(Services.get());
            LockToken lock = lockService.getWriteLock(path, DEFAULT_LOCK_TIMEOUT);
            lock = (LockToken) lockService.getWriteLock(path, DEFAULT_LOCK_TIMEOUT);
            lock = (LockToken) lockService.getWriteLock(path, DEFAULT_LOCK_TIMEOUT);
            assertEquals(lockService.getMemoryLocks().size(), 1);
            lock.release();
            assertEquals(lockService.getMemoryLocks().size(), 1);
            lock.release();
            assertEquals(lockService.getMemoryLocks().size(), 1);
            lock.release();
            checkLockRelease(path, lockService);
        }
        catch (Exception e) {
            fail("Reentrant property, it should have acquired lock");
        }
        finally {
            lockService.destroy();
        }
    }

    public void testLocksAreGarbageCollected() throws ServiceException, InterruptedException {
        String path = new String("a");
        String path1 = new String("a");
        MemoryLocksService lockService = new MemoryLocksService();
        lockService.init(Services.get());
        LockToken lock = lockService.getWriteLock(path, DEFAULT_LOCK_TIMEOUT);
        int oldHash = lockService.getMemoryLocks().getLockMap().get(path).hashCode();
        lock.release();
        lock = lockService.getWriteLock(path1, DEFAULT_LOCK_TIMEOUT);
        int newHash = lockService.getMemoryLocks().getLockMap().get(path1).hashCode();
        assertTrue(oldHash == newHash);
        lock.release();
        lock = null;
        System.gc();
        path = "a";
        lock = lockService.getWriteLock(path, DEFAULT_LOCK_TIMEOUT);
        newHash = lockService.getMemoryLocks().getLockMap().get(path).hashCode();
        assertFalse(oldHash == newHash);

    }

    public void testLocksAreReused() throws ServiceException, InterruptedException {
        String path = "a";
        MemoryLocksService lockService = new MemoryLocksService();
        lockService.init(Services.get());
        LockToken lock = lockService.getWriteLock(path, DEFAULT_LOCK_TIMEOUT);
        int oldHash = System.identityHashCode(lockService.getMemoryLocks().getLockMap().get(path));
        System.gc();
        lock.release();
        lock = lockService.getWriteLock(path, DEFAULT_LOCK_TIMEOUT);
        assertEquals(lockService.getMemoryLocks().size(), 1);
        int newHash = System.identityHashCode(lockService.getMemoryLocks().getLockMap().get(path));
        assertTrue(oldHash == newHash);
    }

    private void checkLockRelease(String path, MemoryLocksService lockService) {
        if (lockService.getMemoryLocks().getLockMap().get(path) == null) {
            // good lock is removed from memory after gc.
        }
        else {
            assertFalse(lockService.getMemoryLocks().getLockMap().get(path).isWriteLocked());
        }
    }

}
