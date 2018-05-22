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

import java.util.UUID;

import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.oozie.lock.LockToken;
import org.apache.oozie.lock.TestMemoryLocks;
import org.apache.oozie.service.ZKLocksService.ZKLockToken;
import org.apache.oozie.test.ZKXTestCase;
import org.apache.oozie.util.Locker;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.ZKUtils;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.data.Stat;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.TimeUnit;

public class TestZKLocksService extends ZKXTestCase {
    private XLog log = XLog.getLog(getClass());

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
        ZKLocksService zkls = new ZKLocksService();
        try {
            zkls.init(Services.get());
            assertEquals(1, ZKUtils.getUsers().size());
            assertEquals(zkls, ZKUtils.getUsers().iterator().next());
            zkls.destroy();
            assertEquals(0, ZKUtils.getUsers().size());
        }
        finally {
           zkls.destroy();
        }
    }

    public class ReadLocker extends Locker {
        private final ZKLocksService zkls;

        public ReadLocker(String name, int nameIndex, long timeout, StringBuffer buffer, ZKLocksService zkls) {
            super(name, nameIndex, timeout, buffer);
            this.zkls = zkls;
        }

        @Override
        protected ZKLocksService.ZKLockToken getLock() throws InterruptedException {
            return (ZKLocksService.ZKLockToken)zkls.getReadLock(name, timeout);
        }
    }

    public class WriteLocker extends Locker {
        private final ZKLocksService zkls;

        public WriteLocker(String name, int nameIndex, long timeout, StringBuffer buffer, ZKLocksService zkls) {
            super(name, nameIndex, timeout, buffer);
            this.zkls = zkls;
        }

        @Override
        protected ZKLocksService.ZKLockToken getLock() throws InterruptedException {
            return (ZKLocksService.ZKLockToken)zkls.getWriteLock(name, timeout);
        }
    }

    public void testWaitWriteLockThreads() throws Exception {
        ZKLocksService zkls = new ZKLocksService();
        try {
            zkls.init(Services.get());
            checkWaitWriteLock(zkls, zkls);
        }
        finally {
            zkls.destroy();
        }
    }

    public void testWaitWriteLockOozies() throws Exception {
        // Simulate having two different Oozies by using two different ZKLocksServices instead of using the same one in two threads
        ZKLocksService zkls1 = new ZKLocksService();
        ZKLocksService zkls2 = new ZKLocksService();
        try {
            zkls1.init(Services.get());
            zkls2.init(Services.get());
            checkWaitWriteLock(zkls1, zkls2);
        }
        finally {
            zkls1.destroy();
            zkls2.destroy();
        }
    }

    public void checkWaitWriteLock(ZKLocksService zkls1, ZKLocksService zkls2) throws Exception {
        StringBuffer sb = new StringBuffer("");
        Locker l1 = new WriteLocker("a", 1, -1, sb, zkls1);
        Locker l2 = new WriteLocker("a", 2, -1, sb, zkls2);

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

    public void testNoWaitWriteLockThreads() throws Exception {
        ZKLocksService zkls = new ZKLocksService();
        try {
            zkls.init(Services.get());
            checkNoWaitWriteLock(zkls, zkls);
        }
        finally {
            zkls.destroy();
        }
    }

    public void testNoWaitWriteLockOozies() throws Exception {
        // Simulate having two different Oozies by using two different ZKLocksServices instead of using the same one in two threads
        ZKLocksService zkls1 = new ZKLocksService();
        ZKLocksService zkls2 = new ZKLocksService();
        try {
            zkls1.init(Services.get());
            zkls2.init(Services.get());
            checkNoWaitWriteLock(zkls1, zkls2);
        }
        finally {
            zkls1.destroy();
            zkls2.destroy();
        }
    }

    public void checkNoWaitWriteLock(ZKLocksService zkls1, ZKLocksService zkls2) throws Exception {
        StringBuffer sb = new StringBuffer("");
        Locker l1 = new WriteLocker("a", 1, 0, sb, zkls1);
        Locker l2 = new WriteLocker("a", 2, 0, sb, zkls2);

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

    public void testTimeoutWaitingWriteLockThreads() throws Exception {
        ZKLocksService zkls = new ZKLocksService();
        try {
            zkls.init(Services.get());
            checkTimeoutWaitingWriteLock(zkls, zkls);
        }
        finally {
            zkls.destroy();
        }
    }

    public void testTimeoutWaitingWriteLockOozies() throws Exception {
        // Simulate having two different Oozies by using two different ZKLocksServices instead of using the same one in two threads
        ZKLocksService zkls1 = new ZKLocksService();
        ZKLocksService zkls2 = new ZKLocksService();
        try {
            zkls1.init(Services.get());
            zkls2.init(Services.get());
            checkTimeoutWaitingWriteLock(zkls1, zkls2);
        }
        finally {
            zkls1.destroy();
            zkls2.destroy();
        }
    }

    public void checkTimeoutWaitingWriteLock(ZKLocksService zkls1, ZKLocksService zkls2) throws Exception {
        StringBuffer sb = new StringBuffer("");
        Locker l1 = new WriteLocker("a", 1, 0, sb, zkls1);
        Locker l2 = new WriteLocker("a", 2, (long) (WAITFOR_RATIO * 2000), sb, zkls2);

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

    public void testTimeoutTimingOutWriteLockThreads() throws Exception {
        ZKLocksService zkls = new ZKLocksService();
        try {
            zkls.init(Services.get());
            checkTimeoutTimingOutWriteLock(zkls, zkls);
        }
        finally {
            zkls.destroy();
        }
    }

    public void testTimeoutTimingOutWriteLockOozies() throws Exception {
        // Simulate having two different Oozies by using two different ZKLocksServices instead of using the same one in two threads
        ZKLocksService zkls1 = new ZKLocksService();
        ZKLocksService zkls2 = new ZKLocksService();
        try {
            zkls1.init(Services.get());
            zkls2.init(Services.get());
            checkTimeoutTimingOutWriteLock(zkls1, zkls2);
        }
        finally {
            zkls1.destroy();
            zkls2.destroy();
        }
    }

    public void checkTimeoutTimingOutWriteLock(ZKLocksService zkls1, ZKLocksService zkls2) throws Exception {
        StringBuffer sb = new StringBuffer("");
        Locker l1 = new WriteLocker("a", 1, 0, sb, zkls1);
        Locker l2 = new WriteLocker("a", 2, 50, sb, zkls2);

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

    public void testReadLockThreads() throws Exception {
        ZKLocksService zkls = new ZKLocksService();
        try {
            zkls.init(Services.get());
            checkReadLock(zkls, zkls);
        }
        finally {
            zkls.destroy();
        }
    }

    public void testReadLockOozies() throws Exception {
        // Simulate having two different Oozies by using two different ZKLocksServices instead of using the same one in two threads
        ZKLocksService zkls1 = new ZKLocksService();
        ZKLocksService zkls2 = new ZKLocksService();
        try {
            zkls1.init(Services.get());
            zkls2.init(Services.get());
            checkReadLock(zkls1, zkls2);
        }
        finally {
            zkls1.destroy();
            zkls2.destroy();
        }
    }

    public void checkReadLock(ZKLocksService zkls1, ZKLocksService zkls2) throws Exception {
        StringBuffer sb = new StringBuffer("");
        Locker l1 = new ReadLocker("a", 1, -1, sb, zkls1);
        Locker l2 = new ReadLocker("a", 2, -1, sb, zkls2);

        new Thread(l1).start();
        l1.awaitLockAcquire();
        new Thread(l2).start();
        l2.awaitLockAcquire();

        l1.proceed();
        l1.awaitTermination();

        l2.proceed();
        l2.awaitTermination();

        assertEquals("a:1-L a:2-L a:1-U a:2-U", sb.toString().trim());
    }

    public void testReadWriteLockThreads() throws Exception {
        ZKLocksService zkls = new ZKLocksService();
        try {
            zkls.init(Services.get());
            checkReadWriteLock(zkls, zkls);
        }
        finally {
            zkls.destroy();
        }
    }

    public void testReadWriteLockOozies() throws Exception {
        // Simulate having two different Oozies by using two different ZKLocksServices instead of using the same one in two threads
        ZKLocksService zkls1 = new ZKLocksService();
        ZKLocksService zkls2 = new ZKLocksService();
        try {
            zkls1.init(Services.get());
            zkls2.init(Services.get());
            checkReadWriteLock(zkls1, zkls2);
        }
        finally {
            zkls1.destroy();
            zkls2.destroy();
        }
    }

    public void checkReadWriteLock(ZKLocksService zkls1, ZKLocksService zkls2) throws Exception {
        StringBuffer sb = new StringBuffer("");
        Locker l1 = new ReadLocker("a", 1, -1, sb, zkls1);
        Locker l2 = new WriteLocker("a", 2, -1, sb, zkls2);

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

    public void testWriteReadLockThreads() throws Exception {
        ZKLocksService zkls = new ZKLocksService();
        try {
            zkls.init(Services.get());
            checkWriteReadLock(zkls, zkls);
        }
        finally {
            zkls.destroy();
        }
    }

    public void testWriteReadLockOozies() throws Exception {
        // Simulate having two different Oozies by using two different ZKLocksServices instead of using the same one in two threads
        ZKLocksService zkls1 = new ZKLocksService();
        ZKLocksService zkls2 = new ZKLocksService();
        try {
            zkls1.init(Services.get());
            zkls2.init(Services.get());
            checkWriteReadLock(zkls1, zkls2);
        }
        finally {
            zkls1.destroy();
            zkls2.destroy();
        }
    }

    public void checkWriteReadLock(ZKLocksService zkls1, ZKLocksService zkls2) throws Exception {
        StringBuffer sb = new StringBuffer("");
        Locker l1 = new WriteLocker("a", 1, -1, sb, zkls1);
        Locker l2 = new ReadLocker("a", 2, -1, sb, zkls2);

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

    public void testLockRelease() throws ServiceException, InterruptedException {
        final String path = UUID.randomUUID().toString();
        ZKLocksService zkls = new ZKLocksService();
        try {
            zkls.init(Services.get());
            ZKLockToken lock = (ZKLockToken) zkls.getWriteLock(path, TestMemoryLocks.DEFAULT_LOCK_TIMEOUT);
            assertTrue(zkls.getLocks().containsKey(path));
            lock.release();
            checkLockRelease(path, zkls);
        }
        finally {
            zkls.destroy();
        }
    }

    public void testReentrantMultipleCall() throws ServiceException, InterruptedException {
        final String path = UUID.randomUUID().toString();
        ZKLocksService zkls = new ZKLocksService();
        try {
            zkls.init(Services.get());
            ZKLockToken lock = (ZKLockToken) zkls.getWriteLock(path, TestMemoryLocks.DEFAULT_LOCK_TIMEOUT);
            lock = (ZKLockToken) zkls.getWriteLock(path, TestMemoryLocks.DEFAULT_LOCK_TIMEOUT);
            lock = (ZKLockToken) zkls.getWriteLock(path, TestMemoryLocks.DEFAULT_LOCK_TIMEOUT);
            assertTrue(zkls.getLocks().containsKey(path));
            lock.release();
            assertTrue(zkls.getLocks().containsKey(path));
            lock.release();
            assertTrue(zkls.getLocks().containsKey(path));
            lock.release();
            checkLockRelease(path, zkls);
        }
        catch (Exception e) {
            fail("Reentrant property, it should have acquired lock");
        }
        finally {
            zkls.destroy();
        }
    }

    public void testReentrantMultipleThread() throws ServiceException, InterruptedException {
        final String path = UUID.randomUUID().toString();
        final ZKLocksService zkls = new ZKLocksService();
        zkls.init(Services.get());
        try {
            ThreadLock t1 = new ThreadLock(zkls, path);
            ThreadLock t2 = new ThreadLock(zkls, path);
            t1.start();
            t1.join();
            checkLockRelease(path, zkls);
            t2.start();
            t2.join();
            checkLockRelease(path, zkls);
        }
        finally {
            zkls.destroy();
        }
    }

    public void testLockReaper() throws Exception {
        ConfigurationService.set(ZKLocksService.REAPING_THRESHOLD, "1");
        ZKLocksService zkls = new ZKLocksService();

        try {
            zkls.init(Services.get());
            for (int i = 0; i < 10; ++i) {
                LockToken l = zkls.getReadLock(String.valueOf(i), 1);
                l.release();
            }

            waitFor(10000, new Predicate() {
                @Override
                public boolean evaluate() throws Exception {
                    Stat stat = getClient().checkExists().forPath(ZKLocksService.LOCKS_NODE);
                    return stat.getNumChildren() == 0;
                }
            });

            Stat stat = getClient().checkExists().forPath(ZKLocksService.LOCKS_NODE);
            assertEquals(0, stat.getNumChildren());
        }
        finally {
            zkls.destroy();
        }
    }

    public void testLocksAreGarbageCollected() throws ServiceException, InterruptedException {
        String path = new String("a");
        String path1 = new String("a");
        ZKLocksService lockService = new ZKLocksService();
        try {
            lockService.init(Services.get());
            LockToken lock = lockService.getWriteLock(path, TestMemoryLocks.DEFAULT_LOCK_TIMEOUT);
            lock.release();
            assertEquals(lockService.getLocks().size(), 1);
            int oldHash = lockService.getLocks().get(path).hashCode();
            lock = lockService.getWriteLock(path1, TestMemoryLocks.DEFAULT_LOCK_TIMEOUT);
            int newHash = lockService.getLocks().get(path1).hashCode();
            assertTrue(oldHash == newHash);
            lock = null;
            System.gc();
            lock = lockService.getWriteLock(path, TestMemoryLocks.DEFAULT_LOCK_TIMEOUT);
            newHash = lockService.getLocks().get(path).hashCode();
            assertFalse(oldHash == newHash);
        }
        finally {
            lockService.destroy();
        }
    }

    public void testLocksAreReused() throws ServiceException, InterruptedException {
        String path = "a";
        ZKLocksService lockService = new ZKLocksService();
        try {
            lockService.init(Services.get());
            LockToken lock = lockService.getWriteLock(path, TestMemoryLocks.DEFAULT_LOCK_TIMEOUT);
            int oldHash = System.identityHashCode(lockService.getLocks().get(path));
            System.gc();
            lock.release();
            lock = lockService.getWriteLock(path, TestMemoryLocks.DEFAULT_LOCK_TIMEOUT);
            assertEquals(lockService.getLocks().size(), 1);
            int newHash = System.identityHashCode(lockService.getLocks().get(path));
            assertTrue(oldHash == newHash);
        }
        finally {
            lockService.destroy();
        }
    }

    public void testRetriableRelease() throws Exception {
        final String path = UUID.randomUUID().toString();
        ZKLocksService zkls = new ZKLocksService();
        try{
            zkls.init(Services.get());

            InterProcessReadWriteLock lockEntry = Mockito.mock(InterProcessReadWriteLock.class);
            final InterProcessMutex writeLock = Mockito.mock(InterProcessMutex.class);
            Mockito.when(lockEntry.writeLock()).thenReturn(writeLock);
            Mockito.doThrow(new ConnectionLossException()).when(writeLock).release();
            Mockito.doNothing().when(writeLock).acquire();

            // put mocked lockEntry
            zkls.getLocks().putIfAbsent(path,lockEntry);

            LockToken lock = zkls.getWriteLock(path, -1);

            final boolean [] lockReleased = new boolean [] {false};
            Runnable exceptionStopper = new Runnable() {
                @Override
                public void run() {
                    try {
                        // Stop the exception on release() after some time in other thread
                        Thread.sleep(TimeUnit.SECONDS.toMillis(13));
                        Mockito.doAnswer(new Answer<Void>() {
                            @Override
                            public Void answer(InvocationOnMock invocation) throws Throwable {
                                lockReleased[0] = true;
                                return null;
                            }
                        }).when(writeLock).release();
                    }
                    catch (Exception e) {
                        log.error(e);
                        fail("Test case failed due to " + e.getMessage());
                    }
                }
            };
            new Thread(exceptionStopper).start();
            //Try to release the lock
            lock.release();
            assertEquals("Failing the test case. The lock should have been released", true, lockReleased[0]);
        }
        finally {
            zkls.destroy();
        }
    }


    private void checkLockRelease(String path, ZKLocksService zkls) {
        if (zkls.getLocks().get(path) == null) {
            // good, lock is removed from memory after gc.
        }
        else {
            assertFalse(zkls.getLocks().get(path).writeLock().isAcquiredInThisProcess());
        }
    }

    static class ThreadLock extends Thread {
        ZKLocksService zkls;
        String path;
        LockToken lock = null;

        public ThreadLock(ZKLocksService zkls, String path) {
            this.zkls = zkls;
            this.path = path;

        }

        public void run() {
            try {
                lock = zkls.getWriteLock(path, TestMemoryLocks.DEFAULT_LOCK_TIMEOUT);
                if (lock != null) {
                    lock = zkls.getWriteLock(path, TestMemoryLocks.DEFAULT_LOCK_TIMEOUT);
                    Thread.sleep(1000);
                    lock.release();
                    Thread.sleep(1000);
                    lock.release();
                }
            }
            catch (InterruptedException e) {
            }
        }
    }
}