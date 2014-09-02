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

import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.Lock;

/**
 * In memory resource locking that provides READ/WRITE lock capabilities.
 */
public class MemoryLocks {
    final private HashMap<String, ReentrantReadWriteLock> locks = new HashMap<String, ReentrantReadWriteLock>();

    private static enum Type {
        READ, WRITE
    }

    /**
     * Implementation of {@link LockToken} for in memory locks.
     */
    class MemoryLockToken implements LockToken {
        private final ReentrantReadWriteLock rwLock;
        private final java.util.concurrent.locks.Lock lock;
        private final String resource;

        private MemoryLockToken(ReentrantReadWriteLock rwLock, java.util.concurrent.locks.Lock lock, String resource) {
            this.rwLock = rwLock;
            this.lock = lock;
            this.resource = resource;
        }

        /**
         * Release the lock.
         */
        @Override
        public void release() {
            int val = rwLock.getQueueLength();
            if (val == 0) {
                synchronized (locks) {
                    locks.remove(resource);
                }
            }
            lock.unlock();
        }
    }

    /**
     * Return the number of active locks.
     *
     * @return the number of active locks.
     */
    public int size() {
        return locks.size();
    }

    /**
     * Obtain a READ lock for a source.
     *
     * @param resource resource name.
     * @param wait time out in milliseconds to wait for the lock, -1 means no timeout and 0 no wait.
     * @return the lock token for the resource, or <code>null</code> if the lock could not be obtained.
     * @throws InterruptedException thrown if the thread was interrupted while waiting.
     */
    public MemoryLockToken getReadLock(String resource, long wait) throws InterruptedException {
        return getLock(resource, Type.READ, wait);
    }

    /**
     * Obtain a WRITE lock for a source.
     *
     * @param resource resource name.
     * @param wait time out in milliseconds to wait for the lock, -1 means no timeout and 0 no wait.
     * @return the lock token for the resource, or <code>null</code> if the lock could not be obtained.
     * @throws InterruptedException thrown if the thread was interrupted while waiting.
     */
    public MemoryLockToken getWriteLock(String resource, long wait) throws InterruptedException {
        return getLock(resource, Type.WRITE, wait);
    }

    private MemoryLockToken getLock(String resource, Type type, long wait) throws InterruptedException {
        ReentrantReadWriteLock lockEntry;
        synchronized (locks) {
            if (locks.containsKey(resource)) {
                lockEntry = locks.get(resource);
            }
            else {
                lockEntry = new ReentrantReadWriteLock(true);
                locks.put(resource, lockEntry);
            }
        }

        Lock lock = (type.equals(Type.READ)) ? lockEntry.readLock() : lockEntry.writeLock();

        if (wait == -1) {
            lock.lock();
        }
        else {
            if (wait > 0) {
                if (!lock.tryLock(wait, TimeUnit.MILLISECONDS)) {
                    return null;
                }
            }
            else {
                if (!lock.tryLock()) {
                    return null;
                }
            }
        }
        synchronized (locks) {
            if (!locks.containsKey(resource)) {
                locks.put(resource, lockEntry);
            }
        }
        return new MemoryLockToken(lockEntry, lock, resource);
    }
}
