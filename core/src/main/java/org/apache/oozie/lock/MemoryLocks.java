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

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.Lock;
import org.apache.oozie.service.MemoryLocksService.Type;

import com.google.common.collect.MapMaker;

/**
 * In memory resource locking that provides READ/WRITE lock capabilities.
 */
public class MemoryLocks {

    final private ConcurrentMap<String, ReentrantReadWriteLock> locks = new MapMaker().weakValues().makeMap();

    /**
     * Implementation of {@link LockToken} for in memory locks.
     */
    class MemoryLockToken implements LockToken {
        private final ReentrantReadWriteLock lockEntry;
        private final Type type;

        public MemoryLockToken(ReentrantReadWriteLock lockEntry, Type type) {
            this.lockEntry = lockEntry;
            this.type = type;

        }

        /**
         * Release the lock.
         */
        @Override
        public void release() {
            switch (type) {
                case WRITE:
                    lockEntry.writeLock().unlock();
                    break;
                case READ:
                    lockEntry.readLock().unlock();
                    break;
            }
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
     * Obtain a lock for a source.
     *
     * @param resource resource name.
     * @param type lock type.
     * @param wait time out in milliseconds to wait for the lock, -1 means no timeout and 0 no wait.
     * @return the lock token for the resource, or <code>null</code> if the lock could not be obtained.
     * @throws InterruptedException thrown if the thread was interrupted while waiting.
     */
    public MemoryLockToken getLock(final String resource, Type type, long wait) throws InterruptedException {
        ReentrantReadWriteLock lockEntry = locks.get(resource);
        if (lockEntry == null) {
            ReentrantReadWriteLock newLock = new ReentrantReadWriteLock(true);
            lockEntry = locks.putIfAbsent(resource, newLock);
            if (lockEntry == null) {
                lockEntry = newLock;
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
        return new MemoryLockToken(lockEntry, type);
    }

    public ConcurrentMap<String, ReentrantReadWriteLock> getLockMap(){
        return locks;
    }
}
