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

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.curator.framework.recipes.locks.ChildReaper;
import org.apache.curator.framework.recipes.locks.Reaper;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.utils.ThreadUtils;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.Instrumentable;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.lock.LockToken;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.ZKUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * Service that provides distributed locks via ZooKeeper.  Requires that a ZooKeeper ensemble is available.  The locks will be
 * located under a ZNode named "locks" under the namespace (see {@link ZKUtils}).  For example, with default settings, if the
 * resource we're locking is called "foo", then the ZNode backing the lock will be at /oozie/locks/foo.
 * <p>
 * ChildReaper is used for deleting unused locks. Only one childreaper will be active in cluster.
 * ZK Path /oozie.zookeeper.namespace/services/locksChildReaperLeaderPath is used for leader selection.
 */

public class ZKLocksService extends MemoryLocksService implements Service, Instrumentable {

    private ZKUtils zk;
    private static XLog LOG = XLog.getLog(ZKLocksService.class);
    public static final String LOCKS_NODE = "/locks";
    private final AtomicLong lockCount = new AtomicLong();

    private static final String REAPING_LEADER_PATH = ZKUtils.ZK_BASE_SERVICES_PATH + "/locksChildReaperLeaderPath";
    public static final int DEFAULT_REAPING_THRESHOLD = 300; // In sec
    public static final String REAPING_THRESHOLD = CONF_PREFIX + "ZKLocksService.locks.reaper.threshold";
    public static final String REAPING_THREADS = CONF_PREFIX + "ZKLocksService.locks.reaper.threads";
    private ChildReaper reaper = null;

    /**
     * Initialize the zookeeper locks service
     *
     * @param services services instance.
     */
    @Override
    public void init(Services services) throws ServiceException {
        super.init(services);
        try {
            zk = ZKUtils.register(this);
            reaper = new ChildReaper(zk.getClient(), LOCKS_NODE, Reaper.Mode.REAP_INDEFINITELY, getExecutorService(),
                    services.getConf().getInt(REAPING_THRESHOLD, DEFAULT_REAPING_THRESHOLD) * 1000, REAPING_LEADER_PATH);
            reaper.start();
        }
        catch (Exception ex) {
            throw new ServiceException(ErrorCode.E1700, ex.getMessage(), ex);
        }
        lockCount.set(0);
    }

    /**
     * Destroy the zookeeper locks service.
     */
    @Override
    public void destroy() {
        if (reaper != null) {
            try {
                reaper.close();
            }
            catch (IOException e) {
                LOG.error("Error closing childReaper", e);
            }
        }

        if (zk != null) {
            zk.unregister(this);
        }
        zk = null;
        super.destroy();
    }

    /**
     * Instruments the zookeeper locks service.
     *
     * @param instr instance to instrument the memory locks service to.
     */
    @Override
    public void instrument(Instrumentation instr) {
        // Similar to MemoryLocksService's instrumentation, though this is only the number of locks this Oozie server currently has
        instr.addVariable(INSTRUMENTATION_GROUP, "locks", new Instrumentation.Variable<Long>() {
            @Override
            public Long getValue() {
                return lockCount.get();
            }
        });
    }

    /**
     * Obtain a READ lock for a source.
     *
     * @param resource resource name.
     * @param wait time out in milliseconds to wait for the lock, -1 means no timeout and 0 no wait.
     * @return the lock token for the resource, or <code>null</code> if the lock could not be obtained.
     * @throws InterruptedException thrown if the thread was interrupted while waiting.
     */
    @Override
    public LockToken getReadLock(String resource, long wait) throws InterruptedException {
        InterProcessReadWriteLock lock = new InterProcessReadWriteLock(zk.getClient(), LOCKS_NODE + "/" + resource);
        InterProcessMutex readLock = lock.readLock();
        return acquireLock(wait, readLock);
    }

    /**
     * Obtain a WRITE lock for a source.
     *
     * @param resource resource name.
     * @param wait time out in milliseconds to wait for the lock, -1 means no timeout and 0 no wait.
     * @return the lock token for the resource, or <code>null</code> if the lock could not be obtained.
     * @throws InterruptedException thrown if the thread was interrupted while waiting.
     */
    @Override
    public LockToken getWriteLock(String resource, long wait) throws InterruptedException {
        InterProcessReadWriteLock lock = new InterProcessReadWriteLock(zk.getClient(), LOCKS_NODE + "/" + resource);
        InterProcessMutex writeLock = lock.writeLock();
        return acquireLock(wait, writeLock);
    }

    private LockToken acquireLock(long wait, InterProcessMutex lock) {
        ZKLockToken token = null;
        try {
            if (wait == -1) {
                lock.acquire();
                token = new ZKLockToken(lock);
            }
            else if (lock.acquire(wait, TimeUnit.MILLISECONDS)) {
                token = new ZKLockToken(lock);
            }
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return token;
    }

    /**
     * Implementation of {@link LockToken} for zookeeper locks.
     */
    class ZKLockToken implements LockToken {
        private final InterProcessMutex lock;

        private ZKLockToken(InterProcessMutex lock) {
            this.lock = lock;
            lockCount.incrementAndGet();
        }

        /**
         * Release the lock.
         */
        @Override
        public void release() {
            try {
                lock.release();
                lockCount.decrementAndGet();
            }
            catch (Exception ex) {
                LOG.warn("Could not release lock: " + ex.getMessage(), ex);
            }
        }
    }

    private static ScheduledExecutorService getExecutorService() {
        return ThreadUtils.newFixedThreadScheduledPool(Services.get().getConf().getInt(REAPING_THREADS, 2),
                "ZKLocksChildReaper");
    }

}
