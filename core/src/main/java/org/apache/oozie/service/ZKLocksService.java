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

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.Instrumentable;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.lock.LockToken;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.ZKUtils;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.curator.framework.recipes.locks.ChildReaper;
import org.apache.curator.framework.recipes.locks.Reaper;
import org.apache.curator.utils.ThreadUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.MapMaker;
import org.apache.zookeeper.KeeperException;

/**
 * Service that provides distributed locks via ZooKeeper.  Requires that a ZooKeeper ensemble is available.  The locks will be
 * located under a ZNode named "locks" under the namespace (see {@link ZKUtils}).  For example, with default settings, if the
 * resource we're locking is called "foo", then the ZNode backing the lock will be at /oozie/locks/foo.
 */
public class ZKLocksService extends MemoryLocksService implements Service, Instrumentable {

    private ZKUtils zk;
    public static final String LOCKS_NODE = "/locks";

    private static final XLog LOG = XLog.getLog(ZKLocksService.class);
    private final ConcurrentMap<String, InterProcessReadWriteLock> zkLocks = new MapMaker().weakValues().makeMap();
    private ChildReaper reaper = null;

    private static final String REAPING_LEADER_PATH = ZKUtils.ZK_BASE_SERVICES_PATH + "/locksChildReaperLeaderPath";
    static final String REAPING_THRESHOLD = CONF_PREFIX + "ZKLocksService.locks.reaper.threshold";
    static final String REAPING_THREADS = CONF_PREFIX + "ZKLocksService.locks.reaper.threads";
    private static final String RELEASE_RETRY_TIME_LIMIT_MINUTES = CONF_PREFIX + "ZKLocksService.lock.release.retry.time.limit"
            + ".minutes";

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
            reaper = new ChildReaper(zk.getClient(), LOCKS_NODE, Reaper.Mode.REAP_UNTIL_GONE, getExecutorService(),
                    ConfigurationService.getInt(services.getConf(), REAPING_THRESHOLD) * 1000, REAPING_LEADER_PATH);
            reaper.start();
        }
        catch (Exception ex) {
            throw new ServiceException(ErrorCode.E1700, ex.getMessage(), ex);
        }
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
        instr.addVariable(INSTRUMENTATION_GROUP, "locks", new Instrumentation.Variable<Integer>() {
            @Override
            public Integer getValue() {
                return zkLocks.size();
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
        return acquireLock(resource, Type.READ, wait);
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
        return acquireLock(resource, Type.WRITE, wait);
    }

    private LockToken acquireLock(final String resource, final Type type, final long wait) throws InterruptedException {
        LOG.debug("Acquiring ZooKeeper lock. [resource={};type={};wait={}]", resource, type, wait);

        InterProcessReadWriteLock lockEntry;
        final String zkPath = LOCKS_NODE + "/" + resource;
        LOG.debug("Checking existing Curator lock or creating new one. [zkPath={}]", zkPath);

        // Creating a Curator InterProcessReadWriteLock is lightweight - only calling acquire() costs real ZooKeeper calls
        final InterProcessReadWriteLock newLockEntry = new InterProcessReadWriteLock(zk.getClient(), zkPath);
        final InterProcessReadWriteLock existingLockEntry = zkLocks.putIfAbsent(resource, newLockEntry);
        if (existingLockEntry == null) {
            lockEntry = newLockEntry;
            LOG.debug("No existing Curator lock present, new one created successfully. [zkPath={}]", zkPath);
        }
        else {
            // We can't destoy newLockEntry and we don't have to - it's taken care of by Curator and JVM GC
            lockEntry = existingLockEntry;
            LOG.debug("Reusing existing Curator lock. [zkPath={}]", zkPath);
        }

        ZKLockToken token = null;
        try {
            LOG.debug("Calling Curator to acquire ZooKeeper lock. [resource={};type={};wait={}]", resource, type, wait);
            final InterProcessMutex lock = (type.equals(Type.READ)) ? lockEntry.readLock() : lockEntry.writeLock();
            if (wait == -1) {
                lock.acquire();
                token = new ZKLockToken(lockEntry, type);
                LOG.debug("ZooKeeper lock acquired successfully. [resource={};type={}]", resource, type);
            }
            else if (lock.acquire(wait, TimeUnit.MILLISECONDS)) {
                token = new ZKLockToken(lockEntry, type);
                LOG.debug("ZooKeeper lock acquired successfully waiting. [resource={};type={};wait={}]", resource, type, wait);
            }
            else {
                LOG.warn("Could not acquire ZooKeeper lock, timed out. [resource={};type={};wait={}]", resource, type, wait);
            }
        }
        catch (final Exception ex) {
            //Not throwing exception. Should return null, so that command can be requeued
            LOG.warn("Could not acquire lock due to a ZooKeeper error. " +
                    "[ex={};resource={};type={};wait={}]", ex, resource, type, wait);
            LOG.error("Error while acquiring lock", ex);
        }

        return token;
    }

    /**
     * Implementation of {@link LockToken} for zookeeper locks.
     */
    class ZKLockToken implements LockToken {
        private final InterProcessReadWriteLock lockEntry;
        private final Type type;

        private ZKLockToken(InterProcessReadWriteLock lockEntry, Type type) {
            this.lockEntry = lockEntry;
            this.type = type;
        }

        /**
         * Release the lock.
         */
        @Override
        public void release() {
            try {
                retriableRelease();
            }
            catch (Exception ex) {
                LOG.warn("Could not release lock: " + ex.getMessage(), ex);
            }
        }

        /**
         * Retires on failure to release lock
         *
         * @throws InterruptedException
         */
        private void retriableRelease() throws Exception {
            long retryTimeLimit = TimeUnit.MINUTES.toSeconds(ConfigurationService.getLong(RELEASE_RETRY_TIME_LIMIT_MINUTES, 30));
            int sleepSeconds = 10;
            for(int retryCount = 1; retryTimeLimit>=0; retryTimeLimit -= sleepSeconds, retryCount++) {
                try {
                    switch (type) {
                        case WRITE:
                            lockEntry.writeLock().release();
                            break;
                        case READ:
                            lockEntry.readLock().release();
                            break;
                    }
                    break;
                }
                catch (KeeperException.ConnectionLossException ex) {
                    LOG.warn("Could not release lock: " + ex.getMessage() + ". Retry will be after " + sleepSeconds + " seconds",
                            ex);
                    Thread.sleep(TimeUnit.SECONDS.toMillis(sleepSeconds));
                    LOG.info("Retrying to release lock. Retry number=" + retryCount);
                }
            }
        }
    }

    @VisibleForTesting
    public ConcurrentMap<String, InterProcessReadWriteLock> getLocks(){
        return zkLocks;
    }

    private static ScheduledExecutorService getExecutorService() {
        return ThreadUtils.newFixedThreadScheduledPool(ConfigurationService.getInt(REAPING_THREADS),
                "ZKLocksChildReaper");
    }

}
