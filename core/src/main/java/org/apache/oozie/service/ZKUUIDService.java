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

import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.lock.LockToken;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.ZKUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * Service that provides distributed job id sequence via ZooKeeper.  Requires that a ZooKeeper ensemble is available.
 * The sequence path will be located under a ZNode named "job_id_sequence" under the namespace (see {@link ZKUtils}).
 * The sequence will be reset to 0, once max is reached.
 */

public class ZKUUIDService extends UUIDService {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "ZKUUIDService.";

    public static final String CONF_SEQUENCE_MAX = CONF_PREFIX + "jobid.sequence.max";

    public static final String ZK_SEQUENCE_PATH = "job_id_sequence";

    public static final long RESET_VALUE = 0l;
    public static final int RETRY_COUNT = 3;

    private final static XLog LOG = XLog.getLog(ZKUUIDService.class);

    private ZKUtils zk;
    private static Long maxSequence =  9999990l;

    DistributedAtomicLong atomicIdGenerator;

    @Override
    public void init(Services services) throws ServiceException {

        super.init(services);
        try {
            zk = ZKUtils.register(this);
            atomicIdGenerator = new DistributedAtomicLong(zk.getClient(), ZK_SEQUENCE_PATH, ZKUtils.getRetryPloicy());
        }
        catch (Exception ex) {
            throw new ServiceException(ErrorCode.E1700, ex.getMessage(), ex);
        }

    }

    /**
     * Gets the unique id.
     *
     * @return the id
     * @throws Exception the exception
     */
    public long getID() {
        return getZKId(0);
    }

    @SuppressWarnings("finally")
    private long getZKId(int retryCount) {
        if (atomicIdGenerator == null) {
            throw new RuntimeException("Sequence generator can't be null. Path : " + ZK_SEQUENCE_PATH);
        }
        AtomicValue<Long> value = null;
        try {
            value = atomicIdGenerator.increment();
        }
        catch (Exception e) {
            throw new RuntimeException("Exception incrementing UID for session ", e);
        }
        finally {
            if (value != null && value.succeeded()) {
                if (value.preValue() >= maxSequence) {
                    if (retryCount >= RETRY_COUNT) {
                        throw new RuntimeException("Can't reset sequence. Tried " + retryCount + " times");
                    }
                    resetSequence();
                    return getZKId(retryCount + 1);
                }
                return value.preValue();
            }
            else {
                throw new RuntimeException("Exception incrementing UID for session ");
            }
        }

    }

    /**
     * Once sequence is reached limit, reset to 0.
     */
    private void resetSequence() {
        synchronized (ZKUUIDService.class) {
            try {
                // Double check if sequence is already reset.
                AtomicValue<Long> value = atomicIdGenerator.get();
                if (value.succeeded()) {
                    if (value.postValue() < maxSequence) {
                        return;
                    }
                }
                else {
                    throw new RuntimeException("Can't reset sequence");
                }
                // Acquire ZK lock, so that other host doesn't reset sequence.
                LockToken lock = Services.get().get(MemoryLocksService.class)
                        .getWriteLock(ZKUUIDService.class.getName(), lockTimeout);
                try {
                    if (lock == null) {
                        LOG.info("Lock is held by other system, returning");
                        return;
                    }
                    else {
                        value = atomicIdGenerator.get();
                        if (value.succeeded()) {
                            if (value.postValue() < maxSequence) {
                                return;
                            }
                        }
                        else {
                            throw new RuntimeException("Can't reset sequence");
                        }
                        atomicIdGenerator.forceSet(RESET_VALUE);
                        resetStartTime();
                    }
                }
                finally {
                    if (lock != null) {
                        lock.release();
                    }
                }
            }
            catch (Exception e) {
                throw new RuntimeException("Can't reset sequence", e);
            }
        }
    }

    @Override
    public void destroy() {
        if (zk != null) {
            zk.unregister(this);
        }
        zk = null;
        super.destroy();
    }

    @VisibleForTesting
    public void setMaxSequence(long sequence) {
        maxSequence = sequence;
    }
}
