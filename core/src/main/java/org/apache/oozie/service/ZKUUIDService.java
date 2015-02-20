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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.atomic.PromotedToLock;
import org.apache.curator.retry.RetryNTimes;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.lock.LockToken;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.ZKUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * Service that provides distributed job id sequence via ZooKeeper. Requires that a ZooKeeper ensemble is available. The
 * sequence path will be located under a ZNode named "job_id_sequence" under the namespace (see {@link ZKUtils}). The
 * sequence will be reset to 0, once max is reached.
 */

public class ZKUUIDService extends UUIDService {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "ZKUUIDService.";

    public static final String CONF_SEQUENCE_MAX = CONF_PREFIX + "jobid.sequence.max";
    public static final String LOCKS_NODE = "/SEQUENCE_LOCK";

    public static final String ZK_SEQUENCE_PATH = "/job_id_sequence";

    public static final long RESET_VALUE = 0L;
    public static final int RETRY_COUNT = 3;

    private final static XLog LOG = XLog.getLog(ZKUUIDService.class);

    private ZKUtils zk;
    private static Long maxSequence = 9999990L;

    DistributedAtomicLong atomicIdGenerator;

    public static final ThreadLocal<SimpleDateFormat> dt = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyMMddHHmmssSSS");
        }
    };


    @Override
    public void init(Services services) throws ServiceException {

        super.init(services);
        try {
            zk = ZKUtils.register(this);
            PromotedToLock.Builder lockBuilder = PromotedToLock.builder().lockPath(getPromotedLock())
                    .retryPolicy(getRetryPolicy()).timeout(Service.lockTimeout, TimeUnit.MILLISECONDS);
            atomicIdGenerator = new DistributedAtomicLong(zk.getClient(), ZK_SEQUENCE_PATH, getRetryPolicy(),
                    lockBuilder.build());

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
    @Override
    protected String createSequence() {
        String localStartTime = super.startTime;
        long id = 0L;
        try {
            id = getZKSequence();
        }
        catch (Exception e) {
            LOG.error("Error getting jobId, switching to old UUIDService", e);
            id = super.getCounter();
            localStartTime = dt.get().format(new Date());
        }
        return appendTimeToSequence(id, localStartTime);
    }

    protected synchronized long getZKSequence() throws Exception {
        long id = getDistributedSequence();

        if (id >= maxSequence) {
            resetSequence();
            id = getDistributedSequence();
        }
        return id;
    }

    @SuppressWarnings("finally")
    private long getDistributedSequence() throws Exception {
        if (atomicIdGenerator == null) {
            throw new Exception("Sequence generator can't be null. Path : " + ZK_SEQUENCE_PATH);
        }
        AtomicValue<Long> value = null;
        try {
            value = atomicIdGenerator.increment();
        }
        catch (Exception e) {
            throw new Exception("Exception incrementing UID for session ", e);
        }
        finally {
            if (value != null && value.succeeded()) {
                return value.preValue();
            }
            else {
                throw new Exception("Exception incrementing UID for session ");
            }
        }
    }

    /**
     * Once sequence is reached limit, reset to 0.
     *
     * @throws Exception
     */
    private  void resetSequence() throws Exception {
        for (int i = 0; i < RETRY_COUNT; i++) {
            AtomicValue<Long> value = atomicIdGenerator.get();
            if (value.succeeded()) {
                if (value.postValue() < maxSequence) {
                    return;
                }
            }
            // Acquire ZK lock, so that other host doesn't reset sequence.
            LockToken lock = null;
            try {
                lock = Services.get().get(MemoryLocksService.class)
                        .getWriteLock(ZKUUIDService.class.getName(), lockTimeout);
            }
            catch (InterruptedException e1) {
                //ignore
            }
            try {
                if (lock == null) {
                    LOG.info("Lock is held by other system, will sleep and try again");
                    Thread.sleep(1000);
                    continue;
                }
                else {
                    value = atomicIdGenerator.get();
                    if (value.succeeded()) {
                        if (value.postValue() < maxSequence) {
                            return;
                        }
                    }
                    try {
                        atomicIdGenerator.forceSet(RESET_VALUE);
                    }
                    catch (Exception e) {
                        LOG.info("Exception while resetting sequence, will try again");
                        continue;
                    }
                    resetStartTime();
                    return;
                }
            }
            finally {
                if (lock != null) {
                    lock.release();
                }
            }
        }
        throw new Exception("Can't reset ID sequence in ZK. Retried " + RETRY_COUNT + " times");
    }

    @Override
    public void destroy() {
        if (zk != null) {
            zk.unregister(this);
        }
        zk = null;
        super.destroy();
    }

    public String getPromotedLock() {
        if (ZKUtils.getZKNameSpace().startsWith("/")) {
            return ZKUtils.getZKNameSpace() + LOCKS_NODE;

        }
        else {
            return "/" + ZKUtils.getZKNameSpace() + LOCKS_NODE;
        }
    }

    @VisibleForTesting
    public void setMaxSequence(long sequence) {
        maxSequence = sequence;
    }

    /**
     * Retries 25 times with delay of 200ms
     *
     * @return RetryNTimes
     */
    private static RetryPolicy getRetryPolicy() {
        return new RetryNTimes(25, 200);
    }

}