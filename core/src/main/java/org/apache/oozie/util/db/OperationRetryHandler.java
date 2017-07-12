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

package org.apache.oozie.util.db;

import java.util.concurrent.Callable;

import com.google.common.annotations.VisibleForTesting;
import org.apache.oozie.util.XLog;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;

public class OperationRetryHandler {
    private static XLog LOG = XLog.getLog(OperationRetryHandler.class);
    @VisibleForTesting
    static final RetryAttemptState RETRY_ATTEMPT_STATE = new RetryAttemptState();

    private final int maxRetryCount;
    private final long initialWaitTime;
    private final long maxWaitTime;
    private final Predicate<Throwable> retryPredicate;
    private final boolean shouldRetry;

    public OperationRetryHandler(final int maxRetryCount, final long initialWaitTime, final long maxWaitTime,
                                 final Predicate<Throwable> retryPredicate) {
        Preconditions.checkArgument(maxRetryCount >= 0, "Retry count must not be less than zero");
        Preconditions.checkArgument(initialWaitTime > 0, "Initial wait time must be greater than zero");
        Preconditions.checkArgument(maxWaitTime >= 0, "Maximum wait time must not be less than zero");

        this.maxRetryCount = maxRetryCount;
        this.initialWaitTime = initialWaitTime;
        this.maxWaitTime = maxWaitTime;
        this.retryPredicate = Preconditions.checkNotNull(retryPredicate, "Retry predicate must not be null");
        this.shouldRetry = !(maxRetryCount == 0 || maxWaitTime == 0);

        LOG.trace("Retry handler parameters are set." +
                "[maxRetryCount={0};initialWaitTime={1};maxWaitTime={2};retryPredicate.class={3};shouldRetry={4}]",
                this.maxRetryCount, this.initialWaitTime, this.maxWaitTime, this.retryPredicate.getClass().getName(), shouldRetry);
    }

    public <V> V executeWithRetry(final Callable<V> operation) throws Exception {
        int retries = 0;
        long waitTime = initialWaitTime;
        Exception lastException = null;

        if (!shouldRetry) {
            try {
                LOG.trace("Configured not to retry, calling operation once.");

                final V result = operation.call();

                LOG.trace("Operation called once successfully.");

                return result;
            }
            catch (final Exception e) {
                LOG.error("An error occurred while calling the operation once. [e.message={0}]", e.getMessage());
                throw e;
            }
        }

        try {
            RETRY_ATTEMPT_STATE.signalStart();

            while (retries < maxRetryCount) {
                try {
                    LOG.trace("Calling operation. [retries={0}]", retries);

                    retries++;
                    final V result = operation.call();

                    LOG.trace("Operation called successfully.");

                    return result;
                } catch (final Exception e) {
                    LOG.warn("Database error", e);

                    // if retries have been done by an inner retry handler,
                    // then we won't make any effort to do it again
                    if (RETRY_ATTEMPT_STATE.isExhausted()) {
                        LOG.error("Retry attempts have been exhausted. [e.message={0}]", e.getMessage());
                        throw e;
                    }

                    if (retryPredicate.apply(e)) {
                        LOG.trace("Exception is not on blacklist, handling retry. [retries={0};e.class={1}]",
                                retries, e.getClass().getName());
                        waitTime = handleRetry(waitTime, retries);
                        lastException = e;
                    }
                    else {
                        LOG.warn("Exception is on blacklist, not handling retry. [retries={0};e.class={1}]",
                                retries, e.getClass().getName());
                        throw e;
                    }
                }
            }

            LOG.error("Number of maximum retry attempts exhausted");
            RETRY_ATTEMPT_STATE.signalExhausted(); // signal to possible outer retry handlers
            throw lastException;
        } finally {
            RETRY_ATTEMPT_STATE.signalEnd();
        }
    }

    private long handleRetry(long sleepBeforeRetryMs, final int retries) throws InterruptedException {
        LOG.warn("Operation failed, sleeping {0} milliseconds before retry #{1}", sleepBeforeRetryMs, retries);
        Thread.sleep(sleepBeforeRetryMs);
        sleepBeforeRetryMs *=  2;

        return sleepBeforeRetryMs > maxWaitTime ? maxWaitTime : sleepBeforeRetryMs;
    }
}
