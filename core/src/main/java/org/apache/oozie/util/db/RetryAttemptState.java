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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * This class tracks nested {@link OperationRetryHandler} calls. Some {@code JPAExecutor} implementations call other
 * {@code JPAExecutor}s. This results in two (or possibly more) {@link OperationRetryHandler#executeWithRetry(Callable)} calls.
 * <p/>
 * If the innermost retry handler has exhausted all attempts and rethrows the {@link Exception}, the outer handler catches that
 * and would restart the JPA operation again.
 * <p/>
 * In order to avoid this, retry handlers must communicate with each other on the same thread by incrementing / decrementing the
 * nesting level and signalling whether the maximum number of attempts have been reached.
 * <p/>
 * We use {@link ThreadLocal}s because retry handlers might be called from different threads in parallel. If the nesting level is
 * zero, it's important to reset the {@link #exhausted} back to {@code false} since this variable is reused in the
 * thread pool.
 */
final class RetryAttemptState {
    private final ThreadLocal<Boolean> exhausted = new ThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue() {
            return Boolean.FALSE;
        }
    };

    private final ThreadLocal<Integer> inProgressCount = new ThreadLocal<Integer>() {
        @Override
        protected Integer initialValue() {
            return 0;
        }
    };

    void signalStart() {
        Preconditions.checkState(!isExhausted(), "retry attempts exhausted");

        inProgressCount.set(inProgressCount.get() + 1);
    }

    void signalEnd() {
        int currentLevel = inProgressCount.get() - 1;
        inProgressCount.set(currentLevel);
        if (currentLevel == 0) {
            // state must be reset
            exhausted.set(false);
        }
    }

    void signalExhausted() {
        exhausted.set(true);
    }

    boolean isExhausted() {
        return exhausted.get();
    }

    @VisibleForTesting
    int getInProgressCount() {
        return inProgressCount.get();
    }
}
