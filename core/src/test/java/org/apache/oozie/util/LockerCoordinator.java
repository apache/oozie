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
package org.apache.oozie.util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

// Helper class for some multithreaded tests
public class LockerCoordinator {
    public static final int LATCH_TIMEOUT_SECONDS = 10;

    private final CountDownLatch startLatch = new CountDownLatch(1);
    private final CountDownLatch acquireLockLatch = new CountDownLatch(1);
    private final CountDownLatch proceedingLatch = new CountDownLatch(1);
    private final CountDownLatch terminationLatch = new CountDownLatch(1);

    // Test thread waits until Locker thread starts to run
    public void awaitStart() throws InterruptedException {
        startLatch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    // Test thread waits until Locker thread terminates
    public void awaitTermination() throws InterruptedException {
        terminationLatch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    // Test thread waits until lock acquisition succeeds
    public void awaitLockAcquire() throws InterruptedException {
        acquireLockLatch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    // Locker thread blocks until test thread unblocks it
    public void awaitContinueSignal() throws InterruptedException {
        proceedingLatch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    // Test thread unblocks Locker thread
    public void signalLockerContinue() {
        proceedingLatch.countDown();
    }

    // Locker thread has started
    public void startDone() {
        startLatch.countDown();
    }

    // Locker thread acquired the lock
    public void lockAcquireDone() {
        acquireLockLatch.countDown();
    }

    // Locker thread finished
    public void terminated() {
        terminationLatch.countDown();
    }
}
