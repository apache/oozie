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

import org.apache.commons.logging.LogFactory;
import org.apache.oozie.lock.LockToken;

public abstract class Locker implements Runnable {
    private static final XLog log = new XLog(LogFactory.getLog(Locker.class));
    private final String nameIndex;
    private final StringBuffer sb;
    private final LockerCoordinator coordinator = new LockerCoordinator();

    protected String name;
    protected long timeout;

    public Locker(String name, int nameIndex, long timeout, StringBuffer buffer) {
        this.name = name;
        this.nameIndex = name + ":" + nameIndex;
        this.sb = buffer;
        this.timeout = timeout;
    }

    @Override
    public void run() {
        try {
            log.info("Getting lock [{0}]", nameIndex);
            coordinator.startDone();
            LockToken token = getLock();
            if (token != null) {
                log.info("Got lock [{0}]", nameIndex);
                sb.append(nameIndex + "-L ");

                coordinator.lockAcquireDone();
                coordinator.awaitContinueSignal();

                sb.append(nameIndex + "-U ");
                token.release();
                log.info("Release lock [{0}]", nameIndex);
            }
            else {
                coordinator.awaitContinueSignal();
                sb.append(nameIndex + "-N ");
                log.info("Did not get lock [{0}]", nameIndex);
            }
            coordinator.terminated();
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void awaitLockAcquire() throws InterruptedException {
        coordinator.awaitLockAcquire();
    }

    public void awaitStart() throws InterruptedException {
        coordinator.awaitStart();
    }

    public void proceed() {
        coordinator.signalLockerContinue();
    }

    public void awaitTermination() throws InterruptedException {
        coordinator.awaitTermination();
    }

    protected abstract LockToken getLock() throws InterruptedException;
}
