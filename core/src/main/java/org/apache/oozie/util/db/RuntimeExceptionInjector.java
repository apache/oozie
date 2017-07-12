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

import com.google.common.base.Preconditions;
import org.apache.oozie.util.XLog;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class RuntimeExceptionInjector<E extends RuntimeException> {
    private static final XLog LOG = XLog.getLog(RuntimeExceptionInjector.class);
    private static final AtomicLong failureCounter = new AtomicLong(0);

    private final Class<E> runtimeExceptionClass;
    private final int failurePercent;

    public RuntimeExceptionInjector(final Class<E> runtimeExceptionClass, final int failurePercent) {
        Preconditions.checkArgument(failurePercent <= 100 && failurePercent >= 0,
                "illegal value for failure %: " + failurePercent);

        this.runtimeExceptionClass = runtimeExceptionClass;
        this.failurePercent = failurePercent;
    }

    public void inject(final String errorMessage) {
        LOG.trace("Trying to inject random failure. [errorMessage={0}]", errorMessage);

        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final int randomVal = random.nextInt(0, 100); // range:  [0..99]

        if (randomVal < failurePercent) {
            final long count = failureCounter.incrementAndGet();
            LOG.warn("Injecting random failure. [runtimeExceptionClass.name={0};count={1};errorMessage={2}]",
                    runtimeExceptionClass.getName(), count, errorMessage);
            E injected;

            try {
                injected = runtimeExceptionClass.getConstructor(String.class).newInstance(
                        "injected random failure #" + count + " ." + errorMessage);
            } catch (final InstantiationException | IllegalAccessException | InvocationTargetException
                    | NoSuchMethodException outer) {
                try {
                    LOG.warn("Instantiating without error message. [runtimeExceptionClass.name={0};outer.message={1}]",
                            runtimeExceptionClass.getName(), outer.getMessage());
                    injected = runtimeExceptionClass.newInstance();
                } catch (final InstantiationException | IllegalAccessException inner) {
                    LOG.error("Could not instantiate. [runtimeExceptionClass.name={0};inner.message={1}]",
                            runtimeExceptionClass.getName(), inner.getMessage());
                    throw new RuntimeException(inner);
                }

            }

            throw injected;
        }

        LOG.trace("Did not inject random failure.");
    }
}
