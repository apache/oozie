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

import java.util.concurrent.Callable;

/**
 * Extends Callable adding the concept of priority. <p/> The priority is useful when queuing callables for later
 * execution via the {@link org.apache.oozie.service.CallableQueueService}. <p/> A higher number means a higher
 * priority. <p/>
 */
public interface XCallable<T> extends Callable<T> {

    /**
     * Return the callable name.
     *
     * @return the callable name.
     */
    public String getName();

    /**
     * Return the priority of the callable.
     *
     * @return the callable priority.
     */
    public int getPriority();

    /**
     * Return the callable type. <p/> The callable type is used for concurrency throttling in the {@link
     * org.apache.oozie.service.CallableQueueService}.
     *
     * @return the callable type.
     */
    public String getType();

    /**
     * Returns the createdTime of the callable in milliseconds
     *
     * @return the callable createdTime
     */
    public long getCreatedTime();

    /**
     * Return the key of the callable
     *
     * @return the callable key
     */
    public String getKey();

    /**
     * Return the lock key of the callable
     *
     * @return the callable Lock key
     */
    public String getEntityKey();

    /**
     * set the mode of execution for the callable. True if in interrupt, false
     * if not
     */
    public void setInterruptMode(boolean mode);

    /**
     * @return the mode of execution. true if it is executed as an Interrupt,
     *         false otherwise
     */
    public boolean inInterruptMode();

}
