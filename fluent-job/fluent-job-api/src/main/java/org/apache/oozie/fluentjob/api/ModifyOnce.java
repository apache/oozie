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

package org.apache.oozie.fluentjob.api;

import com.google.common.base.Preconditions;

/**
 * A generic wrapper class for a value that can be modified once after construction, but only once.
 * @param <T> the generic type that can be modified once, but only once
 */
public class ModifyOnce<T> {
    private T data;
    private boolean modified;

    /**
     * Creates a new {@link ModifyOnce} object initialized to {@code null}.
     */
    public ModifyOnce() {
        this(null);
    }

    /**
     * Creates a new {@link ModifyOnce} object initialized to {@code defaultData}.
     * @param defaultData The initial value of this {@link ModifyOnce} object.
     */
    public ModifyOnce(final T defaultData) {
        this.data = defaultData;
        this.modified = false;
    }

    /**
     * Returns the wrapped value.
     * @return The wrapped value.
     */
    public T get() {
        return data;
    }

    /**
     * Sets the wrapped value. If it is not the first modification attempt, {@link IllegalStateException} is thrown.
     * @param data The new data to store.
     *
     * @throws IllegalStateException if this is not the first modification attempt.
     */
    public void set(final T data) {
        Preconditions.checkState(!modified, "Has already been modified once.");

        this.data = data;
        this.modified = true;
    }
}
