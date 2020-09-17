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

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;

import java.nio.ByteBuffer;

/**
 * Utility methods for working with {@link byte[]} primitive values.
 * <p>
 * Interning {@code byte[]} instances doesn't seem to take too many resources both in terms of CPU and memory: 10k * 10k random
 * {@code byte[]} allocation alone takes around 7.8 seconds, allocation plus interning takes around 8.0 seconds.
 */
public class ByteArrayUtils {
    private static final Interner<ByteBuffer> BYTE_BUFFER_INTERNER = Interners.newWeakInterner();

    /**
     * Return the internalized {@code byte[]}, or {@code null} if the given {@code byte[]} is {@code null}. A weak reference remains
     * to each {@code byte[]} interned, so these are not prevented from being garbage-collected.
     * @param values The {@code byte[]} to intern
     * @return The identical {@code byte[]} cached in the JVM's weak {@link Interner}.
     */
    public static byte[] weakIntern(final byte[] values) {
        if (values == null) {
            return values;
        }

        return BYTE_BUFFER_INTERNER.intern(ByteBuffer.wrap(values)).array();
    }
}
