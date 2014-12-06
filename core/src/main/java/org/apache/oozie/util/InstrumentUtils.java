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

import org.apache.oozie.command.XCommand;

/**
 * Instrument utilities.
 */
public class InstrumentUtils {

    private static final String INSTRUMENTATION_JOB_GROUP = "jobs";

    /**
     * Convenience method to increment counters.
     *
     * @param group the group name.
     * @param name the counter name.
     * @param count increment count.
     * @param instrumentation the {@link Instrumentation} instance
     */
    public static void incrCounter(String group, String name, int count, Instrumentation instrumentation) {
        if (instrumentation != null) {
            instrumentation.incr(group, name, count);
        }
    }

    /**
     * Used to increment command counters.
     *
     * @param name the name
     * @param count the increment count
     * @param instrumentation the {@link Instrumentation} instance
     */
    public static void incrCommandCounter(String name, int count, Instrumentation instrumentation) {
        incrCounter(XCommand.INSTRUMENTATION_GROUP, name, count, instrumentation);
    }

    /**
     * Used to increment job counters. The counter name s the same as the command name.
     *
     * @param name the name
     * @param count the increment count
     * @param instrumentation the {@link Instrumentation} instance
     */
    public static void incrJobCounter(String name, int count, Instrumentation instrumentation) {
        incrCounter(INSTRUMENTATION_JOB_GROUP, name, count, instrumentation);
    }
}
