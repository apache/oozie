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
 * A class representing a condition in the "switch statement" of an Oozie decision node.
 */
public class Condition {
    private final String condition;
    private final boolean isDefault;

    private Condition(final String condition, final boolean isDefault) {
        final boolean bothFieldsSet = condition == null && !isDefault;
        final boolean bothFieldsUnset = condition != null && isDefault;
        Preconditions.checkArgument(!bothFieldsSet && !bothFieldsUnset,
                "Exactly one of 'condition' and 'isDefault' must be non-null or true (respectively).");

        this.condition = condition;
        this.isDefault = isDefault;
    }

    /**
     * Creates an actual condition (as opposed to a default condition).
     * @param condition The string defining the condition.
     * @return A new actual condition.
     */
    public static Condition actualCondition(final String condition) {
        Preconditions.checkArgument(condition != null, "The argument 'condition' must not be null.");

        return new Condition(condition, false);
    }

    /**
     * Creates a new default condition. Every decision node must have a default path which is chosen if no other
     * condition is true.
     * @return A new default condition.
     */
    public static Condition defaultCondition() {
        return new Condition(null, true);
    }

    /**
     * Returns the string defining the condition or {@code null} if this is a default condition.
     * @return The string defining the condition or {@code null} if this is a default condition.
     */
    public String getCondition() {
        return condition;
    }

    /**
     * Returns whether this condition is a default condition.
     * @return {@code true} if this condition is a default condition, {@code false} otherwise.
     */
    public boolean isDefault() {
        return isDefault;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final Condition other = (Condition) o;

        if (isDefault != other.isDefault) {
            return false;
        }

        return condition != null ? condition.equals(other.condition) : other.condition == null;
    }

    @Override
    public int hashCode() {
        int result = condition != null ? condition.hashCode() : 0;
        result = 31 * result + (isDefault ? 1 : 0);

        return result;
    }
}
