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

import org.apache.commons.lang.ObjectUtils;

/**
 * Utility class for holding a pair of data
 * @param <T> type of first data
 * @param <S> type of second data
 */
public class Pair<T, S> {
    private T first;
    private S second;

    public Pair(T first, S second) {
        this.first = first;
        this.second = second;
    }


    public static <T, S> Pair<T, S> of(T first, S second) {
        return new Pair(first, second);
    }

    public T getFist() {
        return first;
    }

    public S getSecond() {
        return second;
    }

    public int hashCode()
    {
        return (first == null ? 1 : first.hashCode()) * 17 + (second == null ? 1 : second.hashCode()) * 19;
    }

    public boolean equals(Object other)
    {
        if (other == null) {
            return false;
        }

        if (!(other instanceof Pair)) {
            return false;
        }

        Pair otherPair = (Pair)other;
        return (ObjectUtils.equals(first, otherPair.first) && ObjectUtils.equals(second, otherPair.second));
    }
}

