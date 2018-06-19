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

package org.apache.oozie.fluentjob.api.action;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A class representing the move command of {@link FSAction}.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class Move {
    private final String source;
    private final String target;

    /**
     * Creates a new {@link Move} object with the provided path.
     * @param source HDFS path
     * @param target HDFS path
     */
    public Move(final String source, final String target) {
        this.source = source;
        this.target = target;
    }

    /**
     * Returns the source path of this {@link Move} operation.
     * @return The source path of this {@link Move} operation.
     */
    public String getSource() {
        return source;
    }

    /**
     * Returns the target path of this {@link Move} operation.
     * @return The target path of this {@link Move} operation.
     */
    public String getTarget() {
        return target;
    }
}
