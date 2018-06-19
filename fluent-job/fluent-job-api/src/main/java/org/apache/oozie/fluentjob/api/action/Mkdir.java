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
 * A class representing the mkdir command of {@link FSAction} and the prepare section of other actions.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class Mkdir {
    private final String path;

    /**
     * Creates a new {@link Mkdir} object with the provided path.
     * @param path The path of the directory that will be created when this operation is run.
     */
    public Mkdir(final String path) {
        this.path = path;
    }

    /**
     * Returns the path of the directory that will be created.
     * @return The path of the directory that will be created.
     */
    public String getPath() {
        return path;
    }

}