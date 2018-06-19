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

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.List;

/**
 * A class representing the prepare section of various actions.
 *
 * Instances of this class should be built using the builder {@link PrepareBuilder}.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class Prepare {
    private final ImmutableList<Delete> deletes;
    private final ImmutableList<Mkdir> mkdirs;

    Prepare(final ImmutableList<Delete> deletes, final ImmutableList<Mkdir> mkdirs) {
        this.deletes = deletes;
        this.mkdirs = mkdirs;
    }

    /**
     * Returns the {@link Delete} objects that specify which directories or files will be deleted.
     * @return The {@link Delete} objects that specify which directories or files will be deleted.
     */
    public List<Delete> getDeletes() {
        return deletes;
    }

    /**
     * Returns the {@link Mkdir} objects that specify which directories will be created.
     * @return The {@link Mkdir} objects that specify which directories will be created.
     */
    public List<Mkdir> getMkdirs() {
        return mkdirs;
    }
}
