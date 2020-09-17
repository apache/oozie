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

/**
 * A builder class for {@link Prepare}.
 * <p>
 * Builder instances can be used to build several elements, although properties already set cannot be changed after
 * a call to {@link FSActionBuilder#build} either.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PrepareBuilder implements Builder<Prepare> {
    private final ImmutableList.Builder<Delete> deletes;
    private final ImmutableList.Builder<Mkdir> mkdirs;

    public PrepareBuilder() {
        deletes = new ImmutableList.Builder<>();
        mkdirs = new ImmutableList.Builder<>();
    }

    /**
     * Registers a {@link Delete} object with this builder. The {@link Delete} object will have the provided path as
     * its target and the default value (true) for skip-trash.
     * @param path The target of the {@link Delete} object.
     * @return this
     */
    public PrepareBuilder withDelete(final String path) {
        return withDelete(path, null);
    }

    /**
     * Registers a {@link Delete} object with this builder. The {@link Delete} object will have the provided path as
     * its target and the given boolean value for skip-trash.
     * @param path The target of the {@link Delete} object.
     * @param skipTrash Whether to skip trash when deleting the items.
     * @return this
     */
    public PrepareBuilder withDelete(final String path, final Boolean skipTrash) {
        deletes.add(new Delete(path, skipTrash));
        return this;
    }

    /**
     * Registers a {@link Mkdir} object with this builder The {@link Mkdir} object will have the provided path as
     * its target.
     * @param path The target of the {@link Mkdir}.
     * @return this
     */
    public PrepareBuilder withMkdir(final String path) {
        mkdirs.add(new Mkdir(path));
        return this;
    }

    /**
     * Creates a new {@link Prepare} object with the properties stores in this builder.
     * The new {@link Prepare} object is independent of this builder and the builder can be used to build
     * new instances.
     * @return A new {@link Prepare} object with the properties stored in this builder.
     */
    @Override
    public Prepare build() {
        return new Prepare(deletes.build(), mkdirs.build());
    }
}
