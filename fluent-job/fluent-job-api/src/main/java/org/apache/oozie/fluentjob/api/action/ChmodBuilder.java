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
import org.apache.oozie.fluentjob.api.ModifyOnce;

/**
 * A builder class for {@link Chmod}.
 *
 * The properties of the builder can only be set once, an attempt to set them a second time will trigger
 * an {@link IllegalStateException}. The properties that are lists are an exception to this rule, of course multiple
 * elements can be added / removed.
 *
 * Builder instances can be used to build several elements, although properties already set cannot be changed after
 * a call to {@link ChmodBuilder#build} either.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ChmodBuilder extends ChFSBaseBuilder<ChmodBuilder> implements Builder<Chmod> {
    private final ModifyOnce<String> permissions;

    public ChmodBuilder() {
        super();
        permissions = new ModifyOnce<>();
    }

    /**
     * Sets the new permissions that will be set by the operation.
     * @param permissions The new permissions that will be set by the operation.
     * @return This builder.
     */
    public ChmodBuilder withPermissions(final String permissions) {
        this.permissions.set(permissions);
        return this;
    }

    /**
     * Builds and returns a new {@link Chmod} object with the properties set in this builder.
     * The new {@link Chmod} object is independent of this builder and the builder can be used to build new instances.
     * @return The newly built {@link Chmod} object.
     */
    public Chmod build() {
        return new Chmod(getConstructionData(), permissions.get());
    }

    @Override
    protected ChmodBuilder getRuntimeSelfReference() {
        return this;
    }
}
