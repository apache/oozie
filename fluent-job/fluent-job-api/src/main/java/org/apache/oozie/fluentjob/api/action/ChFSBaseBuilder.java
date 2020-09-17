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
 * A base class for {@link ChgrpBuilder} and {@link ChmodBuilder}.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class ChFSBaseBuilder <B extends ChFSBaseBuilder<B>> {
    private final ModifyOnce<Boolean> recursive;
    private final ModifyOnce<String> path;
    private final ModifyOnce<String> dirFiles;

    public ChFSBaseBuilder() {
        recursive = new ModifyOnce<>(false);
        path = new ModifyOnce<>();
        dirFiles = new ModifyOnce<>("true");
    }

    /**
     * Sets this file system operation to be recursive.
     * @return This builder.
     */
    public B setRecursive() {
        this.recursive.set(true);
        return ensureRuntimeSelfReference();
    }

    /**
     * Sets this file system operation to be non-recursive.
     * @return This builder.
     */
    public B setNonRecursive() {
        this.recursive.set(false);
        return ensureRuntimeSelfReference();
    }

    /**
     * Sets the path of the target of this file system operation.
     * @param path the path of the target
     * @return This builder.
     */
    public B withPath(final String path) {
        this.path.set(path);
        return ensureRuntimeSelfReference();
    }

    /**
     * Sets whether this file system operation should be applied to all files in the given directory.
     * @param dirFiles {@code true} if the operation should be applied to all files in the given directory;
     *                 {@code false} if it shouldn't.
     * @return This builder.
     */
    public B setDirFiles(final boolean dirFiles) {
        this.dirFiles.set(Boolean.toString(dirFiles));
        return ensureRuntimeSelfReference();
    }

    final B ensureRuntimeSelfReference() {
        final B concrete = getRuntimeSelfReference();
        if (concrete != this) {
            throw new IllegalStateException(
                    "The builder type B doesn't extend ChFSBaseBuilder<B>.");
        }

        return concrete;
    }

    protected ChFSBase.ConstructionData getConstructionData() {
        return new ChFSBase.ConstructionData(recursive.get(), path.get(), dirFiles.get());
    }

    protected abstract B getRuntimeSelfReference();
}
