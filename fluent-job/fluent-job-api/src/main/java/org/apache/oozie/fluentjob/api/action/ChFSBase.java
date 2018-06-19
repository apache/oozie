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
 * A base class for {@link Chgrp} and {@link Chmod}.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ChFSBase {
    private final boolean recursive;
    private final String path;
    private final String dirFiles;

    ChFSBase(final ConstructionData constructionData) {
        this.recursive = constructionData.recursive;
        this.path = constructionData.path;
        this.dirFiles = constructionData.dirFiles;
    }

    /**
     * Returns whether this file system operation is recursive.
     * @return {@code true} if this file system operation is recursive; {@code false} otherwise.
     */
    public boolean isRecursive() {
        return recursive;
    }

    /**
     * Returns the path of the target of this file system operation.
     * @return The path of the target of this file system operation.
     */
    public String getPath() {
        return path;
    }

    /**
     * Returns whether this file system operation should be applied to all files in the given directory.
     * @return "true" if this file system operation should be applied to all files in the given directory;
     *         "false" otherwise.
     */
    public String getDirFiles() {
        return dirFiles;
    }

    /**
     * Helper class that is used by the subclasses of this class and their builders.
     */
    public static class ConstructionData {
        private final boolean recursive;
        private final String path;
        private final String dirFiles;

        public ConstructionData(final boolean recursive,
                                final String path,
                                final String dirFiles) {
            this.recursive = recursive;
            this.path = path;
            this.dirFiles = dirFiles;
        }
    }
}
