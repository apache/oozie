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
 * A class representing the delete operation of {@link FSAction} and the prepare section of other actions.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class Delete {
    private final String path;
    private final Boolean skipTrash;

    /**
     * Creates a new {@link Delete} object.
     * @param path The path of the file or directory to be deleted.
     * @param skipTrash {@code true} if the deleted items should NOT be moved to the trash but deleted completely;
     *                  {@code false} if the items should be moved to trash instead of deleting them conpletely.
     */
    public Delete(final String path, final Boolean skipTrash) {
        this.path = path;
        this.skipTrash = skipTrash;
    }

    /**
     * Returns the path of the item to be deleted.
     * @return The path of the item to be deleted.
     */
    public String getPath() {
        return path;
    }

    /**
     * Returns whether the trash should be skipped when deleting the items.
     * @return {@code true} if the deleted items should NOT be moved to the trash but deleted completely;
     *         {@code false} if the items should be moved to trash instead of deleting them conpletely.
     */
    public Boolean getSkipTrash() {
        return skipTrash;
    }
}