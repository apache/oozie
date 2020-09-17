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
 * A class representing the chgrp command of {@link FSAction}.
 * Instances of this class should be built using the builder {@link ChgrpBuilder}.
 *
 * The properties of the builder can only be set once, an attempt to set them a second time will trigger
 * an {@link IllegalStateException}.
 *
 * Builder instances can be used to build several elements, although properties already set cannot be changed after
 * a call to {@link ChgrpBuilder#build} either.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class Chgrp extends ChFSBase {
    private String group;

    Chgrp(final ChFSBase.ConstructionData constructionData,
          final String group) {
        super(constructionData);
        this.group = group;
    }


    /**
     * Returns the new group that will be set by the command.
     * @return The new group that will be set by the command.
     */
    public String getGroup() {
        return group;
    }
}
