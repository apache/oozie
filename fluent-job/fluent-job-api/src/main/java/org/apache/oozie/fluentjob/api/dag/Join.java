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

package org.apache.oozie.fluentjob.api.dag;

/**
 * A class representing fork nodes in an Oozie workflow definition DAG. These nodes are generated automatically,
 * the end user should not need to use this class directly.
 */
public class Join extends JoiningNodeBase<Fork> {

    /**
     * Create a new end node with the given name.
     * @param name The name of the new end node.
     * @param fork The fork that this {@link Join} node closes.
     */
    public Join(final String name, final Fork fork) {
        super(name, fork);
        fork.close(this);
    }
}
