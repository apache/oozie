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
package org.apache.oozie.command.coord;

import org.apache.oozie.command.Command;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.Store;
import org.apache.oozie.store.WorkflowStore;

public abstract class CoordinatorCommand<T> extends Command<T, CoordinatorStore> {

    public CoordinatorCommand(String name, String type, int priority, int logMask) {
        super(name, type, priority, logMask);
    }

    public CoordinatorCommand(String name, String type, int priority, int logMask, boolean withStore) {
        super(name, type, priority, logMask, withStore);
    }

    public CoordinatorCommand(String name, String type, int priority, int logMask, boolean withStore, boolean dryrun) {
        super(name, type, priority, logMask, (dryrun) ? false : withStore, dryrun);
    }

    /**
     * Return the public interface of the Coordinator Store.
     *
     * @return {@link WorkflowStore}
     */
    public Class<? extends Store> getStoreClass() {
        return CoordinatorStore.class;
    }
}
