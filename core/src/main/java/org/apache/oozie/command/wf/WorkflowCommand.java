/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.command.wf;

import org.apache.oozie.command.Command;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.store.Store;

public abstract class WorkflowCommand<T> extends Command<T, WorkflowStore> {

    /**
     * Create a command that uses a {@link WorkflowStore} instance. <p/> The current {@link XLog.Info} values are
     * captured for execution.
     *
     * @param name command name.
     * @param type command type.
     * @param priority priority of the command, used when queuing for asynchronous execution.
     * @param logMask log mask for the command logging calls.
     */
    public WorkflowCommand(String name, String type, int priority, int logMask) {
        super(name, type, priority, logMask, true);
    }

    /**
     * Create a command. <p/> The current {@link XLog.Info} values are captured for execution.
     *
     * @param name command name.
     * @param type command type.
     * @param priority priority of the command, used when queuing for asynchronous execution.
     * @param logMask log mask for the command logging calls.
     * @param withStore indicates if the command needs a {@link org.apache.oozie.store.WorkflowStore} instance or not.
     */
    public WorkflowCommand(String name, String type, int priority, int logMask, boolean withStore) {
        super(name, type, priority, logMask, withStore);
    }

    /**
     * Return the public interface of the Workflow Store.
     *
     * @return {@link WorkflowStore}
     */
    public Class<? extends Store> getStoreClass() {
        return WorkflowStore.class;
    }
}
