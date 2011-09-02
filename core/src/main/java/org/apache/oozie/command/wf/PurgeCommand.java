/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.command.wf;

import org.apache.oozie.store.StoreException;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.store.Store;
import org.apache.oozie.util.XLog;
import org.apache.oozie.command.Command;
import org.apache.oozie.command.CommandException;

public class PurgeCommand extends WorkflowCommand<Void> {
    private int olderThan;

    public PurgeCommand(int olderThan) {
        super("purge", "purge", 0, XLog.OPS);
        this.olderThan = olderThan;
    }

    @Override
    protected Void call(WorkflowStore store) throws StoreException, CommandException {
        XLog.getLog(getClass()).debug("Attempting to purge Jobs older than [{0}] days.", olderThan);
        store.purge(this.olderThan);
        XLog.getLog(getClass()).debug("Purge succeeded ");
        return null;
    }

}
