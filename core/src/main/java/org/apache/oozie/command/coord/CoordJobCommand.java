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
package org.apache.oozie.command.coord;

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;

/**
 * Command for loading a coordinator job information
 */
public class CoordJobCommand extends CoordinatorCommand<CoordinatorJobBean> {
    private String id;
    private boolean getActionInfo;
    private int start = 1;
    private int len = Integer.MAX_VALUE;

    /**
     * @param id coord jobId
     */
    public CoordJobCommand(String id) {
        this(id, 1, Integer.MAX_VALUE);
    }

    /**
     * @param id coord jobId
     * @param start starting index in the list of actions belonging to the job
     * @param length number of actions to be returned
     */
    public CoordJobCommand(String id, int start, int length) {
        super("job.info", "job.info", 1, XLog.OPS);
        this.id = ParamChecker.notEmpty(id, "id");
        this.getActionInfo = true;
        this.start = start;
        this.len = length;
    }

    /**
     * @param id coord jobId
     * @param getActionInfo false to ignore loading actions for the job
     */
    public CoordJobCommand(String id, boolean getActionInfo) {
        super("job.info", "job.info", 1, XLog.OPS);
        this.id = ParamChecker.notEmpty(id, "id");
        this.getActionInfo = getActionInfo;
    }

    @Override
    protected CoordinatorJobBean call(CoordinatorStore store) throws StoreException, CommandException {
        CoordinatorJobBean coord = store.getCoordinatorJob(id, false);
        if (this.getActionInfo == true) {
            coord.setActions(store.getActionsSubsetForCoordinatorJob(id, start, len));
        }
        else {
            coord.setActions(null);
        }
        return coord;
    }

}
