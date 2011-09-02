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

import java.util.List;
import java.util.Map;

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.CoordinatorJobInfo;
import org.apache.oozie.DagEngineException;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.wf.JobCommand;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;

public class CoordJobsCommand extends CoordinatorCommand<CoordinatorJobInfo> {
    private Map<String, List<String>> filter;
    private int start;
    private int len;

    public CoordJobsCommand(Map<String, List<String>> filter, int start, int length) {
        super("job.info", "job.info", 1, XLog.OPS);
        this.filter = filter;
        this.start = start;
        this.len = length;
    }

    @Override
    protected CoordinatorJobInfo call(CoordinatorStore store) throws StoreException, CommandException {
        CoordinatorJobInfo coord = store.getCoordinatorInfo(filter, start, len);
        // workflow.setConsoleUrl(getJobConsoleUrl(id));
        // workflow.setActions((List) store.getActionsForWorkflow(id,
        // false));
        return coord;
    }
}
