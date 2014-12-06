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
package org.apache.oozie.command.wf;

import org.apache.oozie.AppType;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.XCommand;
import org.apache.oozie.command.coord.CoordActionUpdateXCommand;
import org.apache.oozie.event.WorkflowActionEvent;
import org.apache.oozie.event.WorkflowJobEvent;

/**
 * Abstract coordinator command class derived from XCommand
 *
 * @param <T>
 */
public abstract class WorkflowXCommand<T> extends XCommand<T> {
    /**
     * Base class constructor for workflow commands.
     *
     * @param name command name
     * @param type command type
     * @param priority command priority
     */
    public WorkflowXCommand(String name, String type, int priority) {
        super(name, type, priority);
    }

    /**
     * Base class constructor for workflow commands.
     *
     * @param name command name
     * @param type command type
     * @param priority command priority
     * @param dryrun true if rerun is enabled for command
     */
    public WorkflowXCommand(String name, String type, int priority, boolean dryrun) {
        super(name, type, priority, dryrun);
    }

    protected static void generateEvent(WorkflowJobBean wfJob, String errorCode, String errorMsg) {
        if (eventService.isSupportedApptype(AppType.WORKFLOW_JOB.name())) {
            WorkflowJobEvent event = new WorkflowJobEvent(wfJob.getId(), wfJob.getParentId(), wfJob.getStatus(),
                    wfJob.getUser(), wfJob.getAppName(), wfJob.getStartTime(), wfJob.getEndTime());
            event.setErrorCode(errorCode);
            event.setErrorMessage(errorMsg);
            eventService.queueEvent(event);
        }
    }

    protected static void generateEvent(WorkflowJobBean wfJob) {
        generateEvent(wfJob, null, null);
    }

    protected void generateEvent(WorkflowActionBean wfAction, String wfUser) {
        if (eventService.isSupportedApptype(AppType.WORKFLOW_ACTION.name())) {
            WorkflowActionEvent event = new WorkflowActionEvent(wfAction.getId(), wfAction.getJobId(),
                    wfAction.getStatus(), wfUser, wfAction.getName(), wfAction.getStartTime(), wfAction.getEndTime());
            event.setErrorCode(wfAction.getErrorCode());
            event.setErrorMessage(wfAction.getErrorMessage());
            eventService.queueEvent(event);
        }
    }

    protected void updateParentIfNecessary(WorkflowJobBean wfjob, int maxRetries) throws CommandException {
        // update coordinator action if the wf was actually started by a coord
        if (wfjob.getParentId() != null && wfjob.getParentId().contains("-C@")) {
            new CoordActionUpdateXCommand(wfjob, maxRetries).call();
        }
    }

    protected void updateParentIfNecessary(WorkflowJobBean wfjob) throws CommandException {
        // update coordinator action if the wf was actually started by a coord
        if (wfjob.getParentId() != null && wfjob.getParentId().contains("-C@")) {
            new CoordActionUpdateXCommand(wfjob).call();
        }
    }
}
