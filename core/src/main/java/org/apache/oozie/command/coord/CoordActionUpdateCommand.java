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

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.XException;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.db.SLADbOperations;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.SLAEvent.SlaAppType;
import org.apache.oozie.client.SLAEvent.Status;
import org.apache.oozie.command.CommandException;

public class CoordActionUpdateCommand extends CoordinatorCommand<Void> {
    private final XLog log = XLog.getLog(getClass());
    private WorkflowJobBean workflow;
    private CoordinatorActionBean caction = null;

    public CoordActionUpdateCommand(WorkflowJobBean workflow) {
        super("coord-action-update", "coord-action-update", 1, XLog.OPS);
        this.workflow = workflow;
    }

    @Override
    protected Void call(CoordinatorStore cstore) throws StoreException, CommandException {
        try {
            if (workflow.getStatus() == WorkflowJob.Status.RUNNING
                    || workflow.getStatus() == WorkflowJob.Status.SUSPENDED) {
                //update lastModifiedTime
                cstore.updateCoordinatorAction(caction);
                return null;
            }
            // CoordinatorActionBean caction =
            // cstore.getCoordinatorActionForExternalId(workflow.getId());
            Status slaStatus = null;
            if (caction != null) {
                if (workflow.getStatus() == WorkflowJob.Status.SUCCEEDED) {
                    caction.setStatus(CoordinatorAction.Status.SUCCEEDED);
                    slaStatus = Status.SUCCEEDED;
                }
                else {
                    if (workflow.getStatus() == WorkflowJob.Status.FAILED) {
                        caction.setStatus(CoordinatorAction.Status.FAILED);
                        slaStatus = Status.FAILED;
                    }
                    else {
                        if (workflow.getStatus() == WorkflowJob.Status.KILLED) {
                            caction.setStatus(CoordinatorAction.Status.KILLED);
                            slaStatus = Status.KILLED;
                        }
                        else {
                            log.warn(
                                    "Unexpected workflow " + workflow.getId() + " STATUS " + workflow.getStatus());
                            //update lastModifiedTime
                            cstore.updateCoordinatorAction(caction);
                            return null;
                        }
                    }
                }

                log.info(
                        "Updating Coordintaor id :" + caction.getId() + "status to =" + caction.getStatus());
                cstore.updateCoordinatorAction(caction);
                if (slaStatus != null) {
                    SLADbOperations.writeStausEvent(caction.getSlaXml(), caction.getId(), cstore, slaStatus,
                                                    SlaAppType.COORDINATOR_ACTION);
                }
                queueCallable(new CoordActionReadyCommand(caction.getJobId()));
            }
        }
        catch (XException ex) {
            log.warn("CoordActionUpdate Failed ", ex.getMessage());
            throw new CommandException(ex);
        }
        return null;
    }

    @Override
    protected Void execute(CoordinatorStore store) throws StoreException, CommandException {
        log.info("STARTED CoordActionUpdateCommand for wfId=" + workflow.getId());
        caction = store.getCoordinatorActionForExternalId(workflow.getId());
        if (caction == null) {
            log.info("ENDED CoordActionUpdateCommand for wfId=" + workflow.getId() + ", coord action is null");
            return null;
        }
        setLogInfo(caction);
        String jobId = caction.getJobId();
        try {
            if (lock(jobId)) {
                call(store);
            }
            else {
                queueCallable(new CoordActionUpdateCommand(workflow), LOCK_FAILURE_REQUEUE_INTERVAL);
                log.warn("CoordActionUpdateCommand lock was not acquired - failed JobId=" + jobId + ", wfId="
                        + workflow.getId() + ". Requeing the same.");
            }
        }
        catch (InterruptedException e) {
            queueCallable(new CoordActionUpdateCommand(workflow), LOCK_FAILURE_REQUEUE_INTERVAL);
            log.warn("CoordActionUpdateCommand lock acquiring failed with exception " + e.getMessage() + " for jobId="
                    + jobId + ", wfId=" + workflow.getId() + ". Requeing the same.");
        }
        finally {
            log.info("ENDED CoordActionUpdateCommand for wfId=" + workflow.getId() + ", jobId=" + jobId);
        }
        return null;
    }
}
