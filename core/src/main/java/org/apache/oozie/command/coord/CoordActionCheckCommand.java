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

import java.sql.Timestamp;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.XException;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.StoreService;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.db.SLADbOperations;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.SLAEvent.SlaAppType;
import org.apache.oozie.client.SLAEvent.Status;
import org.apache.oozie.command.CommandException;

public class CoordActionCheckCommand extends CoordinatorCommand<Void> {
    private String actionId;
    private int actionCheckDelay;
    private final XLog log = XLog.getLog(getClass());
    private CoordinatorActionBean coordAction = null;

    public CoordActionCheckCommand(String actionId, int actionCheckDelay) {
        super("coord_action_check", "coord_action_check", 0, XLog.OPS);
        this.actionId = actionId;
        this.actionCheckDelay = actionCheckDelay;
    }

    protected Void call(CoordinatorStore cstore) throws StoreException, CommandException {
        try {
            //if the action has been updated, quit this command
            Timestamp actionCheckTs = new Timestamp(System.currentTimeMillis() - actionCheckDelay * 1000);
            Timestamp cactionLmt = coordAction.getLastModifiedTimestamp();
            if (cactionLmt.after(actionCheckTs)) {
                log.info("The coord action :" + actionId + " has been udated. Ignore CoordActionCheckCommand!");
                return null;
            }
            if (coordAction.getStatus().equals(CoordinatorAction.Status.SUCCEEDED)
                    || coordAction.getStatus().equals(CoordinatorAction.Status.FAILED)
                    || coordAction.getStatus().equals(CoordinatorAction.Status.KILLED)) {
                // do nothing
            }
            else {
                incrJobCounter(1);
                WorkflowStore wstore = Services.get().get(StoreService.class).getStore(WorkflowStore.class, cstore);
                WorkflowJobBean wf = wstore.getWorkflow(coordAction.getExternalId(), false);

                Status slaStatus = null;

                if (wf.getStatus() == WorkflowJob.Status.SUCCEEDED) {
                    coordAction.setStatus(CoordinatorAction.Status.SUCCEEDED);
                    slaStatus = Status.SUCCEEDED;
                }
                else {
                    if (wf.getStatus() == WorkflowJob.Status.FAILED) {
                        coordAction.setStatus(CoordinatorAction.Status.FAILED);
                        slaStatus = Status.FAILED;
                    }
                    else {
                        if (wf.getStatus() == WorkflowJob.Status.KILLED) {
                            coordAction.setStatus(CoordinatorAction.Status.KILLED);
                            slaStatus = Status.KILLED;
                        }
                        else {
                            log.warn("Unexpected workflow " + wf.getId() + " STATUS " + wf.getStatus());
                            cstore.updateCoordinatorAction(coordAction);
                            return null;
                        }
                    }
                }

                log.debug("Updating Coordintaor actionId :" + coordAction.getId() + "status to =" + coordAction.getStatus());
                cstore.updateCoordinatorAction(coordAction);
                if (slaStatus != null) {
                    SLADbOperations.writeStausEvent(coordAction.getSlaXml(), coordAction.getId(), cstore, slaStatus,
                                                    SlaAppType.COORDINATOR_ACTION);
                }
            }

        }
        catch (XException ex) {
            log.warn("CoordActionCheckCommand Failed ", ex);
            throw new CommandException(ex);
        }
        return null;
    }

    @Override
    protected Void execute(CoordinatorStore store) throws StoreException, CommandException {
        log.info("STARTED CoordActionCheckCommand for actionId = " + actionId);
        try {
            coordAction = store.getEntityManager().find(CoordinatorActionBean.class, actionId);
            setLogInfo(coordAction);
            if (lock(coordAction.getJobId())) {
                call(store);
            }
            else {
                queueCallable(new CoordActionCheckCommand(actionId, actionCheckDelay), LOCK_FAILURE_REQUEUE_INTERVAL);
                log.warn("CoordActionCheckCommand lock was not acquired - failed jobId=" + coordAction.getJobId()
                        + ", actionId=" + actionId + ". Requeing the same.");
            }
        }
        catch (InterruptedException e) {
            queueCallable(new CoordActionCheckCommand(actionId, actionCheckDelay), LOCK_FAILURE_REQUEUE_INTERVAL);
            log.warn("CoordActionCheckCommand lock acquiring failed with exception " + e.getMessage() + " for jobId="
                    + coordAction.getJobId() + ", actionId=" + actionId + " Requeing the same.");
        }
        finally {
            log.info("ENDED CoordActionCheckCommand for actionId:" + actionId);
        }
        return null;
    }
}
