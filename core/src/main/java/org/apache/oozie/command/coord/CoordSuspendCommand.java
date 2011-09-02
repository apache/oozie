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
package org.apache.oozie.command.coord;

import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.XException;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;

import org.apache.oozie.command.wf.SuspendCommand;

import java.util.Date;
import java.util.List;

public class CoordSuspendCommand extends CoordinatorCommand<Void> {

    private String jobId;
    private final XLog log = XLog.getLog(getClass());

    public CoordSuspendCommand(String id) {
        super("coord_suspend", "coord_suspend", 0, XLog.STD);
        this.jobId = ParamChecker.notEmpty(id, "id");
    }

    protected Void call(CoordinatorStore store) throws StoreException, CommandException {
        try {
            // CoordinatorJobBean coordJob = store.getCoordinatorJob(jobId,
            // false);
            CoordinatorJobBean coordJob = store.getEntityManager().find(CoordinatorJobBean.class, jobId);
            setLogInfo(coordJob);
            if (coordJob.getStatus() != CoordinatorJob.Status.SUCCEEDED
                    && coordJob.getStatus() != CoordinatorJob.Status.FAILED) {
                incrJobCounter(1);
                coordJob.setStatus(CoordinatorJob.Status.SUSPENDED);
                List<CoordinatorActionBean> actionList = store.getActionsForCoordinatorJob(jobId, false);
                for (CoordinatorActionBean action : actionList) {
                    if (action.getStatus() == CoordinatorActionBean.Status.RUNNING) {
                        // queue a SuspendCommand
                        if (action.getExternalId() != null) {
                            queueCallable(new SuspendCommand(action.getExternalId()));
                        }
                    }
                }
                store.updateCoordinatorJob(coordJob);
            }
            // TODO queueCallable(new NotificationCommand(coordJob));
            else {
                log.info("CoordSuspendCommand not suspended - " + "job finished or does not exist " + jobId);
            }
            return null;
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

    @Override
    protected Void execute(CoordinatorStore store) throws StoreException, CommandException {
        log.info("STARTED CoordSuspendCommand for jobId=" + jobId);
        try {
            if (lock(jobId)) {
                call(store);
            }
            else {
                queueCallable(new CoordSuspendCommand(jobId), LOCK_FAILURE_REQUEUE_INTERVAL);
                log.warn("CoordSuspendCommand lock was not acquired - " + " failed " + jobId + ". Requeing the same.");
            }
        }
        catch (InterruptedException e) {
            queueCallable(new CoordSuspendCommand(jobId), LOCK_FAILURE_REQUEUE_INTERVAL);
            log.warn("CoordSuspendCommand lock acquiring failed " + " with exception " + e.getMessage()
                    + " for job id " + jobId + ". Requeing the same.");
        }
        finally {
            log.info("ENDED CoordSuspendCommand for jobId=" + jobId);
        }
        return null;
    }

}
