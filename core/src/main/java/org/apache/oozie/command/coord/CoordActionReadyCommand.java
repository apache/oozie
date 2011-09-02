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

import java.util.List;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.util.XLog;

public class CoordActionReadyCommand extends CoordinatorCommand<Void> {
    private String jobId;
    private final XLog log = XLog.getLog(getClass());

    public CoordActionReadyCommand(String id) {
        super("coord_action_ready", "coord_action_ready", 0, XLog.STD);
        this.jobId = id;
    }

    @Override
    /**
     * Check for READY actions and change state to SUBMITTED by a command to submit the job to WF engine.
     * This method checks all the actions associated with a jobId to figure out which actions
     * to start (based on concurrency and execution order [FIFO, LIFO, LAST_ONLY])
     *
     * @param store Coordinator Store
     */
    protected Void call(CoordinatorStore store) throws StoreException, CommandException {
        // number of actions to start (-1 means start ALL)
        int numActionsToStart = -1;
        // get CoordinatorJobBean for jobId
        //CoordinatorJobBean coordJob = store.getCoordinatorJob(jobId, false);
        CoordinatorJobBean coordJob = store.getEntityManager().find(CoordinatorJobBean.class, jobId);
        setLogInfo(coordJob);
        // get execution setting for this job (FIFO, LIFO, LAST_ONLY)
        String jobExecution = coordJob.getExecution();
        // get concurrency setting for this job
        int jobConcurrency = coordJob.getConcurrency();
        // if less than 0, then UNLIMITED concurrency
        if (jobConcurrency >= 0) {
            // count number of actions that are already RUNNING or SUBMITTED
            // subtract from CONCURRENCY to calculate number of actions to start
            // in WF engine
            int numRunningJobs = store.getCoordinatorRunningActionsCount(jobId);
            numActionsToStart = jobConcurrency - numRunningJobs;
            if (numActionsToStart < 0) {
                numActionsToStart = 0;
            }
            log.debug("concurrency=" + jobConcurrency + ", execution=" + jobExecution + ", numRunningJobs="
                    + numRunningJobs + ", numLeftover=" + numActionsToStart);
            // no actions to start
            if (numActionsToStart == 0) {
                log.warn("No actions to start! for jobId=" + jobId);
                return null;
            }
        }
        // get list of actions that are READY and fit in the concurrency and
        // execution
        List<CoordinatorActionBean> actions = store.getCoordinatorActionsForJob(jobId, numActionsToStart, jobExecution);
        log.debug("Number of READY actions = " + actions.size());
        String user = coordJob.getUser();
        String authToken = coordJob.getAuthToken();
        // make sure auth token is not null
        // log.denug("user=" + user + ", token=" + authToken);
        int counter = 0;
        for (CoordinatorActionBean action : actions) {
            // continue if numActionsToStart is negative (no limit on number of
            // actions), or if the counter is less than numActionsToStart
            if ((numActionsToStart < 0) || (counter < numActionsToStart)) {
                log.debug("Set status to SUBMITTED for id: " + action.getId());
                // change state of action to SUBMITTED
                action.setStatus(CoordinatorAction.Status.SUBMITTED);
                // queue action to start action
                queueCallable(new CoordActionStartCommand(action.getId(), user, authToken), 100);
                store.updateCoordinatorAction(action);
            }
            else {
                break;
            }
            counter++;

        }
        return null;
    }

    @Override
    protected Void execute(CoordinatorStore store) throws StoreException, CommandException {
        log.info("STARTED CoordActionReadyCommand for jobId=" + jobId);
        try {
            if (lock(jobId)) {
                call(store);
            }
            else {
                queueCallable(new CoordActionReadyCommand(jobId), LOCK_FAILURE_REQUEUE_INTERVAL);
                log.warn("CoordActionReadyCommand lock was not acquired - failed jobId=" + jobId
                        + ". Requeing the same.");
            }
        }
        catch (InterruptedException e) {
            queueCallable(new CoordActionReadyCommand(jobId), LOCK_FAILURE_REQUEUE_INTERVAL);
            log.warn("CoordActionReadyCommand lock acquiring failed with exception " + e.getMessage()
                    + " for jobId=" + jobId + " Requeing the same.");
        }
        finally {
            log.info("ENDED CoordActionReadyCommand for jobId=" + jobId);
        }
        return null;
    }

}
