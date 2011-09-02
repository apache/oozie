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

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.util.XLog;

public class CoordRecoveryCommand extends CoordinatorCommand<Void> {
    private final XLog log = XLog.getLog(getClass());
    private String jobId;

    public CoordRecoveryCommand(String id) {
        super("coord_recovery", "coord_recovery", 0, XLog.STD);
        this.jobId = id;
    }

    @Override
    protected Void call(CoordinatorStore store) throws StoreException {
        //CoordinatorJobBean coordJob = store.getCoordinatorJob(jobId, true);
        CoordinatorJobBean coordJob = store.getEntityManager().find(CoordinatorJobBean.class, jobId);
        setLogInfo(coordJob);
        // update status of job from PREMATER to RUNNING in coordJob
        coordJob.setStatus(CoordinatorJob.Status.RUNNING);
        store.updateCoordinatorJob(coordJob);
        log.debug("[" + jobId + "]: Recover status from PREMATER to RUNNING");
        return null;
    }

    @Override
    protected Void execute(CoordinatorStore store) throws StoreException, CommandException {
        log.info("STARTED CoordRecoveryCommand for jobId=" + jobId);
        try {
            if (lock(jobId)) {
                call(store);
            }
            else {
                queueCallable(new CoordRecoveryCommand(jobId), LOCK_FAILURE_REQUEUE_INTERVAL);
                log.warn("CoordRecoveryCommand lock was not acquired - failed jobId=" + jobId
                        + ". Requeing the same.");
            }
        }
        catch (InterruptedException e) {
            queueCallable(new CoordRecoveryCommand(jobId), LOCK_FAILURE_REQUEUE_INTERVAL);
            log.warn("CoordRecoveryCommand lock acquiring failed with exception " + e.getMessage()
                    + " for jobId=" + jobId + " Requeing the same.");
        }
        finally {
            log.info("ENDED CoordRecoveryCommand for jobId=" + jobId);
        }
        return null;
    }

}