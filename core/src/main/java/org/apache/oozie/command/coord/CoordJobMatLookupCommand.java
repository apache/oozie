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

import java.sql.Timestamp;

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.XLog;

public class CoordJobMatLookupCommand extends CoordinatorCommand<Void> {
    private final XLog log = XLog.getLog(getClass());
    private int materializationWindow;
    private String jobId;

    public CoordJobMatLookupCommand(String id, int materializationWindow) {
        super("materialization_lookup", "materialization_lookup", -1, XLog.STD);
        this.jobId = id;
        this.materializationWindow = materializationWindow;
    }

    @Override
    protected Void call(CoordinatorStore store) throws StoreException, CommandException {
        //CoordinatorJobBean coordJob = store.getCoordinatorJob(jobId, true);
        CoordinatorJobBean coordJob = store.getEntityManager().find(CoordinatorJobBean.class, jobId);
        setLogInfo(coordJob);

        if (!(coordJob.getStatus() == CoordinatorJobBean.Status.PREP || coordJob.getStatus() == CoordinatorJobBean.Status.RUNNING)) {
            log.debug("CoordJobMatLookupCommand for jobId=" + jobId + " job is not in PREP or RUNNING but in "
                    + coordJob.getStatus());
            return null;
        }

        if (coordJob.getNextMaterializedTimestamp() != null
                && coordJob.getNextMaterializedTimestamp().compareTo(coordJob.getEndTimestamp()) >= 0) {
            log.debug("CoordJobMatLookupCommand for jobId=" + jobId + " job is already materialized");
            return null;
        }

        if (coordJob.getNextMaterializedTimestamp() != null
                && coordJob.getNextMaterializedTimestamp().compareTo(new Timestamp(System.currentTimeMillis())) >= 0) {
            log.debug("CoordJobMatLookupCommand for jobId=" + jobId + " job is already materialized");
            return null;
        }

        Timestamp startTime = coordJob.getNextMaterializedTimestamp();
        if (startTime == null) {
            startTime = coordJob.getStartTimestamp();
        }
        // calculate end time by adding materializationWindow to start time.
        // need to convert materializationWindow from secs to milliseconds
        long startTimeMilli = startTime.getTime();
        long endTimeMilli = startTimeMilli + (materializationWindow * 1000);
        Timestamp endTime = new Timestamp(endTimeMilli);
        // if MaterializationWindow end time is greater than endTime
        // for job, then set it to endTime of job
        Timestamp jobEndTime = coordJob.getEndTimestamp();
        if (endTime.compareTo(jobEndTime) > 0) {
            endTime = jobEndTime;
        }
        // update status of job from PREP or RUNNING to PREMATER in coordJob
        coordJob.setStatus(CoordinatorJob.Status.PREMATER);
        store.updateCoordinatorJobStatus(coordJob);

        log.debug("Materializing coord job id=" + jobId + ", start=" + DateUtils.toDate(startTime) + ", end=" + DateUtils.toDate(endTime)
                + ", window=" + materializationWindow + ", status=PREMATER");
        queueCallable(new CoordActionMaterializeCommand(jobId, startTime, endTime), 100);
        return null;
    }

    @Override
    protected Void execute(CoordinatorStore store) throws StoreException, CommandException {
        log.info("STARTED CoordJobMatLookupCommand jobId=" + jobId + ", materializationWindow="
                + materializationWindow);
        try {
            if (lock(jobId)) {
                call(store);
            }
            else {
                queueCallable(new CoordJobMatLookupCommand(jobId, materializationWindow), LOCK_FAILURE_REQUEUE_INTERVAL);
                log.warn("CoordJobMatLookupCommand lock was not acquired - failed jobId=" + jobId
                        + ". Requeing the same.");
            }
        }
        catch (InterruptedException e) {
            queueCallable(new CoordJobMatLookupCommand(jobId, materializationWindow), LOCK_FAILURE_REQUEUE_INTERVAL);
            log.warn("CoordJobMatLookupCommand lock acquiring failed with exception " + e.getMessage() + " for jobId="
                    + jobId + " Requeing the same.");
        }
        finally {
            log.info("ENDED CoordJobMatLookupCommand jobId=" + jobId + ", materializationWindow="
                    + materializationWindow);
        }
        return null;
    }
}
