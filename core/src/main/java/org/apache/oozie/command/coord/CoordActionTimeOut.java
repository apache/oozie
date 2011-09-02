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

import java.util.Date;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.util.XLog;

public class CoordActionTimeOut extends CoordinatorCommand<Void> {
    private CoordinatorActionBean actionBean;
    private final XLog log = XLog.getLog(getClass());

    public CoordActionTimeOut(CoordinatorActionBean actionBean) {
        super("coord_action_timeout", "coord_action_timeout", 0, XLog.STD);
        this.actionBean = actionBean;
    }

    @Override
    protected Void call(CoordinatorStore store) throws StoreException, CommandException {
        // actionBean = store.getCoordinatorAction(actionBean.getId(), false);
        actionBean = store.getEntityManager().find(CoordinatorActionBean.class, actionBean.getId());
        if (actionBean.getStatus() == CoordinatorAction.Status.WAITING) {
            actionBean.setStatus(CoordinatorAction.Status.TIMEDOUT);
            queueCallable(new CoordActionNotification(actionBean), 100);
            store.updateCoordinatorAction(actionBean);
        }
        return null;
    }

    @Override
    protected Void execute(CoordinatorStore store) throws StoreException, CommandException {
        String jobId = actionBean.getJobId();
        setLogInfo(actionBean);
        log.info("STARTED CoordinatorActionTimeOut for Action Id " + actionBean.getId() + " of job Id :"
                + actionBean.getJobId() + ". Timeout value is " + actionBean.getTimeOut() + " mins");
        try {
            if (lock(jobId)) {
                call(store);
            }
            else {
                queueCallable(new CoordActionTimeOut(actionBean), LOCK_FAILURE_REQUEUE_INTERVAL);
                log.warn("CoordinatorActionTimeOut lock was not acquired - " + " failed " + jobId
                        + ". Requeing the same.");
            }
        }
        catch (InterruptedException e) {
            queueCallable(new CoordActionTimeOut(actionBean), LOCK_FAILURE_REQUEUE_INTERVAL);
            log.warn("CoordinatorActionTimeOut lock acquiring failed " + " with exception " + e.getMessage()
                    + " for job id " + jobId + ". Requeing the same.");
        }
        finally {
            log.info("ENDED CoordinatorActionTimeOut for Action Id " + actionBean.getId());
        }
        return null;
    }
}
