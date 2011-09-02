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
package org.apache.oozie.service;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.client.Job;
import org.apache.oozie.executor.jpa.BundleActionsGetByLastModifiedTimeJPAExecutor;
import org.apache.oozie.executor.jpa.BundleActionsGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobsGetPendingJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobsGetRunningJPAExecutor;
import org.apache.oozie.service.SchedulerService;
import org.apache.oozie.service.Service;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.MemoryLocks;
import org.apache.oozie.util.XLog;

/**
 * StateTransitService is scheduled to run at the configured interval. It is to
 * update bundle job's status according to its child actions' status. If all
 * child actions' pending flag equals 0 (job done), we reset the job's pending
 * flag to 0. If all child actions are succeeded, we set the job's status to
 * SUCCEEDED.
 */
public class StatusTransitService implements Service {
    public static final String CONF_PREFIX = Service.CONF_PREFIX + "StatusTransitService.";
    public static final String CONF_STATUSTRANSIT_INTERVAL = CONF_PREFIX + "statusTransit.interval";
    private static int limit = -1;
    private static Date lastInstanceStartTime = null;
    private final static XLog LOG = XLog.getLog(StatusTransitRunnable.class);
    
    /**
     * StateTransitRunnable is the runnable which is scheduled to run at the configured interval. It is to
     * update bundle job's status according to its child actions' status. If all child actions' pending flag equals
     * 0 (job done), we reset the job's pending flag to 0. If all child actions are succeeded, we set the job's status to
     * SUCCEEDED.
     */
    static class StatusTransitRunnable implements Runnable {
        private JPAService jpaService = null;
        private MemoryLocks.LockToken lock;

        public StatusTransitRunnable() {
            jpaService = Services.get().get(JPAService.class);
            if (jpaService == null) {
                LOG.error("Missing JPAService");
            }
        }

        public void run() {
            try {
                Date d = new Date(); // records the start time of this service run;
                
                // first check if there is some other instance running;
                lock = Services.get().get(MemoryLocksService.class).getWriteLock(StatusTransitService.class.getName(), lockTimeout);
                if (lock == null) {
                    LOG.info("This StatusTransitService instance will not run since there is already an instance running");
                }
                else {
                    LOG.info("Acquired lock for [{0}]", StatusTransitService.class.getName());
                    
                    List<BundleJobBean> pendingJobCheckList = null;
                    List<BundleJobBean> runningJobCheckList = null;
                    if (lastInstanceStartTime == null) { // this is the first instance, we need to check for all pending jobs;
                        pendingJobCheckList = jpaService.execute(new BundleJobsGetPendingJPAExecutor(limit));
                        runningJobCheckList = jpaService.execute(new BundleJobsGetRunningJPAExecutor(limit));
                    }
                    else { // this is not the first instance, we should only check jobs that have actions been 
                           // updated >= start time of last service run;
                        List<BundleActionBean> actionList = jpaService.execute(new BundleActionsGetByLastModifiedTimeJPAExecutor(lastInstanceStartTime));
                        Set<String> bundleIds = new HashSet<String>();
                        for (BundleActionBean action : actionList) {
                            bundleIds.add(action.getBundleId());
                        }
                        pendingJobCheckList = new ArrayList<BundleJobBean>();
                        for (String bundleId : bundleIds.toArray(new String[bundleIds.size()])) {
                            BundleJobBean bundle = jpaService.execute(new BundleJobGetJPAExecutor(bundleId));
                            pendingJobCheckList.add(bundle);
                        }
                        
                        runningJobCheckList = pendingJobCheckList;
                    }
                    
                    for (BundleJobBean bundleJob : pendingJobCheckList) {
                        String jobId = bundleJob.getId();
                        List<BundleActionBean> actionList = jpaService.execute(new BundleActionsGetJPAExecutor(jobId));
                        if (checkAllBundleActionsDone(actionList)) {
                            bundleJob.resetPending();
                            jpaService.execute(new BundleJobUpdateJPAExecutor(bundleJob));
                        }
                    }

                    for (BundleJobBean bundleJob : runningJobCheckList) {
                        String jobId = bundleJob.getId();
                        List<BundleActionBean> actionList = jpaService.execute(new BundleActionsGetJPAExecutor(jobId));
                        if (checkAllBundleActionsSucceeded(actionList)) {
                            bundleJob.setStatus(Job.Status.SUCCEEDED);
                            jpaService.execute(new BundleJobUpdateJPAExecutor(bundleJob));
                        }
                    }

                    lastInstanceStartTime = d;
                }
            }
            catch (Exception ex) {
                LOG.warn("Exception happened during bundle job status transition", ex);
            }
            finally {
                // release lock;
                if (lock != null) {
                    lock.release();
                    LOG.info("Released lock for [{0}]", StatusTransitService.class.getName());
                }                
            }
        }

        private boolean checkAllBundleActionsDone(List<BundleActionBean> actionList) {
            boolean done = true;
            for (BundleActionBean action : actionList) {
                if (action.isPending()) { // action is not done;
                    done = false;
                    break;
                }
            }
            
            return done;
        }
        
        private boolean checkAllBundleActionsSucceeded(List<BundleActionBean> actionList) {
            boolean succeeded = true;
            for (BundleActionBean action : actionList) {
                if (action.getStatus() != Job.Status.SUCCEEDED) {
                    succeeded = false;
                    break;
                }
            }
            
            return succeeded;
        }
    }

    /**
     * Initializes the {@link StatusTransitService}.
     *
     * @param services services instance.
     */
    @Override
    public void init(Services services) {
        Configuration conf = services.getConf();
        Runnable stateTransitRunnable = new StatusTransitRunnable();
        services.get(SchedulerService.class).schedule(stateTransitRunnable, 10, conf.getInt(CONF_STATUSTRANSIT_INTERVAL, 60),
                                                      SchedulerService.Unit.SEC);
    }

    /**
     * Destroy the StateTransit Jobs Service.
     */
    @Override
    public void destroy() {
    }

    /**
     * Return the public interface for the purge jobs service.
     *
     * @return {@link StatusTransitService}.
     */
    @Override
    public Class<? extends Service> getInterface() {
        return StatusTransitService.class;
    }
}