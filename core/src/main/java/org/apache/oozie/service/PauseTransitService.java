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

package org.apache.oozie.service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.command.bundle.BundlePauseXCommand;
import org.apache.oozie.command.bundle.BundleStartXCommand;
import org.apache.oozie.command.bundle.BundleUnpauseXCommand;
import org.apache.oozie.command.coord.CoordPauseXCommand;
import org.apache.oozie.command.coord.CoordUnpauseXCommand;
import org.apache.oozie.executor.jpa.BundleJobsGetNeedStartJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobsGetPausedJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobsGetUnpausedJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobsGetPausedJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobsGetUnpausedJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.SchedulerService;
import org.apache.oozie.service.Service;
import org.apache.oozie.service.Services;
import org.apache.oozie.lock.LockToken;
import org.apache.oozie.util.ConfigUtils;
import org.apache.oozie.util.XCallable;
import org.apache.oozie.util.XLog;

import com.google.common.annotations.VisibleForTesting;

/**
 * PauseTransitService is the runnable which is scheduled to run at the configured interval, it checks all bundles to
 * see if they should be paused, un-paused or started.
 */
public class PauseTransitService implements Service {
    public static final String CONF_PREFIX = Service.CONF_PREFIX + "PauseTransitService.";
    public static final String CONF_BUNDLE_PAUSE_START_INTERVAL = CONF_PREFIX + "PauseTransit.interval";
    private final static XLog LOG = XLog.getLog(PauseTransitService.class);

    public static final String CONF_CALLABLE_BATCH_SIZE = CONF_PREFIX + "callable.batch.size";

    /**
     * PauseTransitRunnable is the runnable which is scheduled to run at the configured interval, it checks all bundles
     * to see if they should be paused, un-paused or started.
     */
    @VisibleForTesting
    public static class PauseTransitRunnable implements Runnable {
        private JPAService jpaService = null;
        private LockToken lock;
        private List<XCallable<Void>> callables;

        public PauseTransitRunnable() {
            jpaService = Services.get().get(JPAService.class);
            if (jpaService == null) {
                LOG.error("Missing JPAService");
            }
        }

        public void run() {
            try {
                // first check if there is some other running instance from the same service;
                lock = Services.get().get(MemoryLocksService.class)
                        .getWriteLock(PauseTransitService.class.getName(), lockTimeout);
                if (lock == null) {
                    LOG.info("This PauseTransitService instance will"
                            + "not run since there is already an instance running");
                }
                else {
                    LOG.info("Acquired lock for [{0}]", PauseTransitService.class.getName());

                    updateBundle();
                    updateCoord();
                    if (null != callables) {
                        boolean ret = Services.get().get(CallableQueueService.class).queueSerial(callables);
                        if (ret == false) {
                            XLog.getLog(getClass()).warn(
                                    "Unable to queue the callables commands for PauseTransitService. "
                                            + "Most possibly command queue is full. Queue size is :"
                                            + Services.get().get(CallableQueueService.class).queueSize());
                        }
                        callables = null;
                    }

                }
            }
            catch (Exception ex) {
                LOG.warn("Exception happened when pausing/unpausing/starting bundle/coord jobs", ex);
            }
            finally {
                // release lock;
                if (lock != null) {
                    lock.release();
                    LOG.info("Released lock for [{0}]", PauseTransitService.class.getName());
                }
            }
        }

        private void updateBundle() {
            Date d = new Date(); // records the start time of this service run;
            List<BundleJobBean> jobList = null;
            // pause bundles as needed;
            try {
                jobList = jpaService.execute(new BundleJobsGetUnpausedJPAExecutor(-1));
                if (jobList != null) {
                    for (BundleJobBean bundleJob : jobList) {
                        if ((bundleJob.getPauseTime() != null) && !bundleJob.getPauseTime().after(d)) {
                            queueCallable(new BundlePauseXCommand(bundleJob));
                            LOG.debug("Queuing BundlePauseXCommand for bundle job = " + bundleJob.getId());
                        }
                    }
                }
            }
            catch (JPAExecutorException ex) {
                LOG.warn("JPAExecutorException happened when pausing/unpausing/starting Bundle jobs", ex);
            }
            // unpause bundles as needed;
            try {
                jobList = jpaService.execute(new BundleJobsGetPausedJPAExecutor(-1));
                if (jobList != null) {
                    for (BundleJobBean bundleJob : jobList) {
                        if ((bundleJob.getPauseTime() == null || bundleJob.getPauseTime().after(d))) {
                            queueCallable(new BundleUnpauseXCommand(bundleJob));
                            LOG.debug("Queuing BundleUnpauseXCommand for bundle job = " + bundleJob.getId());
                        }
                    }
                }
            }
            catch (JPAExecutorException ex) {
                LOG.warn("JPAExecutorException happened when pausing/unpausing/starting Bundle jobs", ex);
            }
            // start bundles as needed;
            try {
                jobList = jpaService.execute(new BundleJobsGetNeedStartJPAExecutor(d));
                if (jobList != null) {
                    for (BundleJobBean bundleJob : jobList) {
                        queueCallable(new BundleStartXCommand(bundleJob.getId()));
                        LOG.debug("Queuing BundleStartXCommand for bundle job = " + bundleJob.getId());
                    }
                }
            }
            catch (JPAExecutorException ex) {
                LOG.warn("JPAExecutorException happened when pausing/unpausing/starting Bundle jobs", ex);
            }
        }

        private void updateCoord() {
            Date d = new Date(); // records the start time of this service run;
            List<CoordinatorJobBean> jobList = null;
            boolean backwardSupportForCoordStatus = ConfigUtils.isBackwardSupportForCoordStatus();

            // pause coordinators as needed;
            try {
                jobList = jpaService.execute(new CoordJobsGetUnpausedJPAExecutor(-1));
                if (jobList != null) {
                    for (CoordinatorJobBean coordJob : jobList) {
                        // if namespace 0.1 is used and backward support is true, then ignore this coord job
                        if (backwardSupportForCoordStatus == true && coordJob.getAppNamespace() != null
                                && coordJob.getAppNamespace().equals(SchemaService.COORDINATOR_NAMESPACE_URI_1)) {
                            continue;
                        }
                        if ((coordJob.getPauseTime() != null) && !coordJob.getPauseTime().after(d)) {
                            queueCallable(new CoordPauseXCommand(coordJob));
                            LOG.debug("Queuing CoordPauseXCommand for coordinator job = " + coordJob.getId());
                        }
                    }
                }
            }
            catch (JPAExecutorException ex) {
                LOG.warn("JPAExecutorException happened when pausing/unpausing Coordinator jobs", ex);
            }
            // unpause coordinators as needed;
            try {
                jobList = jpaService.execute(new CoordJobsGetPausedJPAExecutor(-1));
                if (jobList != null) {
                    for (CoordinatorJobBean coordJob : jobList) {
                        // if namespace 0.1 is used and backward support is true, then ignore this coord job
                        if (backwardSupportForCoordStatus == true && coordJob.getAppNamespace() != null
                                && coordJob.getAppNamespace().equals(SchemaService.COORDINATOR_NAMESPACE_URI_1)) {
                            continue;
                        }
                        if ((coordJob.getPauseTime() == null || coordJob.getPauseTime().after(d))) {
                            queueCallable(new CoordUnpauseXCommand(coordJob));
                            LOG.debug("Queuing CoordUnpauseXCommand for coordinator job = " + coordJob.getId());
                        }
                    }
                }
            }
            catch (JPAExecutorException ex) {
                LOG.warn("JPAExecutorException happened when pausing/unpausing Coordinator jobs", ex);
            }
        }

        private void queueCallable(XCallable<Void> callable) {
            if (callables == null) {
                callables = new ArrayList<XCallable<Void>>();
            }
            callables.add(callable);
            if (callables.size() == ConfigurationService.getInt(CONF_CALLABLE_BATCH_SIZE)) {
                boolean ret = Services.get().get(CallableQueueService.class).queueSerial(callables);
                if (ret == false) {
                    XLog.getLog(getClass()).warn(
                            "Unable to queue the callables commands for PauseTransitService. "
                                    + "Most possibly command queue is full. Queue size is :"
                                    + Services.get().get(CallableQueueService.class).queueSize());
                }
                callables = new ArrayList<XCallable<Void>>();
            }
        }
    }

    /**
     * Initializes the {@link PauseTransitService}.
     *
     * @param services services instance.
     */
    @Override
    public void init(Services services) {
        Runnable bundlePauseStartRunnable = new PauseTransitRunnable();
        services.get(SchedulerService.class).schedule(bundlePauseStartRunnable, 10,
                ConfigurationService.getInt(services.getConf(), CONF_BUNDLE_PAUSE_START_INTERVAL),
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
     * @return {@link PauseTransitService}.
     */
    @Override
    public Class<? extends Service> getInterface() {
        return PauseTransitService.class;
    }
}
