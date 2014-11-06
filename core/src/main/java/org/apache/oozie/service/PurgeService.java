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

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.command.PurgeXCommand;

/**
 * The PurgeService schedules purging of completed jobs and associated action older than a specified age for workflow, coordinator and bundle.
 */
public class PurgeService implements Service {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "PurgeService.";
    /**
     * Age of completed jobs to be deleted, in days.
     */
    public static final String CONF_OLDER_THAN = CONF_PREFIX + "older.than";
    public static final String COORD_CONF_OLDER_THAN = CONF_PREFIX + "coord.older.than";
    public static final String BUNDLE_CONF_OLDER_THAN = CONF_PREFIX + "bundle.older.than";
    public static final String PURGE_OLD_COORD_ACTION = CONF_PREFIX + "purge.old.coord.action";
    /**
     * Time interval, in seconds, at which the purge jobs service will be scheduled to run.
     */
    public static final String CONF_PURGE_INTERVAL = CONF_PREFIX + "purge.interval";
    public static final String PURGE_LIMIT = CONF_PREFIX + "purge.limit";

    /**
     * PurgeRunnable is the runnable which is scheduled to run at the configured interval. PurgeCommand is queued to
     * remove completed jobs and associated actions older than the configured age for workflow, coordinator and bundle.
     */
    static class PurgeRunnable implements Runnable {
        private int wfOlderThan;
        private int coordOlderThan;
        private int bundleOlderThan;
        private int limit;
        private boolean purgeOldCoordAction = false;

        public PurgeRunnable(int wfOlderThan, int coordOlderThan, int bundleOlderThan, int limit) {
            this.wfOlderThan = wfOlderThan;
            this.coordOlderThan = coordOlderThan;
            this.bundleOlderThan = bundleOlderThan;
            this.limit = limit;
        }

        public PurgeRunnable(int wfOlderThan, int coordOlderThan, int bundleOlderThan, int limit,
                             boolean purgeOldCoordAction) {
            this.wfOlderThan = wfOlderThan;
            this.coordOlderThan = coordOlderThan;
            this.bundleOlderThan = bundleOlderThan;
            this.limit = limit;
            this.purgeOldCoordAction = purgeOldCoordAction;
        }

        public void run() {
            // Only queue the purge command if this is the leader
            if (Services.get().get(JobsConcurrencyService.class).isLeader()) {
                Services.get().get(CallableQueueService.class).queue(
                        new PurgeXCommand(wfOlderThan, coordOlderThan, bundleOlderThan, limit, purgeOldCoordAction));
            }
        }

    }

    /**
     * Initializes the {@link PurgeService}.
     *
     * @param services services instance.
     */
    @Override
    public void init(Services services) {
        Configuration conf = services.getConf();
        Runnable purgeJobsRunnable = new PurgeRunnable(ConfigurationService.getInt(conf, CONF_OLDER_THAN),
                ConfigurationService.getInt(conf, COORD_CONF_OLDER_THAN),
                ConfigurationService.getInt(conf, BUNDLE_CONF_OLDER_THAN),
                ConfigurationService.getInt(conf, PURGE_LIMIT),
                ConfigurationService.getBoolean(conf, PURGE_OLD_COORD_ACTION));
        services.get(SchedulerService.class).schedule(purgeJobsRunnable, 10,
                ConfigurationService.getInt(conf, CONF_PURGE_INTERVAL), SchedulerService.Unit.SEC);
    }

    /**
     * Destroy the Purge Jobs Service.
     */
    @Override
    public void destroy() {
    }

    /**
     * Return the public interface for the purge jobs service.
     *
     * @return {@link PurgeService}.
     */
    @Override
    public Class<? extends Service> getInterface() {
        return PurgeService.class;
    }
}
