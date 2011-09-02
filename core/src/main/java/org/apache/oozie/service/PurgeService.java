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
package org.apache.oozie.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.command.wf.PurgeCommand;
import org.apache.oozie.service.CallableQueueService;
import org.apache.oozie.service.SchedulerService;
import org.apache.oozie.service.Service;
import org.apache.oozie.service.Services;

/**
 * The PurgeService schedules purging of completed jobs and associated action
 * older than a specified age.
 */
public class PurgeService implements Service {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "PurgeService.";
    /**
     * Age of completed jobs to be deleted, in days.
     */
    public static final String CONF_OLDER_THAN = CONF_PREFIX + "older.than";
    /**
     * Time interval, in seconds, at which the purge jobs service will be
     * scheduled to run.
     */
    public static final String CONF_PURGE_INTERVAL = CONF_PREFIX + "purge.interval";

    /**
     * PurgeRunnable is the runnable which is scheduled to run at the configured
     * interval. PurgeCommand is queued to remove completed jobs and associated
     * actions older than the configured age.
     */
    static class PurgeRunnable implements Runnable {
        private int olderThan;

        public PurgeRunnable(int olderThan) {
            this.olderThan = olderThan;
        }

        public void run() {
            Services.get().get(CallableQueueService.class).queue(new PurgeCommand(olderThan));
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
        Runnable purgeJobsRunnable = new PurgeRunnable(conf.getInt(CONF_OLDER_THAN, 30));
        services.get(SchedulerService.class).schedule(purgeJobsRunnable, 10, conf.getInt(CONF_PURGE_INTERVAL, 3600),
                                                      SchedulerService.Unit.SEC);
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
