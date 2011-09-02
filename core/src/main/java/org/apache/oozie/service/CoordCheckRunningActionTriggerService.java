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
import org.apache.oozie.command.coord.CoordCheckRunningActionCommand;


public class CoordCheckRunningActionTriggerService implements Service {
    public static final String CONF_PREFIX = Service.CONF_PREFIX + "CoordCheckRunningActionTriggerService.";
    /**
     * Time interval, in seconds, at which the Job materialization service will be scheduled to run.
     */
    public static final String CONF_CHECK_INTERVAL = CONF_PREFIX + "check.interval";

    /**
     * This  runnable class will run in every "interval" to queue CoordJobMatLookupTriggerCommand.
     */
    static class CoordCheckRunningActionTriggerRunnable implements Runnable {

        @Override
        public void run() {
            Services.get().get(CallableQueueService.class).queue(new CoordCheckRunningActionCommand());
        }

    }

    @Override
    public void init(Services services) throws ServiceException {
        Configuration conf = services.getConf();
        Runnable checkTriggerJobsRunnable = new CoordCheckRunningActionTriggerRunnable();
        services.get(SchedulerService.class).schedule(checkTriggerJobsRunnable, 10,
                                                      conf.getInt(CONF_CHECK_INTERVAL, 300),//Default is 5 minutes
                                                      SchedulerService.Unit.SEC);
        return;
    }

    @Override
    public void destroy() {
        // TODO Auto-generated method stub

    }

    @Override
    public Class<? extends Service> getInterface() {
        return CoordCheckRunningActionTriggerService.class;
	}

	

}
