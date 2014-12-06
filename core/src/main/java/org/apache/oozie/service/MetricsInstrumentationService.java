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

import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.MetricsInstrumentation;


/**
 * This service provides an {@link Instrumentation} instance mostly compatible with the original Instrumentation, but backed by
 * Codahale Metrics. <p/> This service depends on the {@link SchedulerService}. <p/> The {@link #CONF_LOGGING_INTERVAL}
 * configuration property indicates how often snapshots of the instrumentation should be logged.
 */
public class MetricsInstrumentationService extends InstrumentationService {

    private static boolean isEnabled = false;

    /**
     * Initialize the metrics instrumentation service.
     *
     * @param services services instance.
     * @throws org.apache.oozie.service.ServiceException
     */
    @Override
    public void init(Services services) throws ServiceException {
        final MetricsInstrumentation instr = new MetricsInstrumentation();
        int interval = services.getConf().getInt(CONF_LOGGING_INTERVAL, 60);
        initLogging(services, instr, interval);
        instrumentation = instr;
        isEnabled = true;
    }

    /**
     * Destroy the metrics instrumentation service.
     */
    @Override
    public void destroy() {
        isEnabled = false;
        instrumentation = null;
    }

    /**
     * Returns if the MetricsInstrumentationService is enabled or not.
     *
     * @return true if the MetricsInstrumentationService is enabled; false if not
     */
    public static boolean isEnabled() {
        return isEnabled;
    }
}
