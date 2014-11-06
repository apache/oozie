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
import org.apache.oozie.util.XLog;
import org.apache.oozie.ErrorCode;

import java.util.Map;


/**
 * This service provides an {@link Instrumentation} instance configured to support samplers. <p/> This service depends
 * on the {@link SchedulerService}. <p/> The {@link #CONF_LOGGING_INTERVAL} configuration property indicates how often
 * snapshots of the instrumentation should be logged.
 */
public class InstrumentationService implements Service {
    private static final String JVM_INSTRUMENTATION_GROUP = "jvm";

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "InstrumentationService.";

    public static final String CONF_LOGGING_INTERVAL = CONF_PREFIX + "logging.interval";

    private final XLog log = XLog.getLog(XLog.INSTRUMENTATION_LOG_NAME);

    protected static Instrumentation instrumentation = null;

    private static boolean isEnabled = false;

    /**
     * Initialize the instrumentation service.
     *
     * @param services services instance.
     */
    @Override
    public void init(Services services) throws ServiceException {
        final Instrumentation instr = new Instrumentation();
        int interval = ConfigurationService.getInt(services.getConf(), CONF_LOGGING_INTERVAL);
        initLogging(services, instr, interval);
        instr.addVariable(JVM_INSTRUMENTATION_GROUP, "free.memory", new Instrumentation.Variable<Long>() {
            @Override
            public Long getValue() {
                return Runtime.getRuntime().freeMemory();
            }
        });
        instr.addVariable(JVM_INSTRUMENTATION_GROUP, "max.memory", new Instrumentation.Variable<Long>() {
            @Override
            public Long getValue() {
                return Runtime.getRuntime().maxMemory();
            }
        });
        instr.addVariable(JVM_INSTRUMENTATION_GROUP, "total.memory", new Instrumentation.Variable<Long>() {
            @Override
            public Long getValue() {
                return Runtime.getRuntime().totalMemory();
            }
        });
        instrumentation = instr;
        isEnabled = true;
    }

    protected void initLogging(Services services, final Instrumentation instr, int interval) throws ServiceException {
        log.info("*********** Startup ***********");
        log.info("Java System Properties: {E}{0}", mapToString(instr.getJavaSystemProperties()));
        log.info("OS Env: {E}{0}", mapToString(instr.getOSEnv()));
        SchedulerService schedulerService = services.get(SchedulerService.class);
        if (schedulerService != null) {
            instr.setScheduler(schedulerService.getScheduler());
            if (interval > 0) {
                Runnable instrumentationLogger = new Runnable() {
                    @Override
                    public void run() {
                        try {
                            log.info("\n" + instr.toString());
                        }
                        catch (Throwable ex) {
                            log.warn("Instrumentation logging error", ex);
                        }
                    }
                };
                schedulerService.schedule(instrumentationLogger, interval, interval, SchedulerService.Unit.SEC);
            }
        }
        else {
            throw new ServiceException(ErrorCode.E0100, getClass().getName(), "SchedulerService unavailable");
        }
    }

    protected String mapToString(Map<String, String> map) {
        String E = System.getProperty("line.separator");
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            sb.append("    ").append(entry.getKey()).append(" = ").append(entry.getValue()).append(E);
        }
        return sb.toString();
    }

    /**
     * Destroy the instrumentation service.
     */
    @Override
    public void destroy() {
        isEnabled = false;
        instrumentation = null;
    }

    /**
     * Return the public interface for instrumentation service.
     *
     * @return {@link InstrumentationService}.
     */
    @Override
    public Class<? extends Service> getInterface() {
        return InstrumentationService.class;
    }

    /**
     * Return the instrumentation instance used by the service.
     *
     * @return the instrumentation instance.
     */
    public Instrumentation get() {
        return instrumentation;
    }

    /**
     * Returns if the InstrumentationService is enabled or not.
     *
     * @return true if the InstrumentationService is enabled; false if not
     */
    public static boolean isEnabled() {
        return isEnabled;
    }
}
