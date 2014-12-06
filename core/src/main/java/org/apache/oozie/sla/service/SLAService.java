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
package org.apache.oozie.sla.service;

import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.event.JobEvent.EventStatus;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.service.SchedulerService;
import org.apache.oozie.service.Service;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.SLACalculator;
import org.apache.oozie.sla.SLACalculatorMemory;
import org.apache.oozie.sla.SLARegistrationBean;
import org.apache.oozie.util.XLog;
import com.google.common.annotations.VisibleForTesting;

public class SLAService implements Service {

    public static final String CONF_PREFIX = "oozie.sla.service.SLAService.";
    public static final String CONF_CALCULATOR_IMPL = CONF_PREFIX + "calculator.impl";
    public static final String CONF_CAPACITY = CONF_PREFIX + "capacity";
    public static final String CONF_ALERT_EVENTS = CONF_PREFIX + "alert.events";
    public static final String CONF_EVENTS_MODIFIED_AFTER = CONF_PREFIX + "events.modified.after";
    public static final String CONF_JOB_EVENT_LATENCY = CONF_PREFIX + "job.event.latency";

    private static SLACalculator calcImpl;
    private static boolean slaEnabled = false;
    private EventHandlerService eventHandler;
    public static XLog LOG;
    @Override
    public void init(Services services) throws ServiceException {
        try {
            Configuration conf = services.getConf();
            Class<? extends SLACalculator> calcClazz = (Class<? extends SLACalculator>) conf.getClass(
                    CONF_CALCULATOR_IMPL, null);
            calcImpl = calcClazz == null ? new SLACalculatorMemory() : (SLACalculator) calcClazz.newInstance();
            calcImpl.init(conf);
            eventHandler = Services.get().get(EventHandlerService.class);
            if (eventHandler == null) {
                throw new ServiceException(ErrorCode.E0103, "EventHandlerService", "Add it under config "
                        + Services.CONF_SERVICE_EXT_CLASSES + " or declare it BEFORE SLAService");
            }
            LOG = XLog.getLog(getClass());
            java.util.Set<String> appTypes = eventHandler.getAppTypes();
            appTypes.add("workflow_action");
            eventHandler.setAppTypes(appTypes);

            Runnable slaThread = new SLAWorker(calcImpl);
            // schedule runnable by default every 30 sec
            services.get(SchedulerService.class).schedule(slaThread, 10, 30, SchedulerService.Unit.SEC);
            slaEnabled = true;
            LOG.info("SLAService initialized with impl [{0}] capacity [{1}]", calcImpl.getClass().getName(),
                    conf.get(SLAService.CONF_CAPACITY));
        }
        catch (Exception ex) {
            throw new ServiceException(ErrorCode.E0102, ex.getMessage(), ex);
        }
    }

    @Override
    public void destroy() {
        slaEnabled = false;
    }

    @Override
    public Class<? extends Service> getInterface() {
        return SLAService.class;
    }

    public static boolean isEnabled() {
        return slaEnabled;
    }

    public SLACalculator getSLACalculator() {
        return calcImpl;
    }

    @VisibleForTesting
    public void runSLAWorker() {
        new SLAWorker(calcImpl).run();
    }

    private class SLAWorker implements Runnable {

        SLACalculator calc;

        public SLAWorker(SLACalculator calc) {
            this.calc = calc;
        }

        @Override
        public void run() {
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
            try {
                calc.updateAllSlaStatus();
            }
            catch (Throwable error) {
                XLog.getLog(SLAService.class).debug("Throwable in SLAWorker thread run : ", error);
            }
        }
    }

    public boolean addRegistrationEvent(SLARegistrationBean reg) throws ServiceException {
        try {
            if (calcImpl.addRegistration(reg.getId(), reg)) {
                return true;
            }
            else {
                LOG.warn("SLA queue full. Unable to add new SLA entry for job [{0}]", reg.getId());
            }
        }
        catch (JPAExecutorException ex) {
            LOG.warn("Could not add new SLA entry for job [{0}]", reg.getId(), ex);
        }
        return false;
    }

    public boolean updateRegistrationEvent(SLARegistrationBean reg) throws ServiceException {
        try {
            if (calcImpl.updateRegistration(reg.getId(), reg)) {
                return true;
            }
            else {
                LOG.warn("SLA queue full. Unable to update the SLA entry for job [{0}]", reg.getId());
            }
        }
        catch (JPAExecutorException ex) {
            LOG.warn("Could not update SLA entry for job [{0}]", reg.getId(), ex);
        }
        return false;
    }

    public boolean addStatusEvent(String jobId, String status, EventStatus eventStatus, Date startTime, Date endTime)
            throws ServiceException {
        try {
            if (calcImpl.addJobStatus(jobId, status, eventStatus, startTime, endTime)) {
                return true;
            }
        }
        catch (JPAExecutorException jpe) {
            LOG.error("Exception while adding SLA Status event for Job [{0}]", jobId);
        }
        return false;
    }

    public void removeRegistration(String jobId) {
        calcImpl.removeRegistration(jobId);
    }

}
