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
import org.apache.oozie.ErrorCode;
import org.apache.oozie.event.BundleJobEvent;
import org.apache.oozie.event.CoordinatorActionEvent;
import org.apache.oozie.event.CoordinatorJobEvent;
import org.apache.oozie.client.event.Event;
import org.apache.oozie.client.event.Event.MessageType;
import org.apache.oozie.client.event.JobEvent;
import org.apache.oozie.event.EventQueue;
import org.apache.oozie.event.MemoryEventQueue;
import org.apache.oozie.event.WorkflowActionEvent;
import org.apache.oozie.event.WorkflowJobEvent;
import org.apache.oozie.event.listener.JobEventListener;
import org.apache.oozie.sla.listener.SLAEventListener;
import org.apache.oozie.client.event.SLAEvent;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.XLog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Service class that handles the events system - creating events queue,
 * managing configured properties and managing and invoking various event
 * listeners via worker threads
 */
public class EventHandlerService implements Service {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "EventHandlerService.";
    public static final String CONF_QUEUE_SIZE = CONF_PREFIX + "queue.size";
    public static final String CONF_EVENT_QUEUE = CONF_PREFIX + "event.queue";
    public static final String CONF_LISTENERS = CONF_PREFIX + "event.listeners";
    public static final String CONF_FILTER_APP_TYPES = CONF_PREFIX + "filter.app.types";
    public static final String CONF_BATCH_SIZE = CONF_PREFIX + "batch.size";
    public static final String CONF_WORKER_THREADS = CONF_PREFIX + "worker.threads";
    public static final String CONF_WORKER_INTERVAL = CONF_PREFIX + "worker.interval";

    private static EventQueue eventQueue;
    private XLog LOG;
    private Map<MessageType, List<?>> listenerMap = new HashMap<MessageType, List<?>>();
    private Set<String> apptypes;
    private static boolean eventsEnabled = false;
    private int numWorkers;

    @Override
    public void init(Services services) throws ServiceException {
        try {
            Configuration conf = services.getConf();
            LOG = XLog.getLog(getClass());
            Class<? extends EventQueue> queueImpl = (Class<? extends EventQueue>) ConfigurationService.getClass
                    (conf, CONF_EVENT_QUEUE);
            eventQueue = queueImpl == null ? new MemoryEventQueue() : (EventQueue) queueImpl.newInstance();
            eventQueue.init(conf);
            // initialize app-types to switch on events for
            initApptypes(conf);
            // initialize event listeners
            initEventListeners(conf);
            // initialize worker threads via Scheduler
            initWorkerThreads(conf, services);
            eventsEnabled = true;
            LOG.info("EventHandlerService initialized. Event queue = [{0}], Event listeners configured = [{1}],"
                    + " Events configured for App-types = [{2}], Num Worker Threads = [{3}]", eventQueue.getClass()
                    .getName(), listenerMap.toString(), apptypes, numWorkers);
        }
        catch (Exception ex) {
            throw new ServiceException(ErrorCode.E0100, ex.getMessage(), ex);
        }
    }

    private void initApptypes(Configuration conf) {
        apptypes = new HashSet<String>();
        for (String jobtype : ConfigurationService.getStrings(conf, CONF_FILTER_APP_TYPES)) {
            String tmp = jobtype.trim().toLowerCase();
            if (tmp.length() == 0) {
                continue;
            }
            apptypes.add(tmp);
        }
    }

    private void initEventListeners(Configuration conf) throws Exception {
        Class<?>[] listenerClass = ConfigurationService.getClasses(conf, CONF_LISTENERS);
        for (int i = 0; i < listenerClass.length; i++) {
            Object listener = null;
            try {
                listener = listenerClass[i].newInstance();
            }
            catch (InstantiationException e) {
                LOG.warn("Could not create event listener instance, " + e);
            }
            catch (IllegalAccessException e) {
                LOG.warn("Illegal access to event listener instance, " + e);
            }
            addEventListener(listener, conf, listenerClass[i].getName());
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void addEventListener(Object listener, Configuration conf, String name) throws Exception {
        if (listener instanceof JobEventListener) {
            List listenersList = listenerMap.get(MessageType.JOB);
            if (listenersList == null) {
                listenersList = new ArrayList();
                listenerMap.put(MessageType.JOB, listenersList);
            }
            listenersList.add(listener);
            ((JobEventListener) listener).init(conf);
        }
        else if (listener instanceof SLAEventListener) {
            List listenersList = listenerMap.get(MessageType.SLA);
            if (listenersList == null) {
                listenersList = new ArrayList();
                listenerMap.put(MessageType.SLA, listenersList);
            }
            listenersList.add(listener);
            ((SLAEventListener) listener).init(conf);
        }
        else {
            LOG.warn("Event listener [{0}] is of undefined type", name);
        }
    }

    public static boolean isEnabled() {
        return eventsEnabled;
    }

    private void initWorkerThreads(Configuration conf, Services services) throws ServiceException {
        numWorkers = ConfigurationService.getInt(conf, CONF_WORKER_THREADS);
        int interval = ConfigurationService.getInt(conf, CONF_WORKER_INTERVAL);
        SchedulerService ss = services.get(SchedulerService.class);
        int available = ss.getSchedulableThreads(conf);
        if (numWorkers + 3 > available) {
            throw new ServiceException(ErrorCode.E0100, getClass().getName(), "Event worker threads requested ["
                    + numWorkers + "] cannot be handled with current settings. Increase "
                    + SchedulerService.SCHEDULER_THREADS);
        }
        Runnable eventWorker = new EventWorker();
        // schedule staggered runnables every 1 min interval by default
        for (int i = 0; i < numWorkers; i++) {
            ss.schedule(eventWorker, 10 + i * 20, interval, SchedulerService.Unit.SEC);
        }
    }

    @Override
    public void destroy() {
        eventsEnabled = false;
        for (MessageType type : listenerMap.keySet()) {
            Iterator<?> iter = listenerMap.get(type).iterator();
            while (iter.hasNext()) {
                if (type == MessageType.JOB) {
                    ((JobEventListener) iter.next()).destroy();
                }
                else if (type == MessageType.SLA) {
                    ((SLAEventListener) iter.next()).destroy();
                }
            }
        }
    }

    @Override
    public Class<? extends Service> getInterface() {
        return EventHandlerService.class;
    }

    public boolean isSupportedApptype(String appType) {
        if (!apptypes.contains(appType.toLowerCase())) {
            return false;
        }
        return true;
    }

    public void setAppTypes(Set<String> types) {
        apptypes = types;
    }

    public Set<String> getAppTypes() {
        return apptypes;
    }

    public String listEventListeners() {
        return listenerMap.toString();
    }

    public void queueEvent(Event event) {
        LOG = LogUtils.setLogPrefix(LOG, event);
        LOG.debug("Queueing event : {0}", event);
        LOG.trace("Stack trace while queueing event : {0}", event, new Throwable());
        eventQueue.add(event);
        LogUtils.clearLogPrefix();
    }

    public EventQueue getEventQueue() {
        return eventQueue;
    }

    public class EventWorker implements Runnable {

        @Override
        public void run() {
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
            try {
                if (!eventQueue.isEmpty()) {
                    List<Event> work = eventQueue.pollBatch();
                    for (Event event : work) {
                        LOG = LogUtils.setLogPrefix(LOG, event);
                        LOG.debug("Processing event : {0}", event);
                        MessageType msgType = event.getMsgType();
                        List<?> listeners = listenerMap.get(msgType);
                        if (listeners != null) {
                            Iterator<?> iter = listeners.iterator();
                            while (iter.hasNext()) {
                                try {
                                    if (msgType == MessageType.JOB) {
                                        invokeJobEventListener((JobEventListener) iter.next(), (JobEvent) event);
                                    }
                                    else if (msgType == MessageType.SLA) {
                                        invokeSLAEventListener((SLAEventListener) iter.next(), (SLAEvent) event);
                                    }
                                    else {
                                        iter.next();
                                    }
                                }
                                catch (Throwable error) {
                                    XLog.getLog(EventHandlerService.class).debug("Throwable in EventWorker thread run : ",
                                            error);
                                }
                            }
                        }
                    }
                }
            }
            catch (Throwable error) {
                XLog.getLog(EventHandlerService.class).debug("Throwable in EventWorker thread run : ",
                        error);
            }
        }

        private void invokeJobEventListener(JobEventListener jobListener, JobEvent event) {
            switch (event.getAppType()) {
                case WORKFLOW_JOB:
                    jobListener.onWorkflowJobEvent((WorkflowJobEvent)event);
                    break;
                case WORKFLOW_ACTION:
                    jobListener.onWorkflowActionEvent((WorkflowActionEvent)event);
                    break;
                case COORDINATOR_JOB:
                    jobListener.onCoordinatorJobEvent((CoordinatorJobEvent)event);
                    break;
                case COORDINATOR_ACTION:
                    jobListener.onCoordinatorActionEvent((CoordinatorActionEvent)event);
                    break;
                case BUNDLE_JOB:
                    jobListener.onBundleJobEvent((BundleJobEvent)event);
                    break;
                default:
                    XLog.getLog(EventHandlerService.class).info("Undefined Job Event app-type - {0}",
                            event.getAppType());
            }
        }

        private void invokeSLAEventListener(SLAEventListener slaListener, SLAEvent event) {
            switch (event.getEventStatus()) {
                case START_MET:
                    slaListener.onStartMet(event);
                    break;
                case START_MISS:
                    slaListener.onStartMiss(event);
                    break;
                case END_MET:
                    slaListener.onEndMet(event);
                    break;
                case END_MISS:
                    slaListener.onEndMiss(event);
                    break;
                case DURATION_MET:
                    slaListener.onDurationMet(event);
                    break;
                case DURATION_MISS:
                    slaListener.onDurationMiss(event);
                    break;
                default:
                    XLog.getLog(EventHandlerService.class).info("Undefined SLA event type - {0}", event.getSLAStatus());
            }
        }
    }

}
