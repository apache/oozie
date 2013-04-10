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
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.apache.oozie.client.event.SLAEvent;
import org.apache.oozie.sla.event.listener.SLAEventListener;
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
    public static final String CONF_APP_TYPES = CONF_PREFIX + "app.types";
    public static final String CONF_BATCH_SIZE = CONF_PREFIX + "batch.size";
    public static final String CONF_WORKER_INTERVAL = CONF_PREFIX + "worker.interval";

    private static EventQueue eventQueue;
    private int queueMaxSize;
    private XLog LOG;
    private static Map<MessageType, List<?>> listenerMap = new HashMap<MessageType, List<?>>();
    private Set<String> apptypes;
    private static int batchSize;
    private static boolean eventsConfigured = false;

    @SuppressWarnings("unchecked")
    public void init(Services services) throws ServiceException {
        try {
            eventsConfigured = true;
            Configuration conf = services.getConf();
            queueMaxSize = conf.getInt(CONF_QUEUE_SIZE, 10000);
            LOG = XLog.getLog(getClass());
            Class<? extends EventQueue> queueImpl = (Class<? extends EventQueue>) conf.getClass(CONF_EVENT_QUEUE, null);
            eventQueue = queueImpl == null ? new MemoryEventQueue() : (EventQueue) queueImpl.newInstance();
            batchSize = conf.getInt(CONF_BATCH_SIZE, 10);
            eventQueue.init(queueMaxSize, batchSize);
            // initialize app-types to switch on events for
            initApptypes(conf);
            // initialize event listeners
            initEventListeners(conf);
            // initialize worker threads via Scheduler
            initWorkerThreads(conf, services);
            LOG.info(
                    "EventHandlerService initialized. Event queue = [{0}], Event listeners configured = [{1}], Events configured for App-types = [{3}]",
                    eventQueue.getClass().getName(), listenerMap.toString(), apptypes);
        }
        catch (Exception ex) {
            throw new ServiceException(ErrorCode.E0102, ex.getMessage(), ex);
        }
    }

    private void initApptypes(Configuration conf) {
        apptypes = new HashSet<String>();
        for (String jobtype : conf.getStringCollection(CONF_APP_TYPES)) {
            String tmp = jobtype.trim().toLowerCase();
            if (tmp.length() == 0) {
                continue;
            }
            apptypes.add(tmp);
        }
    }

    private void initEventListeners(Configuration conf) {
        Class<?>[] listenerClass = (Class<?>[]) conf.getClasses(CONF_LISTENERS);
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
            addEventListener(listener);
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void addEventListener(Object listener) {
        if (listener instanceof JobEventListener) {
            List listenersList = listenerMap.get(MessageType.JOB);
            if (listenersList == null) {
                listenersList = new ArrayList();
                listenerMap.put(MessageType.JOB, (List<? extends JobEventListener>) listenersList);
            }
            listenersList.add(listener);
            ((JobEventListener) listener).init();
        }
        else if (listener instanceof SLAEventListener) {
            List listenersList = listenerMap.get(MessageType.SLA);
            if (listenersList == null) {
                listenersList = new ArrayList();
                listenerMap.put(MessageType.SLA, (List<? extends SLAEventListener>) listenersList);
            }
            listenersList.add(listener);
            ((SLAEventListener) listener).init();
        }
        else {
            LOG.warn("Event listener [{0}] is of undefined type", listener.getClass().getCanonicalName());
        }
    }

    public static boolean isEventsConfigured() {
        return eventsConfigured;
    }

    private void initWorkerThreads(Configuration conf, Services services) {
        Runnable eventWorker = new EventWorker();
        // schedule runnable by default every 5 min
        services.get(SchedulerService.class).schedule(eventWorker, 10, conf.getInt(CONF_WORKER_INTERVAL, 60),
                SchedulerService.Unit.SEC);
    }

    @Override
    public void destroy() {
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
        eventsConfigured = false;
    }

    @Override
    public Class<? extends Service> getInterface() {
        return EventHandlerService.class;
    }

    public boolean checkSupportedApptype(String appType) {
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

    public void queueEvent(Event event) {
        eventQueue.add(event);
    }

    public EventQueue getEventQueue() {
        return eventQueue;
    }

    public class EventWorker implements Runnable {

        public void run() {
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
            if (!eventQueue.isEmpty()) {
                Set<Event> work = eventQueue.pollBatch();
                for (Event event : work) {
                    MessageType msgType = event.getMsgType();
                    List<?> listeners = listenerMap.get(msgType);
                    if (listeners != null) {
                        Iterator<?> iter = listeners.iterator();
                        while (iter.hasNext()) {
                            if (msgType == MessageType.JOB) {
                                invokeJobEventListener(iter, (JobEvent) event);
                            }
                            else if (msgType == MessageType.SLA) {
                                invokeSLAEventListener(iter, (SLAEvent) event);
                            }
                        }
                    }
                }
            }
        }

        private void invokeJobEventListener(Iterator<?> iter, JobEvent event) {
            JobEventListener el = (JobEventListener) iter.next();
            switch (event.getAppType()) {
                case WORKFLOW_JOB:
                    onWorkflowJobEvent(event, el);
                    break;
                case WORKFLOW_ACTION:
                    onWorkflowActionEvent(event, el);
                    break;
                case COORDINATOR_JOB:
                    onCoordinatorJobEvent(event, el);
                    break;
                case COORDINATOR_ACTION:
                    onCoordinatorActionEvent(event, el);
                    break;
                case BUNDLE_JOB:
                    onBundleJobEvent(event, el);
                    break;
                default:
                    XLog.getLog(EventHandlerService.class).info("Undefined Job Event app-type - {0}",
                            event.getAppType());
            }
        }

        private void invokeSLAEventListener(Iterator<?> iter, SLAEvent event) {
            SLAEventListener sel = (SLAEventListener) iter.next();
            switch (event.getEventType()) {
                case START_MET:
                    sel.onStartMet((SLAEvent) event);
                    break;
                case START_MISS:
                    sel.onStartMiss((SLAEvent) event);
                    break;
                case END_MET:
                    sel.onEndMet((SLAEvent) event);
                    break;
                case END_MISS:
                    sel.onEndMiss((SLAEvent) event);
                    break;
                case DURATION_MET:
                    sel.onDurationMet((SLAEvent) event);
                    break;
                case DURATION_MISS:
                    sel.onDurationMiss((SLAEvent) event);
                    break;
                default:
                    XLog.getLog(EventHandlerService.class).info("Undefined SLA event type - {0}", event.getEventType());
            }
        }

        private void onWorkflowJobEvent(JobEvent event, JobEventListener el) {
            switch (event.getEventStatus()) {
                case STARTED:
                    el.onWorkflowJobStart((WorkflowJobEvent) event);
                    break;
                case SUCCESS:
                    el.onWorkflowJobSuccess((WorkflowJobEvent) event);
                    break;
                case FAILURE:
                    el.onWorkflowJobFailure((WorkflowJobEvent) event);
                    break;
                case SUSPEND:
                    el.onWorkflowJobSuspend((WorkflowJobEvent) event);
                    break;
                default:
                    XLog.getLog(EventHandlerService.class).info("Undefined WF Job event-status - {0}",
                            event.getEventStatus());
            }
        }

        private void onWorkflowActionEvent(JobEvent event, JobEventListener el) {
            switch (event.getEventStatus()) {
                case STARTED:
                    el.onWorkflowActionStart((WorkflowActionEvent) event);
                    break;
                case SUCCESS:
                    el.onWorkflowActionSuccess((WorkflowActionEvent) event);
                    break;
                case FAILURE:
                    el.onWorkflowActionFailure((WorkflowActionEvent) event);
                    break;
                default:
                    XLog.getLog(EventHandlerService.class).info("Undefined WF action event-status - {0}",
                            event.getEventStatus());
            }
        }

        private void onCoordinatorJobEvent(JobEvent event, JobEventListener el) {
            switch (event.getEventStatus()) {
                case STARTED:
                    el.onCoordinatorJobStart((CoordinatorJobEvent) event);
                    break;
                case SUCCESS:
                    el.onCoordinatorJobSuccess((CoordinatorJobEvent) event);
                    break;
                case FAILURE:
                    el.onCoordinatorJobFailure((CoordinatorJobEvent) event);
                    break;
                case SUSPEND:
                    el.onCoordinatorJobSuspend((CoordinatorJobEvent) event);
                    break;
                default:
                    XLog.getLog(EventHandlerService.class).info("Undefined Coord Job event-status - {0}",
                            event.getEventStatus());
            }
        }

        private void onCoordinatorActionEvent(JobEvent event, JobEventListener el) {
            switch (event.getEventStatus()) {
                case WAITING:
                    el.onCoordinatorActionWaiting((CoordinatorActionEvent) event);
                    break;
                case STARTED:
                    el.onCoordinatorActionStart((CoordinatorActionEvent) event);
                    break;
                case SUCCESS:
                    el.onCoordinatorActionSuccess((CoordinatorActionEvent) event);
                    break;
                case FAILURE:
                    el.onCoordinatorActionFailure((CoordinatorActionEvent) event);
                    break;
                case SUSPEND:
                    el.onCoordinatorActionSuspend((CoordinatorActionEvent) event);
                    break;
                default:
                    XLog.getLog(EventHandlerService.class).info("Undefined Coord action event-status - {0}",
                            event.getEventStatus());
            }
        }

        private void onBundleJobEvent(JobEvent event, JobEventListener el) {
            switch (event.getEventStatus()) {
                case STARTED:
                    el.onBundleJobStart((BundleJobEvent) event);
                    break;
                case SUCCESS:
                    el.onBundleJobSuccess((BundleJobEvent) event);
                    break;
                case FAILURE:
                    el.onBundleJobFailure((BundleJobEvent) event);
                    break;
                case SUSPEND:
                    el.onBundleJobSuspend((BundleJobEvent) event);
                    break;
                default:
                    XLog.getLog(EventHandlerService.class).info("Undefined Bundle Job event-type - {0}",
                            event.getEventStatus());
            }
        }

    }

}
