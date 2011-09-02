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
import org.apache.oozie.client.OozieClient.SYSTEM_MODE;
import org.apache.oozie.util.Instrumentable;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XCallable;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.ParamChecker;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;


/**
 * The callable queue service queues {@link XCallable}s for asynchronous execution. <p/> Callables can be queued for
 * immediate execution or for delayed execution (some time in the future). <p/> Callables are consumed from the queue
 * for execution based on their priority. <p/> When the queues (for immediate execution and for delayed execution) are
 * full, teh callable queue service stops queuing callables. <p/> A threadpool is used to execute the callables
 * asynchronously. <p/> The following configuration parameters control the callable queue service: <p/> {@link
 * #CONF_QUEUE_SIZE} size of the immmediate execution queue. Defaulf value is 1000. <p/> {@link
 * #CONF_DELAYED_QUEUE_SIZE} size of the delayed execution queue. Defaulf value is 1000. <p/> {@link #CONF_THREADS}
 * number of threads in the threadpool used for asynchronous command execution. When this number of threads is reached,
 * commands remain the queue until threads become available.
 *
 * Sets up a priority queue for the execution of Commands via a ThreadPool. Sets up a Delyaed Queue to handle actions
 * which will be ready for execution sometime in the future.
 */
public class CallableQueueService implements Service, Instrumentable {
    private static final String INSTRUMENTATION_GROUP = "callablequeue";
    private static final String INSTR_IN_QUEUE_TIME_TIMER = "time.in.queue";
    private static final String INSTR_EXECUTED_COUNTER = "executed";
    private static final String INSTR_FAILED_COUNTER = "failed";
    private static final String INSTR_QUEUED_COUNTER = "queued";
    private static final String INSTR_DELAYD_QUEUED_COUNTER = "delayed.queued";
    private static final String INSTR_QUEUE_SIZE_SAMPLER = "queue.size";
    private static final String INSTR_DELAYED_QUEUE_SIZE_SAMPLER = "delayed.queue.size";
    private static final String INSTR_THREADS_ACTIVE_SAMPLER = "threads.active";

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "CallableQueueService.";

    public static final String CONF_QUEUE_SIZE = CONF_PREFIX + "queue.size";
    public static final String CONF_DELAYED_QUEUE_SIZE = CONF_PREFIX + "delayed.queue.size";
    public static final String CONF_THREADS = CONF_PREFIX + "threads";
    public static final String CONF_CALLABLE_CONCURRENCY = CONF_PREFIX + "callable.concurrency";

    public static final int CONCURRENCY_DELAY = 500;

    public static final int SAFE_MODE_DELAY = 60000;

    private Map<String, AtomicInteger> activeCallables;
    private int maxCallableConcurrency;

    private synchronized boolean callableBegin(XCallable callable) {
        AtomicInteger counter = activeCallables.get(callable.getType());
        if (counter == null) {
            counter = new AtomicInteger(1);
            activeCallables.put(callable.getType(), counter);
            return true;
        }
        else {
            return counter.incrementAndGet() <= maxCallableConcurrency;
        }
    }

    private synchronized void callableEnd(XCallable callable) {
        AtomicInteger counter = activeCallables.get(callable.getType());
        if (counter == null) {
            throw new IllegalStateException("It should not happen");
        }
        else {
            counter.decrementAndGet();
        }
    }

    // Callables are wrapped with the this wrapper for execution, for logging and instrumentation.
    // The wrapper implements Runnable and Comparable to be able to work with an executor and a priority queue.
    class CallableWrapper implements Runnable, Comparable<CallableWrapper> {
        private XCallable<Void> callable;
        private Instrumentation.Cron cron;

        public CallableWrapper(XCallable<Void> callable) {
            this.callable = callable;
            cron = new Instrumentation.Cron();
            cron.start();
        }

        public void run() {
            if (Services.get().getSystemMode() == SYSTEM_MODE.SAFEMODE) {
                log.info("CallableWrapper[run] System is in SAFEMODE. Hence no callable run. But requeueing in delayQueue " + queue.size());
                delayedQueue.put(new DelayedCallableWrapper(callable, SAFE_MODE_DELAY));
                return;
            }
            try {
                if (callableBegin(callable)) {
                    cron.stop();
                    addInQueueCron(cron);
                    XLog.Info.get().clear();
                    XLog log = XLog.getLog(getClass());
                    log.trace("executing callable [{0}]", callable.getName());
                    try {
                        callable.call();
                        incrCounter(INSTR_EXECUTED_COUNTER, 1);
                        log.trace("executed callable [{0}]", callable.getName());
                    }
                    catch (Exception ex) {
                        incrCounter(INSTR_FAILED_COUNTER, 1);
                        log.warn("exception callable [{0}], {1}", callable.getName(), ex.getMessage(), ex);
                    }
                    finally {
                        XLog.Info.get().clear();
                        callableEnd(callable);
                    }
                }
                else {
                    log.warn("max concurrency for callable type[{0}] exceeded, requeueing with [{1}]ms delay",
                             callable.getType(), CONCURRENCY_DELAY);
                    queue(callable, CONCURRENCY_DELAY);
                    incrCounter(callable.getType() + "#exceeded.concurrency", 1);
                }
            }
            finally {
                callableEnd(callable);
            }
        }

        public int compareTo(CallableWrapper callableWrapper) {
            //priority is descending order
            int diff = callableWrapper.callable.getPriority() - callable.getPriority();
            if (diff == 0) {
                //createdTime is ascending order
                Long lDiff = callable.getCreatedTime() - callableWrapper.callable.getCreatedTime();
                diff = (lDiff < Integer.MIN_VALUE) ? Integer.MIN_VALUE : ((lDiff > Integer.MAX_VALUE) ? Integer.MAX_VALUE : lDiff.intValue());
            }
            return diff;
        }
    }

    class CompositeCallable<Void> implements XCallable<Void> {
        private List<XCallable<Void>> callables;
        private String name;
        private int priority;
        private long createdTime;

        public CompositeCallable(List<XCallable<Void>> callables) {
            this.callables = new ArrayList<XCallable<Void>>(callables);
            priority = Integer.MIN_VALUE;
            createdTime = Long.MAX_VALUE;
            StringBuilder sb = new StringBuilder();
            String separator = "[";
            for (XCallable callable : callables) {
                priority = Math.max(priority, callable.getPriority());
                createdTime = Math.min(createdTime, callable.getCreatedTime());
                sb.append(separator).append(callable.getName());
                separator = ",";
            }
            sb.append("]");
            name = sb.toString();
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getType() {
            return "#composite#";
        }

        @Override
        public int getPriority() {
            return priority;
        }

        @Override
        public long getCreatedTime() {
            return createdTime;
        }

        public Void call() throws Exception {
            XLog log = XLog.getLog(getClass());

            for (XCallable callable : callables) {
                log.trace("executing callable [{0}]", callable.getName());
                try {
                    callable.call();
                    incrCounter(INSTR_EXECUTED_COUNTER, 1);
                    log.trace("executed callable [{0}]", callable.getName());
                }
                catch (Exception ex) {
                    incrCounter(INSTR_FAILED_COUNTER, 1);
                    log.warn("exception callable [{0}], {1}", callable.getName(), ex.getMessage(), ex);
                }
            }

            // ticking -1 not to count the call to the composite callable
            incrCounter(INSTR_EXECUTED_COUNTER, -1);
            return null;
        }

    }

    // delayed callables are wrapped with the this wrapper for delayed queueing.
    // The wrapper implements Comparable to be able to work with a priority queue.
    private static class DelayedCallableWrapper implements Comparable<DelayedCallableWrapper> {
        private Long executionTime;
        private XCallable<Void> callable;

        DelayedCallableWrapper(XCallable<Void> callable, long delay) {
            this.callable = callable;
            this.executionTime = System.currentTimeMillis() + delay;
        }

        public int compareTo(DelayedCallableWrapper dc) {
            return (int) (getExecutionTime() - dc.getExecutionTime());
        }

        public long getExecutionTime() {
            return executionTime;
        }

        public XCallable<Void> getCallable() {
            return callable;
        }

    }

    private XLog log = XLog.getLog(getClass());

    private int queueSize;
    private PriorityBlockingQueue<CallableWrapper> queue;
    private int delayedQueueSize;
    private PriorityBlockingQueue<DelayedCallableWrapper> delayedQueue;
    private AtomicLong delayQueueExecCounter = new AtomicLong(0);
    private ThreadPoolExecutor executor;
    private Instrumentation instrumentation;

    /**
     * Convenience method for instrumentation counters.
     *
     * @param name counter name.
     * @param count count to increment the counter.
     */
    private void incrCounter(String name, int count) {
        if (instrumentation != null) {
            instrumentation.incr(INSTRUMENTATION_GROUP, name, count);
        }
    }

    private void addInQueueCron(Instrumentation.Cron cron) {
        if (instrumentation != null) {
            instrumentation.addCron(INSTRUMENTATION_GROUP, INSTR_IN_QUEUE_TIME_TIMER, cron);
        }
    }

    /**
     * Initialize the command queue service.
     *
     * @param services services instance.
     */
    @Override
    @SuppressWarnings("unchecked")
    public void init(Services services) {
        Configuration conf = services.getConf();

        queueSize = conf.getInt(CONF_QUEUE_SIZE, 1000);
        int threads = conf.getInt(CONF_THREADS, 10);
        delayedQueueSize = conf.getInt(CONF_DELAYED_QUEUE_SIZE, 1000);

        queue = new PriorityBlockingQueue<CallableWrapper>(queueSize);
        delayedQueue = new PriorityBlockingQueue<DelayedCallableWrapper>(delayedQueueSize);
        executor = new ThreadPoolExecutor(threads, threads, 10, TimeUnit.SECONDS, (PriorityBlockingQueue) queue);

        activeCallables = new HashMap<String, AtomicInteger>();
        maxCallableConcurrency = conf.getInt(CONF_CALLABLE_CONCURRENCY, 3);

        // every 100 milliseconds this runnable polls the delayed queue for commands ready for execution.
        // while there are commands ready for execution they are removed from the delayed queue and added to the
        // execution queue.
        // if the execution queue is full, the delayed command is added back to the delay queue and the loop
        // is interruted until the next polling time.
        Runnable delayedQueuePoller = new Runnable() {
            public void run() {
                int queued = 0;
                if (!delayedQueue.isEmpty()) {
                    while (!delayedQueue.isEmpty() && delayedQueue.peek().getExecutionTime() < System.currentTimeMillis()) {
                        DelayedCallableWrapper delayed = delayedQueue.poll();
                        if (!queue(delayed.getCallable())) {
                            delayedQueue.add(delayed);
                            break;
                        }
                        queued++;
                    }
                }
                if (delayQueueExecCounter.get() % 3000 == 0) {
                    XLog.getLog(getClass()).debug(
                            "Total Instances of delayedQueuePoller  " + delayQueueExecCounter + " has queued " + queued
                                    + " of commands from dealy queue to regular queue");
                }
                delayQueueExecCounter.getAndIncrement();
            }
        };
        services.get(SchedulerService.class).schedule(delayedQueuePoller, 0, 100, SchedulerService.Unit.MILLISEC);
    }

    /**
     * Destroy the command queue service.
     */
    @Override
    public void destroy() {
        try {
            long limit = System.currentTimeMillis() + 30 * 1000;// 30 seconds
            executor.shutdownNow();
            while (!executor.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                log.info("Waiting for executor to shutdown");
                if (System.currentTimeMillis() > limit) {
                    log.warn("Gave up, continuing without waiting for executor to shutdown");
                    break;
                }
            }
        }
        catch (InterruptedException ex) {
            log.warn(ex);
        }
    }

    /**
     * Return the public interface for command queue service.
     *
     * @return {@link CallableQueueService}.
     */
    @Override
    public Class<? extends Service> getInterface() {
        return CallableQueueService.class;
    }

    /**
     * Queue a callable for asynchronous execution.
     *
     * @param callable callable to queue.
     * @return <code>true</code> if the callable was queued, <code>false</code> if the queue is full and the callable
     *         was not queued.
     */
    public synchronized boolean queue(XCallable<Void> callable) {
        if (Services.get().getSystemMode() == SYSTEM_MODE.SAFEMODE) {
            log.info("[queue] System is in SAFEMODE. Hence no callable is queued. current queue size " + queue.size());
            return false;
        }

        if (queue.size() < queueSize) {
            incrCounter(INSTR_QUEUED_COUNTER, 1);
            try {
                executor.execute(new CallableWrapper(callable));
            }
            catch (Exception e) {
                log.warn("Didnot able to submit to executor:", e);
            }
            return true;
        }
        return false;
    }

    /**
     * Queue a list of callables for serial execution. <p/> Useful to serialize callables that may compete with each
     * other for resources. <p/> All callables will be processed with the priority of the highest priority of all
     * callables.
     *
     * @param callables callables to be executed by the composite callable.
     * @return <code>true</code> if the callables were queued, <code>false</code> if the queue is full and the callables
     *         were not queued.
     */
    @SuppressWarnings("unchecked")
    public synchronized boolean queueSerial(List<? extends XCallable<Void>> callables) {
        if (Services.get().getSystemMode() == SYSTEM_MODE.SAFEMODE) {
            log.info("[queueSerial] System is in SAFEMODE. Hence no callable is queued current queue size " + queue.size());
            return false;
        }
        ParamChecker.notNullElements(callables, "callables");
        if (callables.size() == 0) {
            return true;
        }
        if (queue.size() < queueSize) {
            incrCounter(INSTR_QUEUED_COUNTER, callables.size());
            executor.execute(new CallableWrapper(new CompositeCallable(callables)));
            return true;
        }
        return false;
    }

    /**
     * @return int size of queue
     */
    public synchronized int queueSize() {
        return queue.size();
    }

    /**
     * @return int size of delayedQueue
     */
    public synchronized int delayedQueueSize() {
        return delayedQueue.size();
    }

    /**
     * Queue a callable for asynchronous execution sometime in the future.
     *
     * @param callable callable to queue for delayed execution
     * @param delay time, in milliseconds, that the callable should be delayed.
     * @return <code>true</code> if the callable was queued, <code>false</code> if the queue is full and the callable
     *         was not queued.
     */
    public synchronized boolean queue(XCallable<Void> callable, long delay) {
        if (Services.get().getSystemMode() == SYSTEM_MODE.SAFEMODE) {
            log.info("[queue(delay)] System is in SAFEMODE. Hence no callable is queued. queue size " + queue.size());
            return false;
        }

        if (delayedQueue.size() < delayedQueueSize) {
            incrCounter(INSTR_DELAYD_QUEUED_COUNTER, 1);
            delayedQueue.put(new DelayedCallableWrapper(callable, delay));
            return true;
        }
        return false;
    }

    /**
     * Queue a list of callables for serial execution sometime in the future. <p/> Useful to serialize callables that
     * may compete with each other for resources. <p/> All callables will be processed with the priority of the highest
     * priority of all callables.
     *
     * @param callables callables to be executed by the composite callable.
     * @param delay time, in milliseconds, that the callable should be delayed.
     * @return <code>true</code> if the callables were queued, <code>false</code> if the queue is full and the callables
     *         were not queued.
     */
    @SuppressWarnings("unchecked")
    public synchronized boolean queueSerial(List<? extends XCallable<Void>> callables, long delay) {
        if (Services.get().getSystemMode() == SYSTEM_MODE.SAFEMODE) {
            log.info("[queue(delay)] System is in SAFEMODE. Hence no callable is queued. queue size " + queue.size());
            return false;
        }
        ParamChecker.notNullElements(callables, "callables");
        if (callables.size() == 0) {
            return true;
        }
        if (queue.size() < queueSize) {
            incrCounter(INSTR_QUEUED_COUNTER, callables.size());
            boolean ret = queue(new CompositeCallable(callables), delay);
            return ret;
        }
        return false;
    }

    /**
     * Instruments the callable queue service.
     *
     * @param instr instance to instrument the callable queue service to.
     */
    public void instrument(Instrumentation instr) {
        instrumentation = instr;
        instr.addSampler(INSTRUMENTATION_GROUP, INSTR_QUEUE_SIZE_SAMPLER, 60, 1, new Instrumentation.Variable<Long>() {
            public Long getValue() {
                return (long) queue.size();
            }
        });
        instr.addSampler(INSTRUMENTATION_GROUP, INSTR_DELAYED_QUEUE_SIZE_SAMPLER, 60, 1, new Instrumentation.Variable<Long>() {
            public Long getValue() {
                return (long) delayedQueue.size();
            }
        });
        instr.addSampler(INSTRUMENTATION_GROUP, INSTR_THREADS_ACTIVE_SAMPLER, 60, 1, new Instrumentation.Variable<Long>() {
            public Long getValue() {
                return (long) executor.getActiveCount();
            }
        });
    }

}