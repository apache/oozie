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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.oozie.service.CallableQueueService.CallableWrapper;
import org.apache.oozie.util.NamedThreadFactory;
import org.apache.oozie.util.XCallable;
import org.apache.oozie.util.XLog;
import org.eclipse.jetty.util.ConcurrentHashSet;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressWarnings("deprecation")
public class AsyncXCommandExecutor {
    public static final int MIN_PRIORITY = 0;
    public static final long ANTI_STARVATION_INTERVAL = 500;
    private static XLog log = XLog.getLog(AsyncXCommandExecutor.class);
    private final ThreadPoolExecutor executor;
    private final ScheduledThreadPoolExecutor scheduledExecutor;
    private final boolean needConcurrencyCheck;
    private final CallableQueueService callableQueueService;
    private final AtomicInteger activeCommands;
    private final long maxActiveCommands;  // equivalent of "queueSize" in CQS
    private final long maxWait;
    private final long maxPriority;
    private final int awaitTerminationTimeoutSeconds;

    private final BlockingQueue<CallableWrapper<?>> priorityBlockingQueue;
    private final BlockingQueue<AccessibleRunnableScheduledFuture<ScheduledXCallable>> delayWorkQueue;
    private final ConcurrentHashMap<String, Set<CallableWrapper<?>>> pendingCommandsPerType;
    private long lastAntiStarvationCheck = 0;

    @SuppressWarnings({"unchecked", "rawtypes"})
    @SuppressFBWarnings( value = "SIC_INNER_SHOULD_BE_STATIC_ANON",
        justification = "Unnecessary to refactor innen classes defined here")
    public AsyncXCommandExecutor(int threads,
            int delayedCallableThreads,
            boolean needConcurrencyCheck,
            CallableQueueService callableAccess,
            long maxActiveCommands,
            long maxWait,
            int priorities,
            int awaitTerminationTimeoutSeconds) {

        priorityBlockingQueue = new PriorityBlockingQueue<CallableWrapper<?>>(100, new PriorityComparator());

        executor = new ThreadPoolExecutor(threads, threads, 10, TimeUnit.SECONDS,
                (BlockingQueue) priorityBlockingQueue,
                new NamedThreadFactory("CallableQueue")) {
            protected void beforeExecute(Thread t, Runnable r) {
                XLog.Info.get().clear();
            }

            protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
                return (RunnableFuture<T>)callable;
            }
        };

        this.scheduledExecutor = new ScheduledThreadPoolExecutor(delayedCallableThreads,
                new NamedThreadFactory("ScheduledCallable")) {
            protected <V> RunnableScheduledFuture<V> decorateTask(
                    Runnable runnable, RunnableScheduledFuture<V> task) {

                    AccessibleRunnableScheduledFuture<V> arsf =
                            new AccessibleRunnableScheduledFuture<>(task, runnable);

                    return arsf;
            }
        };

        this.delayWorkQueue = (BlockingQueue) scheduledExecutor.getQueue();
        this.needConcurrencyCheck = needConcurrencyCheck;
        this.callableQueueService = callableAccess;
        this.maxActiveCommands = maxActiveCommands;
        this.maxWait = maxWait;
        this.activeCommands = new AtomicInteger(0);
        this.pendingCommandsPerType = new ConcurrentHashMap<>();
        Preconditions.checkArgument(priorities > 0, "Number of priorities must be >0");
        this.maxPriority = priorities - 1;
        Preconditions.checkArgument(awaitTerminationTimeoutSeconds > 0,
                String.format("Await termination timeout must be >0, is %s", awaitTerminationTimeoutSeconds));
        this.awaitTerminationTimeoutSeconds = awaitTerminationTimeoutSeconds;
    }

    @VisibleForTesting
    AsyncXCommandExecutor(boolean needConcurrencyCheck,
            CallableQueueService callableAccess,
            long maxActiveCommands,
            ThreadPoolExecutor executor,
            ScheduledThreadPoolExecutor scheduledExecutor,
            PriorityBlockingQueue<CallableWrapper<?>> priorityBlockingQueue,
            BlockingQueue<AccessibleRunnableScheduledFuture<ScheduledXCallable>> delayQueue,
            ConcurrentHashMap<String, Set<CallableWrapper<?>>> pendingCommandsPerType,
            AtomicInteger activeCommands,
            long maxWait,
            long priorities,
            int awaitTerminationTimeoutSeconds) {

        this.priorityBlockingQueue = priorityBlockingQueue;
        this.delayWorkQueue = delayQueue;
        this.pendingCommandsPerType = pendingCommandsPerType;
        this.executor = executor;
        this.scheduledExecutor = scheduledExecutor;
        this.needConcurrencyCheck = needConcurrencyCheck;
        this.callableQueueService = callableAccess;
        this.maxActiveCommands = maxActiveCommands;
        this.activeCommands = activeCommands;
        this.maxWait = maxWait;
        this.maxPriority = priorities - 1;
        this.awaitTerminationTimeoutSeconds = awaitTerminationTimeoutSeconds;
    }

    public synchronized boolean queue(CallableWrapper<?> wrapper, boolean ignoreQueueSize) {
        if (!ignoreQueueSize && activeCommands.get() >= maxActiveCommands) {
            log.warn("queue full, ignoring queuing for [{0}]", wrapper.getElement().getKey());
            return false;
        }

        if (wrapper.filterDuplicates()) {
            wrapper.addToUniqueCallables();

            int priority = wrapper.getPriority();
            long initialDelay = wrapper.getInitialDelay();

            try {
                if (priority > maxPriority || priority < MIN_PRIORITY) {
                    throw new IllegalArgumentException("priority out of range: " + priority);
                }

                if (initialDelay == 0) {
                    executor.execute(wrapper);
                } else {
                    ScheduledXCallable scheduledXCallable = new ScheduledXCallable(wrapper);
                    long schedDelay = wrapper.getDelay(TimeUnit.MILLISECONDS);
                    scheduledExecutor.schedule(scheduledXCallable,
                            schedDelay, TimeUnit.MILLISECONDS);
                }

                activeCommands.incrementAndGet();
            } catch (Throwable ree) {
                wrapper.removeFromUniqueCallables();
                throw new RuntimeException(ree);
            }
        }

        return true;
    }

    public void handleConcurrencyExceeded(CallableWrapper<?> command) {
        String type = command.getElement().getType();

        Set<CallableWrapper<?>> commandsForType = pendingCommandsPerType.get(type);
        if (commandsForType == null) {
            commandsForType = new ConcurrentHashSet<>();
            Set<CallableWrapper<?>> oldCommandForType;
            oldCommandForType = pendingCommandsPerType.putIfAbsent(type, commandsForType);

            if (oldCommandForType != null) {
                // a different thread was faster
                commandsForType = oldCommandForType;
            }
        }

        commandsForType.add(command);
    }

    public void checkMaxConcurrency(String type) {
        Set<CallableWrapper<?>> commandsForType = pendingCommandsPerType.get(type);

        if (commandsForType != null) {
            // Only a single thread should be doing stuff here! Reason: concurrent executions might
            // submit an eligible XCallable multiple times, which must be avoided.
            synchronized (commandsForType) {
                boolean doAntiStarvation = false;
                int priorityModified = 0;
                long now = System.currentTimeMillis();
                if (now - lastAntiStarvationCheck > ANTI_STARVATION_INTERVAL) {
                    doAntiStarvation = true;
                }

                for (Iterator<CallableWrapper<?>> itr = commandsForType.iterator(); itr.hasNext();) {
                    CallableWrapper<?> command = itr.next();

                    // Anti-starvation logic: try to promote callables that have been waiting for too long
                    int currentPrio = command.getPriority();
                    if (doAntiStarvation
                            && command.getDelay(TimeUnit.MILLISECONDS) < -maxWait
                            && currentPrio < maxPriority) {
                        command.setDelay(0, TimeUnit.MILLISECONDS);
                        command.setPriority(++currentPrio);
                        priorityModified++;
                    }

                    if (callableQueueService.canSubmitCallable(command.getElement())) {
                        if (activeCommands.get() >= maxActiveCommands) {
                            log.warn("queue full, ignoring queuing for [{0}]", command.getElement().getKey());
                            activeCommands.decrementAndGet();
                        } else {
                            executor.execute(command);
                        }

                        itr.remove();
                    }
                }

                if (doAntiStarvation) {
                    lastAntiStarvationCheck = System.currentTimeMillis();
                }

                if (priorityModified > 0) {
                    log.debug("Anti-starvation: handled [{0}] elements", priorityModified);
                }
            }
        }
    }

    public void commandFinished() {
        // Note: this is to track the number of elements. Otherwise we'd have to combine the size of
        // two queues + a list.
        activeCommands.decrementAndGet();
    }

    public ThreadPoolExecutor getExecutorService() {
        return executor;
    }

    public void shutdown() {
        try {
            shutdownExecutor(executor, "executor");
            shutdownExecutor(scheduledExecutor, "scheduled executor");
        } catch (InterruptedException e) {
            log.warn("Interrupted while waiting for executor shutdown");
        }
    }

    public boolean isShutDown() {
        return executor.isShutdown() || scheduledExecutor.isShutdown();
    }

    public boolean isTerminated() {
        return executor.isTerminated() || scheduledExecutor.isTerminated();
    }

    public List<String> getQueueDump() {
        List<CallableWrapper<?>> copyOfPending = new ArrayList<>(100);
        List<String> queueDump = new ArrayList<>(100);

        // Safe to iterate
        for (Map.Entry<String, Set<CallableWrapper<?>>> entry : pendingCommandsPerType.entrySet()) {
            Set<CallableWrapper<?>> pendingCommandsPerType = entry.getValue();
            copyOfPending.addAll(pendingCommandsPerType);
        }

        // Safe to iterate
        for (final CallableWrapper<?> wrapper : priorityBlockingQueue) {
            queueDump.add(wrapper.toString());
        }

        // Safe to iterate
        for (final AccessibleRunnableScheduledFuture<ScheduledXCallable> future : delayWorkQueue) {
            ScheduledXCallable delayedXCallable = (ScheduledXCallable) future.getTask();
            queueDump.add(delayedXCallable.getCallableWrapper().toString());
        }

        for (final CallableWrapper<?> wrapper : copyOfPending) {
            queueDump.add(wrapper.toString());
        }

        return queueDump;
    }

    public int getSize() {
        return activeCommands.get();
    }

    public class ScheduledXCallable implements Runnable {
        private CallableWrapper<?> target;

        public ScheduledXCallable(CallableWrapper<?> target) {
            this.target = target;
        }

        @Override
        public void run() {
            if (needConcurrencyCheck && !callableQueueService.canSubmitCallable(target.getElement())) {
                XCallable<?> callable = target.getElement();
                handleConcurrencyExceeded(target);

                // need this to deal with a special race condition: we detect that concurrency
                // exceeded, but an XCommand (or more!) with the same type just happens to finish. If that
                // happens, this callable might never get scheduled again (or much later), so we have to guard
                // against this condition.
                checkMaxConcurrency(callable.getType());
            } else {
                executor.execute(target);
            }
        }

        public CallableWrapper<?> getCallableWrapper() {
            return target;
        }
    }

    @SuppressFBWarnings(value = "SE_COMPARATOR_SHOULD_BE_SERIALIZABLE",
            justification = "PriorityBlockingQueue which uses this comparator will never be serialized")
    public static class PriorityComparator implements Comparator<CallableWrapper<?>> {
        @Override
        public int compare(CallableWrapper<?> o1, CallableWrapper<?> o2) {
            return Integer.compare(o2.getPriority(), o1.getPriority());
        }
    }

    // We have to use this so that scheduled elements in the DelayWorkQueue are accessible
    @SuppressFBWarnings(value = "EQ_COMPARETO_USE_OBJECT_EQUALS",
            justification = "This class has a natural ordering (expiration) which is inconsistent with equals")
    public static class AccessibleRunnableScheduledFuture<V> implements RunnableScheduledFuture<V> {
        private final Runnable task;
        private RunnableScheduledFuture<V> originalFuture;

        public AccessibleRunnableScheduledFuture(RunnableScheduledFuture<V> originalFuture,
                Runnable task) {
            this.task = task;
            this.originalFuture = originalFuture;
        }

        @Override
        public void run() {
            originalFuture.run();
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return originalFuture.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return originalFuture.isCancelled();
        }

        @Override
        public boolean isDone() {
            return originalFuture.isDone();
        }

        @Override
        public V get() throws InterruptedException, ExecutionException {
            return originalFuture.get();
        }

        @Override
        public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return originalFuture.get(timeout, unit);
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return originalFuture.getDelay(unit);
        }

        @Override
        public int compareTo(Delayed o) {
            return originalFuture.compareTo(o);
        }

        @Override
        public boolean isPeriodic() {
            return originalFuture.isPeriodic();
        }

        public Runnable getTask() {
            return task;
        }
    }

    private void shutdownExecutor(ExecutorService executor, String name) throws InterruptedException {
        long limit = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(this.awaitTerminationTimeoutSeconds);
        executor.shutdown();
        while (!executor.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
            log.info("Waiting for [{0}] to shutdown", name);
            if (System.currentTimeMillis() > limit) {
                log.warn("Gave up, continuing without waiting for executor to shutdown");
                break;
            }
        }
    }
}
