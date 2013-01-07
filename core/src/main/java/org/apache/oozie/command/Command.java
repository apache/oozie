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
package org.apache.oozie.command;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.FaultInjection;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.XException;
import org.apache.oozie.service.CallableQueueService;
import org.apache.oozie.service.DagXLogInfoService;
import org.apache.oozie.service.InstrumentationService;
import org.apache.oozie.service.MemoryLocksService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.StoreService;
import org.apache.oozie.service.XLogService;
import org.apache.oozie.store.Store;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XCallable;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.MemoryLocks.LockToken;

/**
 * Base class for all synchronous and asynchronous DagEngine commands.
 */
public abstract class Command<T, S extends Store> implements XCallable<T> {
    /**
     * The instrumentation group used for Commands.
     */
    private static final String INSTRUMENTATION_GROUP = "commands";

    private final long createdTime;

    /**
     * The instrumentation group used for Jobs.
     */
    private static final String INSTRUMENTATION_JOB_GROUP = "jobs";

    private static final long LOCK_TIMEOUT = 1000;
    protected static final long LOCK_FAILURE_REQUEUE_INTERVAL = 30000;

    protected Instrumentation instrumentation;
    private List<XCallable<Void>> callables;
    private List<XCallable<Void>> delayedCallables;
    private long delay = 0;
    private List<XCallable<Void>> exceptionCallables;
    private String name;
    private String type;
    private String key;
    private int priority;
    private int logMask;
    private boolean withStore;
    protected boolean dryrun = false;
    private ArrayList<LockToken> locks = null;

    /**
     * This variable is package private for testing purposes only.
     */
    XLog.Info logInfo;

    /**
     * Create a command that uses a {@link WorkflowStore} instance. <p/> The current {@link XLog.Info} values are
     * captured for execution.
     *
     * @param name command name.
     * @param type command type.
     * @param priority priority of the command, used when queuing for asynchronous execution.
     * @param logMask log mask for the command logging calls.
     */
    public Command(String name, String type, int priority, int logMask) {
        this(name, type, priority, logMask, true);
    }

    /**
     * Create a command. <p/> The current {@link XLog.Info} values are captured for execution.
     *
     * @param name command name.
     * @param type command type.
     * @param priority priority of the command, used when queuing for asynchronous execution.
     * @param logMask log mask for the command logging calls.
     * @param withStore indicates if the command needs a {@link org.apache.oozie.store.WorkflowStore} instance or not.
     */
    public Command(String name, String type, int priority, int logMask, boolean withStore) {
        this.name = ParamChecker.notEmpty(name, "name");
        this.type = ParamChecker.notEmpty(type, "type");
        this.key = name + "_" + UUID.randomUUID();
        this.priority = priority;
        this.withStore = withStore;
        this.logMask = logMask;
        instrumentation = Services.get().get(InstrumentationService.class).get();
        logInfo = new XLog.Info(XLog.Info.get());
        createdTime = System.currentTimeMillis();
        locks = new ArrayList<LockToken>();
    }

    /**
     * Create a command. <p/> The current {@link XLog.Info} values are captured for execution.
     *
     * @param name command name.
     * @param type command type.
     * @param priority priority of the command, used when queuing for asynchronous execution.
     * @param logMask log mask for the command logging calls.
     * @param withStore indicates if the command needs a {@link org.apache.oozie.store.WorkflowStore} instance or not.
     * @param dryrun indicates if dryrun option is enabled. if enabled coordinator will show a diagnostic output without
     * really submitting the job
     */
    public Command(String name, String type, int priority, int logMask, boolean withStore, boolean dryrun) {
        this(name, type, priority, logMask, withStore);
        this.dryrun = dryrun;
    }

    /**
     * Return the name of the command.
     *
     * @return the name of the command.
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * Return the callable type. <p/> The callable type is used for concurrency throttling in the {@link
     * org.apache.oozie.service.CallableQueueService}.
     *
     * @return the callable type.
     */
    @Override
    public String getType() {
        return type;
    }

    /**
     * Return the priority of the command.
     *
     * @return the priority of the command.
     */
    @Override
    public int getPriority() {
        return priority;
    }

    /**
     * Returns the createdTime of the callable in milliseconds
     *
     * @return the callable createdTime
     */
    @Override
    public long getCreatedTime() {
        return createdTime;
    }

    /**
     * Execute the command {@link #call(WorkflowStore)} setting all the necessary context. <p/> The {@link XLog.Info} is
     * set to the values at instance creation time. <p/> The command execution is logged and instrumented. <p/> If a
     * {@link WorkflowStore} is used, a fresh instance will be passed and it will be commited after the {@link
     * #call(WorkflowStore)} execution. It will be closed without committing if an exception is thrown. <p/> Commands
     * queued via the DagCommand queue methods are queued for execution after the workflow store has been committed.
     * <p/> If an exception happends the queued commands will not be effectively queued for execution. Instead, the the
     * commands queued for exception will be effectively queued fro execution..
     *
     * @throws CommandException thrown if the command could not be executed successfully, the workflow store is closed
     * without committing, thus doing a rollback.
     */
    @SuppressWarnings({"ThrowFromFinallyBlock", "unchecked"})
    public final T call() throws CommandException {
        XLog.Info.get().setParameters(logInfo);
        XLog log = XLog.getLog(getClass());
        log.trace(logMask, "Start");
        Instrumentation.Cron cron = new Instrumentation.Cron();
        cron.start();
        callables = new ArrayList<XCallable<Void>>();
        delayedCallables = new ArrayList<XCallable<Void>>();
        exceptionCallables = new ArrayList<XCallable<Void>>();
        delay = 0;
        S store = null;
        boolean exception = false;

        try {
            if (withStore) {
                store = (S) Services.get().get(StoreService.class).getStore(getStoreClass());
                store.beginTrx();
            }
            T result = execute(store);
            /*
             *
             * if (store != null && log != null) { log.info(XLog.STD,
             * "connection log from store Flush Mode {0} ",
             * store.getFlushMode()); }
             */
            if (withStore) {
                if (store == null) {
                    throw new IllegalStateException("WorkflowStore should not be null");
                }
                if (FaultInjection.isActive("org.apache.oozie.command.SkipCommitFaultInjection")) {
                    throw new RuntimeException("Skipping Commit for Failover Testing");
                }
                store.commitTrx();
            }

            // TODO figure out the reject due to concurrency problems and remove
            // the delayed queuing for callables.
            boolean ret = Services.get().get(CallableQueueService.class).queueSerial(callables, 10);
            if (ret == false) {
                logQueueCallableFalse(callables);
            }

            ret = Services.get().get(CallableQueueService.class).queueSerial(delayedCallables, delay);
            if (ret == false) {
                logQueueCallableFalse(delayedCallables);
            }

            return result;
        }
        catch (XException ex) {
            log.error(logMask | XLog.OPS, "XException, {0}", ex.getMessage(), ex);
            if (store != null) {
                log.info(XLog.STD, "XException - connection logs from store {0}, {1}", store.getConnection(), store
                        .isClosed());
            }
            exception = true;
            if (store != null && store.isActive()) {
                try {
                    store.rollbackTrx();
                }
                catch (RuntimeException rex) {
                    log.warn(logMask | XLog.OPS, "openjpa error, {0}, {1}", name, rex.getMessage(), rex);
                }
            }

            // TODO figure out the reject due to concurrency problems and remove
            // the delayed queuing for callables.
            boolean ret = Services.get().get(CallableQueueService.class).queueSerial(exceptionCallables, 10);
            if (ret == false) {
                logQueueCallableFalse(exceptionCallables);
            }
            if (ex instanceof CommandException) {
                throw (CommandException) ex;
            }
            else {
                throw new CommandException(ex);
            }
        }
        catch (Exception ex) {
            log.error(logMask | XLog.OPS, "Exception, {0}", ex.getMessage(), ex);
            exception = true;
            if (store != null && store.isActive()) {
                try {
                    store.rollbackTrx();
                }
                catch (RuntimeException rex) {
                    log.warn(logMask | XLog.OPS, "openjpa error, {0}, {1}", name, rex.getMessage(), rex);
                }
            }
            throw new CommandException(ErrorCode.E0607, name, ex.getMessage(), ex);
        }
        catch (Error er) {
            log.error(logMask | XLog.OPS, "Error, {0}", er.getMessage(), er);
            exception = true;
            if (store != null && store.isActive()) {
                try {
                    store.rollbackTrx();
                }
                catch (RuntimeException rex) {
                    log.warn(logMask | XLog.OPS, "openjpa error, {0}, {1}", name, rex.getMessage(), rex);
                }
            }
            throw er;
        }
        finally {
            FaultInjection.deactivate("org.apache.oozie.command.SkipCommitFaultInjection");
            cron.stop();
            instrumentation.addCron(INSTRUMENTATION_GROUP, name, cron);
            incrCommandCounter(1);
            log.trace(logMask, "End");
            if (locks != null) {
                for (LockToken lock : locks) {
                    lock.release();
                }
                locks.clear();
            }
            if (store != null) {
                if (!store.isActive()) {
                    try {
                        store.closeTrx();
                    }
                    catch (RuntimeException rex) {
                        if (exception) {
                            log.warn(logMask | XLog.OPS, "openjpa error, {0}, {1}", name, rex.getMessage(), rex);
                        }
                        else {
                            throw rex;
                        }
                    }
                }
                else {
                    log.warn(logMask | XLog.OPS, "transaction is not committed or rolled back before closing entitymanager.");
                }
            }
        }
    }

    /**
     * Queue a callable for execution after the current callable call invocation completes and the {@link WorkflowStore}
     * transaction commits. <p/> All queued callables, regardless of the number of queue invocations, are queued for a
     * single serial execution. <p/> If the call invocation throws an exception all queued callables are discarded, they
     * are not queued for execution.
     *
     * @param callable callable to queue for execution.
     */
    protected void queueCallable(XCallable<Void> callable) {
        callables.add(callable);
    }

    /**
     * Queue a list of callables for execution after the current callable call invocation completes and the {@link
     * WorkflowStore} transaction commits. <p/> All queued callables, regardless of the number of queue invocations, are
     * queued for a single serial execution. <p/> If the call invocation throws an exception all queued callables are
     * discarded, they are not queued for execution.
     *
     * @param callables list of callables to queue for execution.
     */
    protected void queueCallable(List<? extends XCallable<Void>> callables) {
        this.callables.addAll(callables);
    }

    /**
     * Queue a callable for delayed execution after the current callable call invocation completes and the {@link
     * WorkflowStore} transaction commits. <p/> All queued delayed callables, regardless of the number of delay queue
     * invocations, are queued for a single serial delayed execution with the highest delay of all queued callables.
     * <p/> If the call invocation throws an exception all queued callables are discarded, they are not queued for
     * execution.
     *
     * @param callable callable to queue for delayed execution.
     * @param delay the queue delay in milliseconds
     */
    protected void queueCallable(XCallable<Void> callable, long delay) {
        this.delayedCallables.add(callable);
        this.delay = Math.max(this.delay, delay);
    }

    /**
     * Queue a callable for execution only in the event of an exception being thrown during the call invocation. <p/> If
     * an exception does not happen, all the callables queued by this method are discarded, they are not queued for
     * execution. <p/> All queued callables, regardless of the number of queue invocations, are queued for a single
     * serial execution.
     *
     * @param callable callable to queue for execution in the case of an exception.
     */
    protected void queueCallableForException(XCallable<Void> callable) {
        exceptionCallables.add(callable);
    }

    /**
     * Logging the info if failed to queue the callables.
     *
     * @param callables
     */
    protected void logQueueCallableFalse(List<? extends XCallable<Void>> callables) {
        StringBuilder sb = new StringBuilder(
                "Unable to queue the callables, delayedQueue is full or system is in SAFEMODE - failed to queue:[");
        int size = callables.size();
        for (int i = 0; i < size; i++) {
            XCallable<Void> callable = callables.get(i);
            sb.append(callable.getName());
            if (i < size - 1) {
                sb.append(", ");
            }
            else {
                sb.append("]");
            }
        }
        XLog.getLog(getClass()).warn(sb.toString());
    }

    /**
     * DagCallable subclasses must implement this method to perform their task. <p/> The workflow store works in
     * transactional mode. The transaction is committed only if this method ends successfully. Otherwise the transaction
     * is rolledback.
     *
     * @param store the workflow store instance for the callable, <code>null</code> if the callable does not use a
     * store.
     * @return the return value of the callable.
     * @throws StoreException thrown if the workflow store could not perform an operation.
     * @throws CommandException thrown if the command could not perform its operation.
     */
    protected abstract T call(S store) throws StoreException, CommandException;

    // to do
    // need to implement on all sub commands and break down the transactions

    // protected abstract T execute(String id) throws CommandException;

    /**
     * Command subclasses must implement this method correct Store can be passed to call(store);
     *
     * @return the Store class for use by Callable
     * @throws CommandException thrown if the command could not perform its operation.
     */
    protected abstract Class<? extends Store> getStoreClass();

    /**
     * Set the log info with the context of the given coordinator bean.
     *
     * @param cBean coordinator bean.
     */
    protected void setLogInfo(CoordinatorJobBean cBean) {
        if (logInfo.getParameter(XLogService.GROUP) == null) {
            logInfo.setParameter(XLogService.GROUP, cBean.getGroup());
        }
        if (logInfo.getParameter(XLogService.USER) == null) {
            logInfo.setParameter(XLogService.USER, cBean.getUser());
        }
        logInfo.setParameter(DagXLogInfoService.JOB, cBean.getId());
        logInfo.setParameter(DagXLogInfoService.TOKEN, "");
        logInfo.setParameter(DagXLogInfoService.APP, cBean.getAppName());
        XLog.Info.get().setParameters(logInfo);
    }

    /**
     * Set the log info with the context of the given coordinator action bean.
     *
     * @param action action bean.
     */
    protected void setLogInfo(CoordinatorActionBean action) {
        logInfo.setParameter(DagXLogInfoService.JOB, action.getJobId());
        // logInfo.setParameter(DagXLogInfoService.TOKEN, action.getLogToken());
        logInfo.setParameter(DagXLogInfoService.ACTION, action.getId());
        XLog.Info.get().setParameters(logInfo);
    }

    /**
     * Set the log info with the context of the given workflow bean.
     *
     * @param workflow workflow bean.
     */
    protected void setLogInfo(WorkflowJobBean workflow) {
        if (logInfo.getParameter(XLogService.GROUP) == null) {
            logInfo.setParameter(XLogService.GROUP, workflow.getGroup());
        }
        if (logInfo.getParameter(XLogService.USER) == null) {
            logInfo.setParameter(XLogService.USER, workflow.getUser());
        }
        logInfo.setParameter(DagXLogInfoService.JOB, workflow.getId());
        logInfo.setParameter(DagXLogInfoService.TOKEN, workflow.getLogToken());
        logInfo.setParameter(DagXLogInfoService.APP, workflow.getAppName());
        XLog.Info.get().setParameters(logInfo);
    }

    /**
     * Set the log info with the context of the given action bean.
     *
     * @param action action bean.
     */
    protected void setLogInfo(WorkflowActionBean action) {
        logInfo.setParameter(DagXLogInfoService.JOB, action.getJobId());
        logInfo.setParameter(DagXLogInfoService.TOKEN, action.getLogToken());
        logInfo.setParameter(DagXLogInfoService.ACTION, action.getId());
        XLog.Info.get().setParameters(logInfo);
    }

    /**
     * Reset the action bean information from the log info.
     */
    // TODO check if they are used, else delete
    protected void resetLogInfoAction() {
        logInfo.clearParameter(DagXLogInfoService.ACTION);
        XLog.Info.get().clearParameter(DagXLogInfoService.ACTION);
    }

    /**
     * Reset the workflow bean information from the log info.
     */
    // TODO check if they are used, else delete
    protected void resetLogInfoWorkflow() {
        logInfo.clearParameter(DagXLogInfoService.JOB);
        logInfo.clearParameter(DagXLogInfoService.APP);
        logInfo.clearParameter(DagXLogInfoService.TOKEN);
        XLog.Info.get().clearParameter(DagXLogInfoService.JOB);
        XLog.Info.get().clearParameter(DagXLogInfoService.APP);
        XLog.Info.get().clearParameter(DagXLogInfoService.TOKEN);
    }

    /**
     * Convenience method to increment counters.
     *
     * @param group the group name.
     * @param name the counter name.
     * @param count increment count.
     */
    private void incrCounter(String group, String name, int count) {
        if (instrumentation != null) {
            instrumentation.incr(group, name, count);
        }
    }

    /**
     * Used to increment command counters.
     *
     * @param count the increment count.
     */
    protected void incrCommandCounter(int count) {
        incrCounter(INSTRUMENTATION_GROUP, name, count);
    }

    /**
     * Used to increment job counters. The counter name s the same as the command name.
     *
     * @param count the increment count.
     */
    protected void incrJobCounter(int count) {
        incrJobCounter(name, count);
    }

    /**
     * Used to increment job counters.
     *
     * @param name the job name.
     * @param count the increment count.
     */
    protected void incrJobCounter(String name, int count) {
        incrCounter(INSTRUMENTATION_JOB_GROUP, name, count);
    }

    /**
     * Return the {@link Instrumentation} instance in use.
     *
     * @return the {@link Instrumentation} instance in use.
     */
    protected Instrumentation getInstrumentation() {
        return instrumentation;
    }

    /**
     * Return the identity.
     *
     * @return the identity.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getType());
        sb.append(",").append(getPriority());
        return sb.toString();
    }

    protected boolean lock(String id) throws InterruptedException {
        if (id == null || id.length() == 0) {
            XLog.getLog(getClass()).warn("lock(): Id is null or empty :" + id + ":");
            return false;
        }
        LockToken token = Services.get().get(MemoryLocksService.class).getWriteLock(id, LOCK_TIMEOUT);
        if (token != null) {
            locks.add(token);
            return true;
        }
        else {
            return false;
        }
    }

    /*
     * TODO - remove store coupling to EM. Store will only contain queries
     * protected EntityManager getEntityManager() { return
     * store.getEntityManager(); }
     */
    protected T execute(S store) throws CommandException, StoreException {
        T result = call(store);
        return result;
    }

    /**
     * Get command key
     *
     * @return command key
     */
    @Override
    public String getKey(){
        return this.key;
    }

    /**
     * Get command lock key returning the key as an entity key, [not used] Just
     * to be able to implement XCallable [to be deprecated]
     *
     * @return key
     */
    @Override
    public String getEntityKey() {
        return this.key;
    }

    /**
     * set the mode of execution for the callable. True if in interrupt, false
     * if not [to be deprecated]
     */
    public void setInterruptMode(boolean mode) {
    }

    /**
     * [to be deprecated]
     *
     * @return the mode of execution. true if it is executed as an Interrupt,
     *         false otherwise
     */
    public boolean inInterruptMode() {
        return false;
    }

}
