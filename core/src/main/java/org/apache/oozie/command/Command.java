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
package org.apache.oozie.command;

import org.apache.oozie.service.XLogService;
import org.apache.oozie.XException;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.FaultInjection;
import org.apache.oozie.service.DagXLogInfoService;
import org.apache.oozie.service.WorkflowStoreService;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.service.CallableQueueService;
import org.apache.oozie.service.InstrumentationService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XCallable;
import org.apache.oozie.util.XLog;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class for all synchronous and asynchronous DagEngine commands.
 */
public abstract class Command<T> implements XCallable<T> {
    /**
     * The instrumentation group used for Commands.
     */
    private static final String INSTRUMENTATION_GROUP = "commands";
    
    /**
     * The instrumentation group used for Jobs.
     */
    private static final String INSTRUMENTATION_JOB_GROUP = "jobs";

    protected Instrumentation instrumentation;
    private List<XCallable<Void>> callables;
    private List<XCallable<Void>> delayedCallables;
    private long delay = 0;
    private List<XCallable<Void>> exceptionCallables;
    private String name;
    private int priority;
    private int logMask;
    private boolean withStore;
    private String type;

    /**
     * This variable is package private for testing purposes only.
     */
    XLog.Info logInfo;

    /**
     * Create a command that uses a {@link WorkflowStore} instance.
     * <p/>
     * The current {@link XLog.Info} values are captured for execution.
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
     * Create a command.
     * <p/>
     * The current {@link XLog.Info} values are captured for execution.
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
        this.priority = priority;
        this.withStore = withStore;
        this.logMask = logMask;
        instrumentation = Services.get().get(InstrumentationService.class).get();
        logInfo = new XLog.Info(XLog.Info.get());
    }

    /**
     * Return the name of the command.
     *
     * @return the name of the command.
     */
    public String getName() {
        return name;
    }

    /**
     * Return the callable type.
     * <p/>
     * The callable type is used for concurrency throttling in the
     * {@link org.apache.oozie.service.CallableQueueService}.
     *
     * @return the callable type.
     */
    public String getType() {
        return type;
    }

    /**
     * Return the priority of the command.
     *
     * @return the priority of the command.
     */
    public int getPriority() {
        return priority;
    }

    /**
     * Execute the command {@link #call(WorkflowStore)} setting all the necessary context.
     * <p/>
     * The {@link XLog.Info} is set to the values at instance creation time.
     * <p/>
     * The command execution is logged and instrumented.
     * <p/>
     * If a {@link WorkflowStore} is used, a fresh instance will be passed and it will be commited after the
     * {@link #call(WorkflowStore)} execution. It will be closed without committing if an exception is thrown.
     * <p/>
     * Commands queued via the DagCommand queue methods are queued for execution after the workflow store has been
     * committed.
     * <p/>
     * If an exception happends the queued commands will not be effectively queued for execution. Instead, the
     * the commands queued for exception will be effectively queued fro execution..
     *
     * @throws CommandException thrown if the command could not be executed successfully, the workflow store is
     * closed without committing, thus doing a rollback.
     */
    @SuppressWarnings({"ThrowFromFinallyBlock"})
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
        WorkflowStore store = null;
        boolean exception = false;
        try {
            if (withStore) {
                store = Services.get().get(WorkflowStoreService.class).create();
            }
            T result = call(store);

            if (withStore) {
                if (store == null) {
                    throw new IllegalStateException("WorkflowStore should not be null");
                }
                if (FaultInjection.isActive("org.apache.oozie.command.SkipCommitFaultInjection")) {
                    throw new RuntimeException("Skipping Commit for Failover Testing");
                }
                store.commit();
            }

            //TODO figure out the reject due to concurrency problems and remove the delayed queuing for callables.
            Services.get().get(CallableQueueService.class).queueSerial(callables, 10);

            Services.get().get(CallableQueueService.class).queueSerial(delayedCallables, delay);

            return result;
        }
        catch (XException ex) {
            exception = true;
            //TODO figure out the reject due to concurrency problems and remove the delayed queuing for callables.
            Services.get().get(CallableQueueService.class).queueSerial(exceptionCallables, 10);
            if (ex instanceof CommandException) {
                throw (CommandException) ex;
            }
            else {
                throw new CommandException(ex);
            }
        }
        catch (RuntimeException ex) {
            exception = true;
            throw ex;
        }
        finally {
            FaultInjection.deactivate("org.apache.oozie.command.SkipCommitFaultInjection");
            cron.stop();
            instrumentation.addCron(INSTRUMENTATION_GROUP, name, cron);
            incrCommandCounter(1);
            log.trace(logMask, "End");
            if (store != null) {
                try {
                    store.close();
                }
                catch (StoreException ex) {
                    if (exception) {
                        log.warn(logMask | XLog.OPS, "store error, {1}", name, ex.getMessage(), ex);
                    }
                    else {
                        throw new CommandException(ex);
                    }
                }
            }
        }
    }

    /**
     * Queue a callable for execution after the current callable call invocation completes and the {@link WorkflowStore}
     * transaction commits.
     * <p/>
     * All queued callables, regardless of the number of queue invocations,   are queued for a single serial execution.
     * <p/>
     * If the call invocation throws an exception all queued callables are discarded, they are not queued for execution.
     *
     * @param callable callable to queue for execution.
     */
    protected void queueCallable(XCallable<Void> callable) {
        callables.add(callable);
    }

    /**
     * Queue a list of callables for execution after the current callable call invocation completes and the
     * {@link WorkflowStore} transaction commits.
     * <p/>
     * All queued callables, regardless of the number of queue invocations,   are queued for a single serial execution.
     * <p/>
     * If the call invocation throws an exception all queued callables are discarded, they are not queued for execution.
     *
     * @param callables list of callables to queue for execution.
     */
    protected void queueCallable(List<? extends XCallable<Void>> callables) {
        this.callables.addAll(callables);
    }

    /**
     * Queue a callable for delayed execution after the current callable call invocation completes and the
     * {@link WorkflowStore} transaction commits.
     * <p/>
     * All queued delayed callables, regardless of the number of delay queue invocations,
     * are queued for a single serial delayed execution with the highest delay of all queued callables.
     * <p/>
     * If the call invocation throws an exception all queued callables are discarded, they are not queued for execution.
     *
     * @param callable callable to queue for delayed execution.
     * @param delay the queue delay in milliseconds
     */
    protected void queueCallable(XCallable<Void> callable, long delay) {
        this.delayedCallables.add(callable);
        this.delay = Math.max(this.delay, delay);
    }

    /**
     * Queue a callable for execution only in the event of an exception being thrown during the call invocation.
     * <p/>
     * If an exception does not happen, all the callables queued by this method are discarded, they are not queued for
     * execution.
     * <p/>
     * All queued callables, regardless of the number of queue invocations,   are queued for a single serial execution.
     *
     * @param callable callable to queue for execution in the case of an exception.
     */
    protected void queueCallableForException(XCallable<Void> callable) {
        exceptionCallables.add(callable);
    }

    /**
     * DagCallable subclasses must implement this method to perform their task.
     * <p/>
     * The workflow store works in transactional mode. The transaction is committed only if this method ends
     * successfully. Otherwise the transaction is rolledback.
     *
     * @param store the workflow store instance for the callable, <code>null</code> if the callable does not use a
     * store.
     * @return the return value of the callable.
     * @throws StoreException thrown if the workflow store could not perform an operation.
     * @throws CommandException thrown if the command could not perform its operation.
     */
    protected abstract T call(WorkflowStore store) throws StoreException, CommandException;

    /**
     * Set the log info with the context of the given workflow bean.
     *
     * @param workflow workflow bean.
     */
    protected void setLogInfo(WorkflowJobBean workflow) {
        if(logInfo.getParameter(XLogService.GROUP) == null) {
            logInfo.setParameter(XLogService.GROUP, workflow.getGroup());
        }
        if(logInfo.getParameter(XLogService.USER) == null) {
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
        logInfo.setParameter(DagXLogInfoService.ACTION, action.getName());
        XLog.Info.get().setParameters(logInfo);
    }

    /**
     * Reset the action bean information from the log info.
     */
    //TODO check if they are used, else delete
    protected void resetLogInfoAction() {
        logInfo.clearParameter(DagXLogInfoService.ACTION);
        XLog.Info.get().clearParameter(DagXLogInfoService.ACTION);
    }

    /**
     * Reset the workflow bean information from the log info.
     */
    //TODO check if they are used, else delete
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
     * Used to increment job counters. The counter name s the same as the
     * command name.
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

}