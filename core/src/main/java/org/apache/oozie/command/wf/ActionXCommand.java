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
package org.apache.oozie.command.wf;

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.DagELFunctions;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.service.CallbackService;
import org.apache.oozie.service.ELService;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.LiteWorkflowStoreService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.InstrumentUtils;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.lite.LiteWorkflowInstance;

/**
 * Base class for Action execution commands. Provides common functionality to handle different types of errors while
 * attempting to start or end an action.
 */
public abstract class ActionXCommand<T> extends WorkflowXCommand<Void> {
    private static final String INSTRUMENTATION_GROUP = "action.executors";

    protected static final String INSTR_FAILED_JOBS_COUNTER = "failed";

    protected static final String RECOVERY_ID_SEPARATOR = "@";

    public ActionXCommand(String name, String type, int priority) {
        super(name, type, priority);
    }

    /**
     * Takes care of Transient failures. Sets the action status to retry and increments the retry count if not enough
     * attempts have been made. Otherwise returns false.
     *
     * @param context the execution context.
     * @param executor the executor instance being used.
     * @param status the status to be set for the action.
     * @return true if the action is scheduled for another retry. false if the number of retries has exceeded the
     *         maximum number of configured retries.
     * @throws CommandException thrown if unable to handle transient
     */
    protected boolean handleTransient(ActionExecutor.Context context, ActionExecutor executor,
            WorkflowAction.Status status) throws CommandException {
        LOG.debug("Attempting to retry");
        ActionExecutorContext aContext = (ActionExecutorContext) context;
        WorkflowActionBean action = (WorkflowActionBean) aContext.getAction();
        incrActionErrorCounter(action.getType(), "transient", 1);

        int actionRetryCount = action.getRetries();
        if (actionRetryCount >= executor.getMaxRetries()) {
            LOG.warn("Exceeded max retry count [{0}]. Suspending Job", executor.getMaxRetries());
            return false;
        }
        else {
            action.setStatus(status);
            action.setPending();
            action.incRetries();
            long retryDelayMillis = executor.getRetryInterval() * 1000;
            action.setPendingAge(new Date(System.currentTimeMillis() + retryDelayMillis));
            LOG.info("Next Retry, Attempt Number [{0}] in [{1}] milliseconds", actionRetryCount + 1, retryDelayMillis);
            this.resetUsed();
            queue(this, retryDelayMillis);
            return true;
        }
    }

    /**
     * Takes care of non transient failures. The job is suspended, and the state of the action is changed to *MANUAL and
     * set pending flag of action to false
     *
     * @param context the execution context.
     * @param executor the executor instance being used.
     * @param status the status to be set for the action.
     * @throws CommandException thrown if unable to suspend job
     */
    protected void handleNonTransient(ActionExecutor.Context context, ActionExecutor executor,
            WorkflowAction.Status status) throws CommandException {
        ActionExecutorContext aContext = (ActionExecutorContext) context;
        WorkflowActionBean action = (WorkflowActionBean) aContext.getAction();
        incrActionErrorCounter(action.getType(), "nontransient", 1);
        WorkflowJobBean workflow = (WorkflowJobBean) context.getWorkflow();
        String id = workflow.getId();
        action.setStatus(status);
        action.resetPendingOnly();
        LOG.warn("Suspending Workflow Job id=" + id);
        try {
            SuspendXCommand.suspendJob(Services.get().get(JPAService.class), workflow, id, action.getId(), null);
        }
        catch (Exception e) {
            throw new CommandException(ErrorCode.E0727, id, e.getMessage());
        }
        finally {
            updateParentIfNecessary(workflow, 3);
        }
    }

    /**
     * Takes care of errors. </p> For errors while attempting to start the action, the job state is updated and an
     * {@link ActionEndCommand} is queued. </p> For errors while attempting to end the action, the job state is updated.
     * </p>
     *
     * @param context the execution context.
     * @param executor the executor instance being used.
     * @param message
     * @param isStart whether the error was generated while starting or ending an action.
     * @param status the status to be set for the action.
     * @throws CommandException thrown if unable to handle action error
     */
    protected void handleError(ActionExecutor.Context context, ActionExecutor executor, String message,
            boolean isStart, WorkflowAction.Status status) throws CommandException {
        LOG.warn("Setting Action Status to [{0}]", status);
        ActionExecutorContext aContext = (ActionExecutorContext) context;
        WorkflowActionBean action = (WorkflowActionBean) aContext.getAction();

        if (!handleUserRetry(action)) {
            incrActionErrorCounter(action.getType(), "error", 1);
            action.setPending();
            if (isStart) {
                action.setExecutionData(message, null);
                queue(new ActionEndXCommand(action.getId(), action.getType()));
            }
            else {
                action.setEndData(status, WorkflowAction.Status.ERROR.toString());
            }
        }
    }

    /**
     * Fail the job due to failed action
     *
     * @param context the execution context.
     * @throws CommandException thrown if unable to fail job
     */
    public void failJob(ActionExecutor.Context context) throws CommandException {
        ActionExecutorContext aContext = (ActionExecutorContext) context;
        WorkflowActionBean action = (WorkflowActionBean) aContext.getAction();
        failJob(context, action);
    }

    /**
     * Fail the job due to failed action
     *
     * @param context the execution context.
     * @param action the action that caused the workflow to fail
     * @throws CommandException thrown if unable to fail job
     */
    public void failJob(ActionExecutor.Context context, WorkflowActionBean action) throws CommandException {
        WorkflowJobBean workflow = (WorkflowJobBean) context.getWorkflow();
        if (!handleUserRetry(action)) {
            incrActionErrorCounter(action.getType(), "failed", 1);
            LOG.warn("Failing Job due to failed action [{0}]", action.getName());
            try {
                workflow.getWorkflowInstance().fail(action.getName());
                WorkflowInstance wfInstance = workflow.getWorkflowInstance();
                ((LiteWorkflowInstance) wfInstance).setStatus(WorkflowInstance.Status.FAILED);
                workflow.setWorkflowInstance(wfInstance);
                workflow.setStatus(WorkflowJob.Status.FAILED);
                action.setStatus(WorkflowAction.Status.FAILED);
                action.resetPending();
                queue(new NotificationXCommand(workflow, action));
                queue(new KillXCommand(workflow.getId()));
                InstrumentUtils.incrJobCounter(INSTR_FAILED_JOBS_COUNTER, 1, getInstrumentation());
            }
            catch (WorkflowException ex) {
                throw new CommandException(ex);
            }
        }
    }

    /**
     * Execute retry for action if this action is eligible for user-retry
     *
     * @param context the execution context.
     * @return true if user-retry has to be handled for this action
     * @throws CommandException thrown if unable to fail job
     */
    public boolean handleUserRetry(WorkflowActionBean action) throws CommandException {
        String errorCode = action.getErrorCode();
        Set<String> allowedRetryCode = LiteWorkflowStoreService.getUserRetryErrorCode();

        if (allowedRetryCode.contains(errorCode) && action.getUserRetryCount() < action.getUserRetryMax()) {
            LOG.info("Preparing retry this action [{0}], errorCode [{1}], userRetryCount [{2}], "
                    + "userRetryMax [{3}], userRetryInterval [{4}]", action.getId(), errorCode, action
                    .getUserRetryCount(), action.getUserRetryMax(), action.getUserRetryInterval());
            int interval = action.getUserRetryInterval() * 60 * 1000;
            action.setStatus(WorkflowAction.Status.USER_RETRY);
            action.incrmentUserRetryCount();
            action.setPending();
            queue(new ActionStartXCommand(action.getId(), action.getType()), interval);
            return true;
        }
        return false;
    }

	/*
	 * In case of action error increment the error count for instrumentation
	 */
    private void incrActionErrorCounter(String type, String error, int count) {
        getInstrumentation().incr(INSTRUMENTATION_GROUP, type + "#ex." + error, count);
    }

	/**
	 * Increment the action counter in the instrumentation log. indicating how
	 * many times the action was executed since the start Oozie server
	 */
    protected void incrActionCounter(String type, int count) {
        getInstrumentation().incr(INSTRUMENTATION_GROUP, type + "#" + getName(), count);
    }

	/**
	 * Adding a cron for the instrumentation time for the given Instrumentation
	 * group
	 */
    protected void addActionCron(String type, Instrumentation.Cron cron) {
        getInstrumentation().addCron(INSTRUMENTATION_GROUP, type + "#" + getName(), cron);
    }

    /**
     * Workflow action executor context
     *
     */
    public static class ActionExecutorContext implements ActionExecutor.Context {
        private final WorkflowJobBean workflow;
        private Configuration protoConf;
        private final WorkflowActionBean action;
        private final boolean isRetry;
        private final boolean isUserRetry;
        private boolean started;
        private boolean ended;
        private boolean executed;

		/**
		 * Constructing the ActionExecutorContext, setting the private members
		 * and constructing the proto configuration
		 */
        public ActionExecutorContext(WorkflowJobBean workflow, WorkflowActionBean action, boolean isRetry, boolean isUserRetry) {
            this.workflow = workflow;
            this.action = action;
            this.isRetry = isRetry;
            this.isUserRetry = isUserRetry;
            try {
                protoConf = new XConfiguration(new StringReader(workflow.getProtoActionConf()));
            }
            catch (IOException ex) {
                throw new RuntimeException("It should not happen", ex);
            }
        }

        /*
         * (non-Javadoc)
         * @see org.apache.oozie.action.ActionExecutor.Context#getCallbackUrl(java.lang.String)
         */
        public String getCallbackUrl(String externalStatusVar) {
            return Services.get().get(CallbackService.class).createCallBackUrl(action.getId(), externalStatusVar);
        }

        /*
         * (non-Javadoc)
         * @see org.apache.oozie.action.ActionExecutor.Context#getProtoActionConf()
         */
        public Configuration getProtoActionConf() {
            return protoConf;
        }

        /*
         * (non-Javadoc)
         * @see org.apache.oozie.action.ActionExecutor.Context#getWorkflow()
         */
        public WorkflowJob getWorkflow() {
            return workflow;
        }

        /**
         * Returns the workflow action of the given action context
         *
         * @return the workflow action of the given action context
         */
        public WorkflowAction getAction() {
            return action;
        }

        /*
         * (non-Javadoc)
         * @see org.apache.oozie.action.ActionExecutor.Context#getELEvaluator()
         */
        public ELEvaluator getELEvaluator() {
            ELEvaluator evaluator = Services.get().get(ELService.class).createEvaluator("workflow");
            DagELFunctions.configureEvaluator(evaluator, workflow, action);
            return evaluator;
        }

        /*
         * (non-Javadoc)
         * @see org.apache.oozie.action.ActionExecutor.Context#setVar(java.lang.String, java.lang.String)
         */
        public void setVar(String name, String value) {
            name = action.getName() + WorkflowInstance.NODE_VAR_SEPARATOR + name;
            WorkflowInstance wfInstance = workflow.getWorkflowInstance();
            wfInstance.setVar(name, value);
            workflow.setWorkflowInstance(wfInstance);
        }

        /*
         * (non-Javadoc)
         * @see org.apache.oozie.action.ActionExecutor.Context#getVar(java.lang.String)
         */
        public String getVar(String name) {
            name = action.getName() + WorkflowInstance.NODE_VAR_SEPARATOR + name;
            return workflow.getWorkflowInstance().getVar(name);
        }

        /*
         * (non-Javadoc)
         * @see org.apache.oozie.action.ActionExecutor.Context#setStartData(java.lang.String, java.lang.String, java.lang.String)
         */
        public void setStartData(String externalId, String trackerUri, String consoleUrl) {
            action.setStartData(externalId, trackerUri, consoleUrl);
            started = true;
        }

        /**
         * Setting the start time of the action
         */
        public void setStartTime() {
            Date now = new Date();
            action.setStartTime(now);
        }

        /*
         * (non-Javadoc)
         * @see org.apache.oozie.action.ActionExecutor.Context#setExecutionData(java.lang.String, java.util.Properties)
         */
        public void setExecutionData(String externalStatus, Properties actionData) {
            action.setExecutionData(externalStatus, actionData);
            executed = true;
        }

        /*
         * (non-Javadoc)
         * @see org.apache.oozie.action.ActionExecutor.Context#setExecutionStats(java.lang.String)
         */
        public void setExecutionStats(String jsonStats) {
            action.setExecutionStats(jsonStats);
            executed = true;
        }

        /*
         * (non-Javadoc)
         * @see org.apache.oozie.action.ActionExecutor.Context#setExternalChildIDs(java.lang.String)
         */
        public void setExternalChildIDs(String externalChildIDs) {
            action.setExternalChildIDs(externalChildIDs);
            executed = true;
        }

        /*
         * (non-Javadoc)
         * @see org.apache.oozie.action.ActionExecutor.Context#setEndData(org.apache.oozie.client.WorkflowAction.Status, java.lang.String)
         */
        public void setEndData(WorkflowAction.Status status, String signalValue) {
            action.setEndData(status, signalValue);
            ended = true;
        }

        /*
         * (non-Javadoc)
         * @see org.apache.oozie.action.ActionExecutor.Context#isRetry()
         */
        public boolean isRetry() {
            return isRetry;
        }

        /**
         * Return if the executor invocation is a user retry or not.
         *
         * @return if the executor invocation is a user retry or not.
         */
        public boolean isUserRetry() {
            return isUserRetry;
        }

        /**
         * Returns whether setStartData has been called or not.
         *
         * @return true if start completion info has been set.
         */
        public boolean isStarted() {
            return started;
        }

        /**
         * Returns whether setExecutionData has been called or not.
         *
         * @return true if execution completion info has been set, otherwise false.
         */
        public boolean isExecuted() {
            return executed;
        }

        /**
         * Returns whether setEndData has been called or not.
         *
         * @return true if end completion info has been set.
         */
        public boolean isEnded() {
            return ended;
        }

        public void setExternalStatus(String externalStatus) {
            action.setExternalStatus(externalStatus);
        }

        @Override
        public String getRecoveryId() {
            return action.getId() + RECOVERY_ID_SEPARATOR + workflow.getRun();
        }

        /* (non-Javadoc)
         * @see org.apache.oozie.action.ActionExecutor.Context#getActionDir()
         */
        public Path getActionDir() throws HadoopAccessorException, IOException, URISyntaxException {
            String name = getWorkflow().getId() + "/" + action.getName() + "--" + action.getType();
            FileSystem fs = getAppFileSystem();
            String actionDirPath = Services.get().getSystemId() + "/" + name;
            Path fqActionDir = new Path(fs.getHomeDirectory(), actionDirPath);
            return fqActionDir;
        }

        /* (non-Javadoc)
         * @see org.apache.oozie.action.ActionExecutor.Context#getAppFileSystem()
         */
        public FileSystem getAppFileSystem() throws HadoopAccessorException, IOException, URISyntaxException {
            WorkflowJob workflow = getWorkflow();
            URI uri = new URI(getWorkflow().getAppPath());
            HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
            Configuration fsConf = has.createJobConf(uri.getAuthority());
            return has.createFileSystem(workflow.getUser(), uri, fsConf);

        }

        /* (non-Javadoc)
         * @see org.apache.oozie.action.ActionExecutor.Context#setErrorInfo(java.lang.String, java.lang.String)
         */
        @Override
        public void setErrorInfo(String str, String exMsg) {
            action.setErrorInfo(str, exMsg);
        }
    }

}
