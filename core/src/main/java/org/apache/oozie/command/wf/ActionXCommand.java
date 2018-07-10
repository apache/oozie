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
import java.util.HashMap;
import java.util.Map;
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
import org.apache.oozie.client.Job;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.service.CallbackService;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.ELService;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.LiteWorkflowStoreService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.InstrumentUtils;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.JobUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.lite.LiteWorkflowApp;
import org.apache.oozie.workflow.lite.LiteWorkflowInstance;
import org.apache.oozie.workflow.lite.NodeDef;

/**
 * Base class for Action execution commands. Provides common functionality to handle different types of errors while
 * attempting to start or end an action.
 */
public abstract class ActionXCommand<T> extends WorkflowXCommand<T> {
    private static final String INSTRUMENTATION_GROUP = "action.executors";
    public static final String RETRY = "retry.";

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
            long retryDelayMillis = getRetryDelay(actionRetryCount, executor.getRetryInterval(), executor.getRetryPolicy());
            action.setPendingAge(new Date(System.currentTimeMillis() + retryDelayMillis));
            LOG.info("Next Retry, Attempt Number [{0}] in [{1}] milliseconds", actionRetryCount + 1, retryDelayMillis);
            this.resetUsed();
            queueCommandForTransientFailure(retryDelayMillis);
            return true;
        }
    }

    protected void queueCommandForTransientFailure(long retryDelayMillis){
        queue(this, retryDelayMillis);
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
     * Takes care of errors. <p> For errors while attempting to start the action, the job state is updated and an
     * {@link ActionEndXCommand} is queued. <p> For errors while attempting to end the action, the job state is updated.
     * <p>
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
        if (!handleUserRetry(context, action)) {
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
        if (!handleUserRetry(context, action)) {
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
                queue(new WorkflowNotificationXCommand(workflow, action));
                queue(new KillXCommand(workflow.getId()));
                InstrumentUtils.incrJobCounter(INSTR_FAILED_JOBS_COUNTER_NAME, 1, getInstrumentation());
            }
            catch (WorkflowException ex) {
                throw new CommandException(ex);
            }
        }
    }

    /**
     * Execute retry for action if this action is eligible for user-retry
     *
     * @param action the Workflow action bean
     * @return true if user-retry has to be handled for this action
     * @throws CommandException thrown if unable to fail job
     */
    public boolean handleUserRetry(ActionExecutor.Context context, WorkflowActionBean action) throws CommandException {
        WorkflowJobBean wfJob = (WorkflowJobBean) context.getWorkflow();
        String errorCode = action.getErrorCode();
        Set<String> allowedRetryCode = LiteWorkflowStoreService.getUserRetryErrorCode();

        if ((allowedRetryCode.contains(LiteWorkflowStoreService.USER_ERROR_CODE_ALL) || allowedRetryCode.contains(errorCode))
                && action.getUserRetryCount() < action.getUserRetryMax()) {
            LOG.info("Preparing retry this action [{0}], errorCode [{1}], userRetryCount [{2}], "
                    + "userRetryMax [{3}], userRetryInterval [{4}]", action.getId(), errorCode, action
                    .getUserRetryCount(), action.getUserRetryMax(), action.getUserRetryInterval());
            ActionExecutor.RETRYPOLICY retryPolicy = getUserRetryPolicy(action, wfJob);
            long interval = getRetryDelay(action.getUserRetryCount(), action.getUserRetryInterval() * 60, retryPolicy);
            action.setStatus(WorkflowAction.Status.USER_RETRY);
            context.setVar(JobUtils.getRetryKey(action, JsonTags.WORKFLOW_ACTION_END_TIME), String.valueOf(new Date().getTime()));
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

    /*
     * Returns the next retry time in milliseconds, based on retry policy algorithm.
     */
    private long getRetryDelay(int retryCount, long retryInterval, ActionExecutor.RETRYPOLICY retryPolicy) {
        switch (retryPolicy) {
            case EXPONENTIAL:
                long retryTime = ((long) Math.pow(2, retryCount) * retryInterval * 1000L);
                return retryTime;
            case PERIODIC:
                return retryInterval * 1000L;
            default:
                throw new UnsupportedOperationException("Retry policy not supported");
        }
    }

    /**
     * Workflow action executor context
     *
     */
    public static class ActionExecutorContext implements ActionExecutor.Context {
        protected final WorkflowJobBean workflow;
        private Configuration protoConf;
        protected final WorkflowActionBean action;
        private final boolean isRetry;
        private final boolean isUserRetry;
        private boolean started;
        private boolean ended;
        private boolean executed;
        private boolean shouldEndWF;
        private Job.Status jobStatus;

        /**
         * Constructing the ActionExecutorContext, setting the private members
         * and constructing the proto configuration
         */
        public ActionExecutorContext(WorkflowJobBean workflow, WorkflowActionBean action, boolean isRetry,
                boolean isUserRetry) {
            this.workflow = workflow;
            this.action = action;
            this.isRetry = isRetry;
            this.isUserRetry = isUserRetry;
            if (null != workflow.getProtoActionConf()) {
                try {
                    protoConf = new XConfiguration(new StringReader(workflow.getProtoActionConf()));
                }
                catch (IOException ex) {
                    throw new RuntimeException("Failed to construct the proto configuration", ex);
                }
            }
        }

        public ActionExecutorContext(WorkflowJobBean workflow, WorkflowActionBean action) {
            this(workflow, action, false, false);
        }

        public String getCallbackUrl(String externalStatusVar) {
            return Services.get().get(CallbackService.class).createCallBackUrl(action.getId(), externalStatusVar);
        }

        public Configuration getProtoActionConf() {
            return protoConf;
        }

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

        public ELEvaluator getELEvaluator() {
            ELEvaluator evaluator = Services.get().get(ELService.class).createEvaluator("workflow");
            DagELFunctions.configureEvaluator(evaluator, workflow, action);
            return evaluator;
        }

        public void setVar(String name, String value) {
            setVarToWorkflow(name, value);
        }

        /**
         * This is not thread safe, don't use if workflowjob is shared among multiple actions command
         * @param name
         * @param value
         */
        public void setVarToWorkflow(String name, String value) {
            name = action.getName() + WorkflowInstance.NODE_VAR_SEPARATOR + name;
            WorkflowInstance wfInstance = workflow.getWorkflowInstance();
            wfInstance.setVar(name, value);
            workflow.setWorkflowInstance(wfInstance);
        }

        public String getVar(String name) {
            name = action.getName() + WorkflowInstance.NODE_VAR_SEPARATOR + name;
            return workflow.getWorkflowInstance().getVar(name);
        }

        public void setStartData(String externalId, String trackerUri, String consoleUrl) {
            setVar(JobUtils.getRetryKey(action, JsonTags.WORKFLOW_ACTION_CONSOLE_URL), consoleUrl);
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

        public void setExecutionData(String externalStatus, Properties actionData) {
            action.setExecutionData(externalStatus, actionData);
            executed = true;
        }

        public void setExecutionStats(String jsonStats) {
            action.setExecutionStats(jsonStats);
            executed = true;
        }

        public void setExternalChildIDs(String externalChildIDs) {
            setVar(JobUtils.getRetryKey(action, JsonTags.WORKFLOW_ACTION_EXTERNAL_CHILD_IDS), externalChildIDs);
            action.setExternalChildIDs(externalChildIDs);
            executed = true;
        }

        public void setEndData(WorkflowAction.Status status, String signalValue) {
            action.setEndData(status, signalValue);
            ended = true;
        }

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

        public Path getActionDir() throws HadoopAccessorException, IOException, URISyntaxException {
            String name = getWorkflow().getId() + "/" + action.getName() + "--" + action.getType();
            FileSystem fs = getAppFileSystem();
            String actionDirPath = Services.get().getSystemId() + "/" + name;
            Path fqActionDir = new Path(fs.getHomeDirectory(), actionDirPath);
            return fqActionDir;
        }

        public FileSystem getAppFileSystem() throws HadoopAccessorException, IOException, URISyntaxException {
            WorkflowJob workflow = getWorkflow();
            URI uri = new URI(getWorkflow().getAppPath());
            HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
            Configuration fsConf = has.createConfiguration(uri.getAuthority());
            return has.createFileSystem(workflow.getUser(), uri, fsConf);

        }

        @Override
        public void setErrorInfo(String str, String exMsg) {
            action.setErrorInfo(str, exMsg);
        }

        public boolean isShouldEndWF() {
            return shouldEndWF;
        }

        public void setShouldEndWF(boolean shouldEndWF) {
            this.shouldEndWF = shouldEndWF;
        }

        public Job.Status getJobStatus() {
            return jobStatus;
        }

        public void setJobStatus(Job.Status jobStatus) {
            this.jobStatus = jobStatus;
        }
    }

    public static class ForkedActionExecutorContext extends ActionExecutorContext {
        private Map<String, String> contextVariableMap = new HashMap<String, String>();

        public ForkedActionExecutorContext(WorkflowJobBean workflow, WorkflowActionBean action, boolean isRetry,
                boolean isUserRetry) {
            super(workflow, action, isRetry, isUserRetry);
        }

        public void setVar(String name, String value) {
            if (value == null) {
                contextVariableMap.remove(name);
            }
            else {
                contextVariableMap.put(name, value);
            }
        }

        public String getVar(String name) {
            return contextVariableMap.get(name);
        }

        public Map<String, String> getContextMap() {
            return contextVariableMap;
        }
    }

    /*
     * Returns user retry policy
     */
    private ActionExecutor.RETRYPOLICY getUserRetryPolicy(WorkflowActionBean wfAction, WorkflowJobBean wfJob) {
        WorkflowInstance wfInstance = wfJob.getWorkflowInstance();
        LiteWorkflowApp wfApp = (LiteWorkflowApp) wfInstance.getApp();
        NodeDef nodeDef = wfApp.getNode(wfAction.getName());
        if (nodeDef == null) {
            return ActionExecutor.RETRYPOLICY.valueOf(LiteWorkflowStoreService.DEFAULT_USER_RETRY_POLICY);
        }
        String userRetryPolicy = nodeDef.getUserRetryPolicy().toUpperCase();
        String userRetryPolicyInSysConfig = ConfigurationService.get(LiteWorkflowStoreService.CONF_USER_RETRY_POLICY)
                .toUpperCase();
        if (isValidRetryPolicy(userRetryPolicy)) {
            return ActionExecutor.RETRYPOLICY.valueOf(userRetryPolicy);
        }
        else if (isValidRetryPolicy(userRetryPolicyInSysConfig)) {
            return ActionExecutor.RETRYPOLICY.valueOf(userRetryPolicyInSysConfig);
        }
        else {
            return ActionExecutor.RETRYPOLICY.valueOf(LiteWorkflowStoreService.DEFAULT_USER_RETRY_POLICY);
        }
    }

    /*
     * Returns true if policy is valid, otherwise false
     */
    private static boolean isValidRetryPolicy(String policy) {
        try {
            ActionExecutor.RETRYPOLICY.valueOf(policy.toUpperCase().trim());
        }
        catch (IllegalArgumentException e) {
            // Invalid Policy
            return false;
        }
        return true;
    }

}
