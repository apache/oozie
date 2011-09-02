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
package org.apache.oozie.command.wf;

import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.DagELFunctions;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.command.Command;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.service.CallbackService;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.service.ELService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.HadoopAccessorService;

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.Properties;

/**
 * Base class for Action execution commands. Provides common functionality to
 * handle different types of errors while attempting to start or end an action.
 */
public abstract class ActionCommand<T> extends Command<Void> {
    private static final String INSTRUMENTATION_GROUP = "action.executors";

    protected static final String INSTR_FAILED_JOBS_COUNTER = "failed";

    protected static final String RECOVERY_ID_SEPARATOR = "@";

    public ActionCommand(String name, String type, int priority) {
        super(name, type, priority, XLog.STD);
    }

    /**
     * Takes care of Transient failures. Sets the action status to retry and
     * increments the retry count if not enough attempts have been made.
     * Otherwise returns false.
     * 
     * @param context the execution context.
     * @param executor the executor instance being used.
     * @param status the status to be set for the action.
     * @return true if the action is scheduled for another retry. false if the
     *         number of retries has exceeded the maximum number of configured
     *         retries.
     * @throws StoreException
     * @throws org.apache.oozie.command.CommandException
     */
    protected boolean handleTransient(ActionExecutor.Context context, ActionExecutor executor, WorkflowAction.Status status)
            throws StoreException, CommandException {
        XLog.getLog(getClass()).debug("Attempting to retry");
        ActionExecutorContext aContext = (ActionExecutorContext) context;
        WorkflowActionBean action = (WorkflowActionBean) aContext.getAction();
        incrActionErrorCounter(action.getType(), "transient", 1);

        int actionRetryCount = action.getRetries();

        if (actionRetryCount >= executor.getMaxRetries()) {
            XLog.getLog(getClass()).warn("Exceeded max retry count [{0}]. Suspending Job", executor.getMaxRetries());
            return false;
        }
        else {
            action.setStatus(status);
            action.setPending();
            action.incRetries();
            long retryDelayMillis = executor.getRetryInterval() * 1000;
            action.setPendingAge(new Date(System.currentTimeMillis() + retryDelayMillis));
            XLog.getLog(getClass()).info("Next Retry, Attempt Number [{0}] in [{1}] milliseconds",
                    actionRetryCount + 1, retryDelayMillis);
            queueCallable(this, retryDelayMillis);
            return true;
        }
    }

    /**
     * Takes care of non transient failures. The job is suspended, and the state
     * of the action is changed to *MANUAL
     * 
     * @param context the execution context.
     * @param executor the executor instance being used.
     * @param status the status to be set for the action.
     * @throws StoreException
     * @throws org.apache.oozie.command.CommandException
     */
    protected void handleNonTransient(ActionExecutor.Context context, ActionExecutor executor, WorkflowAction.Status status)
            throws StoreException, CommandException {
        XLog.getLog(getClass()).warn("Suspending Job");
        ActionExecutorContext aContext = (ActionExecutorContext) context;
        WorkflowActionBean action = (WorkflowActionBean) aContext.getAction();
        incrActionErrorCounter(action.getType(), "nontransient", 1);
        WorkflowJobBean workflow = (WorkflowJobBean) context.getWorkflow();
        String id = workflow.getId();
        action.setStatus(status);
        try {
            SuspendCommand.suspendJob(workflow, id);
        }
        catch (WorkflowException e) {
            throw new CommandException(e);
        }
    }

    /**
     * Takes care of errors.
     * </p>
     * For errors while attempting to start the action, the job state is updated
     * and an {@link ActionEndCommand} is queued.
     * </p>
     * For errors while attempting to end the action, the job state is updated.
     * </p>
     * 
     * @param context the execution context.
     * @param executor the executor instance being used.
     * @param message
     * @param isStart whether the error was generated while starting or ending
     *        an action.
     * @param status the status to be set for the action.
     * @throws org.apache.oozie.command.CommandException
     */
    protected void handleError(ActionExecutor.Context context, ActionExecutor executor, String message,
            boolean isStart, WorkflowAction.Status status) throws CommandException {
        XLog.getLog(getClass()).warn("Setting Action Status to [{0}]", status);
        ActionExecutorContext aContext = (ActionExecutorContext) context;
        WorkflowActionBean action = (WorkflowActionBean) aContext.getAction();
        incrActionErrorCounter(action.getType(), "error", 1);
        action.setPending();
        if (isStart) {
            action.setExecutionData(message, null);
            queueCallable(new ActionEndCommand(action.getId(), action.getType()));
        }
        else {
            action.setEndData(status, WorkflowAction.Status.ERROR.toString());
        }
    }

    public void failJob(ActionExecutor.Context context) throws CommandException {
        ActionExecutorContext aContext = (ActionExecutorContext) context;
        WorkflowActionBean action = (WorkflowActionBean) aContext.getAction();
        incrActionErrorCounter(action.getType(), "failed", 1);
        WorkflowJobBean workflow = (WorkflowJobBean) context.getWorkflow();
        XLog.getLog(getClass()).warn("Failing Job due to failed action [{0}]", action.getName());
        try {
            workflow.getWorkflowInstance().fail(action.getName());
            workflow.setStatus(WorkflowJob.Status.FAILED);
            action.setStatus(WorkflowAction.Status.FAILED);
            queueCallable(new NotificationCommand(workflow, action));
            queueCallable(new KillCommand(workflow.getId()));
            incrJobCounter(INSTR_FAILED_JOBS_COUNTER, 1);
        } catch (WorkflowException ex) {
            throw new CommandException(ex);
        }
    }

    private void incrActionErrorCounter(String type, String error, int count) {
        getInstrumentation().incr(INSTRUMENTATION_GROUP, type + "#ex." + error, count);
    }

    protected void incrActionCounter(String type, int count) {
        getInstrumentation().incr(INSTRUMENTATION_GROUP, type + "#" + getName(), count);
    }

    protected void addActionCron(String type, Instrumentation.Cron cron) {
        getInstrumentation().addCron(INSTRUMENTATION_GROUP, type + "#" + getName(), cron);
    }

    public static class ActionExecutorContext implements ActionExecutor.Context {
        private WorkflowJobBean workflow;
        private Configuration protoConf;
        private WorkflowActionBean action;
        private boolean isRetry;
        private boolean started;
        private boolean ended;
        private boolean executed;

        public ActionExecutorContext(WorkflowJobBean workflow, WorkflowActionBean action, boolean isRetry) {
            this.workflow = workflow;
            this.action = action;
            this.isRetry = isRetry;
            try {
                protoConf = new XConfiguration(new StringReader(workflow.getProtoActionConf()));
            }
            catch (IOException ex) {
                throw new RuntimeException("It should not happen", ex);
            }
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

        public WorkflowAction getAction() {
            return action;
        }

        public ELEvaluator getELEvaluator() {
            ELEvaluator evaluator = Services.get().get(ELService.class).createEvaluator();
            DagELFunctions.configureEvaluator(evaluator, workflow, action);
            return evaluator;
        }

        public void setVar(String name, String value) {
            name = action.getName() + WorkflowInstance.NODE_VAR_SEPARATOR + name;
            workflow.getWorkflowInstance().setVar(name, value);
        }

        public String getVar(String name) {
            name = action.getName() + WorkflowInstance.NODE_VAR_SEPARATOR + name;
            return workflow.getWorkflowInstance().getVar(name);
        }

        public void setStartData(String externalId, String trackerUri, String consoleUrl) {
            action.setStartData(externalId, trackerUri, consoleUrl);
            started = true;
        }

        public void setExecutionData(String externalStatus, Properties actionData) {
            action.setExecutionData(externalStatus, actionData);
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

        public Path getActionDir() throws URISyntaxException, IOException{
            String name = getWorkflow().getId() + "/" + action.getName() + "--" + action.getType();
            FileSystem fs = getAppFileSystem();
            String actionDirPath = Services.get().getSystemId() + "/" + name;
            Path fqActionDir = new Path(fs.getHomeDirectory(), actionDirPath);
            return fqActionDir;
        }

        public FileSystem getAppFileSystem() throws IOException, URISyntaxException{
            WorkflowJob workflow = getWorkflow();
            XConfiguration jobConf = new XConfiguration(new StringReader(workflow.getConf()));
            Configuration fsConf = new Configuration();
            XConfiguration.copy(jobConf, fsConf);
            return Services.get().get(HadoopAccessorService.class).
                    createFileSystem(workflow.getUser(), workflow.getGroup(), new URI(getWorkflow().getAppPath()),
                                     fsConf);
        }
    }
}