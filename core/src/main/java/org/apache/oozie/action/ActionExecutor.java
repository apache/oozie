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

package org.apache.oozie.action;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.Services;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.LinkedHashMap;

/**
 * Base action executor class. <p/> All the action executors should extend this class.
 */
public abstract class ActionExecutor {

    /**
     * Configuration prefix for action executor (sub-classes) properties.
     */
	public static final String CONF_PREFIX = "oozie.action.";

    public static final String MAX_RETRIES = CONF_PREFIX + "retries.max";

    public static final String ACTION_RETRY_INTERVAL = CONF_PREFIX + "retry.interval";

    public static final String ACTION_RETRY_POLICY = CONF_PREFIX + "retry.policy";

    /**
     * Error code used by {@link #convertException} when there is not register error information for an exception.
     */
    public static final String ERROR_OTHER = "OTHER";
    
    public boolean requiresNNJT = false;

    public static enum RETRYPOLICY {
        EXPONENTIAL, PERIODIC
    }

    private static class ErrorInfo {
        ActionExecutorException.ErrorType errorType;
        String errorCode;
        Class<?> errorClass;

        private ErrorInfo(ActionExecutorException.ErrorType errorType, String errorCode, Class<?> errorClass) {
            this.errorType = errorType;
            this.errorCode = errorCode;
            this.errorClass = errorClass;
        }
    }

    private static boolean initMode = false;
    private static Map<String, Map<String, ErrorInfo>> ERROR_INFOS = new HashMap<String, Map<String, ErrorInfo>>();

    /**
     * Context information passed to the ActionExecutor methods.
     */
    public interface Context {

        /**
         * Create the callback URL for the action.
         *
         * @param externalStatusVar variable for the caller to inject the external status.
         * @return the callback URL.
         */
        public String getCallbackUrl(String externalStatusVar);

        /**
         * Return a proto configuration for actions with auth properties already set.
         *
         * @return a proto configuration for actions with auth properties already set.
         */
        public Configuration getProtoActionConf();

        /**
         * Return the workflow job.
         *
         * @return the workflow job.
         */
        public WorkflowJob getWorkflow();

        /**
         * Return an ELEvaluator with the context injected.
         *
         * @return configured ELEvaluator.
         */
        public ELEvaluator getELEvaluator();

        /**
         * Set a workflow action variable. <p/> Convenience method that prefixes the variable name with the action name
         * plus a '.'.
         *
         * @param name variable name.
         * @param value variable value, <code>null</code> removes the variable.
         */
        public void setVar(String name, String value);

        /**
         * Get a workflow action variable. <p/> Convenience method that prefixes the variable name with the action name
         * plus a '.'.
         *
         * @param name variable name.
         * @return the variable value, <code>null</code> if not set.
         */
        public String getVar(String name);

        /**
         * Set the action tracking information for an successfully started action.
         *
         * @param externalId the action external ID.
         * @param trackerUri the action tracker URI.
         * @param consoleUrl the action console URL.
         */
        void setStartData(String externalId, String trackerUri, String consoleUrl);

        /**
         * Set the action execution completion information for an action. The action status is set to {@link
         * org.apache.oozie.client.WorkflowAction.Status#DONE}
         *
         * @param externalStatus the action external end status.
         * @param actionData the action data on completion, <code>null</code> if none.
         */
        void setExecutionData(String externalStatus, Properties actionData);

        /**
         * Set execution statistics information for a particular action. The action status is set to {@link
         * org.apache.oozie.client.WorkflowAction.Status#DONE}
         *
         * @param jsonStats the JSON string representation of the stats.
         */
        void setExecutionStats(String jsonStats);

        /**
         * Set external child IDs for a particular action (Eg: pig). The action status is set to {@link
         * org.apache.oozie.client.WorkflowAction.Status#DONE}
         *
         * @param externalChildIDs the external child IDs as a comma-delimited string.
         */
        void setExternalChildIDs(String externalChildIDs);

        /**
         * Set the action end completion information for a completed action.
         *
         * @param status the action end status, it can be {@link org.apache.oozie.client.WorkflowAction.Status#OK} or
         * {@link org.apache.oozie.client.WorkflowAction.Status#ERROR}.
         * @param signalValue the action external end status.
         */
        void setEndData(WorkflowAction.Status status, String signalValue);

        /**
         * Return if the executor invocation is a retry or not.
         *
         * @return if the executor invocation is a retry or not.
         */
        boolean isRetry();

        /**
         * Sets the external status for the action in context.
         *
         * @param externalStatus the external status.
         */
        void setExternalStatus(String externalStatus);

        /**
         * Get the Action Recovery ID.
         *
         * @return recovery ID.
         */
        String getRecoveryId();

        /*
         * @return the path that will be used to store action specific data
         * @throws IOException @throws URISyntaxException @throws HadoopAccessorException
         */
        public Path getActionDir() throws HadoopAccessorException, IOException, URISyntaxException;

        /**
         * @return filesystem handle for the application deployment fs.
         * @throws IOException
         * @throws URISyntaxException
         * @throws HadoopAccessorException
         */
        public FileSystem getAppFileSystem() throws HadoopAccessorException, IOException, URISyntaxException;

        public void setErrorInfo(String str, String exMsg);
    }


    /**
     * Define the default inteval in seconds between retries.
     */
    public static final long RETRY_INTERVAL = 60;

    private String type;
    private int maxRetries;
    private long retryInterval;
    private RETRYPOLICY retryPolicy;

    /**
     * Create an action executor with default retry parameters.
     *
     * @param type action executor type.
     */
    protected ActionExecutor(String type) {
        this(type, RETRY_INTERVAL);
    }

    /**
     * Create an action executor.
     *
     * @param type action executor type.
     * @param defaultRetryInterval retry interval, in seconds.
     */
    protected ActionExecutor(String type, long defaultRetryInterval) {
        this.type = ParamChecker.notEmpty(type, "type");
        this.maxRetries = ConfigurationService.getInt(MAX_RETRIES);
        int retryInterval = ConfigurationService.getInt(ACTION_RETRY_INTERVAL);
        this.retryInterval = retryInterval > 0 ? retryInterval : defaultRetryInterval;
        this.retryPolicy = getRetryPolicyFromConf();
    }

    private RETRYPOLICY getRetryPolicyFromConf() {
        String retryPolicy = ConfigurationService.get(ACTION_RETRY_POLICY);
        if (StringUtils.isBlank(retryPolicy)) {
            return RETRYPOLICY.PERIODIC;
        } else {
            try {
                return RETRYPOLICY.valueOf(retryPolicy.toUpperCase().trim());
            } catch (IllegalArgumentException e) {
                return RETRYPOLICY.PERIODIC;
            }
        }
    }

    /**
     * Clear all init settings for all action types.
     */
    public static void resetInitInfo() {
        if (!initMode) {
            throw new IllegalStateException("Error, action type info locked");
        }
        ERROR_INFOS.clear();
    }

    /**
     * Enable action type initialization.
     */
    public static void enableInit() {
        initMode = true;
    }

    /**
     * Disable action type initialization.
     */
    public static void disableInit() {
        initMode = false;
    }

    /**
     * Invoked once at system initialization time. <p/> It can be used to register error information for the expected
     * exceptions. Exceptions should be register from subclasses to superclasses to ensure proper detection, same thing
     * that it is done in a normal catch. <p/> This method should invoke the {@link #registerError} method to register
     * all its possible errors. <p/> Subclasses overriding must invoke super.
     */
    public void initActionType() {
        XLog.getLog(getClass()).trace(" Init Action Type : [{0}]", getType());
        ERROR_INFOS.put(getType(), new LinkedHashMap<String, ErrorInfo>());
    }

    /**
     * Return the system ID, this ID is defined in Oozie configuration.
     *
     * @return the system ID.
     */
    public String getOozieSystemId() {
        return Services.get().getSystemId();
    }

    /**
     * Return the runtime directory of the Oozie instance. <p/> The directory is created under TMP and it is always a
     * new directory per system initialization.
     *
     * @return the runtime directory of the Oozie instance.
     */
    public String getOozieRuntimeDir() {
        return Services.get().getRuntimeDir();
    }

    /**
     * Return Oozie configuration. <p/> This is useful for actions that need access to configuration properties.
     *
     * @return Oozie configuration.
     */
    public Configuration getOozieConf() {
        return Services.get().getConf();
    }

    /**
     * Register error handling information for an exception.
     *
     * @param exClass excpetion class name (to work in case of a particular exception not being in the classpath, needed
     * to be able to handle multiple version of Hadoop  or other JARs used by executors with the same codebase).
     * @param errorType error type for the exception.
     * @param errorCode error code for the exception.
     */
    protected void registerError(String exClass, ActionExecutorException.ErrorType errorType, String errorCode) {
        if (!initMode) {
            throw new IllegalStateException("Error, action type info locked");
        }
        try {
            Class errorClass = Thread.currentThread().getContextClassLoader().loadClass(exClass);
            Map<String, ErrorInfo> executorErrorInfo = ERROR_INFOS.get(getType());
            executorErrorInfo.put(exClass, new ErrorInfo(errorType, errorCode, errorClass));
        }
        catch (ClassNotFoundException cnfe) {
            XLog.getLog(getClass()).warn(
                    "Exception [{0}] not in classpath, ActionExecutor [{1}] will handle it as ERROR", exClass,
                    getType());
        }
        catch (java.lang.NoClassDefFoundError err) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            err.printStackTrace(new PrintStream(baos));
            XLog.getLog(getClass()).warn(baos.toString());
        }
    }

    /**
     * Return the action executor type.
     *
     * @return the action executor type.
     */
    public String getType() {
        return type;
    }

    /**
     * Return the maximum number of retries for the action executor.
     *
     * @return the maximum number of retries for the action executor.
     */
    public int getMaxRetries() {
        return maxRetries;
    }

    /**
     * Set the maximum number of retries for the action executor.
     *
     * @param maxRetries the maximum number of retries.
     */
    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    /**
     * Return the retry policy for the action executor.
     *
     * @return the retry policy for the action executor.
     */
    public RETRYPOLICY getRetryPolicy() {
        return retryPolicy;
    }

    /**
     * Sets the retry policy for the action executor.
     *
     * @param retryPolicy retry policy for the action executor.
     */
    public void setRetryPolicy(RETRYPOLICY retryPolicy) {
        this.retryPolicy = retryPolicy;
    }

    /**
     * Return the retry interval for the action executor in seconds.
     *
     * @return the retry interval for the action executor in seconds.
     */
    public long getRetryInterval() {
        return retryInterval;
    }

    /**
     * Sets the retry interval for the action executor.
     *
     * @param retryInterval retry interval in seconds.
     */
    public void setRetryInterval(long retryInterval) {
        this.retryInterval = retryInterval;
    }

    /**
     * Utility method to handle exceptions in the {@link #start}, {@link #end}, {@link #kill} and {@link #check} methods
     * <p/> It uses the error registry to convert exceptions to {@link ActionExecutorException}s.
     *
     * @param ex exception to convert.
     * @return ActionExecutorException converted exception.
     */
    @SuppressWarnings({"ThrowableInstanceNeverThrown"})
    protected ActionExecutorException convertException(Exception ex) {
        if (ex instanceof ActionExecutorException) {
            return (ActionExecutorException) ex;
        }

        ActionExecutorException aee = null;
        // Check the cause of the exception first
        if (ex.getCause() != null) {
            aee = convertExceptionHelper(ex.getCause());
        }
        // If the cause isn't registered or doesn't exist, check the exception itself
        if (aee == null) {
            aee = convertExceptionHelper(ex);
            // If the cause isn't registered either, then just create a new ActionExecutorException
            if (aee == null) {
                String exClass = ex.getClass().getName();
                String errorCode = exClass.substring(exClass.lastIndexOf(".") + 1);
                aee = new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, errorCode, "{0}", ex.getMessage(), ex);
            }
        }
        return aee;
    }

    private ActionExecutorException convertExceptionHelper(Throwable ex) {
        Map<String, ErrorInfo> executorErrorInfo = ERROR_INFOS.get(getType());
        // Check if we have registered ex
        ErrorInfo classErrorInfo = executorErrorInfo.get(ex.getClass().getName());
        if (classErrorInfo != null) {
            return new ActionExecutorException(classErrorInfo.errorType, classErrorInfo.errorCode, "{0}", ex.getMessage(), ex);
        }
        // Else, check if a parent class of ex is registered
        else {
            for (ErrorInfo errorInfo : executorErrorInfo.values()) {
                if (errorInfo.errorClass.isInstance(ex)) {
                    return new ActionExecutorException(errorInfo.errorType, errorInfo.errorCode, "{0}", ex.getMessage(), ex);
                }
            }
        }
        return null;
    }

    /**
     * Convenience method that return the signal for an Action based on the action status.
     *
     * @param status action status.
     * @return the action signal.
     */
    protected String getActionSignal(WorkflowAction.Status status) {
        switch (status) {
            case OK:
                return "OK";
            case ERROR:
            case KILLED:
                return "ERROR";
            default:
                throw new IllegalArgumentException("Action status for signal can only be OK or ERROR");
        }
    }

    /**
     * Return the path that will be used to store action specific data
     *
     * @param jobId Worfklow ID
     * @param action Action
     * @param key An Identifier
     * @param temp temp directory flag
     * @return A string that has the path
     */
    protected String getActionDirPath(String jobId, WorkflowAction action, String key, boolean temp) {
        String name = jobId + "/" + action.getName() + "--" + key;
        if (temp) {
            name += ".temp";
        }
        return getOozieSystemId() + "/" + name;
    }

    /**
     * Return the path that will be used to store action specific data.
     *
     * @param jobId Workflow ID
     * @param action Action
     * @param key An identifier
     * @param temp Temp directory flag
     * @return Path to the directory
     */
    public Path getActionDir(String jobId, WorkflowAction action, String key, boolean temp) {
        return new Path(getActionDirPath(jobId, action, key, temp));
    }

    /**
     * Start an action. <p/> The {@link Context#setStartData} method must be called within this method. <p/> If the
     * action has completed, the {@link Context#setExecutionData} method must be called within this method.
     *
     * @param context executor context.
     * @param action the action to start.
     * @throws ActionExecutorException thrown if the action could not start.
     */
    public abstract void start(Context context, WorkflowAction action) throws ActionExecutorException;

    /**
     * End an action after it has executed. <p/> The {@link Context#setEndData} method must be called within this
     * method.
     *
     * @param context executor context.
     * @param action the action to end.
     * @throws ActionExecutorException thrown if the action could not end.
     */
    public abstract void end(Context context, WorkflowAction action) throws ActionExecutorException;

    /**
     * Check if an action has completed. This method must be implemented by Async Action Executors. <p/> If the action
     * has completed, the {@link Context#setExecutionData} method must be called within this method. <p/> If the action
     * has not completed, the {@link Context#setExternalStatus} method must be called within this method.
     *
     * @param context executor context.
     * @param action the action to end.
     * @throws ActionExecutorException thrown if the action could not be checked.
     */
    public abstract void check(Context context, WorkflowAction action) throws ActionExecutorException;

    /**
     * Kill an action. <p/> The {@link Context#setEndData} method must be called within this method.
     *
     * @param context executor context.
     * @param action the action to kill.
     * @throws ActionExecutorException thrown if the action could not be killed.
     */
    public abstract void kill(Context context, WorkflowAction action) throws ActionExecutorException;

    /**
     * Return if the external status indicates that the action has completed.
     *
     * @param externalStatus external status to check.
     * @return if the external status indicates that the action has completed.
     */
    public abstract boolean isCompleted(String externalStatus);

}
