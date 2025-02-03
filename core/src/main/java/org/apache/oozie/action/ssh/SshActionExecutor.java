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

package org.apache.oozie.action.ssh;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.hadoop.util.StringUtils;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowAction.Status;
import org.apache.oozie.service.CallbackService;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.Services;
import org.apache.oozie.servlet.CallbackServlet;
import org.apache.oozie.util.BufferDrainer;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.PropertiesUtils;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.Namespace;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Ssh action executor. <ul> <li>Execute the shell commands on the remote host</li> <li>Copies the base and wrapper
 * scripts on to the remote location</li> <li>Base script is used to run the command on the remote host</li> <li>Wrapper
 * script is used to check the status of the submitted command</li> <li>handles the submission failures</li> </ul>
 */
public class SshActionExecutor extends ActionExecutor {
    public static final String ACTION_TYPE = "ssh";

    /**
     * Configuration parameter which specifies whether the specified ssh user is allowed, or has to be the job user.
     */
    public static final String CONF_SSH_ALLOW_USER_AT_HOST = CONF_PREFIX + "ssh.allow.user.at.host";

    protected static final String SSH_COMMAND_OPTIONS =
            "-o PasswordAuthentication=no -o KbdInteractiveDevices=no -o StrictHostKeyChecking=no -o ConnectTimeout=20 ";

    protected static final String SSH_COMMAND_BASE = "ssh " + SSH_COMMAND_OPTIONS;
    protected static final String SCP_COMMAND_BASE = "scp " + SSH_COMMAND_OPTIONS;

    public static final String ERR_SETUP_FAILED = "SETUP_FAILED";
    public static final String ERR_EXECUTION_FAILED = "EXECUTION_FAILED";
    public static final String ERR_UNKNOWN_ERROR = "UNKNOWN_ERROR";
    public static final String ERR_COULD_NOT_CONNECT = "COULD_NOT_CONNECT";
    public static final String ERR_HOST_RESOLUTION = "COULD_NOT_RESOLVE_HOST";
    public static final String ERR_FNF = "FNF";
    public static final String ERR_AUTH_FAILED = "AUTH_FAILED";
    public static final String ERR_NO_EXEC_PERM = "NO_EXEC_PERM";
    public static final String ERR_USER_MISMATCH = "ERR_USER_MISMATCH";
    public static final String ERR_EXCEDE_LEN = "ERR_OUTPUT_EXCEED_MAX_LEN";

    public static final String DELETE_TMP_DIR = "oozie.action.ssh.delete.remote.tmp.dir";

    public static final String HTTP_COMMAND = "oozie.action.ssh.http.command";

    public static final String HTTP_COMMAND_OPTIONS = "oozie.action.ssh.http.command.post.options";

    public static final String CHECK_MAX_RETRIES = "oozie.action.ssh.check.retries.max";

    public static final String CHECK_INITIAL_RETRY_WAIT_TIME = "oozie.action.ssh.check.initial.retry.wait.time";

    private static final String EXT_STATUS_VAR = "#status";

    private static final int SSH_CONNECT_ERROR_CODE = 255;

    private static int maxLen;
    private static boolean allowSshUserAtHost;

    private final XLog LOG = XLog.getLog(getClass())
;
    protected SshActionExecutor() {
        super(ACTION_TYPE);
    }

    /**
     * Initialize Action.
     */
    @Override
    public void initActionType() {
        super.initActionType();
        maxLen = getOozieConf().getInt(CallbackServlet.CONF_MAX_DATA_LEN, 2 * 1024);
        allowSshUserAtHost = ConfigurationService.getBoolean(CONF_SSH_ALLOW_USER_AT_HOST);
        registerError(InterruptedException.class.getName(), ActionExecutorException.ErrorType.ERROR, "SH001");
        registerError(JDOMException.class.getName(), ActionExecutorException.ErrorType.ERROR, "SH002");
        initSshScripts();
    }

    /**
     * Check ssh action status.
     *
     * @param context action execution context.
     * @param action action object.
     * @throws org.apache.oozie.action.ActionExecutorException in case if action cannot be executed
     */
    @SuppressFBWarnings(value = {"COMMAND_INJECTION", "PATH_TRAVERSAL_OUT"},
            justification = "Tracker URI is specified in the WF action, and action dir path is from context")
    @Override
    public void check(Context context, WorkflowAction action) throws ActionExecutorException {
        LOG.trace("check() start for action={0}", action.getId());
        Status status = getActionStatus(context, action);
        boolean captureOutput;
        try {
            Element eConf = XmlUtils.parseXml(action.getConf());
            Namespace ns = eConf.getNamespace();
            captureOutput = eConf.getChild("capture-output", ns) != null;
        }
        catch (JDOMException ex) {
            throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "ERR_XML_PARSE_FAILED",
                                              "unknown error", ex);
        }
        LOG.debug("Capture Output: {0}", captureOutput);
        if (status == Status.OK) {
            if (captureOutput) {
                String outFile = getRemoteFileName(context, action, "stdout", false, true);
                String dataCommand = SSH_COMMAND_BASE + action.getTrackerUri() + " cat " + outFile;
                LOG.debug("Ssh command [{0}]", dataCommand);
                try {
                    final StringBuffer outBuffer = getActionOutputMessage(dataCommand);
                    context.setExecutionData(status.toString(), PropertiesUtils.stringToProperties(outBuffer.toString()));
                    LOG.trace("Execution data set. status={0}, properties={1}", status,
                            PropertiesUtils.stringToProperties(outBuffer.toString()));
                }
                catch (Exception ex) {
                    throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "ERR_UNKNOWN_ERROR",
                                                      "unknown error", ex);
                }
            }
            else {
                LOG.trace("Execution data set to null. status={0}", status);
                context.setExecutionData(status.toString(), null);
            }
        }
        else {
            if (status == Status.ERROR) {
                LOG.warn("Execution data set to null in ERROR");
                context.setExecutionData(status.toString(), null);
                String actionErrorMsg = getActionErrorMessage(context, action);
                LOG.warn("{0}: Script failed on remote host with [{1}]", ErrorCode.E1111, actionErrorMsg);
                context.setErrorInfo(ErrorCode.E1111.toString(), actionErrorMsg);
            }
            else {
                LOG.warn("Execution data not set");
                context.setExternalStatus(status.toString());
            }
        }
        LOG.trace("check() end for action={0}", action);
    }

    private StringBuffer getActionOutputMessage(String dataCommand) throws IOException, ActionExecutorException {
        final Process process = Runtime.getRuntime().exec(dataCommand.split("\\s"));
        boolean overflow = false;
        final BufferDrainer bufferDrainer = new BufferDrainer(process, maxLen);
        bufferDrainer.drainBuffers();
        final StringBuffer outBuffer = bufferDrainer.getInputBuffer();
        final StringBuffer errBuffer = bufferDrainer.getErrorBuffer();
        LOG.debug("outBuffer={0}", outBuffer);
        LOG.debug("errBuffer={0}", errBuffer);
        if (outBuffer.length() > maxLen) {
            overflow = true;
        }
        if (overflow) {
            throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR,
                    "ERR_OUTPUT_EXCEED_MAX_LEN", "unknown error");
        }
        return outBuffer;
    }

    private String getActionErrorMessage(Context context, WorkflowAction action) throws ActionExecutorException {
        String outFile = getRemoteFileName(context, action, "error", false, true);
        String errorMsgCmd = SSH_COMMAND_BASE + action.getTrackerUri() + " cat " + outFile;
        LOG.debug("Get error message command: [{0}]", errorMsgCmd);
        String errorMessage;
        try {
            final StringBuffer outBuffer = getActionOutputMessage(errorMsgCmd);
            errorMessage = outBuffer.toString().replaceAll("\n", "");
        } catch (Exception ex) {
            throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "ERR_UNKNOWN_ERROR",
                    "unknown error", ex);
        }
        return errorMessage;
    }

    /**
     * Kill ssh action.
     *
     * @param context action execution context.
     * @param action object.
     * @throws org.apache.oozie.action.ActionExecutorException in case if action cannot be executed
     */
    @Override
    public void kill(Context context, WorkflowAction action) throws ActionExecutorException {
        LOG.info("Killing action");
        String command = "ssh " + action.getTrackerUri() + " kill --  -$(ps -o pgid= " + action.getExternalId() + " | grep -o [0-9]*)";
        int returnValue = getReturnValue(command);
        if (returnValue != 0) {
            throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "FAILED_TO_KILL", XLog.format(
                    "Unable to kill process {0} on {1}", action.getExternalId(), action.getTrackerUri()));
        }
        context.setEndData(WorkflowAction.Status.KILLED, "ERROR");
    }

    /**
     * Start the ssh action execution.
     *
     * @param context action execution context.
     * @param action action object.
     * @throws org.apache.oozie.action.ActionExecutorException in case if action cannot be executed
     */
    @SuppressWarnings("unchecked")
    @Override
    public void start(final Context context, final WorkflowAction action) throws ActionExecutorException {
        LOG.info("Starting action");
        String confStr = action.getConf();
        Element conf;
        try {
            conf = XmlUtils.parseXml(confStr);
        }
        catch (Exception ex) {
            throw convertException(ex);
        }
        Namespace nameSpace = conf.getNamespace();
        Element hostElement = conf.getChild("host", nameSpace);
        String hostString = hostElement.getValue().trim();
        hostString = prepareUserHost(hostString, context);
        final String host = hostString;
        final String dirLocation = execute(new Callable<String>() {
            public String call() throws Exception {
                return setupRemote(host, context, action);
            }
        });

        String runningPid = execute(new Callable<String>() {
            public String call() throws Exception {
                return checkIfRunning(host, context, action);
            }
        });
        String pid = "";

        LOG.trace("runningPid={0}", runningPid);

        if (runningPid == null) {
            final Element commandElement = conf.getChild("command", nameSpace);
            final boolean ignoreOutput = conf.getChild("capture-output", nameSpace) == null;

            boolean preserve = false;
            if (commandElement != null) {
                String[] args = null;
                // Will either have <args>, <arg>, or neither (but not both)
                List<Element> argsList = conf.getChildren("args", nameSpace);
                // Arguments in an <args> are "flattened" (spaces are delimiters)
                if (argsList != null && argsList.size() > 0) {
                    StringBuilder argsString = new StringBuilder("");
                    for (Element argsElement : argsList) {
                        argsString = argsString.append(argsElement.getValue()).append(" ");
                    }
                    args = new String[]{argsString.toString()};
                }
                else {
                    // Arguments in an <arg> are preserved, even with spaces
                    argsList = conf.getChildren("arg", nameSpace);
                    if (argsList != null && argsList.size() > 0) {
                        preserve = true;
                        args = new String[argsList.size()];
                        for (int i = 0; i < argsList.size(); i++) {
                            Element argsElement = argsList.get(i);
                            args[i] = argsElement.getValue();
                            // Even though we're keeping the args as an array, if they contain a space we still have to either quote
                            // them or escape their space (because the scripts will split them up otherwise)
                            if (args[i].contains(" ") &&
                                    !(args[i].startsWith("\"") && args[i].endsWith("\"") ||
                                      args[i].startsWith("'") && args[i].endsWith("'"))) {
                                args[i] = StringUtils.escapeString(args[i], '\\', ' ');
                            }
                        }
                    }
                }
                final String[] argsF = args;
                final String recoveryId = context.getRecoveryId();
                final boolean preserveF = preserve;
                pid = execute(new Callable<String>() {

                    @Override
                    public String call() throws Exception {
                        return doExecute(host, dirLocation, commandElement.getValue(), argsF, ignoreOutput, action, recoveryId,
                                preserveF);
                    }
                });
            }
            context.setStartData(pid, host, host);
        }
        else {
            pid = runningPid;
            context.setStartData(pid, host, host);
            check(context, action);
        }
    }

    private String checkIfRunning(String host, final Context context, final WorkflowAction action) {
        String outFile = getRemoteFileName(context, action, "pid", false, false);
        String getOutputCmd = SSH_COMMAND_BASE + host + " cat " + outFile;
        try {
            final Process process = Runtime.getRuntime().exec(getOutputCmd.split("\\s"));
            final BufferDrainer bufferDrainer = new BufferDrainer(process, maxLen);
            bufferDrainer.drainBuffers();
            final StringBuffer buffer = bufferDrainer.getInputBuffer();
            String pid = getFirstLine(buffer);
            if (Long.valueOf(pid) > 0) {
                return pid;
            }
            else {
                return null;
            }
        }
        catch (Exception e) {
            return null;
        }
    }

    /**
     * Get remote host working location.
     *
     * @param context action execution context
     * @param action Action
     * @param fileExtension Extension to be added to file name
     * @param dirOnly Get the Directory only
     * @param useExtId Flag to use external ID in the path
     * @return remote host file name/Directory.
     */
    public String getRemoteFileName(Context context, WorkflowAction action, String fileExtension, boolean dirOnly,
                                    boolean useExtId) {
        String path = getActionDirPath(context.getWorkflow().getId(), action, ACTION_TYPE, false) + "/";
        if (dirOnly) {
            return path;
        }
        if (useExtId) {
            path = path + action.getExternalId() + ".";
        }
        path = path + context.getRecoveryId() + "." + fileExtension;
        return path;
    }

    /**
     * Utility method to execute command.
     *
     * @param command Command to execute as String.
     * @return exit status of the execution.
     * @throws IOException if processSettings exits with status nonzero.
     * @throws InterruptedException if processSettings does not run properly.
     */
    public int executeCommand(String command) throws IOException, InterruptedException {
        Runtime runtime = Runtime.getRuntime();
        Process p = runtime.exec(command.split("\\s"));

        final BufferDrainer bufferDrainer = new BufferDrainer(p, maxLen);
        final int exitValue = bufferDrainer.drainBuffers();
        final StringBuffer errorBuffer = bufferDrainer.getErrorBuffer();

        if (exitValue != 0) {
            String error = getTruncatedString(errorBuffer);
            throw new IOException(XLog.format("Not able to perform operation [{0}]", command) + " | " + "ErrorStream: "
                    + error);
        }
        return exitValue;
    }

    /**
     * Do ssh action execution setup on remote host.
     *
     * @param host host name.
     * @param context action execution context.
     * @param action action object.
     * @return remote host working directory.
     * @throws IOException thrown if failed to setup.
     * @throws InterruptedException thrown if any interruption happens.
     */
    protected String setupRemote(String host, Context context, WorkflowAction action) throws IOException, InterruptedException {
        LOG.info("Attempting to copy ssh base scripts to remote host [{0}]", host);
        String localDirLocation = Services.get().getRuntimeDir() + "/ssh";
        if (localDirLocation.endsWith("/")) {
            localDirLocation = localDirLocation.substring(0, localDirLocation.length() - 1);
        }
        File file = new File(localDirLocation + "/ssh-base.sh");
        if (!file.exists()) {
            throw new IOException("Required Local file " + file.getAbsolutePath() + " not present.");
        }
        file = new File(localDirLocation + "/ssh-wrapper.sh");
        if (!file.exists()) {
            throw new IOException("Required Local file " + file.getAbsolutePath() + " not present.");
        }
        String remoteDirLocation = getRemoteFileName(context, action, null, true, true);
        String command = XLog.format("{0}{1}  mkdir -p {2} ", SSH_COMMAND_BASE, host, remoteDirLocation).toString();
        executeCommand(command);
        command = XLog.format("{0}{1}/ssh-base.sh {2}/ssh-wrapper.sh {3}:{4}", SCP_COMMAND_BASE, localDirLocation,
                              localDirLocation, host, remoteDirLocation);
        executeCommand(command);
        command = XLog.format("{0}{1}  chmod +x {2}ssh-base.sh {3}ssh-wrapper.sh ", SSH_COMMAND_BASE, host,
                              remoteDirLocation, remoteDirLocation);
        executeCommand(command);
        return remoteDirLocation;
    }

    /**
     * Execute the ssh command.
     *
     * @param host hostname.
     * @param dirLocation location of the base and wrapper scripts.
     * @param cmnd command to be executed.
     * @param args command arguments.
     * @param ignoreOutput ignore output option.
     * @param action action object.
     * @param recoveryId action id + run number to enable recovery in rerun
     * @param preserveArgs tell the ssh scripts to preserve or flatten the arguments
     * @return processSettings id of the running command.
     * @throws IOException thrown if failed to run the command.
     * @throws InterruptedException thrown if any interruption happens.
     */
    protected String doExecute(String host, String dirLocation, String cmnd, String[] args, boolean ignoreOutput,
                               WorkflowAction action, String recoveryId, boolean preserveArgs)
                               throws IOException, InterruptedException {
        Runtime runtime = Runtime.getRuntime();
        String callbackPost = ignoreOutput ? "_" : ConfigurationService.get(HTTP_COMMAND_OPTIONS).replace(" ", "%%%");
        String preserveArgsS = preserveArgs ? "PRESERVE_ARGS" : "FLATTEN_ARGS";
        // TODO check
        String callBackUrl = Services.get().get(CallbackService.class)
                .createCallBackUrl(action.getId(), EXT_STATUS_VAR);
        String command = XLog.format("{0}{1} {2}ssh-base.sh {3} {4} \"{5}\" \"{6}\" {7} {8} ", SSH_COMMAND_BASE, host, dirLocation,
                preserveArgsS, ConfigurationService.get(HTTP_COMMAND), callBackUrl, callbackPost, recoveryId, cmnd);
        String[] commandArray = command.split("\\s");
        String[] finalCommand;
        if (args == null) {
            finalCommand = commandArray;
        }
        else {
            finalCommand = new String[commandArray.length + args.length];
            System.arraycopy(commandArray, 0, finalCommand, 0, commandArray.length);
            System.arraycopy(args, 0, finalCommand, commandArray.length, args.length);
        }

        LOG.trace("Executing SSH command [finalCommand={0}]", Arrays.toString(finalCommand));
        final Process p = runtime.exec(finalCommand);

        BufferDrainer bufferDrainer = new BufferDrainer(p, maxLen);
        final int exitValue = bufferDrainer.drainBuffers();
        final StringBuffer inputBuffer = bufferDrainer.getInputBuffer();
        final StringBuffer errorBuffer = bufferDrainer.getErrorBuffer();
        final String pid = getFirstLine(inputBuffer);
        if (exitValue != 0) {
            String error = getTruncatedString(errorBuffer);
            throw new IOException(XLog.format("Not able to execute ssh-base.sh on {0}", host) + " | " + "ErrorStream: "
                    + error);
        }

        LOG.trace("After execution pid={0}", pid);

        return pid;
    }

    /**
     * End action execution.
     *
     * @param context action execution context.
     * @param action action object.
     * @throws ActionExecutorException thrown if action end execution fails.
     */
    public void end(final Context context, final WorkflowAction action) throws ActionExecutorException {
        if (action.getExternalStatus().equals("OK")) {
            context.setEndData(WorkflowAction.Status.OK, WorkflowAction.Status.OK.toString());
        }
        else {
            context.setEndData(WorkflowAction.Status.ERROR, WorkflowAction.Status.ERROR.toString());
        }
        boolean deleteTmpDir = ConfigurationService.getBoolean(DELETE_TMP_DIR);
        if (deleteTmpDir) {
            String tmpDir = getRemoteFileName(context, action, null, true, false);
            String removeTmpDirCmd = SSH_COMMAND_BASE + action.getTrackerUri() + " rm -rf " + tmpDir;
            int retVal = getReturnValue(removeTmpDirCmd);
            if (retVal != 0) {
                XLog.getLog(getClass()).warn("Cannot delete temp dir {0}", tmpDir);
            }
        }
        LOG.info("Action ended with external status [{0}]", action.getExternalStatus());
    }

    /**
     * Get the return value of a processSettings.
     *
     * @param command command to be executed.
     * @return zero if execution is successful and any non zero value for failure.
     * @throws ActionExecutorException
     */
    private int getReturnValue(String command) throws ActionExecutorException {
        LOG.trace("Getting return value for command={0}", command);

        int returnValue;
        Process ps = null;
        try {
            ps = Runtime.getRuntime().exec(command.split("\\s"));
            final BufferDrainer bufferDrainer = new BufferDrainer(ps, 0);
            returnValue = bufferDrainer.drainBuffers();
        }
        catch (IOException e) {
            throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "FAILED_OPERATION", XLog.format(
                    "Not able to perform operation {0}", command), e);
        }
        finally {
            ps.destroy();
        }

        LOG.trace("returnValue={0}", returnValue);

        return returnValue;
    }

    /**
     * Copy the ssh base and wrapper scripts to the local directory.
     */
    @SuppressFBWarnings(value ="PATH_TRAVERSAL_OUT", justification = "Path is created runtime")
    private void initSshScripts() {
        String dirLocation = Services.get().getRuntimeDir() + "/ssh";
        File path = new File(dirLocation);
        path.mkdirs();
        if (!path.exists()) {
            throw new RuntimeException(XLog.format("Not able to create required directory {0}", dirLocation));
        }
        try {
            IOUtils.copyCharStream(IOUtils.getResourceAsReader("ssh-base.sh", -1), new OutputStreamWriter(
                    new FileOutputStream(dirLocation + "/ssh-base.sh"), StandardCharsets.UTF_8));
            IOUtils.copyCharStream(IOUtils.getResourceAsReader("ssh-wrapper.sh", -1), new OutputStreamWriter(
                    new FileOutputStream(dirLocation + "/ssh-wrapper.sh"), StandardCharsets.UTF_8));
        }
        catch (IOException ie) {
            throw new RuntimeException(XLog.format("Not able to copy required scripts file to {0} "
                    + "for SshActionHandler", dirLocation));
        }
    }

    /**
     * Get action status.
     *
     * @param context executor context
     * @param action action object.
     * @return status of the action(RUNNING/OK/ERROR).
     * @throws ActionExecutorException thrown if there is any error in getting status.
     */
    protected Status getActionStatus(Context context, WorkflowAction action) throws ActionExecutorException {
        String command = SSH_COMMAND_BASE + action.getTrackerUri() + " ps -p " + action.getExternalId();
        Status aStatus;
        int returnValue = getReturnValue(command);
        if (returnValue == SSH_CONNECT_ERROR_CODE) {
            int maxRetryCount = ConfigurationService.getInt(CHECK_MAX_RETRIES, 3);
            long waitTime = ConfigurationService.getLong(CHECK_INITIAL_RETRY_WAIT_TIME, 3000);
            for (int retries = 1; retries <= maxRetryCount; retries++) {
                waitTime = handleRetry(waitTime, retries);
                returnValue = getReturnValue(command);
                if (returnValue != SSH_CONNECT_ERROR_CODE) {
                    break;
                }
            }
            if (returnValue == SSH_CONNECT_ERROR_CODE) {
                throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED, ERR_COULD_NOT_CONNECT,
                        "Failed to connect to host [" + action.getTrackerUri() + "] for ssh action status check.");
            }
        }
        if (returnValue == 0) {
            aStatus = Status.RUNNING;
        }
        else {
            if (checkSSHActionFileExistence(context, action, "error")) {
                aStatus = Status.ERROR;
            }
            else {
                if (checkSSHActionFileExistence(context, action, "success")) {
                    aStatus = Status.OK;
                } else {
                    aStatus = Status.ERROR;
                }
            }
        }
        return aStatus;
    }

    private boolean checkSSHActionFileExistence(final Context context, final WorkflowAction action,
            String fileExtension) throws ActionExecutorException {
        String outFile = getRemoteFileName(context, action, fileExtension, false, true);
        String checkCmd = SSH_COMMAND_BASE + action.getTrackerUri() + " ls " + outFile;
        int retVal = getReturnValue(checkCmd);
        return retVal == 0 ? true : false;
    }

    private long handleRetry(long sleepBeforeRetryMs, final int retries) {
        LOG.warn("failed to check ssh action status, sleeping {0} milliseconds before retry #{1}", sleepBeforeRetryMs,
                retries);
        try {
            Thread.sleep(sleepBeforeRetryMs);
        } catch (InterruptedException e) {
            LOG.error("ssh action status check retry get interrupted during wait, caused by {0}", e.getMessage());
        }
        sleepBeforeRetryMs *= 2;
        return sleepBeforeRetryMs;
    }

    /**
     * Execute the callable.
     *
     * @param callable required callable.
     * @throws ActionExecutorException thrown if there is any error in command execution.
     */
    private <T> T execute(Callable<T> callable) throws ActionExecutorException {
        XLog log = XLog.getLog(getClass());
        try {
            return callable.call();
        }
        catch (IOException ex) {
            log.warn("Error while executing ssh EXECUTION");
            String errorMessage = ex.getMessage();
            if (null == errorMessage) { // Unknown IOException
                throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, ERR_UNKNOWN_ERROR, ex
                        .getMessage(), ex);
            } // Host Resolution Issues
            else {
                if (errorMessage.contains("Could not resolve hostname") ||
                        errorMessage.contains("service not known")) {
                    throw new ActionExecutorException(ActionExecutorException.ErrorType.TRANSIENT, ERR_HOST_RESOLUTION, ex
                            .getMessage(), ex);
                } // Connection Timeout. Host temporarily down.
                else {
                    if (errorMessage.contains("timed out")) {
                        throw new ActionExecutorException(ActionExecutorException.ErrorType.TRANSIENT, ERR_COULD_NOT_CONNECT,
                                                          ex.getMessage(), ex);
                    }// Local ssh-base or ssh-wrapper missing
                    else {
                        if (errorMessage.contains("Required Local file")) {
                            throw new ActionExecutorException(ActionExecutorException.ErrorType.TRANSIENT, ERR_FNF,
                                                              ex.getMessage(), ex); // local_FNF
                        }// Required oozie bash scripts missing, after the copy was
                        // successful
                        else {
                            if (errorMessage.contains("No such file or directory")
                                    && (errorMessage.contains("ssh-base") || errorMessage.contains("ssh-wrapper"))) {
                                throw new ActionExecutorException(ActionExecutorException.ErrorType.TRANSIENT, ERR_FNF,
                                                                  ex.getMessage(), ex); // remote
                                // FNF
                            } // Required application execution binary missing (either
                            // caught by ssh-wrapper
                            else {
                                if (errorMessage.contains("command not found")) {
                                    throw new ActionExecutorException(ActionExecutorException.ErrorType.NON_TRANSIENT, ERR_FNF, ex
                                            .getMessage(), ex); // remote
                                    // FNF
                                } // Permission denied while connecting
                                else {
                                    if (errorMessage.contains("Permission denied")) {
                                        throw new ActionExecutorException(ActionExecutorException.ErrorType.NON_TRANSIENT,
                                                ERR_AUTH_FAILED, ex.getMessage(), ex);
                                    } // Permission denied while executing
                                    else {
                                        if (errorMessage.contains(": Permission denied")) {
                                            throw new ActionExecutorException(ActionExecutorException.ErrorType.NON_TRANSIENT,
                                                    ERR_NO_EXEC_PERM, ex.getMessage(), ex);
                                        }
                                        else {
                                            throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR,
                                                    ERR_UNKNOWN_ERROR, ex.getMessage(), ex);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } // Any other type of exception
        catch (Exception ex) {
            throw convertException(ex);
        }
    }

    /**
     * Checks whether the system is configured to always use the oozie user for ssh, and injects the user if required.
     *
     * @param host the host string.
     * @param context the execution context.
     * @return the modified host string with a user parameter added on if required.
     * @throws ActionExecutorException in case the flag to use the oozie user is turned on and there is a mismatch
     * between the user specified in the host and the oozie user.
     */
    private String prepareUserHost(String host, Context context) throws ActionExecutorException {
        String oozieUser = context.getProtoActionConf().get(OozieClient.USER_NAME);
        if (allowSshUserAtHost) {
            if (!host.contains("@")) {
                host = oozieUser + "@" + host;
            }
        }
        else {
            if (host.contains("@")) {
                if (!host.toLowerCase().startsWith(oozieUser + "@")) {
                    throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, ERR_USER_MISMATCH,
                                                      XLog.format("user mismatch between oozie user [{0}] and ssh host [{1}]",
                                                              oozieUser, host));
                }
            }
            else {
                host = oozieUser + "@" + host;
            }
        }

        LOG.trace("User host is {0}", host);

        return host;
    }

    @Override
    public boolean isCompleted(String externalStatus) {
        return true;
    }

    /**
     * Truncate the string to max length.
     *
     * @param strBuffer
     * @return truncated string string
     */
    private String getTruncatedString(StringBuffer strBuffer) {
        if (strBuffer.length() <= maxLen) {
            return strBuffer.toString();
        }
        else {
            return strBuffer.substring(0, maxLen);
        }
    }

    /**
     * Returns the first line from a StringBuffer, recognized by the new line character \n.
     *
     * @param buffer The StringBuffer from which the first line is required.
     * @return The first line of the buffer.
     */
    private String getFirstLine(StringBuffer buffer) {
        int newLineIndex = buffer.indexOf("\n");
        if (newLineIndex == -1) {
            return buffer.toString();
        }
        else {
            return buffer.substring(0, newLineIndex);
        }
    }
}
