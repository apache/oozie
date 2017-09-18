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
package org.apache.oozie.action.hadoop;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.Permission;
import java.security.PrivilegedExceptionAction;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class LauncherAM {
    public static final String OOZIE_ACTION_CONF_XML = "oozie.action.conf.xml";
    public static final String OOZIE_LAUNCHER_JOB_ID = "oozie.launcher.job.id";

    public static final String JAVA_CLASS_PATH = "java.class.path";
    public static final String OOZIE_ACTION_ID = "oozie.action.id";
    public static final String OOZIE_JOB_ID = "oozie.job.id";
    public static final String ACTION_PREFIX = "oozie.action.";
    static final String OOZIE_ACTION_RECOVERY_ID = ACTION_PREFIX + "recovery.id";
    public static final String CONF_OOZIE_ACTION_MAX_OUTPUT_DATA = ACTION_PREFIX + "max.output.data";
    public static final String CONF_OOZIE_ACTION_MAIN_ARG_PREFIX = ACTION_PREFIX + "main.arg.";
    public static final String CONF_OOZIE_ACTION_MAIN_ARG_COUNT = CONF_OOZIE_ACTION_MAIN_ARG_PREFIX + "count";
    public static final String CONF_OOZIE_EXTERNAL_STATS_MAX_SIZE = "oozie.external.stats.max.size";
    public static final String OOZIE_ACTION_DIR_PATH = ACTION_PREFIX + "dir.path";
    public static final String ACTION_PREPARE_XML = ACTION_PREFIX + "prepare.xml";
    public static final String ACTION_DATA_SEQUENCE_FILE = "action-data.seq"; // COMBO FILE
    public static final String ACTION_DATA_EXTERNAL_CHILD_IDS = "externalChildIDs";
    public static final String ACTION_DATA_OUTPUT_PROPS = "output.properties";
    public static final String ACTION_DATA_STATS = "stats.properties";
    public static final String ACTION_DATA_NEW_ID = "newId";
    public static final String ACTION_DATA_ERROR_PROPS = "error.properties";
    public static final String CONF_OOZIE_ACTION_MAIN_CLASS = "oozie.launcher.action.main.class";

    // TODO: OYA: more unique file names?  action.xml may be stuck for backwards compat though
    public static final String LAUNCHER_JOB_CONF_XML = "launcher.xml";
    public static final String ACTION_CONF_XML = "action.xml";
    public static final String ACTION_DATA_FINAL_STATUS = "final.status";

    private final UserGroupInformation ugi;
    private final AMRMCallBackHandler callbackHandler;
    private final AMRMClientAsyncFactory amRmClientAsyncFactory;
    private final HdfsOperations hdfsOperations;
    private final LocalFsOperations localFsOperations;
    private final PrepareActionsHandler prepareHandler;
    private final LauncherAMCallbackNotifierFactory callbackNotifierFactory;
    private final LauncherSecurityManager launcherSecurityManager;
    private final ContainerId containerId;

    private Configuration launcherJobConf;
    private AMRMClientAsync<?> amRmClientAsync;
    private Path actionDir;
    private Map<String, String> actionData = new HashMap<String,String>();

    public LauncherAM(UserGroupInformation ugi,
            AMRMClientAsyncFactory amRmClientAsyncFactory,
            AMRMCallBackHandler callbackHandler,
            HdfsOperations hdfsOperations,
            LocalFsOperations localFsOperations,
            PrepareActionsHandler prepareHandler,
            LauncherAMCallbackNotifierFactory callbackNotifierFactory,
            LauncherSecurityManager launcherSecurityManager,
            String containerId) {
        this.ugi = Preconditions.checkNotNull(ugi, "ugi should not be null");
        this.amRmClientAsyncFactory = Preconditions.checkNotNull(amRmClientAsyncFactory,
                "amRmClientAsyncFactory should not be null");
        this.callbackHandler = Preconditions.checkNotNull(callbackHandler, "callbackHandler should not be null");
        this.hdfsOperations = Preconditions.checkNotNull(hdfsOperations, "hdfsOperations should not be null");
        this.localFsOperations = Preconditions.checkNotNull(localFsOperations, "localFsOperations should not be null");
        this.prepareHandler = Preconditions.checkNotNull(prepareHandler, "prepareHandler should not be null");
        this.callbackNotifierFactory = Preconditions.checkNotNull(callbackNotifierFactory,
                "callbackNotifierFactory should not be null");
        this.launcherSecurityManager = Preconditions.checkNotNull(launcherSecurityManager,
                "launcherSecurityManager should not be null");
        this.containerId = ContainerId.fromString(Preconditions.checkNotNull(containerId, "containerId should not be null"));
    }

    public static void main(String[] args) throws Exception {
        UserGroupInformation ugi = null;
        String submitterUser = System.getProperty("submitter.user", "").trim();
        Preconditions.checkArgument(!submitterUser.isEmpty(), "Submitter user is undefined");
        System.out.println("Submitter user is: " + submitterUser);

        // We don't need remote/proxy user if the current login user is the workflow submitter
        // Otherwise we have to create a remote user
        if (UserGroupInformation.getLoginUser().getShortUserName().equals(submitterUser)) {
            System.out.println("Using login user for UGI");
            ugi = UserGroupInformation.getLoginUser();
        } else {
            ugi = UserGroupInformation.createRemoteUser(submitterUser);
            ugi.addCredentials(UserGroupInformation.getLoginUser().getCredentials());
        }

        AMRMClientAsyncFactory amRmClientAsyncFactory = new AMRMClientAsyncFactory();
        AMRMCallBackHandler callbackHandler = new AMRMCallBackHandler();
        HdfsOperations hdfsOperations = new HdfsOperations(new SequenceFileWriterFactory(), ugi);
        LocalFsOperations localFSOperations = new LocalFsOperations();
        PrepareActionsHandler prepareHandler = new PrepareActionsHandler();
        LauncherAMCallbackNotifierFactory callbackNotifierFactory = new LauncherAMCallbackNotifierFactory();
        LauncherSecurityManager launcherSecurityManager = new LauncherSecurityManager();

        LauncherAM launcher = new LauncherAM(ugi,
                amRmClientAsyncFactory,
                callbackHandler,
                hdfsOperations,
                localFSOperations,
                prepareHandler,
                callbackNotifierFactory,
                launcherSecurityManager,
                System.getenv("CONTAINER_ID"));

        launcher.run();
    }

    public void run() throws Exception {
        final ErrorHolder errorHolder = new ErrorHolder();
        OozieActionResult actionResult = OozieActionResult.FAILED;
        boolean launcherExecutedProperly = false;
        boolean backgroundAction = false;

        try {
            try {
                launcherJobConf = localFsOperations.readLauncherConf();
                System.out.println("Launcher AM configuration loaded");
            } catch (Exception ex) {
                errorHolder.setErrorMessage("Could not load the Launcher AM configuration file");
                errorHolder.setErrorCause(ex);
                throw ex;
            }
            actionDir = new Path(launcherJobConf.get(OOZIE_ACTION_DIR_PATH));

            registerWithRM();
            executePrepare(ugi, errorHolder);
            final String[] mainArgs = getMainArguments(launcherJobConf);
            printDebugInfo();
            setupMainConfiguration();
            launcherExecutedProperly = runActionMain(mainArgs, errorHolder, ugi);

            if (launcherExecutedProperly) {
                handleActionData();
                if (actionData.get(ACTION_DATA_OUTPUT_PROPS) != null) {
                    System.out.println();
                    System.out.println("Oozie Launcher, capturing output data:");
                    System.out.println("=======================");
                    System.out.println(actionData.get(ACTION_DATA_OUTPUT_PROPS));
                    System.out.println();
                    System.out.println("=======================");
                    System.out.println();
                }
                if (actionData.get(ACTION_DATA_NEW_ID) != null) {
                    System.out.println();
                    System.out.println("Oozie Launcher, propagating new Hadoop job id to Oozie");
                    System.out.println("=======================");
                    System.out.println(actionData.get(ACTION_DATA_NEW_ID));
                    System.out.println("=======================");
                    System.out.println();
                    backgroundAction = true;
                }
            }
        } catch (Exception e) {
            System.out.println("Launcher AM execution failed");
            System.err.println("Launcher AM execution failed");
            e.printStackTrace(System.out);
            e.printStackTrace(System.err);
            launcherExecutedProperly = false;
            if (!errorHolder.isPopulated()) {
                errorHolder.setErrorCause(e);
                errorHolder.setErrorMessage(e.getMessage());
            }
            throw e;
        } finally {
            try {
                ErrorHolder callbackErrorHolder = callbackHandler.getError();

                if (launcherExecutedProperly) {
                    actionResult = backgroundAction ? OozieActionResult.RUNNING : OozieActionResult.SUCCEEDED;
                }

                if (!launcherExecutedProperly) {
                    updateActionDataWithFailure(errorHolder, actionData);
                } else if (callbackErrorHolder != null) {  // async error from the callback
                    actionResult = OozieActionResult.FAILED;
                    updateActionDataWithFailure(callbackErrorHolder, actionData);
                }

                actionData.put(ACTION_DATA_FINAL_STATUS, actionResult.toString());
                hdfsOperations.uploadActionDataToHDFS(launcherJobConf, actionDir, actionData);
            } finally {
                try {
                    unregisterWithRM(actionResult, errorHolder.getErrorMessage());
                } finally {
                    LauncherAMCallbackNotifier cn = callbackNotifierFactory.createCallbackNotifier(launcherJobConf);
                    cn.notifyURL(actionResult);
                }
            }
        }
    }

    @VisibleForTesting
    Map<String, String> getActionData() {
        return actionData;
    }

    private void printDebugInfo() throws IOException {
        localFsOperations.printContentsOfDir(new File("."));

        System.out.println();
        System.out.println("Oozie Launcher Application Master configuration");
        System.out.println("===============================================");
        System.out.println("Workflow job id   : " + launcherJobConf.get(OOZIE_JOB_ID));
        System.out.println("Workflow action id: " + launcherJobConf.get(OOZIE_ACTION_ID));
        System.out.println();
        System.out.println("Classpath         :");
        System.out.println("------------------------");
        StringTokenizer st = new StringTokenizer(System.getProperty(JAVA_CLASS_PATH), ":");
        while (st.hasMoreTokens()) {
            System.out.println("  " + st.nextToken());
        }
        System.out.println("------------------------");
        System.out.println();
        String mainClass = launcherJobConf.get(CONF_OOZIE_ACTION_MAIN_CLASS);
        System.out.println("Main class        : " + mainClass);
        System.out.println();
        System.out.println("Maximum output    : "
                + launcherJobConf.getInt(CONF_OOZIE_ACTION_MAX_OUTPUT_DATA, 2 * 1024));
        System.out.println();

        System.out.println();
        System.out.println("Java System Properties:");
        System.out.println("------------------------");
        System.getProperties().store(System.out, "");
        System.out.println("------------------------");
        System.out.println();

        System.out.println("Environment variables");
        Map<String, String> env = System.getenv();
        System.out.println("------------------------");
        for (Map.Entry<String, String> entry : env.entrySet()) {
            System.out.println(entry.getKey() + "=" + entry.getValue());
        }
        System.out.println("------------------------");
        System.out.println("=================================================================");
        System.out.println();
        System.out.println(">>> Invoking Main class now >>>");
        System.out.println();
    }

    private void registerWithRM() throws IOException, YarnException {
        // TODO: OYA: make heartbeat interval configurable & make interval higher to put less load on RM, but lower than timeout
        amRmClientAsync = amRmClientAsyncFactory.createAMRMClientAsync(60000);
        amRmClientAsync.init(new Configuration(launcherJobConf));
        amRmClientAsync.start();

        // hostname and tracking url are determined automatically
        amRmClientAsync.registerApplicationMaster("", 0, "");
    }

    private void unregisterWithRM(OozieActionResult actionResult, String message) throws YarnException, IOException {
        if (amRmClientAsync != null) {
            System.out.println("Stopping AM");
            try {
                message = (message == null) ? "" : message;
                // tracking url is determined automatically
                amRmClientAsync.unregisterApplicationMaster(actionResult.getYarnStatus(), message, "");
            } catch (Exception ex) {
                System.out.println("Error un-registering AM client");
                throw ex;
            } finally {
                amRmClientAsync.stop();
            }
        }
    }

    // Method to execute the prepare actions
    private void executePrepare(UserGroupInformation ugi, ErrorHolder errorHolder) throws Exception {
        try {
            System.out.println("\nStarting the execution of prepare actions");
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    String prepareXML = launcherJobConf.get(ACTION_PREPARE_XML);
                    if (prepareXML != null) {
                        if (prepareXML.length() != 0) {
                            Configuration actionConf = new Configuration(launcherJobConf);
                            actionConf.addResource(ACTION_CONF_XML);
                            prepareHandler.prepareAction(prepareXML, actionConf);
                        } else {
                            System.out.println("There are no prepare actions to execute.");
                        }
                    }
                    return null;
                }
            });
            System.out.println("Completed the execution of prepare actions successfully");
        } catch (Exception ex) {
            errorHolder.setErrorMessage("Prepare execution in the Launcher AM has failed");
            errorHolder.setErrorCause(ex);
            throw ex;
        }
    }

    private void setupMainConfiguration() throws IOException {
        System.setProperty(OOZIE_LAUNCHER_JOB_ID, launcherJobConf.get(OOZIE_JOB_ID));
        System.setProperty(OOZIE_JOB_ID, launcherJobConf.get(OOZIE_JOB_ID));
        System.setProperty(OOZIE_ACTION_ID, launcherJobConf.get(OOZIE_ACTION_ID));
        System.setProperty(OOZIE_ACTION_CONF_XML, new File(ACTION_CONF_XML).getAbsolutePath());
        System.setProperty(ACTION_PREFIX + ACTION_DATA_EXTERNAL_CHILD_IDS,
                new File(ACTION_DATA_EXTERNAL_CHILD_IDS).getAbsolutePath());
        System.setProperty(ACTION_PREFIX + ACTION_DATA_STATS, new File(ACTION_DATA_STATS).getAbsolutePath());
        System.setProperty(ACTION_PREFIX + ACTION_DATA_NEW_ID, new File(ACTION_DATA_NEW_ID).getAbsolutePath());
        System.setProperty(ACTION_PREFIX + ACTION_DATA_OUTPUT_PROPS, new File(ACTION_DATA_OUTPUT_PROPS).getAbsolutePath());
        System.setProperty(ACTION_PREFIX + ACTION_DATA_ERROR_PROPS, new File(ACTION_DATA_ERROR_PROPS).getAbsolutePath());

        System.setProperty("oozie.job.launch.time", String.valueOf(System.currentTimeMillis()));
    }

    private boolean runActionMain(final String[] mainArgs, final ErrorHolder eHolder, UserGroupInformation ugi) throws Exception {
        // using AtomicBoolean because we want to modify it inside run()
        final AtomicBoolean actionMainExecutedProperly = new AtomicBoolean(false);

        ugi.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                try {
                    setRecoveryId();
                    Class<?> klass = launcherJobConf.getClass(CONF_OOZIE_ACTION_MAIN_CLASS, null);
                    Preconditions.checkNotNull(klass, "Launcher class should not be null");
                    System.out.println("Launcher class: " + klass.toString());
                    Method mainMethod = klass.getMethod("main", String[].class);
                    // Enable LauncherSecurityManager to catch System.exit calls
                    launcherSecurityManager.enable();
                    mainMethod.invoke(null, (Object) mainArgs);

                    System.out.println();
                    System.out.println("<<< Invocation of Main class completed <<<");
                    System.out.println();
                    actionMainExecutedProperly.set(true);
                } catch (InvocationTargetException ex) {
                    ex.printStackTrace(System.out);
                    // Get what actually caused the exception
                    Throwable cause = ex.getCause();
                    // If we got a JavaMainException from JavaMain, then we need to unwrap it
                    if (JavaMain.JavaMainException.class.isInstance(cause)) {
                        cause = cause.getCause();
                    }
                    if (LauncherMainException.class.isInstance(cause)) {
                        int errorCode = ((LauncherMainException) ex.getCause()).getErrorCode();
                        String mainClass = launcherJobConf.get(CONF_OOZIE_ACTION_MAIN_CLASS);
                        eHolder.setErrorMessage("Main Class [" + mainClass + "], exit code [" +
                                errorCode + "]");
                        eHolder.setErrorCode(errorCode);
                    } else if (SecurityException.class.isInstance(cause)) {
                        if (launcherSecurityManager.getExitInvoked()) {
                            final int exitCode = launcherSecurityManager.getExitCode();
                            System.out.println("Intercepting System.exit(" + exitCode + ")");
                            // if 0 main() method finished successfully
                            // ignoring
                            eHolder.setErrorCode(exitCode);
                            if (exitCode != 0) {
                                String mainClass = launcherJobConf.get(CONF_OOZIE_ACTION_MAIN_CLASS);
                                eHolder.setErrorMessage("Main Class [" + mainClass + "],"
                                        + " exit code [" + eHolder.getErrorCode() + "]");
                            } else {
                                actionMainExecutedProperly.set(true);
                            }
                        } else {
                            // just SecurityException, no exit was invoked
                            eHolder.setErrorCode(0);
                            eHolder.setErrorCause(cause);
                            eHolder.setErrorMessage(cause.getMessage());
                        }
                    } else {
                        eHolder.setErrorMessage(cause.getMessage());
                        eHolder.setErrorCause(cause);
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                    eHolder.setErrorMessage(t.getMessage());
                    eHolder.setErrorCause(t);
                } finally {
                    // Disable LauncherSecurityManager
                    launcherSecurityManager.disable();
                }

                return null;
            }
        });

        return actionMainExecutedProperly.get();
    }

    private void setRecoveryId() throws LauncherException {
        try {
            ApplicationId applicationId = containerId.getApplicationAttemptId().getApplicationId();
            String applicationIdStr = applicationId.toString();

            String recoveryId = Preconditions.checkNotNull(launcherJobConf.get(OOZIE_ACTION_RECOVERY_ID),
                            "RecoveryID should not be null");

            Path path = new Path(actionDir, recoveryId);
            if (!hdfsOperations.fileExists(path, launcherJobConf)) {
                hdfsOperations.writeStringToFile(path, launcherJobConf, applicationIdStr);
            } else {
                String id = hdfsOperations.readFileContents(path, launcherJobConf);

                if (!applicationIdStr.equals(id)) {
                    throw new LauncherException(MessageFormat.format(
                            "YARN Id mismatch, action file [{0}] declares Id [{1}] current Id [{2}]", path, id,
                            applicationIdStr));
                }
            }
        } catch (RuntimeException | InterruptedException | IOException ex) {
            throw new LauncherException("IO error", ex);
        }
    }

    private void handleActionData() throws IOException {
        // external child IDs
        processActionData(ACTION_PREFIX + ACTION_DATA_EXTERNAL_CHILD_IDS, null,
                ACTION_DATA_EXTERNAL_CHILD_IDS, -1);

        // external stats
        processActionData(ACTION_PREFIX + ACTION_DATA_STATS, CONF_OOZIE_EXTERNAL_STATS_MAX_SIZE,
                ACTION_DATA_STATS, Integer.MAX_VALUE);

        // output data
        processActionData(ACTION_PREFIX + ACTION_DATA_OUTPUT_PROPS, CONF_OOZIE_ACTION_MAX_OUTPUT_DATA,
                ACTION_DATA_OUTPUT_PROPS, 2048);

        // id swap
        processActionData(ACTION_PREFIX + ACTION_DATA_NEW_ID, null,
                ACTION_DATA_NEW_ID, -1);
    }

    private void processActionData(String propertyName, String maxSizePropertyName, String actionDataPropertyName,
            int maxSizeDefault) throws IOException {
        String propValue = System.getProperty(propertyName);
        int maxSize = maxSizeDefault;

        if (maxSizePropertyName != null) {
            maxSize = launcherJobConf.getInt(maxSizePropertyName, maxSizeDefault);
        }

        if (propValue != null) {
            File actionDataFile = new File(propValue);
            if (localFsOperations.fileExists(actionDataFile)) {
                actionData.put(actionDataPropertyName, localFsOperations.getLocalFileContentAsString(actionDataFile,
                        actionDataPropertyName, maxSize));
            }
        }
    }

    private void updateActionDataWithFailure(ErrorHolder eHolder, Map<String, String> actionData) {
        if (eHolder.getErrorCause() != null && eHolder.getErrorCause().getMessage() != null) {
            if (Objects.equal(eHolder.getErrorMessage(), eHolder.getErrorCause().getMessage())) {
                eHolder.setErrorMessage(eHolder.getErrorMessage());
            } else {
                eHolder.setErrorMessage(eHolder.getErrorMessage() + ", " + eHolder.getErrorCause().getMessage());
            }
        }

        Properties errorProps = new Properties();
        errorProps.setProperty("error.code", Integer.toString(eHolder.getErrorCode()));
        String errorMessage = eHolder.getErrorMessage() == null ? "<empty>" : eHolder.getErrorMessage();
        errorProps.setProperty("error.reason", errorMessage);
        if (eHolder.getErrorCause() != null) {
            if (eHolder.getErrorCause().getMessage() != null) {
                errorProps.setProperty("exception.message", eHolder.getErrorCause().getMessage());
            }
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            eHolder.getErrorCause().printStackTrace(pw);
            pw.close();
            errorProps.setProperty("exception.stacktrace", sw.toString());
        }

        StringWriter sw = new StringWriter();
        try {
            errorProps.store(sw, "");
            sw.close();
            actionData.put(LauncherAM.ACTION_DATA_ERROR_PROPS, sw.toString());

            // external child IDs
            String externalChildIdsProp = System.getProperty(LauncherAM.ACTION_PREFIX + LauncherAM.ACTION_DATA_EXTERNAL_CHILD_IDS);
            if (externalChildIdsProp != null) {
                File externalChildIDs = new File(externalChildIdsProp);
                if (localFsOperations.fileExists(externalChildIDs)) {
                    actionData.put(LauncherAM.ACTION_DATA_EXTERNAL_CHILD_IDS,
                            localFsOperations.getLocalFileContentAsString(externalChildIDs, ACTION_DATA_EXTERNAL_CHILD_IDS, -1));
                }
            }
        } catch (IOException ioe) {
            System.out.println("A problem occured trying to fail the launcher");
            ioe.printStackTrace();
        } finally {
            System.out.print("Failing Oozie Launcher, " + eHolder.getErrorMessage() + "\n");
            if (eHolder.getErrorCause() != null) {
                eHolder.getErrorCause().printStackTrace(System.out);
            }
        }
    }

    private String[] getMainArguments(Configuration conf) {
        return LauncherAMUtils.getMainArguments(conf);
    }

    public static class LauncherSecurityManager extends SecurityManager {
        private boolean exitInvoked;
        private int exitCode;
        private SecurityManager originalSecurityManager;

        public LauncherSecurityManager() {
            exitInvoked = false;
            exitCode = 0;
            originalSecurityManager = System.getSecurityManager();
        }

        @Override
        public void checkPermission(Permission perm, Object context) {
            if (originalSecurityManager != null) {
                // check everything with the original SecurityManager
                originalSecurityManager.checkPermission(perm, context);
            }
        }

        @Override
        public void checkPermission(Permission perm) {
            if (originalSecurityManager != null) {
                // check everything with the original SecurityManager
                originalSecurityManager.checkPermission(perm);
            }
        }

        @Override
        public void checkExit(int status) throws SecurityException {
            exitInvoked = true;
            exitCode = status;
            throw new SecurityException("Intercepted System.exit(" + status + ")");
        }

        public boolean getExitInvoked() {
            return exitInvoked;
        }

        public int getExitCode() {
            return exitCode;
        }

        public void enable() {
            if (System.getSecurityManager() != this) {
                System.setSecurityManager(this);
            }
        }

        public void disable() {
            if (System.getSecurityManager() == this) {
                System.setSecurityManager(originalSecurityManager);
            }
        }

        public void reset() {
            exitInvoked = false;
            exitCode = 0;
        }
    }

    public enum OozieActionResult {
        SUCCEEDED(FinalApplicationStatus.SUCCEEDED),
        FAILED(FinalApplicationStatus.FAILED),
        RUNNING(FinalApplicationStatus.SUCCEEDED);

        // YARN-equivalent status
        private FinalApplicationStatus yarnStatus;

        OozieActionResult(FinalApplicationStatus yarnStatus) {
            this.yarnStatus = yarnStatus;
        }

        public FinalApplicationStatus getYarnStatus() {
            return yarnStatus;
        }
    }
}
