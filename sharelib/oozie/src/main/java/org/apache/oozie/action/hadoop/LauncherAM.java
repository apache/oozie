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
import java.security.PrivilegedExceptionAction;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.oozie.action.hadoop.security.LauncherSecurityManager;

public class LauncherAM {
    public static final String OOZIE_ACTION_CONF_XML = "oozie.action.conf.xml";
    public static final String OOZIE_LAUNCHER_JOB_ID = "oozie.launcher.job.id";

    public static final String OOZIE_LAUNCHER_VCORES_PROPERTY = "oozie.launcher.vcores";
    public static final String OOZIE_LAUNCHER_MEMORY_MB_PROPERTY = "oozie.launcher.memory.mb";
    public static final String OOZIE_LAUNCHER_PRIORITY_PROPERTY = "oozie.launcher.priority";
    public static final String OOZIE_LAUNCHER_QUEUE_PROPERTY = "oozie.launcher.queue";
    public static final String OOZIE_LAUNCHER_JAVAOPTS_PROPERTY = "oozie.launcher.javaopts";
    public static final String OOZIE_LAUNCHER_ENV_PROPERTY = "oozie.launcher.env";
    public static final String OOZIE_LAUNCHER_SHARELIB_PROPERTY = "oozie.launcher.sharelib";
    public static final String OOZIE_LAUNCHER_LOG_LEVEL_PROPERTY = "oozie.launcher.log.level";
    public static final String OOZIE_LAUNCHER_MAX_ATTEMPTS = "oozie.launcher.max.attempts";

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
    public static final String OOZIE_SUBMITTER_USER = "oozie.submitter.user";

    private final AMRMClientAsyncFactory amrmClientAsyncFactory;
    private final HdfsOperations hdfsOperations;
    private final LocalFsOperations localFsOperations;
    private final PrepareActionsHandler prepareHandler;
    private final LauncherAMCallbackNotifierFactory callbackNotifierFactory;
    private final LauncherSecurityManager launcherSecurityManager;
    private final ContainerId containerId;

    private final Configuration launcherConf;
    private final AMRMCallBackHandler amrmCallBackHandler;
    private AMRMClientAsync<?> amRmClientAsync;
    private Path actionDir;
    private Map<String, String> actionData = new HashMap<String,String>();

    public LauncherAM(
            AMRMClientAsyncFactory amrmClientAsyncFactory,
            AMRMCallBackHandler amrmCallBackHandler,
            HdfsOperations hdfsOperations,
            LocalFsOperations localFsOperations,
            PrepareActionsHandler prepareHandler,
            LauncherAMCallbackNotifierFactory callbackNotifierFactory,
            LauncherSecurityManager launcherSecurityManager,
            String containerId,
            Configuration launcherConf) {
        this.amrmClientAsyncFactory = Preconditions.checkNotNull(amrmClientAsyncFactory,
                "amrmClientAsyncFactory should not be null");
        this.amrmCallBackHandler = Preconditions.checkNotNull(amrmCallBackHandler, "amrmCallBackHandler should not be null");
        this.hdfsOperations = Preconditions.checkNotNull(hdfsOperations, "hdfsOperations should not be null");
        this.localFsOperations = Preconditions.checkNotNull(localFsOperations, "localFsOperations should not be null");
        this.prepareHandler = Preconditions.checkNotNull(prepareHandler, "prepareHandler should not be null");
        this.callbackNotifierFactory = Preconditions.checkNotNull(callbackNotifierFactory,
                "callbackNotifierFactory should not be null");
        this.launcherSecurityManager = Preconditions.checkNotNull(launcherSecurityManager,
                "launcherSecurityManager should not be null");
        this.containerId = ContainerId.fromString(Preconditions.checkNotNull(containerId, "containerId should not be null"));
        this.launcherConf = Preconditions.checkNotNull(launcherConf, "launcherConf should not be null");
    }

    public static void main(String[] args) throws Exception {
        final LocalFsOperations localFsOperations = new LocalFsOperations();
        final Configuration launcherConf = readLauncherConfiguration(localFsOperations);
        UserGroupInformation.setConfiguration(launcherConf);
        // MRAppMaster adds this call as well, but it's included only in Hadoop 2.9+
        // SecurityUtil.setConfiguration(launcherConf);
        UserGroupInformation ugi = getUserGroupInformation(launcherConf);
        printTokens("Executing Oozie Launcher with tokens:", ugi.getTokens());
        // Executing code inside a doAs with an ugi equipped with correct tokens.
        ugi.doAs(new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {
                  LauncherAM launcher = new LauncherAM(new AMRMClientAsyncFactory(),
                        new AMRMCallBackHandler(),
                        new HdfsOperations(new SequenceFileWriterFactory()),
                        new LocalFsOperations(),
                        new PrepareActionsHandler(new LauncherURIHandlerFactory(null)),
                        new LauncherAMCallbackNotifierFactory(),
                        new LauncherSecurityManager(),
                        System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name()),
                        launcherConf);
                    launcher.run();
                    return null;
            }
        });

    }

    private static void printTokens(String message, Collection<Token<? extends TokenIdentifier>> tokens) {
        System.out.println(message);
        for(Token<?> token : tokens) {
            System.out.println(token);
        }
    }

    private static UserGroupInformation getUserGroupInformation(Configuration launcherConf, Text... kindToFilter)
            throws IOException {
        final String submitterUser = launcherConf.get(OOZIE_SUBMITTER_USER);
        Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
        filterTokensByKind(credentials, kindToFilter);

        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(submitterUser);
        ugi.addCredentials(credentials);
        return ugi;
    }

    private static void filterTokensByKind(Credentials credentials, Text[] kindToFilter) throws IOException {
        Iterator<Token<? extends TokenIdentifier>> iterator = credentials.getAllTokens().iterator();

        while (iterator.hasNext()) {
            Token<?> token = iterator.next();
            for (int i = 0; i < kindToFilter.length; i++) {
                if (kindToFilter[i].equals(token.getKind())) {
                    System.out.println("Removing token from the Ugi: " + kindToFilter[i]);
                    iterator.remove();
                }
            }
        }
    }

    static Configuration readLauncherConfiguration(LocalFsOperations localFsOperations) {
        Configuration launcherConf = null;
        try {
            launcherConf = localFsOperations.readLauncherConf();
            System.out.println("Launcher AM configuration loaded");
        } catch (Exception ex) {
            System.err.println("Could not load the Launcher AM configuration file");
            ex.printStackTrace();
            throw ex;
        }
        return launcherConf;
    }

    public void run() throws Exception {
        final ErrorHolder errorHolder = new ErrorHolder();
        OozieActionResult actionResult = OozieActionResult.FAILED;
        boolean backgroundAction = false;
        try {

            actionDir = new Path(launcherConf.get(OOZIE_ACTION_DIR_PATH));

            registerWithRM(amrmCallBackHandler);
            // Run user code without the AM_RM_TOKEN so users can't request containers
            UserGroupInformation ugi = getUserGroupInformation(launcherConf, AMRMTokenIdentifier.KIND_NAME);
            printTokens(  "Executing Action Main with tokens:", ugi.getTokens());
            ugi.doAs(new PrivilegedExceptionAction<Object>() {
                @Override
                public Object run() throws Exception {
                    executePrepare(errorHolder);
                    printDebugInfo();
                    setupMainConfiguration();
                    runActionMain(errorHolder);
                    return null;
                }
            });

            if (!errorHolder.isPopulated()) {
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
            if (!errorHolder.isPopulated()) {
                errorHolder.setErrorCause(e);
                errorHolder.setErrorMessage(e.getMessage());
            }
            throw e;
        } finally {
            try {
                ErrorHolder callbackErrorHolder = amrmCallBackHandler.getError();

                if (!errorHolder.isPopulated()) {
                    actionResult = backgroundAction ? OozieActionResult.RUNNING : OozieActionResult.SUCCEEDED;
                }

                if (errorHolder.isPopulated()) {
                    updateActionDataWithFailure(errorHolder, actionData);
                } else if (callbackErrorHolder != null) {  // async error from the callback
                    actionResult = OozieActionResult.FAILED;
                    updateActionDataWithFailure(callbackErrorHolder, actionData);
                }

                actionData.put(ACTION_DATA_FINAL_STATUS, actionResult.toString());
                hdfsOperations.uploadActionDataToHDFS(launcherConf, actionDir, actionData);
            } finally {
                try {
                    unregisterWithRM(actionResult, errorHolder.getErrorMessage());
                } finally {
                    LauncherAMCallbackNotifier cn = callbackNotifierFactory.createCallbackNotifier(launcherConf);
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
        System.out.println("Workflow job id   : " + launcherConf.get(OOZIE_JOB_ID));
        System.out.println("Workflow action id: " + launcherConf.get(OOZIE_ACTION_ID));
        System.out.println();
        System.out.println("Classpath         :");
        System.out.println("------------------------");
        StringTokenizer st = new StringTokenizer(System.getProperty(JAVA_CLASS_PATH), ":");
        while (st.hasMoreTokens()) {
            System.out.println("  " + st.nextToken());
        }
        System.out.println("------------------------");
        System.out.println();
        String mainClass = launcherConf.get(CONF_OOZIE_ACTION_MAIN_CLASS);
        System.out.println("Main class        : " + mainClass);
        System.out.println();
        System.out.println("Maximum output    : "
                + launcherConf.getInt(CONF_OOZIE_ACTION_MAX_OUTPUT_DATA, 2 * 1024));
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

    private void registerWithRM(AMRMCallBackHandler amrmCallBackHandler) throws IOException, YarnException {
        // TODO: OYA: make heartbeat interval configurable & make interval higher to put less load on RM, but lower than timeout
        amRmClientAsync = amrmClientAsyncFactory.createAMRMClientAsync(60000, amrmCallBackHandler);
        amRmClientAsync.init(new Configuration(launcherConf));
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
    private void executePrepare(ErrorHolder errorHolder) throws Exception {
        try {
            System.out.println("\nStarting the execution of prepare actions");
            String prepareXML = launcherConf.get(ACTION_PREPARE_XML);
            if (prepareXML != null) {
                if (prepareXML.length() != 0) {
                    Configuration actionConf = new Configuration(launcherConf);
                    actionConf.addResource(ACTION_CONF_XML);
                    prepareHandler.prepareAction(prepareXML, actionConf);
                } else {
                    System.out.println("There are no prepare actions to execute.");
                }
            }
            System.out.println("Completed the execution of prepare actions successfully");
        } catch (Exception ex) {
            errorHolder.setErrorMessage("Prepare execution in the Launcher AM has failed");
            errorHolder.setErrorCause(ex);
            throw ex;
        }
    }

    private void setupMainConfiguration() throws IOException {
        System.setProperty(OOZIE_LAUNCHER_JOB_ID, launcherConf.get(OOZIE_JOB_ID));
        System.setProperty(OOZIE_JOB_ID, launcherConf.get(OOZIE_JOB_ID));
        System.setProperty(OOZIE_ACTION_ID, launcherConf.get(OOZIE_ACTION_ID));
        System.setProperty(OOZIE_ACTION_CONF_XML, new File(ACTION_CONF_XML).getAbsolutePath());
        System.setProperty(ACTION_PREFIX + ACTION_DATA_EXTERNAL_CHILD_IDS,
                new File(ACTION_DATA_EXTERNAL_CHILD_IDS).getAbsolutePath());
        System.setProperty(ACTION_PREFIX + ACTION_DATA_STATS, new File(ACTION_DATA_STATS).getAbsolutePath());
        System.setProperty(ACTION_PREFIX + ACTION_DATA_NEW_ID, new File(ACTION_DATA_NEW_ID).getAbsolutePath());
        System.setProperty(ACTION_PREFIX + ACTION_DATA_OUTPUT_PROPS, new File(ACTION_DATA_OUTPUT_PROPS).getAbsolutePath());
        System.setProperty(ACTION_PREFIX + ACTION_DATA_ERROR_PROPS, new File(ACTION_DATA_ERROR_PROPS).getAbsolutePath());

        System.setProperty("oozie.job.launch.time", String.valueOf(System.currentTimeMillis()));
    }

    private boolean runActionMain(final ErrorHolder eHolder) throws Exception {
        boolean actionMainExecutedProperly = false;
        try {
            final String[] mainArgs = getMainArguments(launcherConf);
            setRecoveryId();
            Class<?> klass = launcherConf.getClass(CONF_OOZIE_ACTION_MAIN_CLASS, null);
            Preconditions.checkNotNull(klass, "Launcher class should not be null");
            System.out.println("Launcher class: " + klass.toString());
            Method mainMethod = klass.getMethod("main", String[].class);
            // Enable LauncherSecurityManager to catch System.exit calls
            launcherSecurityManager.enable();
            mainMethod.invoke(null, (Object) mainArgs);
            System.out.println();
            System.out.println("<<< Invocation of Main class completed <<<");
            System.out.println();
            actionMainExecutedProperly = true;
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
                String mainClass = launcherConf.get(CONF_OOZIE_ACTION_MAIN_CLASS);
                eHolder.setErrorMessage("Main Class [" + mainClass + "], exit code [" +
                        errorCode + "]");
                eHolder.setErrorCode(errorCode);
            } else if (SecurityException.class.isInstance(cause)) {
                if (launcherSecurityManager.getExitInvoked()) {
                    final int exitCode = launcherSecurityManager.getExitCode();
                    System.out.println("Intercepting System.exit(" + exitCode + ")");
                    // if 0 main() method finished successfully
                    // ignoring
                    if (exitCode != 0) {
                        eHolder.setErrorCode(exitCode);
                        String mainClass = launcherConf.get(CONF_OOZIE_ACTION_MAIN_CLASS);
                        eHolder.setErrorMessage("Main Class [" + mainClass + "],"
                                + " exit code [" + eHolder.getErrorCode() + "]");
                    } else {
                        actionMainExecutedProperly = true;
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
        return actionMainExecutedProperly;
    }

    private void setRecoveryId() throws LauncherException {
        try {
            ApplicationId applicationId = containerId.getApplicationAttemptId().getApplicationId();
            final String applicationIdStr = applicationId.toString();

            final String recoveryId = Preconditions.checkNotNull(launcherConf.get(OOZIE_ACTION_RECOVERY_ID),
                            "RecoveryID should not be null");

            final Path path = new Path(actionDir, recoveryId);
            if (!hdfsOperations.fileExists(path, launcherConf)) {
                hdfsOperations.writeStringToFile(path, launcherConf, applicationIdStr);
            } else {
                final String id = hdfsOperations.readFileContents(path, launcherConf);

                if (id == null || id.isEmpty()) {
                    hdfsOperations.writeStringToFile(path, launcherConf, applicationIdStr);
                } else if (!applicationIdStr.equals(id)) {
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
            maxSize = launcherConf.getInt(maxSizePropertyName, maxSizeDefault);
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
