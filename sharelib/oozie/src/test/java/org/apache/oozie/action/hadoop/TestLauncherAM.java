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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import static org.apache.oozie.action.hadoop.LauncherAM.ACTION_DATA_EXTERNAL_CHILD_IDS;
import static org.apache.oozie.action.hadoop.LauncherAM.ACTION_DATA_NEW_ID;
import static org.apache.oozie.action.hadoop.LauncherAM.ACTION_DATA_OUTPUT_PROPS;
import static org.apache.oozie.action.hadoop.LauncherAM.ACTION_DATA_STATS;
import static org.apache.oozie.action.hadoop.LauncherAMTestMainClass.JAVA_EXCEPTION;
import static org.apache.oozie.action.hadoop.LauncherAMTestMainClass.JAVA_EXCEPTION_MESSAGE;
import static org.apache.oozie.action.hadoop.LauncherAMTestMainClass.LAUNCHER_ERROR_CODE;
import static org.apache.oozie.action.hadoop.LauncherAMTestMainClass.LAUNCHER_EXCEPTION;
import static org.apache.oozie.action.hadoop.LauncherAMTestMainClass.SECURITY_EXCEPTION;
import static org.apache.oozie.action.hadoop.LauncherAMTestMainClass.SECURITY_EXCEPTION_MESSAGE;
import static org.apache.oozie.action.hadoop.LauncherAMTestMainClass.THROWABLE;
import static org.apache.oozie.action.hadoop.LauncherAMTestMainClass.THROWABLE_MESSAGE;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import org.mockito.Mockito;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.text.MessageFormat;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.oozie.action.hadoop.security.LauncherSecurityManager;
import org.apache.oozie.action.hadoop.LauncherAM.OozieActionResult;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.when;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestLauncherAM {
    private static final String DEFAULT_CONTAINER_ID = "container_1479473450392_0001_01_000001";
    private static final String ACTIONDATA_ERROR_PROPERTIES = "error.properties";
    private static final String ACTIONDATA_FINAL_STATUS_PROPERTY = "final.status";
    private static final String ERROR_CODE_PROPERTY = "error.code";
    private static final String EXCEPTION_STACKTRACE_PROPERTY = "exception.stacktrace";
    private static final String EXCEPTION_MESSAGE_PROPERTY = "exception.message";
    private static final String ERROR_REASON_PROPERTY = "error.reason";

    private static final String EMPTY_STRING = "";
    private static final String EXIT_CODE_1 = "1";
    private static final String EXIT_CODE_0 = "0";
    private static final String DUMMY_XML = "<dummy>dummyXml</dummy>";

    public static final String ACTION_DIR = "/tmp/";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private AMRMClientAsyncFactory amRMClientAsyncFactoryMock;

    @Mock
    private AMRMClientAsync<?> amRmAsyncClientMock;

    @Mock
    private AMRMCallBackHandler callbackHandlerMock;

    @Mock
    private HdfsOperations hdfsOperationsMock;

    @Mock
    private LocalFsOperations localFsOperationsMock;

    @Mock
    private PrepareActionsHandler prepareHandlerMock;

    @Mock
    private LauncherAMCallbackNotifierFactory launcherCallbackNotifierFactoryMock;

    @Mock
    private LauncherAMCallbackNotifier launcherCallbackNotifierMock;

    @Mock
    private LauncherSecurityManager launcherSecurityManagerMock;

    private Configuration launcherJobConfig = new Configuration();

    private String containerId = DEFAULT_CONTAINER_ID;

    private String applicationId = ConverterUtils.toContainerId(containerId)
            .getApplicationAttemptId().getApplicationId().toString();

    private LauncherAM launcherAM;

    private ExpectedFailureDetails failureDetails = new ExpectedFailureDetails();

    @Before
    public void setup() throws Exception {
        configureMocksForHappyPath();
        launcherJobConfig.set(LauncherAMUtils.OOZIE_ACTION_RECOVERY_ID, "1");
        launcherJobConfig.set(LauncherAM.OOZIE_SUBMITTER_USER, System.getProperty("user.name"));
        instantiateLauncher();
    }

    @Test
    public void testMainIsSuccessfullyInvokedWithActionData() throws Exception {
        setupActionOutputContents();

        executeLauncher();

        verifyZeroInteractions(prepareHandlerMock);
        assertSuccessfulExecution(OozieActionResult.RUNNING);
        assertActionOutputDataPresentAndCorrect();
    }

    @Test
    public void testMainIsSuccessfullyInvokedWithoutActionData() throws Exception {
        executeLauncher();

        verifyZeroInteractions(prepareHandlerMock);
        assertSuccessfulExecution(OozieActionResult.SUCCEEDED);
        assertNoActionOutputData();
    }

    @Test
    public void testActionHasPrepareXML() throws Exception {
        launcherJobConfig.set(LauncherAM.ACTION_PREPARE_XML, DUMMY_XML);

        executeLauncher();

        verify(prepareHandlerMock).prepareAction(eq(DUMMY_XML), any(Configuration.class));
        assertSuccessfulExecution(OozieActionResult.SUCCEEDED);
    }

    @Test
    public void testActionHasEmptyPrepareXML() throws Exception {
        launcherJobConfig.set(LauncherAM.ACTION_PREPARE_XML, EMPTY_STRING);

        executeLauncher();

        verifyZeroInteractions(prepareHandlerMock);
        assertSuccessfulExecution(OozieActionResult.SUCCEEDED);
        assertNoActionOutputData();
    }

    @Test
    public void testLauncherClassNotDefined() throws Exception {
        launcherJobConfig.unset(LauncherAM.CONF_OOZIE_ACTION_MAIN_CLASS);

        executeLauncher();

        failureDetails.expectedExceptionMessage("Launcher class should not be null")
            .expectedErrorCode(EXIT_CODE_0)
            .expectedErrorReason("Launcher class should not be null")
            .withStackTrace();

        assertFailedExecution();
    }

    @Test
    public void testMainIsSuccessfullyInvokedAndAsyncErrorReceived() throws Exception {
        ErrorHolder errorHolder = new ErrorHolder();
        errorHolder.setErrorCode(6);
        errorHolder.setErrorMessage("dummy error");
        errorHolder.setErrorCause(new Exception());
        given(callbackHandlerMock.getError()).willReturn(errorHolder);

        executeLauncher();

        failureDetails.expectedExceptionMessage(null)
                    .expectedErrorCode("6")
                    .expectedErrorReason("dummy error")
                    .withStackTrace();

        assertFailedExecution();
    }

    @Test
    public void testMainClassNotFound() throws Exception {
        launcherJobConfig.set(LauncherAM.CONF_OOZIE_ACTION_MAIN_CLASS, "org.apache.non.existing.Klass");

        executeLauncher();

        failureDetails.expectedExceptionMessage(ClassNotFoundException.class.getCanonicalName())
                .expectedErrorCode(EXIT_CODE_0)
                .expectedErrorReason(ClassNotFoundException.class.getCanonicalName())
                .withStackTrace();

        assertFailedExecution();
    }

    @Test(expected = RuntimeException.class)
    public void testLauncherJobConfCannotBeLoaded() throws Exception {
        given(localFsOperationsMock.readLauncherConf()).willThrow(new RuntimeException());
        LauncherAM.readLauncherConfiguration(localFsOperationsMock);
    }

    @Test
    public void testActionPrepareFails() throws Exception {
        launcherJobConfig.set(LauncherAM.ACTION_PREPARE_XML, DUMMY_XML);
        willThrow(new IOException()).given(prepareHandlerMock).prepareAction(anyString(), any(Configuration.class));
        thrown.expect(IOException.class);

        try {
            executeLauncher();
        } finally {
            failureDetails.expectedExceptionMessage(null)
                .expectedErrorCode(EXIT_CODE_0)
                .expectedErrorReason("Prepare execution in the Launcher AM has failed")
                .withStackTrace();

            assertFailedExecution();
        }
    }

    @Test
    public void testActionThrowsJavaMainException() throws Exception {
        setupArgsForMainClass(JAVA_EXCEPTION);

        executeLauncher();

        failureDetails.expectedExceptionMessage(JAVA_EXCEPTION_MESSAGE)
            .expectedErrorCode(EXIT_CODE_0)
            .expectedErrorReason(JAVA_EXCEPTION_MESSAGE)
            .withStackTrace();

        assertFailedExecution();
    }

    @Test
    public void testActionThrowsLauncherException() throws Exception {
        setupArgsForMainClass(LAUNCHER_EXCEPTION);

        executeLauncher();

        failureDetails.expectedExceptionMessage(null)
            .expectedErrorCode(String.valueOf(LAUNCHER_ERROR_CODE))
            .expectedErrorReason("exit code [" + LAUNCHER_ERROR_CODE + "]")
            .withoutStackTrace();

        assertFailedExecution();
    }

    @Test
    public void testActionThrowsSecurityExceptionWithExitCode0() throws Exception {
        setupArgsForMainClass(SECURITY_EXCEPTION);
        given(launcherSecurityManagerMock.getExitInvoked()).willReturn(true);
        given(launcherSecurityManagerMock.getExitCode()).willReturn(0);

        executeLauncher();

        assertSuccessfulExecution(OozieActionResult.SUCCEEDED);
    }

    @Test
    public void testActionThrowsSecurityExceptionWithExitCode1() throws Exception {
        setupArgsForMainClass(SECURITY_EXCEPTION);
        given(launcherSecurityManagerMock.getExitInvoked()).willReturn(true);
        given(launcherSecurityManagerMock.getExitCode()).willReturn(1);

        executeLauncher();

        failureDetails.expectedExceptionMessage(null)
            .expectedErrorCode(EXIT_CODE_1)
            .expectedErrorReason("exit code ["+ EXIT_CODE_1 + "]")
            .withoutStackTrace();

        assertFailedExecution();
    }

    @Test
    public void testActionThrowsSecurityExceptionWithoutSystemExit() throws Exception {
        setupArgsForMainClass(SECURITY_EXCEPTION);
        given(launcherSecurityManagerMock.getExitInvoked()).willReturn(false);

        executeLauncher();

        failureDetails.expectedExceptionMessage(SECURITY_EXCEPTION_MESSAGE)
            .expectedErrorCode(EXIT_CODE_0)
            .expectedErrorReason(SECURITY_EXCEPTION_MESSAGE)
            .withStackTrace();

        assertFailedExecution();
    }

    @Test
    public void testActionThrowsThrowable() throws Exception {
        setupArgsForMainClass(THROWABLE);

        executeLauncher();

        failureDetails.expectedExceptionMessage(THROWABLE_MESSAGE)
            .expectedErrorCode(EXIT_CODE_0)
            .expectedErrorReason(THROWABLE_MESSAGE)
            .withStackTrace();

        assertFailedExecution();
    }

    @Test
    public void testActionThrowsThrowableAndAsyncErrorReceived() throws Exception {
        setupArgsForMainClass(THROWABLE);
        ErrorHolder errorHolder = new ErrorHolder();
        errorHolder.setErrorCode(6);
        errorHolder.setErrorMessage("dummy error");
        errorHolder.setErrorCause(new Exception());
        given(callbackHandlerMock.getError()).willReturn(errorHolder);

        executeLauncher();

        // sync problem overrides async problem
        failureDetails.expectedExceptionMessage(THROWABLE_MESSAGE)
            .expectedErrorCode(EXIT_CODE_0)
            .expectedErrorReason(THROWABLE_MESSAGE)
            .withStackTrace();

        assertFailedExecution();
    }

    @Test
    public void testYarnUnregisterFails() throws Exception {
        willThrow(new IOException()).given(amRmAsyncClientMock).unregisterApplicationMaster(any(FinalApplicationStatus.class),
                anyString(), anyString());
        thrown.expect(IOException.class);

        try {
            executeLauncher();
        } finally {
            // TODO: check if this behaviour is correct (url callback: successful, but unregister fails)
            assertSuccessfulExecution(OozieActionResult.SUCCEEDED);
        }
    }

    @Test
    public void testUpdateActionDataFailsWithActionError() throws Exception {
        setupActionOutputContents();
        given(localFsOperationsMock.getLocalFileContentAsString(any(File.class), eq(ACTION_DATA_EXTERNAL_CHILD_IDS), anyInt()))
            .willThrow(new IOException());
        thrown.expect(IOException.class);

        try {
            executeLauncher();
        } finally {
            Map<String, String> actionData = launcherAM.getActionData();
            assertThat(actionData, not(hasKey(ACTION_DATA_EXTERNAL_CHILD_IDS)));
            verify(launcherCallbackNotifierMock).notifyURL(OozieActionResult.FAILED);
        }
    }

    @Test
    public void testRecoveryIdNotSet() throws Exception {
        launcherJobConfig.unset(LauncherAMUtils.OOZIE_ACTION_RECOVERY_ID);
        instantiateLauncher();

        executeLauncher();

        failureDetails.expectedExceptionMessage("IO error")
            .expectedErrorCode(EXIT_CODE_0)
            .expectedErrorReason("IO error")
            .withStackTrace();

        assertFailedExecution();
    }

    @Test
    public void testRecoveryIdExistsAndRecoveryIsdMatch() throws Exception {
        given(hdfsOperationsMock.fileExists(any(Path.class), eq(launcherJobConfig))).willReturn(true);
        given(hdfsOperationsMock.readFileContents(any(Path.class), eq(launcherJobConfig))).willReturn(applicationId);

        executeLauncher();

        verify(hdfsOperationsMock).readFileContents(any(Path.class), eq(launcherJobConfig));
    }

    @Test
    public void testRecoveryIdExistsAndRecoveryIdsDoNotMatch() throws Exception {
        String newAppId = "not_matching_appid";
        given(hdfsOperationsMock.fileExists(any(Path.class), eq(launcherJobConfig))).willReturn(true);
        given(hdfsOperationsMock.readFileContents(any(Path.class), eq(launcherJobConfig))).willReturn(newAppId);

        executeLauncher();

        String errorMessage = MessageFormat.format(
                "YARN Id mismatch, action file [{0}] declares Id [{1}] current Id [{2}]", "dummy/1",
                newAppId,
                applicationId);

        failureDetails.expectedExceptionMessage(errorMessage)
            .expectedErrorCode(EXIT_CODE_0)
            .expectedErrorReason(errorMessage)
            .withStackTrace();

        verify(hdfsOperationsMock).readFileContents(any(Path.class), eq(launcherJobConfig));
        assertFailedExecution();
    }

    @Test
    public void testReadingRecoveryIdFails() throws Exception {
        willThrow(new IOException()).given(hdfsOperationsMock)
            .writeStringToFile(any(Path.class), eq(launcherJobConfig), eq(applicationId));

        executeLauncher();

        failureDetails.expectedExceptionMessage("IO error")
            .expectedErrorCode(EXIT_CODE_0)
            .expectedErrorReason("IO error")
            .withStackTrace();

        assertFailedExecution();
    }

    private void instantiateLauncher() {
        launcherAM = new LauncherAM(amRMClientAsyncFactoryMock,
                callbackHandlerMock,
                hdfsOperationsMock,
                localFsOperationsMock,
                prepareHandlerMock,
                launcherCallbackNotifierFactoryMock,
                launcherSecurityManagerMock,
                containerId, launcherJobConfig);
    }

     @SuppressWarnings("unchecked")
    private void configureMocksForHappyPath() throws Exception {
        launcherJobConfig.set(LauncherAM.OOZIE_ACTION_DIR_PATH, "dummy");
        launcherJobConfig.set(LauncherAM.OOZIE_JOB_ID, "dummy");
        launcherJobConfig.set(LauncherAM.OOZIE_ACTION_ID, "dummy");
        launcherJobConfig.set(LauncherAM.CONF_OOZIE_ACTION_MAIN_CLASS, LauncherAMTestMainClass.class.getCanonicalName());

        given(localFsOperationsMock.readLauncherConf()).willReturn(launcherJobConfig);
        given(localFsOperationsMock.fileExists(any(File.class))).willReturn(true);
        willReturn(amRmAsyncClientMock).given(amRMClientAsyncFactoryMock)
                .createAMRMClientAsync(anyInt(), any(AMRMCallBackHandler.class));
        given(launcherCallbackNotifierFactoryMock.createCallbackNotifier(any(Configuration.class)))
            .willReturn(launcherCallbackNotifierMock);
    }

    private void setupActionOutputContents() throws IOException {
        // output files generated by an action
        given(localFsOperationsMock.getLocalFileContentAsString(any(File.class), eq(ACTION_DATA_EXTERNAL_CHILD_IDS), anyInt()))
            .willReturn(ACTION_DATA_EXTERNAL_CHILD_IDS);

        given(localFsOperationsMock.getLocalFileContentAsString(any(File.class), eq(ACTION_DATA_NEW_ID), anyInt()))
            .willReturn(ACTION_DATA_NEW_ID);

        given(localFsOperationsMock.getLocalFileContentAsString(any(File.class), eq(ACTION_DATA_OUTPUT_PROPS), anyInt()))
            .willReturn(ACTION_DATA_OUTPUT_PROPS);

        given(localFsOperationsMock.getLocalFileContentAsString(any(File.class), eq(ACTION_DATA_STATS), anyInt()))
            .willReturn(ACTION_DATA_STATS);
    }

    private void setupArgsForMainClass(final String...  args) {
        launcherJobConfig.set(String.valueOf(LauncherAM.CONF_OOZIE_ACTION_MAIN_ARG_COUNT), String.valueOf(args.length));

        for (int i = 0; i < args.length; i++) {
            launcherJobConfig.set(String.valueOf(LauncherAM.CONF_OOZIE_ACTION_MAIN_ARG_PREFIX + i), args[i]);
        }
    }

    private void executeLauncher() throws Exception {
        launcherAM.run();
    }

    @SuppressWarnings("unchecked")
    private void assertSuccessfulExecution(OozieActionResult actionResult) throws Exception {
        verify(amRmAsyncClientMock).registerApplicationMaster(anyString(), anyInt(), anyString());
        verify(amRmAsyncClientMock).unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, EMPTY_STRING, EMPTY_STRING);
        verify(amRmAsyncClientMock).stop();
        verify(hdfsOperationsMock).uploadActionDataToHDFS(any(Configuration.class), any(Path.class), any(Map.class));
        verify(launcherCallbackNotifierFactoryMock).createCallbackNotifier(any(Configuration.class));
        verify(launcherCallbackNotifierMock).notifyURL(actionResult);
        verify(hdfsOperationsMock).writeStringToFile(any(Path.class), any(Configuration.class), any(String.class));

        Map<String, String> actionData = launcherAM.getActionData();
        verifyFinalStatus(actionData, actionResult);
        verifyNoError(actionData);
    }

    private void assertActionOutputDataPresentAndCorrect() {
        Map<String, String> actionData = launcherAM.getActionData();
        String extChildId = actionData.get(ACTION_DATA_EXTERNAL_CHILD_IDS);
        String stats = actionData.get(ACTION_DATA_STATS);
        String output = actionData.get(ACTION_DATA_OUTPUT_PROPS);
        String idSwap = actionData.get(ACTION_DATA_NEW_ID);

        assertThat("extChildID output", ACTION_DATA_EXTERNAL_CHILD_IDS, equalTo(extChildId));
        assertThat("stats output", ACTION_DATA_STATS, equalTo(stats));
        assertThat("action output", ACTION_DATA_OUTPUT_PROPS, equalTo(output));
        assertThat("idSwap output", ACTION_DATA_NEW_ID, equalTo(idSwap));
    }

    private void assertNoActionOutputData() {
        Map<String, String> actionData = launcherAM.getActionData();
        String extChildId = actionData.get(ACTION_DATA_EXTERNAL_CHILD_IDS);
        String stats = actionData.get(ACTION_DATA_STATS);
        String output = actionData.get(ACTION_DATA_OUTPUT_PROPS);
        String idSwap = actionData.get(ACTION_DATA_NEW_ID);

        assertThat("extChildId", extChildId, nullValue());
        assertThat("stats", stats, nullValue());
        assertThat("Output", output, nullValue());
        assertThat("idSwap", idSwap, nullValue());
    }

    private void assertFailedExecution() throws Exception {
        Map<String, String> actionData = launcherAM.getActionData();
        verify(launcherCallbackNotifierFactoryMock).createCallbackNotifier(any(Configuration.class));
        verify(launcherCallbackNotifierMock).notifyURL(OozieActionResult.FAILED);
        verifyFinalStatus(actionData, OozieActionResult.FAILED);

        // Note: actionData contains properties inside a property, so we have to extract them into a new Property object
        String fullError = actionData.get(ACTIONDATA_ERROR_PROPERTIES);
        Properties props = new Properties();
        props.load(new StringReader(fullError));

        String errorReason = props.getProperty(ERROR_REASON_PROPERTY);
        if (failureDetails.expectedErrorReason != null) {
            assertThat("errorReason", errorReason, containsString(failureDetails.expectedErrorReason));
        } else {
            assertThat("errorReason", errorReason, nullValue());
        }

        String exceptionMessage = props.getProperty(EXCEPTION_MESSAGE_PROPERTY);
        if (failureDetails.expectedExceptionMessage != null) {
            assertThat("exceptionMessage", exceptionMessage, containsString(failureDetails.expectedExceptionMessage));
        } else {
            assertThat("exceptionMessage", exceptionMessage, nullValue());
        }

        String stackTrace = props.getProperty(EXCEPTION_STACKTRACE_PROPERTY);
        if (failureDetails.hasStackTrace) {
            assertThat("stackTrace", stackTrace, notNullValue());
        } else {
            assertThat("stackTrace", stackTrace, nullValue());
        }

        String errorCode = props.getProperty(ERROR_CODE_PROPERTY);
        assertThat("errorCode", errorCode, equalTo(failureDetails.expectedErrorCode));
    }

    private void verifyFinalStatus(Map<String, String> actionData, OozieActionResult actionResult) {
        String finalStatus = actionData.get(ACTIONDATA_FINAL_STATUS_PROPERTY);
        assertThat("actionResult", actionResult.toString(), equalTo(finalStatus));
    }

    private void verifyNoError(Map<String, String> actionData) {
        String fullError = actionData.get(ACTIONDATA_ERROR_PROPERTIES);
        assertThat("error properties", fullError, nullValue());
    }

    private class ExpectedFailureDetails {
        String expectedExceptionMessage;
        String expectedErrorCode;
        String expectedErrorReason;
        boolean hasStackTrace;

        public ExpectedFailureDetails expectedExceptionMessage(String expectedExceptionMessage) {
            this.expectedExceptionMessage = expectedExceptionMessage;
            return this;
        }

        public ExpectedFailureDetails expectedErrorCode(String expectedErrorCode) {
            this.expectedErrorCode = expectedErrorCode;
            return this;
        }

        public ExpectedFailureDetails expectedErrorReason(String expectedErrorReason) {
            this.expectedErrorReason = expectedErrorReason;
            return this;
        }

        public ExpectedFailureDetails withStackTrace() {
            this.hasStackTrace = true;
            return this;
        }

        public ExpectedFailureDetails withoutStackTrace() {
            this.hasStackTrace = false;
            return this;
        }
    }

    @Test
    public void testRecoveryWritesJobId() throws IOException, InterruptedException, LauncherException,
            NoSuchFieldException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        //create empty file on the following path: ACTION_DIR/RECOVERY_ID
        final Path path = new Path(ACTION_DIR, "1");
        when(hdfsOperationsMock.readFileContents(any(Path.class), eq(launcherJobConfig))).thenReturn(EMPTY_STRING);
        when(hdfsOperationsMock.fileExists(any(Path.class), eq(launcherJobConfig))).thenReturn(true);

        //run launchermapper with the same ACTION_DIR/RECOVERY_ID
        final Field f = launcherAM.getClass().getDeclaredField("actionDir");
        f.setAccessible(true);
        f.set(launcherAM, path);
        final Method m = launcherAM.getClass().getDeclaredMethod("setRecoveryId");
        m.setAccessible(true);
        m.invoke(launcherAM);

        //check empty file, launcherMapper should have written RECOVERY_ID into it
        Mockito.verify(hdfsOperationsMock).writeStringToFile(
                eq(new Path(path, "1")),
                eq(launcherJobConfig),
                eq("application_1479473450392_0001"));

    }
}
