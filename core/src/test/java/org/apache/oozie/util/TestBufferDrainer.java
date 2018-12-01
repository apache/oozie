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

package org.apache.oozie.util;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestBufferDrainer extends DrainerTestCase {
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private AssertionError assertionError;
    private IOException ioException;

    @Test
    public void testTooEarlyInputStreamRead() {
        final BufferDrainer bufferDrainer = new BufferDrainer(null, 0);
        expectedException.expect(IllegalStateException.class);
        bufferDrainer.getInputBuffer();
    }

    @Test
    public void testTooEarlyErrorStreamRead() {
        final BufferDrainer bufferDrainer = new BufferDrainer(null, 0);
        expectedException.expect(IllegalStateException.class);
        bufferDrainer.getErrorBuffer();
    }

    @Test
    public void testMultipleDrainBufferCalls() throws IOException {
        Process process = BlockingWritesExitValueProcess.createFastWritingProcess("", "");
        final BufferDrainer bufferDrainer = new BufferDrainer(process, 0);
        bufferDrainer.drainBuffers();
        expectedException.expect(IllegalStateException.class);
        bufferDrainer.drainBuffers();
    }

    @Test
    public void testReadSinglePass() throws Exception {
        checkSampleStringWithDifferentMaxLength( new StringAndIntProcessingCallback() {
            public void call(String sampleString, int suggestedMaxLength) throws IOException {
                readSinglePassAndAssert(sampleString, suggestedMaxLength);
            }
        });
    }

    private void readSinglePassAndAssert(String sampleString, int suggestedMaxLength) throws IOException {
        BufferedReader sampleBufferedReader = new BufferedReader(new StringReader(sampleString));
        StringBuffer storageBuffer = new StringBuffer();
        boolean readAll = false;
        int charRead = BufferDrainer.drainBuffer(sampleBufferedReader, storageBuffer, suggestedMaxLength, 0, readAll);
        assertTrue("Some characters should have been read but none was", sampleString.length() == 0 || charRead > 0);
        assertTrue("Read character count mismatch", charRead <= sampleString.length());
        assertTrue("Content read mismatch", sampleString.startsWith(storageBuffer.toString()));
    }

    @Test
    public void testReadTillAvailable() throws Exception {
        checkSampleStringWithDifferentMaxLength( new StringAndIntProcessingCallback() {
            public void call(String sampleString, int suggestedMaxLength) throws IOException {
                readTillAvailableAndAssert(sampleString, suggestedMaxLength);
            }
        });
    }

    private void readTillAvailableAndAssert(String sampleString, int suggestedMaxLength) throws IOException {
        BufferedReader sampleBufferedReader = new BufferedReader(new StringReader(sampleString));
        StringBuffer storageBuffer = new StringBuffer();
        boolean readAll = true;
        int charRead = BufferDrainer.drainBuffer(sampleBufferedReader, storageBuffer, suggestedMaxLength, 0, readAll);
        assertTrue("Read character count mismatch", charRead <= sampleString.length());
        assertTrue("Content read mismatch", sampleString.startsWith(storageBuffer.toString()));
    }

    @Test
    public void testDrainBuffersImmediatelyEndingProcess() throws Exception {
        checkSampleStringWithDifferentMaxLength( new StringAndIntProcessingCallback() {
            public void call(String sampleString, int suggestedMaxLength) throws IOException {
                checkDrainBuffers(1, sampleString, "", suggestedMaxLength);
                checkDrainBuffers(1, "", sampleString.toLowerCase(), suggestedMaxLength);
                checkDrainBuffers(1, sampleString, sampleString.toLowerCase(), suggestedMaxLength);
            }
        });
    }

    @Test
    public void testDrainBuffersShortProcess() throws Exception {
        checkSampleStringWithDifferentMaxLength( new StringAndIntProcessingCallback() {
            public void call(String sampleString, int suggestedMaxLength) throws IOException {
                checkDrainBuffers(2, sampleString, "", suggestedMaxLength);
                checkDrainBuffers(2, "", sampleString.toLowerCase(), suggestedMaxLength);
                checkDrainBuffers(2, sampleString, sampleString.toLowerCase(), suggestedMaxLength);
            }
        });
    }

    private void checkDrainBuffers(int runningSteps, String outputString, String errorString, int maxLength) throws IOException {
        Process process = mock(Process.class);
        when(process.exitValue()).thenAnswer(new Answer() {
            private int invocationCounter = 0;

            public Object answer(InvocationOnMock invocation) {
                if (++invocationCounter == runningSteps) {
                    return BlockingWritesExitValueProcess.EXIT_VALUE;
                }
                throw new IllegalThreadStateException("Process is still running");
            }
        });
        byte []outputByteArray = outputString.getBytes(StandardCharsets.UTF_8);
        byte []errorByteArray = errorString.getBytes(StandardCharsets.UTF_8);
        doReturn(new ByteArrayInputStream(outputByteArray)).when(process).getInputStream();
        doReturn(new ByteArrayInputStream(errorByteArray)).when(process).getErrorStream();
        checkDrainBuffers(process, outputString, errorString, maxLength);
    }

    @Test
    public void testDrainBuffersFast() throws Exception {
        checkSampleStringWithDifferentMaxLength( new StringAndIntProcessingCallback() {
            public void call(String sampleString, int suggestedMaxLength) throws IOException, InterruptedException {
                checkDrainBufferFast(sampleString, "", suggestedMaxLength);
                checkDrainBufferFast("", sampleString.toLowerCase(), suggestedMaxLength);
                checkDrainBufferFast(sampleString, sampleString.toLowerCase(), suggestedMaxLength);
            }
        });
    }

    private void checkDrainBufferFast(String outputString, String errorString, int suggestedMaxLength)
            throws IOException, InterruptedException {
        Process process = BlockingWritesExitValueProcess.createFastWritingProcess(outputString, errorString);
        int timeout = calculateTimeoutForTest(false, null);
        drainProcessAndCheckTimeout(process, timeout, outputString, errorString, suggestedMaxLength);
    }

    @Test
    public void testDrainBuffersSlowWrite() throws Exception {
        checkSampleStringWithDifferentMaxLength( new StringAndIntProcessingCallback() {
            public void call(String sampleString, int suggestedMaxLength) throws IOException, InterruptedException {
                checkDrainBufferSlow( sampleString, "", suggestedMaxLength);
                checkDrainBufferSlow("", sampleString.toLowerCase(), suggestedMaxLength);
                checkDrainBufferSlow(sampleString, sampleString.toLowerCase(), suggestedMaxLength);
            }
        });
    }

    private void checkDrainBufferSlow(String outputString, String errorString, int suggestedMaxLength)
            throws IOException, InterruptedException {
        int catBufferSize = Math.max(outputString.length()/2, errorString.length()/2);
        Process process = BlockingWritesExitValueProcess.createBufferLimitedProcess(outputString, errorString, catBufferSize);
        int []defaultPauseTimes = {500};
        int timeout = calculateTimeoutForTest(true, defaultPauseTimes);
        drainProcessAndCheckTimeout(process, timeout, outputString, errorString, suggestedMaxLength);
    }

    private void drainProcessAndCheckTimeout(Process process, int timeout, String outputString, String errorString, int maxLength)
            throws IOException, InterruptedException {
        long timeBefore = System.currentTimeMillis();
        assertionError = null;
        Thread t = createBufferDrainerThread(process, outputString, errorString, maxLength);
        t.start();
        t.join(timeout);
        long timeToRun = System.currentTimeMillis() - timeBefore;
        assertTrue("drainBuffer test timed out after "+ timeToRun+" ms",  timeToRun < timeout);
        if (assertionError != null) {
            throw assertionError;
        }
        if (ioException != null) {
            throw ioException;
        }
    }

    private int calculateTimeoutForTest(boolean simulateSlowWriting, int[] pauseTimes) {
        int basicTimeout = 1000;
        if (simulateSlowWriting) {
            int timeout = basicTimeout;
            for (int i=0; i<4; ++i) {
                int nextPause = pauseTimes == null ? 500 : pauseTimes[i % pauseTimes.length];
                timeout += 2 * nextPause;
            }
            return timeout;
        }
        else {
            return basicTimeout;
        }
    }

    @Test
    public void testDrainBuffersLongPause() throws Exception {
        checkSampleStringWithDifferentMaxLength( new StringAndIntProcessingCallback() {
            public void call(String sampleString, int suggestedMaxLength) throws IOException, InterruptedException {
                // only run some of the tests because of the speed limit
                boolean runThisTest = sampleString.length() == 0 || sampleString.length() < 1024*1024 && suggestedMaxLength > 1024;
                if (runThisTest) {
                    checkDrainBuffersLongPause( sampleString, "", suggestedMaxLength);
                    checkDrainBuffersLongPause("", sampleString.toLowerCase(), suggestedMaxLength);
                    checkDrainBuffersLongPause(sampleString, sampleString.toLowerCase(), suggestedMaxLength);
                }
            }
        });
    }

    private void checkDrainBuffersLongPause(String outputString, String errorString, int suggestedMaxLength)
            throws IOException, InterruptedException {
        int[] longPauseMs = {10*1000};
        Process process = BlockingWritesExitValueProcess.createPausedProcess(outputString, errorString, longPauseMs);
        int timeout = calculateTimeoutForTest(true, longPauseMs);
        drainProcessAndCheckTimeout(process, timeout, outputString, errorString, suggestedMaxLength);
    }

    @Test
    public void testDrainBuffersRandomPause() throws Exception {
        checkSampleStringWithDifferentMaxLength( new StringAndIntProcessingCallback() {
            public void call(String sampleString, int suggestedMaxLength) throws IOException, InterruptedException {
                checkDrainBuffersRandomPause( sampleString, "", suggestedMaxLength);
                checkDrainBuffersRandomPause("", sampleString.toLowerCase(), suggestedMaxLength);
                checkDrainBuffersRandomPause(sampleString, sampleString.toLowerCase(), suggestedMaxLength);
            }
        });
    }

    private void checkDrainBuffersRandomPause(String outputString, String errorString, int suggestedMaxLength)
            throws IOException, InterruptedException {
        int[] pauseTimes = generateRandomPauseIntervals();
        Process process = BlockingWritesExitValueProcess.createPausedProcess(outputString, errorString, pauseTimes);
        int timeout = calculateTimeoutForTest(true, pauseTimes);
        drainProcessAndCheckTimeout(process, timeout, outputString, errorString, suggestedMaxLength);
    }

    private int[] generateRandomPauseIntervals() {
        int[] longPauseMs = new int[4];
        for (int i=0; i<longPauseMs.length; ++i) {
            longPauseMs[i] = new Random().nextInt((2000)) + 100;
        }
        return longPauseMs;
    }

    private void checkDrainBuffers(Process process, String outputString, String errorString, int maxLength) throws IOException {
        BufferDrainer bufferDrainer = new BufferDrainer(process, maxLength);
        int exitValue = bufferDrainer.drainBuffers();
        assertEquals("Invalid exit Value", BlockingWritesExitValueProcess.EXIT_VALUE, exitValue);
        StringBuffer inputBuffer = bufferDrainer.getInputBuffer();
        StringBuffer errorBuffer = bufferDrainer.getErrorBuffer();
        assertTrue("Invalid input buffer length", inputBuffer.toString().length() >= Math.min(outputString.length(), maxLength));
        assertTrue("Invalid input buffer", outputString.startsWith(inputBuffer.toString()));
        assertTrue("Invalid error buffer", errorString.startsWith(errorBuffer.toString()));
    }

    @Test
    public void testParallelDrainBuffers() throws Exception {
        String sampleString = generateString(1024);
        checkParallelDrainBuffers(20, sampleString, sampleString.toLowerCase(), false);
    }

    @Test
    public void testParallelDrainBuffersWithFailure() throws Exception {
        String sampleString = generateString(1024);
        checkParallelDrainBuffers(20, sampleString, sampleString.toLowerCase(), true);
    }

    private void checkParallelDrainBuffers(int threadNum, String outputString, String errorString, boolean alsoAddFailingThread)
            throws IOException, InterruptedException {
        Thread []threads = new Thread[threadNum];
        assertionError = null;
        int maxTimeout = 0;
        if (alsoAddFailingThread) {
            BlockingWritesExitValueProcess failingProcess = BlockingWritesExitValueProcess.createFastWritingProcess("","");
            createBufferDrainerThread(failingProcess, "", "", 1024).start();
            failingProcess.simulateFailure();
        }
        for (int i=0; i<threads.length; ++i) {
            String randomizedOutputString = randomSubstring(outputString);
            String randomizedErrorString = randomSubstring(errorString);
            int[] pauseTimes = generateRandomPauseIntervals();
            Process process = BlockingWritesExitValueProcess.createPausedProcess(randomizedOutputString, randomizedErrorString,
                    pauseTimes);
            int timeout = calculateTimeoutForTest(true, pauseTimes);
            if (timeout > maxTimeout) {
                maxTimeout = timeout;
            }
            threads[i] = createBufferDrainerThread(process, randomizedOutputString, randomizedErrorString, 1024);
            threads[i].start();
        }
        waitThreadsToFinishInTime(threads, maxTimeout);
        if (assertionError != null) {
            throw assertionError;
        }
        if (ioException != null) {
            throw ioException;
        }
    }

    private Thread createBufferDrainerThread(Process process, String randomizedOutputString, String randomizedErrorString,
                                             int maxLength) {
        return new Thread() {
            public void run() {
                try {
                    checkDrainBuffers(process, randomizedOutputString, randomizedErrorString, maxLength);
                } catch (IOException e) {
                    ioException = e;
                } catch (AssertionError e) {
                    assertionError = e;
                }
            }
        };
    }

    private void waitThreadsToFinishInTime(Thread[] threads, int maxTimeout) throws InterruptedException {
        long timeBefore = System.currentTimeMillis();
        for (Thread t : threads) {
            long timeToRunSoFar = System.currentTimeMillis() - timeBefore;
            long remainingTime = maxTimeout - timeToRunSoFar;
            if (remainingTime > 0) {
                t.join(remainingTime);
            }
            else {
                fail("drainBuffer test timed out after "+ timeToRunSoFar+" ms");
            }
        }
        long timeToRunTotal = System.currentTimeMillis() - timeBefore;
        assertTrue("drainBuffer test timed out after "+ timeToRunTotal+" ms",  timeToRunTotal < maxTimeout);
    }

    private String randomSubstring(String string) {
        int minLength = (int)(0.8 * string.length());
        int maxLength = string.length();
        int length = new Random().nextInt((maxLength - minLength) + 1) + minLength;
        return string.substring(0, length);
    }
}