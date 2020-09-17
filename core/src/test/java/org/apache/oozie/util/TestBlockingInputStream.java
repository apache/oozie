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

import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestBlockingInputStream extends DrainerTestCase {
    @Test
    public void testFastWritingBlockingInputStream() throws Exception {
        checkSampleStrings( new StringProcessingCallback() {
            public void call(String sampleString) {
                readFastBlockingInputStreamAndAssert(sampleString);
            }
        });
    }

    private void readFastBlockingInputStreamAndAssert(String sampleString) {
        byte []sampleByteArray = sampleString.getBytes(StandardCharsets.UTF_8);
        boolean simulateSlowWriting = false;
        BlockingInputStream sampleStream = new BlockingInputStream(sampleByteArray, simulateSlowWriting);
        readTillStreamEndAndAssert(sampleByteArray, sampleStream, true);
    }

    @Test
    public void testSlowWritingBlockingInputStream() throws Exception {
        checkSampleStrings( new StringProcessingCallback() {
            public void call(String sampleString) {
                readSlowBlockingInputStreamAndAssert(sampleString);
            }
        });
    }

    private void readSlowBlockingInputStreamAndAssert(String sampleString) {
        byte []sampleByteArray = sampleString.getBytes(StandardCharsets.UTF_8);
        boolean simulateSlowWriting = true;
        BlockingInputStream sampleStream = new BlockingInputStream(sampleByteArray, simulateSlowWriting);
        assertEquals("No characters should be available to read", 0, sampleStream.available());
        assertTrue("Stream should be in blocked state",sampleString.isEmpty() || sampleStream.checkBlockedAndTryWriteNextChunk());
        waitAndWriteNextChunks(sampleStream, 600);
        readTillStreamEndAndAssert(sampleByteArray, sampleStream, true);
    }

    private void waitAndWriteNextChunks(BlockingInputStream sampleStream, int sleepTimeMs) {
        // at most 4 steps should be enough to produce the full content or reach the buffer limit
        for (int i = 0; i < 4; ++i) {
            waitAndWriteNextChunk(sampleStream, sleepTimeMs);
        }
    }

    private void waitAndWriteNextChunk(BlockingInputStream sampleStream, int sleepTimeMs) {
        try {
            Thread.sleep(sleepTimeMs);
        } catch (final InterruptedException ignoredException) {
        }
        sampleStream.checkBlockedAndTryWriteNextChunk();
    }

    @Test
    public void testLimitedWritingBlockingInputStream() throws Exception {
        checkSampleStrings( new StringProcessingCallback() {
            public void call(String sampleString) {
                readLimitedBlockingInputStreamAndAssert(sampleString);
            }
        });
    }

    private void readLimitedBlockingInputStreamAndAssert(String sampleString) {
        byte []sampleByteArray = sampleString.getBytes(StandardCharsets.UTF_8);
        boolean simulateSlowWriting = true;
        BlockingInputStream sampleStream = new BlockingInputStream(sampleByteArray, simulateSlowWriting);
        int bufferSize = sampleString.length() / 2;
        int []smallPauseTimes = {10};
        sampleStream.setPauseTimes(smallPauseTimes);
        sampleStream.setBufferSize(bufferSize);
        assertEquals("No characters should be available to read", 0, sampleStream.available());
        assertTrue("Stream should be in blocked state",sampleString.isEmpty() || sampleStream.checkBlockedAndTryWriteNextChunk());
        waitAndWriteNextChunks(sampleStream, 20);
        // buffersize should block the stream
        assertTrue("Stream should be in blocked state",sampleString.isEmpty() || sampleStream.checkBlockedAndTryWriteNextChunk());
        assertEquals("Invalid number of characters available", bufferSize, sampleStream.available());
        readTillStreamEndAndAssert(sampleByteArray, sampleStream, false);
    }

    private void readTillStreamEndAndAssert(byte[] sampleByteArray, BlockingInputStream sampleStream,
                                            boolean assertingAlreadyNonBlocked) {
        int availableCharacters = sampleStream.available();
        if (assertingAlreadyNonBlocked) {
            assertFalse("Stream should not be in blocked state", sampleStream.checkBlockedAndTryWriteNextChunk());
            assertEquals("Invalid number of characters available", sampleByteArray.length, availableCharacters);
        }
        byte[] outputByteArray = new byte[sampleByteArray.length];
        int writeIndex=0;
        while (availableCharacters > 0) {
            int bytesRead = sampleStream.read(outputByteArray, writeIndex, availableCharacters);
            assertEquals("Invalid number of characters read", availableCharacters, bytesRead);
            writeIndex += bytesRead;
            waitAndWriteNextChunk(sampleStream, 20);
            availableCharacters = sampleStream.available();
        }
        assertTrue("Content read mismatch", Arrays.equals(sampleByteArray, outputByteArray));
    }

    @Test
    public void testFailure() throws Exception {
        checkSampleStrings( new StringProcessingCallback() {
            public void call(String sampleString) {
                simulateFailureAndAssert(sampleString);
            }
        });
    }

    private void simulateFailureAndAssert(String sampleString) {
        byte []sampleByteArray = sampleString.getBytes(StandardCharsets.UTF_8);
        boolean simulateSlowWriting = false;
        BlockingInputStream sampleStream = new BlockingInputStream(sampleByteArray, simulateSlowWriting);
        int availableCharacters = sampleStream.available();
        assertFalse("Stream should not be in blocked state", sampleStream.checkBlockedAndTryWriteNextChunk());
        assertEquals("Invalid number of characters available", sampleByteArray.length, availableCharacters);
        if (availableCharacters > 0) {
            byte []outputByteArray = new byte[1];
            int bytesRead = sampleStream.read(outputByteArray, 0, 1);
            assertEquals("Invalid number of characters read", 1, bytesRead);
        }
        sampleStream.simulateFailure();
        assertTrue("Stream should be in blocked state", sampleStream.checkBlockedAndTryWriteNextChunk());
        int availableCharactersAfterFailure = sampleStream.available();
        assertEquals("Invalid numbe of characters available after failure",
                Math.max(availableCharacters-1, 0), availableCharactersAfterFailure);
    }
}