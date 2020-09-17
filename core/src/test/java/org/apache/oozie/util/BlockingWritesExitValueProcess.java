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

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Simulates different processes writing to standard output / error.
 * <p>
 * {code exitValue()} throws {@code IllegalThreadStateException} while the buffers are blocked.
 * <p>
 * Uses {@code BlockInputStream}
 */
public class BlockingWritesExitValueProcess extends Process {
    static final int EXIT_VALUE = 1;

    private BlockingInputStream inputStream;
    private BlockingInputStream errorStream;
    private String outputString;
    private String errorString;
    private int bufferSize = Integer.MAX_VALUE;
    private boolean simulateSlowWriting;
    private int[] outputPauseTimes;
    private int[] errorPauseTimes;

    private BlockingWritesExitValueProcess(String outputString, String errorString) {
        this.outputString = outputString;
        this.errorString = errorString;
    }

    static BlockingWritesExitValueProcess createFastWritingProcess(String outputString, String errorString) {
        BlockingWritesExitValueProcess process = new BlockingWritesExitValueProcess(outputString, errorString);
        process.generateStreams();
        return process;
    }

    static BlockingWritesExitValueProcess createBufferLimitedProcess(String outputString, String errorString, int bufferSize) {
        BlockingWritesExitValueProcess process = new BlockingWritesExitValueProcess(outputString, errorString);
        process.simulateSlowWriting = true;
        process.bufferSize = bufferSize;
        process.generateStreams();
        return process;
    }

    static BlockingWritesExitValueProcess createPausedProcess(String outputString, String errorString, int[] pauseTimes) {
        BlockingWritesExitValueProcess process = new BlockingWritesExitValueProcess(outputString, errorString);
        process.simulateSlowWriting = true;
        process.outputPauseTimes = pauseTimes;
        process.errorPauseTimes = pauseTimes;
        process.generateStreams();
        return process;
    }

    private void generateStreams() {
        byte []outputByteArray = outputString.getBytes(StandardCharsets.UTF_8);
        byte []errorByteArray = errorString.getBytes(StandardCharsets.UTF_8);
        inputStream = new BlockingInputStream(outputByteArray, simulateSlowWriting);
        if (simulateSlowWriting) {
            inputStream.setBufferSize(bufferSize);
        }
        if (outputPauseTimes != null) {
            inputStream.setPauseTimes(outputPauseTimes);
        }
        errorStream = new BlockingInputStream(errorByteArray, simulateSlowWriting);
        if (simulateSlowWriting) {
            errorStream.setBufferSize(bufferSize);
        }
        if (errorPauseTimes != null) {
            errorStream.setPauseTimes(errorPauseTimes);
        }
    }


    @Override
    public OutputStream getOutputStream() {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getInputStream() {
        return inputStream;
    }

    @Override
    public InputStream getErrorStream() {
        return errorStream;
    }

    @Override
    public int waitFor() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int exitValue() {
        boolean inputStreamBlocked = inputStream.checkBlockedAndTryWriteNextChunk();
        boolean errorStreamBlocked = errorStream.checkBlockedAndTryWriteNextChunk();
        if (inputStreamBlocked || errorStreamBlocked) {
            throw new IllegalThreadStateException("Process is still running");
        }
        return EXIT_VALUE;
    }

    @Override
    public void destroy() {
        throw new UnsupportedOperationException();
    }

    void simulateFailure() {
        inputStream.simulateFailure();
        errorStream.simulateFailure();
    }

}
