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

import com.google.common.annotations.VisibleForTesting;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class BufferDrainer {

    private final XLog LOG = XLog.getLog(getClass());
    private static final int DRAIN_BUFFER_SLEEP_TIME_MS = 500;
    private final Process process;
    private final int maxLength;
    private boolean drainBuffersFinished;
    private final StringBuffer inputBuffer;
    private final StringBuffer errorBuffer;

    /**
     * @param process The Process instance.
     * @param maxLength The maximum data length to be stored in these buffers. This is an indicative value, and the
     * store content may exceed this length.
     */
    public BufferDrainer(Process process, int maxLength) {
        this.process = process;
        this.maxLength = maxLength;
        drainBuffersFinished = false;
        inputBuffer = new StringBuffer();
        errorBuffer = new StringBuffer();
    }

    /**
     * Drains the inputStream and errorStream of the Process being executed. The contents of the streams are stored if a
     * buffer is provided for the stream.
     *
     * @return the exit value of the processSettings.
     * @throws IOException if IO related issue occurs
     */
    public int drainBuffers() throws IOException {
        if (drainBuffersFinished) {
            throw new IllegalStateException("Buffer draining has already been finished");
        }
        LOG.trace("drainBuffers() start");

        int exitValue = -1;

        int inBytesRead = 0;
        int errBytesRead = 0;

        boolean processEnded = false;

        try (final BufferedReader ir =
                     new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
             final BufferedReader er =
                     new BufferedReader(new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8))) {
            // Here we do some kind of busy waiting, checking whether the process has finished by calling Process#exitValue().
            // If not yet finished, an IllegalThreadStateException is thrown and ignored, the progress on stdout and stderr read,
            // and retried until the process has ended.
            // Note that Process#waitFor() may block sometimes, that's why we do a polling mechanism using Process#exitValue()
            // instead. Until we extend unit and integration test coverage for SSH action, and we can introduce a more
            // sophisticated error handling based on the extended coverage, this solution should stay in place.
            while (!processEnded) {
                try {
                    // Doesn't block but throws IllegalThreadStateException if the process hasn't finished yet
                    exitValue = process.exitValue();
                    processEnded = true;
                }
                catch (final IllegalThreadStateException itse) {
                    // Continue to drain
                }

                // Drain input and error streams
                inBytesRead += drainBuffer(ir, inputBuffer, maxLength, inBytesRead, processEnded);
                errBytesRead += drainBuffer(er, errorBuffer, maxLength, errBytesRead, processEnded);

                // Necessary evil: sleep and retry
                if (!processEnded) {
                    try {
                        LOG.trace("Sleeping {}ms during buffer draining", DRAIN_BUFFER_SLEEP_TIME_MS);
                        Thread.sleep(DRAIN_BUFFER_SLEEP_TIME_MS);
                    }
                    catch (final InterruptedException ie) {
                        // Sleep a little, then check again
                    }
                }
            }
        }

        LOG.trace("drainBuffers() end [exitValue={0}]", exitValue);
        drainBuffersFinished = true;
        return exitValue;
    }

    public StringBuffer getInputBuffer() {
        if (drainBuffersFinished) {
            return inputBuffer;
        }
        else {
            throw new IllegalStateException("Buffer draining has not been finished yet");
        }
    }

    public StringBuffer getErrorBuffer() {
        if (drainBuffersFinished) {
            return errorBuffer;
        }
        else {
            throw new IllegalStateException("Buffer draining has not been finished yet");
        }
    }

    /**
     * Reads the contents of a stream and stores them into the provided buffer.
     *
     * @param br The stream to be read.
     * @param storageBuf The buffer into which the contents of the stream are to be stored.
     * @param maxLength The maximum number of bytes to be stored in the buffer. An indicative value and may be
     * exceeded.
     * @param bytesRead The number of bytes read from this stream to date.
     * @param readAll If true, the stream is drained while their is data available in it. Otherwise, only a single chunk
     * of data is read, irrespective of how much is available.
     * @return bReadSession returns drainBuffer for stream of contents
     * @throws IOException
     */
    @VisibleForTesting
    static int drainBuffer(BufferedReader br, StringBuffer storageBuf, int maxLength, int bytesRead, boolean readAll)
            throws IOException {
        int bReadSession = 0;
        int chunkSize = 1024;
        if (br.ready()) {
            char[] buf = new char[1024];
            int bReadCurrent;
            boolean wantsToReadFurther;
            do {
                bReadCurrent = br.read(buf, 0, chunkSize);
                if (storageBuf != null && bytesRead < maxLength && bReadCurrent != -1) {
                    storageBuf.append(buf, 0, bReadCurrent);
                }
                if (bReadCurrent != -1) {
                    bReadSession += bReadCurrent;
                }
                wantsToReadFurther = bReadCurrent != -1 && (readAll || bReadCurrent == chunkSize);
            } while (br.ready() && wantsToReadFurther);
        }
        return bReadSession;
    }
}
