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

import java.io.ByteArrayInputStream;
import java.util.Random;

/**
 * A {@code ByteArrayInputStream} where the full data is not available at the beginning but produced on the fly. Simulates
 * an external process generating the data.
 *
 * <p>
 * {@code simulateSlowWriting} simulates when the process writes data in chunks instead of writing all the data
 * at the beginning.
 * <p>
 * It is possible to specify the pause times before writing the next chunk.
 * <p>
 * It is possible to specify {@code catBufferSize}, if the buffer contains more unread data, the process reports
 * itself as running.
 *
 */
public class BlockingInputStream extends ByteArrayInputStream {
    private static final int CHUNK_WRITE_DELAY_MS = 500;
    private static final double CHUNK_WRITE_MIN_RATIO = 0.25;
    private int readPosition;
    private int writePosition;
    private boolean simulateSlowWriting;
    private int bufferSize;
    private long lastTimeDataWritten;
    private int pauseIndex;
    private int[] pauseTimes = {CHUNK_WRITE_DELAY_MS};
    private boolean failed;

    BlockingInputStream(byte buf[], boolean simulateSlowWriting) {
        super(buf);
        this.bufferSize = buf.length;
        this.simulateSlowWriting = simulateSlowWriting;
        writeFirstChunk(buf, simulateSlowWriting);
    }

    void setBufferSize(int bufferSize) {
        if (!simulateSlowWriting) {
            throw new IllegalArgumentException("Cannot specify bufferSize");
        }
        this.bufferSize = bufferSize;
    }

    void setPauseTimes(int[] pauseTimes) {
        this.pauseTimes = pauseTimes;
    }

    private void writeFirstChunk(byte[] buf, boolean simulateSlowWriting) {
        if (simulateSlowWriting) {
            writeNextChunk(0);
        }
        else {
            writeNextChunk(buf.length);
        }
    }

    @Override
    public synchronized int read(byte b[], int off, int len) {
        final int bytesReadyToRead = writePosition - readPosition;
        final int bytesToRead = Math.min(len, bytesReadyToRead);
        final int readBytes = super.read(b, off, bytesToRead);
        final boolean someBytesRead = readBytes != -1;
        if (someBytesRead) {
            readPosition += readBytes;
        }
        return readBytes;
    }

    @Override
    public synchronized int available() {
        return writePosition - readPosition;
    }

    @VisibleForTesting
    boolean checkBlockedAndTryWriteNextChunk() {
        boolean fullBufferReady = writePosition == buf.length;
        boolean readFullBuffer = readPosition == buf.length;
        boolean thereAreNoBytesInBuffer = writePosition - readPosition == 0;
        boolean isBlocked = failed || (!readFullBuffer && (!fullBufferReady || thereAreNoBytesInBuffer ));
        tryWriteNextChunk();
        return isBlocked;
    }

    private boolean readyToWriteNextChunk() {
        return System.currentTimeMillis() - lastTimeDataWritten > getPauseTimeMs(pauseIndex);
    }

    private int getPauseTimeMs(int index) {
        return pauseTimes[index % pauseTimes.length];
    }

    private void tryWriteNextChunk() {
        if (!failed && writePosition < buf.length && available() < bufferSize && readyToWriteNextChunk()) {
            int maxSize = Math.min(bufferSize - available(), buf.length - writePosition);
            int minSize = Math.min((int)(CHUNK_WRITE_MIN_RATIO * buf.length +1), maxSize);
            int nextChunkSize = new Random().nextInt((maxSize - minSize) + 1) + minSize;
            writeNextChunk(nextChunkSize);
        }
    }

    private void writeNextChunk(int chunkSize) {
        writePosition += chunkSize;
        lastTimeDataWritten = System.currentTimeMillis();
        if (chunkSize > 0) {
            ++pauseIndex;
        }
    }

    void simulateFailure() {
        failed = true;
    }
}
