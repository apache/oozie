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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.Charsets;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.TimeUnit;

/**
 * Dump the content of a shell script to output stream.
 *
 * If the file is binary or larger than a configurable size, the file content will not be printed.
 */
public class ShellContentWriter {

    private final long maxLen;
    private final OutputStream outputStream;
    private final OutputStream errorStream;
    private final String filename;

    /**
     * This class prints out the script file to the specified stream.
     *
     * @param maxLenKb max length of file to print, in KiB
     * @param outputStream stream where script content is written
     * @param errorStream stream for error messages are written
     * @param filename filename of script to print
     */
    public ShellContentWriter(int maxLenKb, OutputStream outputStream, OutputStream errorStream, String filename) {
        this.outputStream = outputStream;
        this.errorStream = errorStream;
        this.filename = filename;
        this.maxLen = maxLenKb > 0 ? ((long) maxLenKb) * 1024L : 0L;
    }

    /**
     * Print the script being executed.
     *
     * If the file can't be found, appears to be binary, or is too large, it isn't printed.
     */
    public void print() {

        Path path = Paths.get(filename);

        try {
            if (isValidScript(path)) {
                BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class);
                long len = attributes.size();
                if (len < maxLen) {
                    byte[] buffer = new byte[(int)len];
                    if (!readFile(path, buffer)) {
                        return;
                    }

                    writeLine(outputStream, "Content of script " + filename + " (size = " + len + "b):");
                    writeLine(outputStream, "------------------------------------");
                    writeLine(outputStream, buffer);
                    writeLine(outputStream, "------------------------------------");
                } else {
                    String message = "Not printing script file as configured, content suppressed.";
                    if (maxLen > 0) {
                        message = message + " File size=" + len + "b; max printable size=" + maxLen + "b";
                    }
                    writeLine(errorStream, message);
                }
            } else {
                writeLine(errorStream, "Path " + filename + " doesn't appear to exist, not printing its content.");
            }
        } catch (IOException ignored) { }
    }

    @SuppressFBWarnings(value = {"COMMAND_INJECTION", "PATH_TRAVERSAL_OUT"},
            justification = "pathToScript is specified in the WF action. It will surely be executed later on, no need to filter")
    private boolean isValidScript(final Path pathToScript) {
        if (Files.exists(pathToScript)) {
            return true;
        }

        try {
            // command -v is not present on every tested platform, using which instead
            final Process presentOnPathProcess = new ProcessBuilder()
                    .command("which", pathToScript.toString())
                    .start();

            final boolean hasFinished = presentOnPathProcess.waitFor(1, TimeUnit.SECONDS);
            if (!hasFinished) {
                return false;
            }

            final int exitValue = presentOnPathProcess.exitValue();

            return exitValue == 0;
        } catch (final IOException | InterruptedException | IllegalThreadStateException e) {
            return false;
        }
    }

    // Read the file into the given buffer.
    // Return true if the file appears to be printable, false otherwise.
    private boolean readFile(Path path,  byte[] buffer) throws IOException {
        int index = 0;
        try (InputStream stream = new BufferedInputStream(new FileInputStream(path.toFile()))) {
            int c;
            while ((c = stream.read()) >= 0 && index < buffer.length) {
                // a byte less than 0x0a is a hint that the file is binary
                if (c < 0x0a) {
                    writeLine(errorStream, "File " + filename + " appears to be a binary file, content suppressed.");
                    return false;
                }
                buffer[index++] = (byte)c;
            }
        }

        return true;
    }

    private void writeLine(OutputStream stream, String s) throws IOException {
        writeLine(stream, s.getBytes(Charsets.UTF_8));
    }

    private void writeLine(OutputStream stream, byte[] arr) throws IOException {
        stream.write(arr);
        stream.write('\n');
    }
}
