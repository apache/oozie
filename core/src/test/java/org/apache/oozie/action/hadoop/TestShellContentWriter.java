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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

public class TestShellContentWriter {

    public static final int MAX_TEST_SCRIPT_SIZE_KB = 128;

    ByteArrayOutputStream outputStream;
    ByteArrayOutputStream errorStream;
    File scriptFile;

    @Before
    public void setUp() throws IOException {
        outputStream = new ByteArrayOutputStream(MAX_TEST_SCRIPT_SIZE_KB);
        errorStream = new ByteArrayOutputStream(MAX_TEST_SCRIPT_SIZE_KB);
        scriptFile = folder.newFile("shell_script.sh");
    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testPrintShellFile() throws Exception {
        writeScript("echo Hello World");

        Assert.assertTrue(outputStream.toString().contains("echo Hello World"));
        Assert.assertTrue(errorStream.toString().isEmpty());
    }

    @Test
    public void testPrintShellNullByte() throws Exception {
        writeScript("echo Hello World\0");

        Assert.assertFalse(outputStream.toString().contains("Hello World"));
        Assert.assertTrue(errorStream.toString().contains("appears to be a binary file"));
    }

    @Test
    public void testPrintShellValidShellCommand() {
        callPrint(1, "echo");

        Assert.assertTrue(String.format("output stream must be empty but is [%s]", outputStream.toString()),
                outputStream.toString().isEmpty());
        Assert.assertTrue(String.format("error stream must be empty but is [%s]", errorStream.toString()),
                errorStream.toString().isEmpty());
    }

    @Test
    public void testPrintShellInvalidShellCommand() {
        callPrint(1, "invalid command");

        Assert.assertTrue(String.format("output stream must be empty but is [%s]", outputStream.toString()),
                outputStream.toString().isEmpty());
        Assert.assertTrue(String.format("invalid error stream message [%s]", errorStream.toString()),
                errorStream.toString().contains("doesn't appear to exist"));
    }
    @Test
    public void testPrintControlCharacter() throws Exception {
        writeScript("echo Hello World\011");

        Assert.assertFalse(outputStream.toString().contains("Hello World"));
        Assert.assertTrue(errorStream.toString().contains("appears to be a binary file"));
    }

    @Test
    public void testEmptyFile() throws Exception {
        writeScript("");

        Assert.assertTrue(outputStream.toString().contains("---\n\n---"));
        Assert.assertTrue(errorStream.toString().isEmpty());
    }

    @Test
    public void testTooLargeFile() throws Exception {
        byte[] arr = new byte[2048];
        Arrays.fill(arr, (byte) Character.getNumericValue('x'));

        writeScript(new String(arr));

        Assert.assertTrue(outputStream.toString().isEmpty());
        Assert.assertTrue(errorStream.toString().contains("content suppressed."));
        Assert.assertTrue(errorStream.toString().contains("File size=2048b; max printable size=1024b"));
    }

    @Test
    public void testNegativeMaxSize() throws Exception {
        writeScript("test script", -1);

        Assert.assertTrue(outputStream.toString().isEmpty());
        Assert.assertTrue(errorStream.toString().contains("Not printing script file as configured, content suppressed."));
    }

    @Test
    public void testMissingFile() throws Exception {
        scriptFile.delete();

        writeScript("");

        Assert.assertTrue(outputStream.toString().isEmpty());
        Assert.assertTrue(String.format("invalid error stream message [%s]", errorStream.toString()),
                errorStream.toString().contains("doesn't appear to exist"));
    }

    private void writeScript(String content) throws IOException {
        writeScript(content, 1);
    }

    // Write a stub script with the given content, and invoke the writer to print its content
    private void writeScript(String content, int maxLen) throws IOException {
        if (content != null && !content.isEmpty()) {
            Files.write(scriptFile.toPath(), content.getBytes());
        }

        callPrint(maxLen, scriptFile.getAbsolutePath());
    }

    private void callPrint(final int maxLen, final String filename) {
        ShellContentWriter writer = new ShellContentWriter(
                maxLen,
                outputStream,
                errorStream,
                filename
        );

        writer.print();
    }
}
