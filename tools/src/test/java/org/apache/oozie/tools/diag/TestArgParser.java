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

package org.apache.oozie.tools.diag;

import org.apache.commons.cli.CommandLine;
import org.apache.oozie.action.hadoop.security.LauncherSecurityManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class TestArgParser {
    private CommandLine mockCommandLine = mock(CommandLine.class);
    private final ArgParser argParser =  new ArgParser();
    private static LauncherSecurityManager launcherSecurityManager;
    @BeforeClass
    public static void setUp() throws Exception {
        launcherSecurityManager = new LauncherSecurityManager();
        launcherSecurityManager.enable();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        launcherSecurityManager.disable();
    }

    @Before
    public void initTestCase() {
        doReturn("-1").when(mockCommandLine).getOptionValue(anyString(), anyString());
        argParser.setCommandLine(mockCommandLine);
    }

    @Test
    public void testUsageDisplayedWithoutUserInput() throws Exception {
        final String[] args = {""};
        final OutputStream interceptedStdout = new ByteArrayOutputStream();
        final OutputStream interceptedStderr = new ByteArrayOutputStream();

        interceptSystemStreams(interceptedStdout, interceptedStderr);

        try {
            DiagBundleCollectorDriver.main(args);
        }
        catch (final SecurityException securityException){
            assertTrue(interceptedStdout.toString().contains("usage: "));
            assertTrue(interceptedStderr.toString().contains("Missing required options: oozie, output"));
        }
        finally {
            restoreSystemStreams();
        }
    }

    private void restoreSystemStreams() {
        System.setErr(System.err);
        System.setErr(System.out);
    }

    private void interceptSystemStreams(OutputStream interceptedStdout, OutputStream interceptedStderr) {
        System.setOut(new PrintStream(interceptedStdout));
        System.setErr(new PrintStream(interceptedStderr));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExceptionThrownWithInvalidNumWorkflows() throws Exception {
        argParser.getNumWorkflows();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExceptionThrownWithInvalidNumCoordinators() throws Exception {
        argParser.getNumCoordinators();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExceptionThrownWithInvalidNumBundles() throws Exception {
        argParser.getNumBundles();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExceptionThrownWithInvalidMaxChildActions() throws Exception {
        argParser.getMaxChildActions();
    }
}