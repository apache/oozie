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


package org.apache.oozie.tools;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.action.hadoop.security.LauncherSecurityManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.oozie.tools.OozieSharelibCLI.getExtraLibs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Test OozieSharelibCLI
 */
public class TestOozieSharelibCLI {

    private final static String TEST_SHAERELIBNAME1 = "sharelibName";
    private final static String TEST_SHAERELIBNAME2 = "sharelibName2";
    private final static String TEST_EXTRALIBS_PATHS1 = "/path/to/source/,/path/to/some/file";
    private final static String TEST_EXTRALIBS_PATHS2 = "hdfs://my/jar.jar#myjar.jar";

    private LauncherSecurityManager launcherSecurityManager;

    @Before
    public void setUp() {
        launcherSecurityManager = new LauncherSecurityManager();
        launcherSecurityManager.enable();
    }

    @After
    public void tearDown() {
        launcherSecurityManager.disable();
    }

    @Test
    public void testHelpCommand() throws Exception {
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        PrintStream oldPrintStream = System.out;
        System.setOut(new PrintStream(data));
        try {
            String[] argsHelp = {"help"};
            assertEquals("Exit code mismatch",0, execOozieSharelibCLICommands(argsHelp));
            String helpMessage = data.toString();
            assertTrue("Missing create <OPTIONS> description from help message", helpMessage.contains(
                    "oozie-setup.sh create <OPTIONS> : create a new timestamped version of oozie sharelib"));
            assertTrue("Missing  upgrade <OPTIONS> description from help message",
                    helpMessage.contains("oozie-setup.sh upgrade <OPTIONS> : [deprecated][use command \"create\" "
                    + "to create new version]   upgrade oozie sharelib "));
            assertTrue("Help message mismatch", helpMessage.contains(" oozie-setup.sh help"));
            assertTrue("Missing -concurrency from help message",
                    helpMessage.contains("-concurrency <arg>   Number of threads to be used for copy operations."));
        } finally {
            System.setOut(oldPrintStream);
        }
    }

    @Test
    public void testFakeCommand() throws Exception {

        ByteArrayOutputStream data = new ByteArrayOutputStream();
        PrintStream oldPrintStream = System.err;
        System.setErr(new PrintStream(data));
        try {
            String[] argsFake = {"fakeCommand"};
            assertEquals("Exit code mismatch", 1, execOozieSharelibCLICommands(argsFake));
            assertTrue("Error message missing",
                    data.toString().contains("Invalid sub-command: invalid sub-command [fakeCommand]"));
            assertTrue("Help message missing", data.toString().contains("use 'help [sub-command]' for help details"));
        } finally {
            System.setErr(oldPrintStream);
        }
    }

    @Test
    public void testCopyingExtraSharelibs() throws IOException {
        OozieSharelibCLI oozieSharelibCLI = spy(new OozieSharelibCLI() {
            @Override
            protected void checkIfSourceFilesExist(File srcFile) { }

            @Override
            protected void copyToSharelib(int threadPoolSize, File srcFile, Path srcPath, Path dstPath, FileSystem fs) {
                String dstStr = dstPath.toString();
                String actualSharelibName = dstStr.substring(dstStr.lastIndexOf(Path.SEPARATOR) + 1);
                Assert.assertTrue("Expected sharelib name missing",
                        Arrays.asList(TEST_SHAERELIBNAME1, TEST_SHAERELIBNAME2).contains(actualSharelibName));

                List<File> expectedTestFiles = new ArrayList<>();
                for (String s : Arrays.asList(TEST_EXTRALIBS_PATHS1.split(","))) {
                    expectedTestFiles.add(new File(s));
                }
                for (String s : Arrays.asList(TEST_EXTRALIBS_PATHS2.split(","))) {
                    expectedTestFiles.add(new File(s));
                }
                Assert.assertTrue("Expected file missing", expectedTestFiles.contains(srcFile));
            }
        });

        final FileSystem fs = mock(FileSystem.class);
        final Path dst = new Path("/share/lib");
        String extraLibOption1 = TEST_SHAERELIBNAME1 + "=" + TEST_EXTRALIBS_PATHS1;
        String extraLibOption2 = TEST_SHAERELIBNAME2 + "=" + TEST_EXTRALIBS_PATHS2;
        Map<String, String> addLibs = getExtraLibs(new String[] {extraLibOption1, extraLibOption2});
        oozieSharelibCLI.copyExtraLibs(1, addLibs, dst, fs);
        verify(oozieSharelibCLI, times(1))
                .copyExtraLibs(anyInt(), anyMap(), any(Path.class), any(FileSystem.class));
    }

    private int execOozieSharelibCLICommands(String[] args) throws Exception {
        try {
            OozieSharelibCLI.main(args);
        } catch (SecurityException ex) {
            if (launcherSecurityManager.getExitInvoked()) {
                System.out.println("Intercepting System.exit(" + launcherSecurityManager.getExitCode() + ")");
                System.err.println("Intercepting System.exit(" + launcherSecurityManager.getExitCode() + ")");
                return launcherSecurityManager.getExitCode();
            } else {
                throw ex;
            }
        }
        return 1;
    }
}
