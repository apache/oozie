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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.action.hadoop.security.LauncherSecurityManager;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.ShareLibService;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.test.XTestCase;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URI;

/**
 * Test OozieSharelibCLI
 */
public class TestOozieSharelibCLI extends XTestCase {
    public final String outPath = "outFolder";
    private final TemporaryFolder tmpFolder = new TemporaryFolder();
    private File libDirectory;
    private Services services = null;
    private Path dstPath = null;
    private FileSystem fs;
    private LauncherSecurityManager launcherSecurityManager;
    @Override
    protected void setUp() throws Exception {
        launcherSecurityManager = new LauncherSecurityManager();
        launcherSecurityManager.enable();
        tmpFolder.create();
        libDirectory = tmpFolder.newFolder("lib");
        super.setUp(false);

    }

    @Override
    protected void tearDown() throws Exception {
        launcherSecurityManager.disable();
        if (services != null) {
            services.destroy();
        }
        super.tearDown();
    }

    /**
     * Test help command
     */
    public void testHelp() throws Exception {
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        PrintStream oldPrintStream = System.out;
        System.setOut(new PrintStream(data));
        try {
            String[] argsHelp = { "help" };
            assertEquals(0, execOozieSharelibCLICommands(argsHelp));
            String helpMessage = data.toString();
            assertTrue(helpMessage.contains(
                    "oozie-setup.sh create <OPTIONS> : create a new timestamped version of oozie sharelib"));
            assertTrue(helpMessage.contains(
                    "oozie-setup.sh upgrade <OPTIONS> : [deprecated][use command \"create\" "
                            + "to create new version]   upgrade oozie sharelib "));
            assertTrue(helpMessage.contains(" oozie-setup.sh help"));
            assertTrue(helpMessage.contains(
                    "-concurrency <arg>   Number of threads to be used for copy operations."));
        }
        finally {
            System.setOut(oldPrintStream);
        }

    }

    /**
     * test copy libraries
     */
    public void testOozieSharelibCLICreate() throws Exception {

        OozieSharelibFileOperations.writeFile(libDirectory, "file1", "test File");
        OozieSharelibFileOperations.writeFile(libDirectory, "file2", "test File2");

        String[] argsCreate = { "create", "-fs", outPath, "-locallib", libDirectory.getParentFile().getAbsolutePath() };
        assertEquals(0, execOozieSharelibCLICommands(argsCreate));

        FileSystem fs = getTargetFileSysyem();
        ShareLibService sharelibService = getServices().get(ShareLibService.class);

        // test files in new folder
        assertEquals(9, fs.getFileStatus(new Path(sharelibService.getLatestLibPath(getDistPath(),
                ShareLibService.SHARE_LIB_PREFIX), "file1")).getLen());
        assertEquals(10, fs.getFileStatus(new Path(sharelibService.getLatestLibPath(getDistPath(),
                ShareLibService.SHARE_LIB_PREFIX), "file2")).getLen());

    }

    /**
     * test parallel copy libraries
     */
    public void testOozieSharelibCLICreateConcurrent() throws Exception {

        final int testFiles = 7;
        final int concurrency = 5;

        for (int i = 0; i < testFiles; i++) {
            OozieSharelibFileOperations.writeFile(libDirectory, OozieSharelibFileOperations.generateFileName(i),
                    OozieSharelibFileOperations.generateFileContent(i));
        }

        String[] argsCreate = {"create", "-fs", outPath, "-locallib", libDirectory.getParentFile().getAbsolutePath(),
            "-concurrency", String.valueOf(concurrency)};
        assertEquals(0, execOozieSharelibCLICommands(argsCreate));

        getTargetFileSysyem();
        ShareLibService sharelibService = getServices().get(ShareLibService.class);
        Path latestLibPath = sharelibService.getLatestLibPath(getDistPath(),
                ShareLibService.SHARE_LIB_PREFIX);

        // test files in new folder

        for (int i = 0; i < testFiles; i++) {
            String fileName = OozieSharelibFileOperations.generateFileName(i);
            String expectedFileContent = OozieSharelibFileOperations.generateFileContent(i);
            InputStream in = null;
            try {
                in = getTargetFileSysyem().open(new Path(latestLibPath, fileName));
                String actualFileContent = IOUtils.toString(in);
                assertEquals(fileName, expectedFileContent, actualFileContent);
            } finally {
                IOUtils.closeQuietly(in);
            }
        }

    }

    /**
     * test fake command
     */
    public void testFakeCommand() throws Exception {

        ByteArrayOutputStream data = new ByteArrayOutputStream();
        PrintStream oldPrintStream = System.err;
        System.setErr(new PrintStream(data));
        try {
            String[] argsFake = { "fakeCommand" };
            assertEquals(1, execOozieSharelibCLICommands(argsFake));
            assertTrue(data.toString().contains("Invalid sub-command: invalid sub-command [fakeCommand]"));
            assertTrue(data.toString().contains("use 'help [sub-command]' for help details"));
        }
        finally {
            System.setErr(oldPrintStream);
        }

    }

    private FileSystem getTargetFileSysyem() throws Exception {
        if (fs == null) {
            HadoopAccessorService has = getServices().get(HadoopAccessorService.class);
            URI uri = new Path(outPath).toUri();
            Configuration fsConf = has.createConfiguration(uri.getAuthority());
            fs = has.createFileSystem(System.getProperty("user.name"), uri, fsConf);
        }
        return fs;

    }

    private Services getServices() throws ServiceException {
        if (services == null) {
            services = new Services();
            services.get(ConfigurationService.class).getConf()
                    .set(Services.CONF_SERVICE_CLASSES,"org.apache.oozie.service.LiteWorkflowAppService,"
                            + "org.apache.oozie.service.SchedulerService,"
                            + "org.apache.oozie.service.HadoopAccessorService,"
                            + "org.apache.oozie.service.ShareLibService");
            services.init();
        }
        return services;
    }

    private Path getDistPath() throws Exception {
        if (dstPath == null) {
            WorkflowAppService lwas = getServices().get(WorkflowAppService.class);
            dstPath = lwas.getSystemLibPath();
        }
        return dstPath;
    }

    private int execOozieSharelibCLICommands(String[] args) throws Exception {
        try {
            OozieSharelibCLI.main(args);
        }
        catch (SecurityException ex) {
            if (launcherSecurityManager.getExitInvoked()) {
                System.out.println("Intercepting System.exit(" + launcherSecurityManager.getExitCode() + ")");
                System.err.println("Intercepting System.exit(" + launcherSecurityManager.getExitCode() + ")");
                return launcherSecurityManager.getExitCode();
            }
            else {
                throw ex;
            }
        }
        return 1;
    }
}
