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

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.List;

import static org.apache.oozie.tools.OozieSharelibCLI.EXTRALIBS_SHARELIB_KEY_VALUE_SEPARATOR;


public class IntegrationTestOozieSharelibCLI extends XTestCase {

    private final TemporaryFolder tmpFolder = new TemporaryFolder();
    private File libDirectory;
    private Services services = null;
    private Path dstPath = null;
    private FileSystem fs;
    private LauncherSecurityManager launcherSecurityManager;
    public final String outPath = "outFolder";

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
     * test copy libraries
     */
    public void testOozieSharelibCLICreate() throws Exception {

        final int fileNr = 2;
        List<File> sharelibFiles = OozieSharelibFileOperations.generateAndWriteFiles(libDirectory, fileNr);

        String[] argsCreate = { "create", "-fs", outPath, "-locallib", libDirectory.getParentFile().getAbsolutePath() };
        assertEquals("Exit code mismatch", 0, execOozieSharelibCLICommands(argsCreate));

        ShareLibService sharelibService = getServices().get(ShareLibService.class);
        Path latestLibPath = sharelibService.getLatestLibPath(getDistPath(),
                ShareLibService.SHARE_LIB_PREFIX);

        checkCopiedSharelibFiles(sharelibFiles, latestLibPath);
    }

    /**
     * test parallel copy libraries
     */
    public void testOozieSharelibCLICreateConcurrent() throws Exception {

        final int testFiles = 7;
        final int concurrency = 5;

        List<File> sharelibFiles = OozieSharelibFileOperations.generateAndWriteFiles(libDirectory, testFiles);
        String[] argsCreate = {"create", "-fs", outPath, "-locallib", libDirectory.getParentFile().getAbsolutePath(),
                "-concurrency", String.valueOf(concurrency)};
        assertEquals("Exit code mismatch",0, execOozieSharelibCLICommands(argsCreate));

        getTargetFileSysyem();
        ShareLibService sharelibService = getServices().get(ShareLibService.class);
        Path latestLibPath = sharelibService.getLatestLibPath(getDistPath(),
                ShareLibService.SHARE_LIB_PREFIX);

        checkCopiedSharelibFiles(sharelibFiles, latestLibPath);
    }

    public void testOozieSharelibCreateExtraLibs () throws Exception {
        File extraLibBirectory1 = tmpFolder.newFolder("extralib1");
        File extraLibBirectory2 = tmpFolder.newFolder("extralib2");
        final int sharelibFileNr = 3;
        final int extraSharelib1FileNr = 4;
        final int extraSharelib2FileNr = 4;
        List<File> shareLibFiles = OozieSharelibFileOperations.generateAndWriteFiles(libDirectory, sharelibFileNr);
        List<File> extraShareLibFiles1 = OozieSharelibFileOperations
                .generateAndWriteFiles(extraLibBirectory1, extraSharelib1FileNr);
        List<File> extraShareLibFiles2 = OozieSharelibFileOperations
                .generateAndWriteFiles(extraLibBirectory2, extraSharelib2FileNr);

        String extraLib1 = extraLibBirectory1.getName() + EXTRALIBS_SHARELIB_KEY_VALUE_SEPARATOR
                + extraLibBirectory1.getAbsolutePath();
        String extraLib2 = extraLibBirectory2.getName() + EXTRALIBS_SHARELIB_KEY_VALUE_SEPARATOR
                + extraLibBirectory2.getAbsolutePath();
        String[] argsCreate = { "create", "-fs", outPath, "-locallib", libDirectory.getParentFile().getAbsolutePath(),
                "-" + OozieSharelibCLI.EXTRALIBS, extraLib1, "-" + OozieSharelibCLI.EXTRALIBS, extraLib2};

        assertEquals("Exit code mismatch",0, execOozieSharelibCLICommands(argsCreate));

        ShareLibService sharelibService = getServices().get(ShareLibService.class);
        Path latestLibPath = sharelibService.getLatestLibPath(getDistPath(), ShareLibService.SHARE_LIB_PREFIX);
        Path extraSharelibPath1 = new Path(latestLibPath + Path.SEPARATOR + extraLibBirectory1.getName());
        Path extraSharelibPath2 = new Path(latestLibPath + Path.SEPARATOR + extraLibBirectory2.getName());

        checkCopiedSharelibFiles(shareLibFiles, latestLibPath);
        checkCopiedSharelibFiles(extraShareLibFiles1, extraSharelibPath1);
        checkCopiedSharelibFiles(extraShareLibFiles2, extraSharelibPath2);
    }

    private void checkCopiedSharelibFiles(List<File> fileList, Path libPath) throws Exception {
        FileSystem fileSystem = getTargetFileSysyem();
        for (File f: fileList) {
            try (InputStream originalFileStream = new FileInputStream(f);
                 InputStream copiedFileStream = fileSystem.open(new Path(libPath, f.getName()))) {
                assertTrue("The content of the files must be equal", IOUtils.contentEquals(originalFileStream, copiedFileStream));
            }
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
