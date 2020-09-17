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
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.test.XTestCase;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class TestConcurrentCopyFromLocal extends XTestCase {

    private final String outPath = "outFolder";
    private final TemporaryFolder tmpFolder = new TemporaryFolder();
    private File libDirectory;
    private Services services = null;
    private Path dstPath = null;
    private FileSystem fs;

    @Override
    protected void setUp() throws Exception {
        super.setUp(false);
        tmpFolder.create();
        libDirectory = tmpFolder.newFolder("lib");
        services = new Services();
        services.get(ConfigurationService.class).getConf()
                .set(Services.CONF_SERVICE_CLASSES,"org.apache.oozie.service.LiteWorkflowAppService,"
                        + "org.apache.oozie.service.SchedulerService,"
                        + "org.apache.oozie.service.HadoopAccessorService,"
                        + "org.apache.oozie.service.ShareLibService");
        services.init();

        HadoopAccessorService has = services.get(HadoopAccessorService.class);
        URI uri = new Path(outPath).toUri();
        Configuration fsConf = has.createConfiguration(uri.getAuthority());
        fs = has.createFileSystem(System.getProperty("user.name"), uri, fsConf);

        WorkflowAppService lwas = services.get(WorkflowAppService.class);
        dstPath = lwas.getSystemLibPath();
    }

    @Override
    protected void tearDown() throws Exception {
        tmpFolder.delete();
        services.destroy();
        super.tearDown();
    }

    public void testConcurrentCopyFromLocalSameFileNrAndThreadNr() throws Exception {
        final int testFiles = 15;
        final int  threadPoolSize = 15;
        final long fsLimitsMinBlockSize = 1048576;
        final long bytesPerChecksum = 512;
        performAndCheckConcurrentCopy(testFiles, threadPoolSize, fsLimitsMinBlockSize, bytesPerChecksum);
    }

    public void testConcurrentCopyFromLocalMoreThreadsThanFiles() throws Exception {
        final int testFiles = 15;
        final int  threadPoolSize = 35;
        final long fsLimitsMinBlockSize = 1048576;
        final long bytesPerChecksum = 512;
        performAndCheckConcurrentCopy(testFiles, threadPoolSize, fsLimitsMinBlockSize, bytesPerChecksum);
    }

    public void testConcurrentCopyFromLocalHighThreadNr() throws Exception {
        final int testFiles = 200;
        final int  threadPoolSize = 150;
        final long fsLimitsMinBlockSize = 1048576;
        final long bytesPerChecksum = 512;
        performAndCheckConcurrentCopy(testFiles, threadPoolSize, fsLimitsMinBlockSize, bytesPerChecksum);
    }

    private void performAndCheckConcurrentCopy(final int testFiles, final int threadPoolSize, final long fsLimitsMinBlockSize,
                                               final long bytesPerChecksum) throws Exception {
        List<File> fileList = OozieSharelibFileOperations.generateAndWriteFiles(libDirectory, testFiles);
        File srcFile = new File(libDirectory.getParentFile().getAbsolutePath());
        OozieSharelibCLI.ConcurrentCopyFromLocal concurrentCopy = new OozieSharelibCLI
                .ConcurrentCopyFromLocal(threadPoolSize, fsLimitsMinBlockSize, bytesPerChecksum);
        concurrentCopy.concurrentCopyFromLocal(fs, srcFile, dstPath);

        for (int i = 0; i < testFiles; i++) {
            try (
                    InputStream originalFileStream = new FileInputStream(fileList.get(i));
                    InputStream copiedFileStream = fs.open(new Path(dstPath + File.separator + "lib",
                            fileList.get(i).getName()))){

                assertTrue("The content of the files must be equal", IOUtils.contentEquals(originalFileStream, copiedFileStream));
            }
        }
    }
}
