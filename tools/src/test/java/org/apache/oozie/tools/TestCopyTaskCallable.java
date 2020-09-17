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
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.test.XTestCase;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


public class TestCopyTaskCallable extends XTestCase {
    private final String outPath =  "outFolder";
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
        services.getConf()
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

    public void testCallCopyTaskSameFileNrAndThreadNr() throws Exception {
        final long blockSize = 1048576;
        final int testFiles = 150;
        final int poolSize = 150;
        performAndCheckCallCopyTask(blockSize, poolSize, testFiles);
    }

    public void testCallCopyTaskOneThread() throws Exception {
        final long blockSize = 1048576;
        final int testFiles = 15;
        final int poolSize = 1;
        performAndCheckCallCopyTask(blockSize, poolSize, testFiles);
    }

    public void testCallCopyTaskMoreFilesThanThreads() throws Exception {
        final long blockSize = 1048576;
        final int testFiles = 150;
        final int poolSize = 10;
        performAndCheckCallCopyTask(blockSize, poolSize, testFiles);
    }

    public void testCallCopyTaskMoreThreadsThanFiles() throws Exception {
        final long blockSize = 1048576;
        final int testFiles = 15;
        final int poolSize = 20;
        performAndCheckCallCopyTask(blockSize, poolSize, testFiles);
    }

    private void performAndCheckCallCopyTask(final long blockSize, final int poolSize, final int testFiles) throws Exception {
        Set<OozieSharelibCLI.CopyTaskConfiguration> failedCopyTasks = new ConcurrentHashSet<>();

        List<File> fileList = OozieSharelibFileOperations.generateAndWriteFiles(libDirectory, testFiles);

        File srcFile = new File(libDirectory.getParentFile().getAbsolutePath());

        OozieSharelibCLI.CopyTaskConfiguration copyTask =
                new OozieSharelibCLI.CopyTaskConfiguration(fs, srcFile, dstPath);
        List<Future<OozieSharelibCLI.CopyTaskConfiguration>> taskList = new ArrayList<>();

        final ExecutorService threadPool = Executors.newFixedThreadPool(poolSize);
        try {
            for (final File file : libDirectory.listFiles()) {
                final Path trgName = new Path(dstPath, file.getName());
                taskList.add(threadPool
                        .submit(new OozieSharelibCLI.CopyTaskCallable(copyTask, file, trgName, blockSize, failedCopyTasks)));
            }
            for (Future<OozieSharelibCLI.CopyTaskConfiguration> future : taskList) {
                future.get();
            }
        } finally {
            threadPool.shutdown();
        }

        for (int i = 0; i < testFiles; i++) {

            try (
                    InputStream originalFileStream = new FileInputStream(fileList.get(i));
                    InputStream copiedFileStream = fs.open(new Path(dstPath, fileList.get(i).getName()))){
                assertTrue("The content of the files must be equal", IOUtils.contentEquals(originalFileStream, copiedFileStream));
            }
        }
    }
}
