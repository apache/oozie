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
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.oozie.cli.CLIParser;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.WorkflowAppService;
import org.eclipse.jetty.util.ConcurrentHashSet;

public class OozieSharelibCLI {
    public static final String[] HELP_INFO = {
            "",
            "OozieSharelibCLI creates or upgrade sharelib for oozie",
    };
    public static final String HELP_CMD = "help";
    public static final String CREATE_CMD = "create";
    public static final String UPGRADE_CMD = "upgrade";
    public static final String LIB_OPT = "locallib";
    public static final String EXTRALIBS = "extralib";
    public static final String FS_OPT = "fs";
    public static final String CONCURRENCY_OPT = "concurrency";
    public static final String OOZIE_HOME = "oozie.home.dir";
    public static final String SHARE_LIB_PREFIX = "lib_";
    public static final String NEW_LINE = System.lineSeparator();
    public static final String EXTRALIBS_USAGE = "Extra sharelib resources. " +
            "This option requires a pair of sharelibname and coma-separated list of pathnames" +
            " in the following format:" + NEW_LINE +
            "\"sharelib_name=pathname[,pathname...]\"" + NEW_LINE +
            "Caveats:" + NEW_LINE +
            "* Each pathname is either a directory or a regular file (compressed files are not extracted prior to " +
            "the upload operation)." + NEW_LINE +
            "* Sharelibname shall be specified only once." + NEW_LINE + NEW_LINE +
            "* Do not upload multiple conflicting library versions for an extra sharelib directory as it may " +
            "cause runtime issues." + NEW_LINE +
            "This option can be present multiple times, in case of more than one sharelib" + NEW_LINE +
            "Example command:" + NEW_LINE + NEW_LINE +
            "$ oozie-setup.sh sharelib create -fs hdfs://localhost:9000 -locallib oozie-sharelib.tar.gz " +
            "-extralib share2=dir2,file2 -extralib share3=file3";
    public static final String EXTRALIBS_PATH_SEPARATOR = ",";
    public static final String EXTRALIBS_SHARELIB_KEY_VALUE_SEPARATOR = "=";

    private boolean used;

    public static void main(String[] args) throws Exception{
        System.exit(new OozieSharelibCLI().run(args));
    }

    public OozieSharelibCLI() {
        used = false;
    }

    protected Options createUpgradeOptions(String subCommand){
        Option sharelib = new Option(LIB_OPT, true, "Local share library directory");
        Option uri = new Option(FS_OPT, true, "URI of the fileSystem to " + subCommand + " oozie share library");
        Option concurrency = new Option(CONCURRENCY_OPT, true, "Number of threads to be used for copy operations. (default=1)");
        Options options = new Options();
        options.addOption(sharelib);
        options.addOption(uri);
        options.addOption(concurrency);
        Option addLibsOption = new Option(EXTRALIBS, true, EXTRALIBS_USAGE);
        options.addOption(addLibsOption);
        return options;
    }

    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "False positive")
    public synchronized int run(String[] args) throws Exception{
        if (used) {
            throw new IllegalStateException("CLI instance already used");
        }

        used = true;

        CLIParser parser = new CLIParser("oozie-setup.sh", HELP_INFO);
        String oozieHome = System.getProperty(OOZIE_HOME);
        parser.addCommand(HELP_CMD, "", "display usage for all commands or specified command", new Options(), false);
        parser.addCommand(CREATE_CMD, "", "create a new timestamped version of oozie sharelib",
                createUpgradeOptions(CREATE_CMD), false);
        parser.addCommand(UPGRADE_CMD, "",
                "[deprecated][use command \"create\" to create new version]   upgrade oozie sharelib \n",
                createUpgradeOptions(UPGRADE_CMD), false);

        try {
            final CLIParser.Command command = parser.parse(args);
            String sharelibAction = command.getName();

            if (sharelibAction.equals(HELP_CMD)){
                parser.showHelp(command.getCommandLine());
                return 0;
            }

            if (!command.getCommandLine().hasOption(FS_OPT)){
                throw new Exception("-fs option must be specified");
            }

            int threadPoolSize = Integer.valueOf(command.getCommandLine().getOptionValue(CONCURRENCY_OPT, "1"));
            File srcFile = null;

            //Check whether user provided locallib
            if (command.getCommandLine().hasOption(LIB_OPT)){
                srcFile = new File(command.getCommandLine().getOptionValue(LIB_OPT));
            }
            else {
                //Since user did not provide locallib, find the default one under oozie home dir
                Collection<File> files =
                        FileUtils.listFiles(new File(oozieHome), new WildcardFileFilter("oozie-sharelib*.tar.gz"), null);

                if (files.size() > 1){
                    throw new IOException("more than one sharelib tar found at " + oozieHome);
                }

                if (files.isEmpty()){
                    throw new IOException("default sharelib tar not found in oozie home dir: " + oozieHome);
                }

                srcFile = files.iterator().next();
            }

            Map<String, String> extraLibs = new HashMap<>();
            if (command.getCommandLine().hasOption(EXTRALIBS)) {
                String[] param = command.getCommandLine().getOptionValues(EXTRALIBS);
                extraLibs = getExtraLibs(param);
            }

            File temp = File.createTempFile("oozie", ".dir");
            temp.delete();
            temp.mkdir();
            temp.deleteOnExit();

            //Check whether the lib is a tar file or folder
            if (!srcFile.isDirectory()){
                FileUtil.unTar(srcFile, temp);
                srcFile = new File(temp.toString() + "/share/lib");
            }
            else {
                //Get the lib directory since it's a folder
                srcFile = new File(srcFile, "lib");
            }

            String hdfsUri = command.getCommandLine().getOptionValue(FS_OPT);
            Path srcPath = new Path(srcFile.toString());

            Services services = new Services();
            services.getConf().set(Services.CONF_SERVICE_CLASSES,
                "org.apache.oozie.service.LiteWorkflowAppService, org.apache.oozie.service.HadoopAccessorService");
            services.getConf().set(Services.CONF_SERVICE_EXT_CLASSES, "");
            services.init();
            WorkflowAppService lwas = services.get(WorkflowAppService.class);
            HadoopAccessorService has = services.get(HadoopAccessorService.class);
            Path dstPath = lwas.getSystemLibPath();

            URI uri = new Path(hdfsUri).toUri();
            Configuration fsConf = has.createConfiguration(uri.getAuthority());
            FileSystem fs = FileSystem.get(uri, fsConf);

            if (!fs.exists(dstPath)) {
                fs.mkdirs(dstPath);
            }
            ECPolicyDisabler.tryDisableECPolicyForPath(fs, dstPath);

            if (sharelibAction.equals(CREATE_CMD) || sharelibAction.equals(UPGRADE_CMD)){
                dstPath= new Path(dstPath.toString() +  Path.SEPARATOR +  SHARE_LIB_PREFIX + getTimestampDirectory()  );
            }

            System.out.println("the destination path for sharelib is: " + dstPath);

            checkIfSourceFilesExist(srcFile);
            copyToSharelib(threadPoolSize, srcFile, srcPath, dstPath, fs);
            copyExtraLibs(threadPoolSize, extraLibs, dstPath, fs);

            services.destroy();
            FileUtils.deleteDirectory(temp);

            return 0;
        }
        catch (ParseException ex) {
            System.err.println("Invalid sub-command: " + ex.getMessage());
            System.err.println();
            System.err.println(parser.shortHelp());
            return 1;
        }
        catch (NumberFormatException ex) {
            logError("Invalid configuration value: ", ex);
            return 1;
        }
        catch (Exception ex) {
            logError(ex.getMessage(), ex);
            return 1;
        }
    }

    @VisibleForTesting
    static Map<String,String> getExtraLibs(String[] param) {
        Map<String, String> extraLibs = new HashMap<>();

        for (String lib : param) {
            String[] addLibParts = lib.split(EXTRALIBS_SHARELIB_KEY_VALUE_SEPARATOR);
            if (addLibParts.length != 2) {
                printExtraSharelibUsage();
                throw new IllegalArgumentException(String
                        .format("Argument of extralibs '%s' is in a wrong format. Exiting.", param));
            }
            String sharelibName = addLibParts[0];
            String sharelibPaths = addLibParts[1];
            if (extraLibs.containsKey(sharelibName)) {
                printExtraSharelibUsage();
                throw new IllegalArgumentException(String
                        .format("Extra sharelib, '%s', has been specified multiple times. " + "Exiting.", param));
            }
            extraLibs.put(sharelibName, sharelibPaths);
        }
        return extraLibs;
    }

    private static void printExtraSharelibUsage() {
        System.err.println(EXTRALIBS_USAGE);
    }


    @VisibleForTesting
    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "FilenameUtils is used to filter user input. JDK8+ is used.")
    void copyExtraLibs(int threadPoolSize, Map<String, String> extraLibs, Path dstPath, FileSystem fs) throws IOException {
        for (Map.Entry<String, String> sharelib : extraLibs.entrySet()) {
            Path libDestPath = new Path(dstPath.toString() + Path.SEPARATOR + sharelib.getKey());
            for (String libPath : sharelib.getValue().split(EXTRALIBS_PATH_SEPARATOR)) {
                File srcFile = new File(FilenameUtils.getFullPath(libPath) + FilenameUtils.getName(libPath));
                Path srcPath = new Path(FilenameUtils.getFullPath(libPath) + FilenameUtils.getName(libPath));
                checkIfSourceFilesExist(srcFile);
                copyToSharelib(threadPoolSize, srcFile, srcPath, libDestPath, fs);
            }
        }
    }

    @VisibleForTesting
    protected void copyToSharelib(int threadPoolSize, File srcFile, Path srcPath, Path dstPath, FileSystem fs) throws IOException {
        if (threadPoolSize > 1) {
            long fsLimitsMinBlockSize = fs.getConf()
                    .getLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_DEFAULT);
            long bytesPerChecksum = fs.getConf()
                    .getLong(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT);
            new ConcurrentCopyFromLocal(threadPoolSize, fsLimitsMinBlockSize, bytesPerChecksum)
                    .concurrentCopyFromLocal(fs, srcFile, dstPath);

        } else {
            fs.copyFromLocalFile(false, srcPath, dstPath);
        }
    }

    @VisibleForTesting
    protected void checkIfSourceFilesExist(File srcFile) throws IOException {
        if (!srcFile.exists()){
            throw new IOException(srcFile + " cannot be found");
        }
    }


    private static void logError(String errorMessage, Throwable ex) {
        System.err.println();
        System.err.println("Error: " + errorMessage);
        System.err.println();
        System.err.println("Stack trace for the error was (for debug purposes):");
        System.err.println("--------------------------------------");
        ex.printStackTrace(System.err);
        System.err.println("--------------------------------------");
        System.err.println();
    }

    public String getTimestampDirectory() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        Date date = new Date();
        return dateFormat.format(date).toString();
    }

    @VisibleForTesting
    static final class CopyTaskConfiguration {
        private final FileSystem fs;
        private final File srcFile;
        private final Path dstPath;

        CopyTaskConfiguration(FileSystem fs, File srcFile, Path dstPath) {
            this.fs = fs;
            this.srcFile = srcFile;
            this.dstPath = dstPath;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            CopyTaskConfiguration that = (CopyTaskConfiguration) o;
            if (!srcFile.equals(that.srcFile)) {
                return false;
            }
            return dstPath.equals(that.dstPath);
        }

        @Override
        public int hashCode() {
            int result = srcFile.hashCode();
            result = 31 * result + dstPath.hashCode();
            return result;
        }

    }

    @VisibleForTesting
    static final class BlockSizeCalculator {

        protected static long getValidBlockSize (long fileLenght, long fsLimitsMinBlockSize, long bytesPerChecksum) {
            if (fsLimitsMinBlockSize > fileLenght) {
                return fsLimitsMinBlockSize;
            }
            // bytesPerChecksum must divide block size
            if (fileLenght % bytesPerChecksum == 0) {
                return fileLenght;
            }
            long ratio = fileLenght/bytesPerChecksum;
            return (ratio + 1) * bytesPerChecksum;
        }
    }

    @VisibleForTesting
    static final class CopyTaskCallable implements Callable<CopyTaskConfiguration> {

        private final static short REPLICATION_FACTOR = 3;
        private final FileSystem fileSystem;
        private final File file;
        private final Path destinationPath;
        private final Path targetName;
        private final long blockSize;

        private final Set<CopyTaskConfiguration> failedCopyTasks;

        CopyTaskCallable(CopyTaskConfiguration copyTask, File file, Path trgName, long blockSize,
                                 Set<CopyTaskConfiguration> failedCopyTasks) {
            Preconditions.checkNotNull(copyTask);
            Preconditions.checkNotNull(file);
            Preconditions.checkNotNull(trgName);
            Preconditions.checkNotNull(failedCopyTasks);
            Preconditions.checkNotNull(copyTask.dstPath);
            Preconditions.checkNotNull(copyTask.fs);
            this.file = file;
            this.destinationPath = copyTask.dstPath;
            this.failedCopyTasks = failedCopyTasks;
            this.fileSystem = copyTask.fs;
            this.blockSize = blockSize;
            this.targetName = trgName;
        }

        @Override
        public CopyTaskConfiguration call() throws Exception {
            CopyTaskConfiguration cp = new CopyTaskConfiguration(fileSystem, file, targetName);
            failedCopyTasks.add(cp);
            final Path destinationFilePath = new Path(destinationPath + File.separator +  file.getName());
            final boolean overwrite = true;
            final int bufferSize = CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
            try (FSDataOutputStream out = fileSystem
                    .create(destinationFilePath, overwrite, bufferSize, REPLICATION_FACTOR, blockSize)) {
                Files.copy(file.toPath(), out);
            }
            return cp;
        }
    }

    @VisibleForTesting
    static final class ConcurrentCopyFromLocal {

        private static final int DEFAULT_RETRY_COUNT = 5;
        private static final int STARTING_RETRY_DELAY_IN_MS = 1000;
        private int retryCount;
        private int retryDelayInMs;
        private long fsLimitsMinBlockSize;
        private long bytesPerChecksum;

        private final int threadPoolSize;
        private final ExecutorService threadPool;
        private final Set<CopyTaskConfiguration> failedCopyTasks = new ConcurrentHashSet<>();

        public ConcurrentCopyFromLocal(int threadPoolSize, long fsLimitsMinBlockSize, long bytesPerChecksum) {
            Preconditions.checkArgument(threadPoolSize > 0, "Thread Pool size must be greater than 0");
            Preconditions.checkArgument(fsLimitsMinBlockSize > 0, "Minimun block size must be greater than 0");
            Preconditions.checkArgument(bytesPerChecksum > 0, "Bytes per checksum must be greater than 0");
            this.bytesPerChecksum = bytesPerChecksum;
            this.fsLimitsMinBlockSize = fsLimitsMinBlockSize;
            this.threadPoolSize = threadPoolSize;
            this.threadPool = Executors.newFixedThreadPool(threadPoolSize);
            this.retryCount = DEFAULT_RETRY_COUNT;
            this.retryDelayInMs = STARTING_RETRY_DELAY_IN_MS;
        }

        @VisibleForTesting
        void concurrentCopyFromLocal(FileSystem fs, File srcFile, Path dstPath) throws IOException {
            List<Future<CopyTaskConfiguration>> futures = Collections.emptyList();
            CopyTaskConfiguration copyTask = new CopyTaskConfiguration(fs, srcFile, dstPath);
            try {
                futures = copyFolderRecursively(copyTask);
                System.out.println("Running " + futures.size() + " copy tasks on " + threadPoolSize + " threads");
            } finally {
                checkCopyResults(futures);
                System.out.println("Copy tasks are done");
                threadPool.shutdown();
            }
        }

        private List<Future<CopyTaskConfiguration>> copyFolderRecursively(final CopyTaskConfiguration copyTask) {
            List<Future<CopyTaskConfiguration>> taskList = new ArrayList<>();
            File[] fileList = copyTask.srcFile.listFiles();
            if (fileList != null) {
                for (final File file : fileList) {
                    final Path trgName = new Path(copyTask.dstPath, file.getName());
                    if (file.isDirectory()) {
                        taskList.addAll(copyFolderRecursively(
                                new CopyTaskConfiguration(copyTask.fs, file, trgName)));
                    } else {
                        final long blockSize = BlockSizeCalculator
                                .getValidBlockSize(file.length(), fsLimitsMinBlockSize, bytesPerChecksum);
                        taskList.add(threadPool
                                .submit(new CopyTaskCallable(copyTask, file, trgName, blockSize, failedCopyTasks)));
                    }
                }
            }
            return taskList;
        }

        private void checkCopyResults(final List<Future<CopyTaskConfiguration>> futures)
                throws IOException {
            boolean exceptionOccurred = false;
            for (Future<CopyTaskConfiguration> future : futures) {
                CopyTaskConfiguration cp;
                try {
                    cp = future.get();
                    if (cp != null) {
                        failedCopyTasks.remove(cp);
                    }
                } catch (CancellationException ce) {
                    exceptionOccurred = true;
                    logError("Copy task was cancelled", ce);
                } catch (ExecutionException ee) {
                    exceptionOccurred = true;
                    logError("Copy task failed with exception", ee.getCause());
                } catch (InterruptedException ie) {
                    exceptionOccurred = true;
                    Thread.currentThread().interrupt();
                }
            }
            if (exceptionOccurred) {
                System.err.println("At least one copy task failed with exception. Retrying failed copy tasks.");
                retryFailedCopyTasks();

                if (!failedCopyTasks.isEmpty() && retryCount == 0) {
                    throw new IOException("At least one copy task failed with exception");
                }
            }
        }

        private void retryFailedCopyTasks() throws IOException {

            while (retryCount > 0 && !failedCopyTasks.isEmpty()) {
                try {
                    System.err.println("Waiting " + retryDelayInMs + " ms before retrying failed copy tasks.");
                    Thread.sleep(retryDelayInMs);
                    retryDelayInMs = retryDelayInMs * 2;
                } catch (InterruptedException e) {
                    System.err.println(e.getMessage());
                }

                for (CopyTaskConfiguration cp : failedCopyTasks) {
                    System.err.println("Retrying to copy " + cp.srcFile + " to " + cp.dstPath);
                    try {
                        copyFromLocalFile(cp);
                        failedCopyTasks.remove(cp);
                    }
                    catch (IOException e) {
                        System.err.printf("Copying [%s] to [%s] failed with exception: [%s]%n. Proceed to next file.%n"
                                ,cp.srcFile, cp.dstPath, e.getMessage());
                    }
                }

                --retryCount;
            }

            if (!failedCopyTasks.isEmpty() && retryCount == 0) {
                throw new IOException("Could not install Oozie ShareLib properly.");
            }
        }

        private void copyFromLocalFile(CopyTaskConfiguration cp) throws IOException{
            final FileSystem fs = cp.fs;
            fs.delete(cp.dstPath, false);
            fs.copyFromLocalFile(false, new Path(cp.srcFile.toURI()), cp.dstPath);
        }
    }
}
