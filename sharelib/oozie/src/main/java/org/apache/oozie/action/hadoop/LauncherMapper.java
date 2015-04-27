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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.Permission;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class LauncherMapper<K1, V1, K2, V2> implements Mapper<K1, V1, K2, V2>, Runnable {

    static final String CONF_OOZIE_ACTION_MAIN_CLASS = "oozie.launcher.action.main.class";

    static final String ACTION_PREFIX = "oozie.action.";
    public static final String CONF_OOZIE_ACTION_MAX_OUTPUT_DATA = ACTION_PREFIX + "max.output.data";
    static final String CONF_OOZIE_ACTION_MAIN_ARG_COUNT = ACTION_PREFIX + "main.arg.count";
    static final String CONF_OOZIE_ACTION_MAIN_ARG_PREFIX = ACTION_PREFIX + "main.arg.";
    static final String CONF_OOZIE_EXTERNAL_STATS_MAX_SIZE = "oozie.external.stats.max.size";
    static final String OOZIE_ACTION_CONFIG_CLASS = ACTION_PREFIX + "config.class";
    static final String CONF_OOZIE_ACTION_FS_GLOB_MAX = ACTION_PREFIX + "fs.glob.max";

    static final String COUNTER_GROUP = "oozie.launcher";
    static final String COUNTER_LAUNCHER_ERROR = "oozie.launcher.error";

    static final String OOZIE_JOB_ID = "oozie.job.id";
    static final String OOZIE_ACTION_ID = ACTION_PREFIX + "id";
    static final String OOZIE_ACTION_RECOVERY_ID = ACTION_PREFIX + "recovery.id";

    static final String OOZIE_ACTION_DIR_PATH = ACTION_PREFIX + "dir.path";
    static final String ACTION_CONF_XML = "action.xml";
    static final String ACTION_PREPARE_XML = "oozie.action.prepare.xml";
    static final String ACTION_DATA_SEQUENCE_FILE = "action-data.seq"; // COMBO FILE
    static final String ACTION_DATA_EXTERNAL_CHILD_IDS = "externalChildIDs";
    static final String ACTION_DATA_OUTPUT_PROPS = "output.properties";
    static final String ACTION_DATA_STATS = "stats.properties";
    static final String ACTION_DATA_NEW_ID = "newId";
    static final String ACTION_DATA_ERROR_PROPS = "error.properties";
    public static final String HADOOP2_WORKAROUND_DISTRIBUTED_CACHE = "oozie.hadoop-2.0.2-alpha.workaround.for.distributed.cache";
    public static final String PROPAGATION_CONF_XML = "propagation-conf.xml";
    public static final String OOZIE_LAUNCHER_JOB_ID = "oozie.launcher.job.id";

    private void setRecoveryId(Configuration launcherConf, Path actionDir, String recoveryId) throws LauncherException {
        try {
            String jobId = launcherConf.get("mapred.job.id");
            Path path = new Path(actionDir, recoveryId);
            FileSystem fs = FileSystem.get(path.toUri(), launcherConf);
            if (!fs.exists(path)) {
                try {
                    java.io.Writer writer = new OutputStreamWriter(fs.create(path));
                    writer.write(jobId);
                    writer.close();
                }
                catch (IOException ex) {
                    failLauncher(0, "IO error", ex);
                }
            }
            else {
                InputStream is = fs.open(path);
                BufferedReader reader = new BufferedReader(new InputStreamReader(is));
                String id = reader.readLine();
                reader.close();
                if (!jobId.equals(id)) {
                    failLauncher(0, MessageFormat.format(
                            "Hadoop job Id mismatch, action file [{0}] declares Id [{1}] current Id [{2}]", path, id,
                            jobId), null);
                }

            }
        }
        catch (IOException ex) {
            failLauncher(0, "IO error", ex);
        }
    }

    private JobConf jobConf;
    private Path actionDir;
    private ScheduledThreadPoolExecutor timer;

    private boolean configFailure = false;
    private LauncherException configureFailureEx;
    private Map<String,String> actionData;

    public LauncherMapper() {
        actionData = new HashMap<String,String>();
    }

    @Override
    public void configure(JobConf jobConf) {
        System.out.println();
        System.out.println("Oozie Launcher starts");
        System.out.println();
        this.jobConf = jobConf;
        actionDir = new Path(getJobConf().get(OOZIE_ACTION_DIR_PATH));
        String recoveryId = jobConf.get(OOZIE_ACTION_RECOVERY_ID, null);
        try {
            setRecoveryId(jobConf, actionDir, recoveryId);
        }
        catch (LauncherException ex) {
            System.out.println("Launcher config error "+ex.getMessage());
            configureFailureEx = ex;
            configFailure = true;
        }
    }

    @Override
    public void map(K1 key, V1 value, OutputCollector<K2, V2> collector, Reporter reporter) throws IOException {
        try {
            if (configFailure) {
                throw configureFailureEx;
            }
            else {
                String mainClass = getJobConf().get(CONF_OOZIE_ACTION_MAIN_CLASS);
                if (getJobConf().getBoolean("oozie.hadoop-2.0.2-alpha.workaround.for.distributed.cache", false)) {
                  System.err.println("WARNING, workaround for Hadoop 2.0.2-alpha distributed cached issue (MAPREDUCE-4820) enabled");
                }
                String msgPrefix = "Main class [" + mainClass + "], ";
                int errorCode = 0;
                Throwable errorCause = null;
                String errorMessage = null;

                try {
                    new LauncherSecurityManager();
                }
                catch (SecurityException ex) {
                    errorMessage = "Could not set LauncherSecurityManager";
                    errorCause = ex;
                }

                try {
                    setupHeartBeater(reporter);

                    setupMainConfiguration();

                    // Propagating the conf to use by child job.
                    propagateToHadoopConf();

                    try {
                        System.out.println("Starting the execution of prepare actions");
                        executePrepare();
                        System.out.println("Completed the execution of prepare actions successfully");
                    } catch (Exception ex) {
                        System.out.println("Prepare execution in the Launcher Mapper has failed");
                        throw new LauncherException(ex.getMessage(), ex);
                    }

                    String[] args = getMainArguments(getJobConf());

                    printContentsOfCurrentDir();

                    System.out.println();
                    System.out.println("Oozie Java/Map-Reduce/Pig action launcher-job configuration");
                    System.out.println("=================================================================");
                    System.out.println("Workflow job id   : " + System.getProperty("oozie.job.id"));
                    System.out.println("Workflow action id: " + System.getProperty("oozie.action.id"));
                    System.out.println();
                    System.out.println("Classpath         :");
                    System.out.println("------------------------");
                    StringTokenizer st = new StringTokenizer(System.getProperty("java.class.path"), ":");
                    while (st.hasMoreTokens()) {
                        System.out.println("  " + st.nextToken());
                    }
                    System.out.println("------------------------");
                    System.out.println();
                    System.out.println("Main class        : " + mainClass);
                    System.out.println();
                    System.out.println("Maximum output    : "
                            + getJobConf().getInt(CONF_OOZIE_ACTION_MAX_OUTPUT_DATA, 2 * 1024));
                    System.out.println();
                    System.out.println("Arguments         :");
                    for (String arg : args) {
                        System.out.println("                    " + arg);
                    }

                    System.out.println();
                    System.out.println("Java System Properties:");
                    System.out.println("------------------------");
                    System.getProperties().store(System.out, "");
                    System.out.flush();
                    System.out.println("------------------------");
                    System.out.println();

                    System.out.println("=================================================================");
                    System.out.println();
                    System.out.println(">>> Invoking Main class now >>>");
                    System.out.println();
                    System.out.flush();

                    try {
                        Class klass = getJobConf().getClass(CONF_OOZIE_ACTION_MAIN_CLASS, Object.class);
                        Method mainMethod = klass.getMethod("main", String[].class);
                        mainMethod.invoke(null, (Object) args);
                    }
                    catch (InvocationTargetException ex) {
                        // Get what actually caused the exception
                        Throwable cause = ex.getCause();
                        // If we got a JavaMainException from JavaMain, then we need to unwrap it
                        if (JavaMainException.class.isInstance(cause)) {
                            cause = cause.getCause();
                        }
                        if (LauncherMainException.class.isInstance(cause)) {
                            errorMessage = msgPrefix + "exit code [" +((LauncherMainException)ex.getCause()).getErrorCode()
                                + "]";
                            errorCause = null;
                        }
                        else if (SecurityException.class.isInstance(cause)) {
                            if (LauncherSecurityManager.getExitInvoked()) {
                                System.out.println("Intercepting System.exit(" + LauncherSecurityManager.getExitCode()
                                        + ")");
                                System.err.println("Intercepting System.exit(" + LauncherSecurityManager.getExitCode()
                                        + ")");
                                // if 0 main() method finished successfully
                                // ignoring
                                errorCode = LauncherSecurityManager.getExitCode();
                                if (errorCode != 0) {
                                    errorMessage = msgPrefix + "exit code [" + errorCode + "]";
                                    errorCause = null;
                                }
                            }
                        }
                        else {
                            throw ex;
                        }
                    }
                    finally {
                        System.out.println();
                        System.out.println("<<< Invocation of Main class completed <<<");
                        System.out.println();
                    }
                    if (errorMessage == null) {
                        handleActionData();
                        if (actionData.get(ACTION_DATA_OUTPUT_PROPS) != null) {
                            System.out.println();
                            System.out.println("Oozie Launcher, capturing output data:");
                            System.out.println("=======================");
                            System.out.println(actionData.get(ACTION_DATA_OUTPUT_PROPS));
                            System.out.println();
                            System.out.println("=======================");
                            System.out.println();
                        }
                        if (actionData.get(ACTION_DATA_NEW_ID) != null) {
                            System.out.println();
                            System.out.println("Oozie Launcher, propagating new Hadoop job id to Oozie");
                            System.out.println("=======================");
                            System.out.println(actionData.get(ACTION_DATA_NEW_ID));
                            System.out.println("=======================");
                            System.out.println();
                        }
                    }
                }
                catch (NoSuchMethodException ex) {
                    errorMessage = msgPrefix + "main() method not found";
                    errorCause = ex;
                }
                catch (InvocationTargetException ex) {
                    errorMessage = msgPrefix + "main() threw exception";
                    errorCause = ex.getTargetException();
                }
                catch (Throwable ex) {
                    errorMessage = msgPrefix + "exception invoking main()";
                    errorCause = ex;
                }
                finally {
                    destroyHeartBeater();
                    if (errorMessage != null) {
                        failLauncher(errorCode, errorMessage, errorCause);
                    }
                }
            }
        }
        catch (LauncherException ex) {
            reporter.incrCounter(COUNTER_GROUP, COUNTER_LAUNCHER_ERROR, 1);
            System.out.println();
            System.out.println("Oozie Launcher failed, finishing Hadoop job gracefully");
            System.out.println();
        }
        finally {
            uploadActionDataToHDFS();
        }
    }

    @Override
    public void close() throws IOException {
        System.out.println();
        System.out.println("Oozie Launcher ends");
        System.out.println();
    }

    /**
     * Pushing all important conf to hadoop conf for the action
     */
    private void propagateToHadoopConf() throws IOException {
        Configuration propagationConf = new Configuration(false);
        if (System.getProperty(OOZIE_ACTION_ID) != null) {
            propagationConf.set(OOZIE_ACTION_ID, System.getProperty(OOZIE_ACTION_ID));
        }
        if (System.getProperty(OOZIE_JOB_ID) != null) {
            propagationConf.set(OOZIE_JOB_ID, System.getProperty(OOZIE_JOB_ID));
        }
        if(System.getProperty(OOZIE_LAUNCHER_JOB_ID) != null) {
            propagationConf.set(OOZIE_LAUNCHER_JOB_ID, System.getProperty(OOZIE_LAUNCHER_JOB_ID));
        }

        // loading action conf prepared by Oozie
        Configuration actionConf = LauncherMain.loadActionConf();

        if(actionConf.get(LauncherMainHadoopUtils.CHILD_MAPREDUCE_JOB_TAGS) != null) {
            propagationConf.set(LauncherMain.MAPREDUCE_JOB_TAGS,
                    actionConf.get(LauncherMainHadoopUtils.CHILD_MAPREDUCE_JOB_TAGS));
        }

        propagationConf.writeXml(new FileWriter(PROPAGATION_CONF_XML));
        Configuration.dumpConfiguration(propagationConf, new OutputStreamWriter(System.out));
        Configuration.addDefaultResource(PROPAGATION_CONF_XML);
    }

    protected JobConf getJobConf() {
        return jobConf;
    }

    private void handleActionData() throws IOException, LauncherException {
        // external child IDs
        String externalChildIdsProp = System.getProperty(ACTION_PREFIX + ACTION_DATA_EXTERNAL_CHILD_IDS);
        if (externalChildIdsProp != null) {
            File externalChildIDs = new File(externalChildIdsProp);
            if (externalChildIDs.exists()) {
                actionData.put(ACTION_DATA_EXTERNAL_CHILD_IDS, getLocalFileContentStr(externalChildIDs, "", -1));
            }
        }

        // external stats
        String statsProp = System.getProperty(ACTION_PREFIX + ACTION_DATA_STATS);
        if (statsProp != null) {
            File actionStatsData = new File(statsProp);
            if (actionStatsData.exists()) {
                int statsMaxOutputData = getJobConf().getInt(CONF_OOZIE_EXTERNAL_STATS_MAX_SIZE, Integer.MAX_VALUE);
                actionData.put(ACTION_DATA_STATS, getLocalFileContentStr(actionStatsData, "Stats", statsMaxOutputData));
            }
        }

        // output data
        String outputProp = System.getProperty(ACTION_PREFIX + ACTION_DATA_OUTPUT_PROPS);
        if (outputProp != null) {
            File actionOutputData = new File(outputProp);
            if (actionOutputData.exists()) {
                int maxOutputData = getJobConf().getInt(CONF_OOZIE_ACTION_MAX_OUTPUT_DATA, 2 * 1024);
                actionData.put(ACTION_DATA_OUTPUT_PROPS,
                        getLocalFileContentStr(actionOutputData, "Output", maxOutputData));
            }
        }

        // id swap
        String newIdProp = System.getProperty(ACTION_PREFIX + ACTION_DATA_NEW_ID);
        if (newIdProp != null) {
            File newId = new File(newIdProp);
            if (newId.exists()) {
                actionData.put(ACTION_DATA_NEW_ID, getLocalFileContentStr(newId, "", -1));
            }
        }
    }

    public static String getLocalFileContentStr(File file, String type, int maxLen) throws LauncherException, IOException {
        StringBuffer sb = new StringBuffer();
        FileReader reader = new FileReader(file);
        char[] buffer = new char[2048];
        int read;
        int count = 0;
        while ((read = reader.read(buffer)) > -1) {
            count += read;
            if (maxLen > -1 && count > maxLen) {
                throw new LauncherException(type + " data exceeds its limit ["+ maxLen + "]");
            }
            sb.append(buffer, 0, read);
        }
        reader.close();
        return sb.toString();
    }

    private void uploadActionDataToHDFS() throws IOException {
        if (!actionData.isEmpty()) {
            Path finalPath = new Path(actionDir, ACTION_DATA_SEQUENCE_FILE);
            FileSystem fs = FileSystem.get(finalPath.toUri(), getJobConf());
            // upload into sequence file
            System.out.println("Oozie Launcher, uploading action data to HDFS sequence file: "
                    + new Path(actionDir, ACTION_DATA_SEQUENCE_FILE).toUri());

            SequenceFile.Writer wr = null;
            try {
                wr = SequenceFile.createWriter(fs, getJobConf(), finalPath, Text.class, Text.class);
                if (wr != null) {
                    Set<String> keys = actionData.keySet();
                    for (String propsKey : keys) {
                        wr.append(new Text(propsKey), new Text(actionData.get(propsKey)));
                    }
                }
                else {
                    throw new IOException("SequenceFile.Writer is null for " + finalPath);
                }
            }
            catch(IOException e) {
                e.printStackTrace();
                throw e;
            }
            finally {
                if (wr != null) {
                    wr.close();
                }
            }
        }
    }

    private void setupMainConfiguration() throws IOException {
        Path pathNew = new Path(new Path(actionDir, ACTION_CONF_XML),
                new Path(new File(ACTION_CONF_XML).getAbsolutePath()));
        FileSystem fs = FileSystem.get(pathNew.toUri(), getJobConf());
        fs.copyToLocalFile(new Path(actionDir, ACTION_CONF_XML), new Path(new File(ACTION_CONF_XML).getAbsolutePath()));

        System.setProperty("oozie.launcher.job.id", getJobConf().get("mapred.job.id"));
        System.setProperty(OOZIE_JOB_ID, getJobConf().get(OOZIE_JOB_ID));
        System.setProperty(OOZIE_ACTION_ID, getJobConf().get(OOZIE_ACTION_ID));
        System.setProperty("oozie.action.conf.xml", new File(ACTION_CONF_XML).getAbsolutePath());
        System.setProperty(ACTION_PREFIX + ACTION_DATA_EXTERNAL_CHILD_IDS,
                new File(ACTION_DATA_EXTERNAL_CHILD_IDS).getAbsolutePath());
        System.setProperty(ACTION_PREFIX + ACTION_DATA_STATS, new File(ACTION_DATA_STATS).getAbsolutePath());
        System.setProperty(ACTION_PREFIX + ACTION_DATA_NEW_ID, new File(ACTION_DATA_NEW_ID).getAbsolutePath());
        System.setProperty(ACTION_PREFIX + ACTION_DATA_OUTPUT_PROPS, new File(ACTION_DATA_OUTPUT_PROPS).getAbsolutePath());
        System.setProperty(ACTION_PREFIX + ACTION_DATA_ERROR_PROPS, new File(ACTION_DATA_ERROR_PROPS).getAbsolutePath());
        System.setProperty(LauncherMainHadoopUtils.OOZIE_JOB_LAUNCH_TIME,
                getJobConf().get(LauncherMainHadoopUtils.OOZIE_JOB_LAUNCH_TIME));

        String actionConfigClass = getJobConf().get(OOZIE_ACTION_CONFIG_CLASS);
        if (actionConfigClass != null) {
            System.setProperty(OOZIE_ACTION_CONFIG_CLASS, actionConfigClass);
        }
    }

    // Method to execute the prepare actions
    private void executePrepare() throws IOException, LauncherException {
        String prepareXML = getJobConf().get(ACTION_PREPARE_XML);
        if (prepareXML != null) {
             if (!prepareXML.equals("")) {
                 Configuration actionConf = new Configuration(getJobConf());
                 String actionXml = System.getProperty("oozie.action.conf.xml");
                 actionConf.addResource(new Path("file:///", actionXml));
                 PrepareActionsDriver.doOperations(prepareXML, actionConf);
             } else {
                 System.out.println("There are no prepare actions to execute.");
             }
        }
    }

    public static String[] getMainArguments(Configuration conf) {
        String[] args = new String[conf.getInt(CONF_OOZIE_ACTION_MAIN_ARG_COUNT, 0)];
        for (int i = 0; i < args.length; i++) {
            args[i] = conf.get(CONF_OOZIE_ACTION_MAIN_ARG_PREFIX + i);
        }
        return args;
    }

    private void setupHeartBeater(Reporter reporter) {
        timer = new ScheduledThreadPoolExecutor(1);
        timer.scheduleAtFixedRate(new LauncherMapper(reporter), 0, 30, TimeUnit.SECONDS);
    }

    private void destroyHeartBeater() {
        timer.shutdownNow();
    }

    private Reporter reporter;

    private LauncherMapper(Reporter reporter) {
        this.reporter = reporter;
    }

    @Override
    public void run() {
        System.out.println("Heart beat");
        reporter.progress();
    }

    private void failLauncher(int errorCode, String reason, Throwable ex) throws LauncherException {
        if (ex != null) {
            reason += ", " + ex.getMessage();
        }
        Properties errorProps = new Properties();
        errorProps.setProperty("error.code", Integer.toString(errorCode));
        errorProps.setProperty("error.reason", reason);
        if (ex != null) {
            if (ex.getMessage() != null) {
                errorProps.setProperty("exception.message", ex.getMessage());
            }
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            pw.close();
            errorProps.setProperty("exception.stacktrace", sw.toString());
        }
        StringWriter sw = new StringWriter();
        try {
            errorProps.store(sw, "");
            sw.close();
            actionData.put(ACTION_DATA_ERROR_PROPS, sw.toString());

            // external child IDs
            String externalChildIdsProp = System.getProperty(ACTION_PREFIX + ACTION_DATA_EXTERNAL_CHILD_IDS);
            if (externalChildIdsProp != null) {
                File externalChildIDs = new File(externalChildIdsProp);
                if (externalChildIDs.exists()) {
                    actionData.put(ACTION_DATA_EXTERNAL_CHILD_IDS, getLocalFileContentStr(externalChildIDs, "", -1));
                }
            }
        }
        catch (IOException ioe) {
            throw new LauncherException(ioe.getMessage(), ioe);
        }
        finally {
            System.out.print("Failing Oozie Launcher, " + reason + "\n");
            System.err.print("Failing Oozie Launcher, " + reason + "\n");
            if (ex != null) {
                ex.printStackTrace(System.out);
                ex.printStackTrace(System.err);
            }
        }
        throw new LauncherException(reason, ex);
    }

    /**
     * Print files and directories in current directory. Will list files in the sub-directory (only 1 level deep)
     */
    protected void printContentsOfCurrentDir() {
        File folder = new File(".");
        System.out.println();
        System.out.println("Files in current dir:" + folder.getAbsolutePath());
        System.out.println("======================");

        File[] listOfFiles = folder.listFiles();
        for (File fileName : listOfFiles) {
            if (fileName.isFile()) {
                System.out.println("File: " + fileName.getName());
            }
            else if (fileName.isDirectory()) {
                System.out.println("Dir: " + fileName.getName());
                File subDir = new File(fileName.getName());
                File[] moreFiles = subDir.listFiles();
                for (File subFileName : moreFiles) {
                    if (subFileName.isFile()) {
                        System.out.println("  File: " + subFileName.getName());
                    }
                    else if (subFileName.isDirectory()) {
                        System.out.println("  Dir: " + subFileName.getName());
                    }
                }
            }
        }
    }

}

class LauncherSecurityManager extends SecurityManager {
    private static boolean exitInvoked;
    private static int exitCode;
    private SecurityManager securityManager;

    public LauncherSecurityManager() {
        reset();
        securityManager = System.getSecurityManager();
        System.setSecurityManager(this);
    }

    @Override
    public void checkPermission(Permission perm, Object context) {
        if (securityManager != null) {
            // check everything with the original SecurityManager
            securityManager.checkPermission(perm, context);
        }
    }

    @Override
    public void checkPermission(Permission perm) {
        if (securityManager != null) {
            // check everything with the original SecurityManager
            securityManager.checkPermission(perm);
        }
    }

    @Override
    public void checkExit(int status) throws SecurityException {
        exitInvoked = true;
        exitCode = status;
        throw new SecurityException("Intercepted System.exit(" + status + ")");
    }

    public static boolean getExitInvoked() {
        return exitInvoked;
    }

    public static int getExitCode() {
        return exitCode;
    }

    public static void reset() {
        exitInvoked = false;
        exitCode = 0;
    }
}

/**
 * Used by JavaMain to wrap a Throwable when an Exception occurs
 */
@SuppressWarnings("serial")
class JavaMainException extends Exception {
    public JavaMainException(Throwable t) {
        super(t);
    }
}
