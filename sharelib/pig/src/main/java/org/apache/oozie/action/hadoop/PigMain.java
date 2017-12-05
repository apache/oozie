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

import org.apache.pig.PigRunner;
import org.apache.pig.scripting.ScriptEngine;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.PigStats;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashSet;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

public class PigMain extends LauncherMain {
    private static final Set<String> DISALLOWED_PIG_OPTIONS = new HashSet<>();
    public static final int STRING_BUFFER_SIZE = 100;
    public static final String LOG_EXPANDED_PIG_SCRIPT = LauncherAMUtils.ACTION_PREFIX + "pig.log.expandedscript";

    @VisibleForTesting
    static final Pattern[] PIG_JOB_IDS_PATTERNS = {
            Pattern.compile("HadoopJobId: (job_\\S*)"),
            Pattern.compile("Submitted application (application[0-9_]*)")
    };

    static {
        DISALLOWED_PIG_OPTIONS.add("-4");
        DISALLOWED_PIG_OPTIONS.add("-log4jconf");
        DISALLOWED_PIG_OPTIONS.add("-e");
        DISALLOWED_PIG_OPTIONS.add("-execute");
        DISALLOWED_PIG_OPTIONS.add("-f");
        DISALLOWED_PIG_OPTIONS.add("-file");
        DISALLOWED_PIG_OPTIONS.add("-l");
        DISALLOWED_PIG_OPTIONS.add("-logfile");
        DISALLOWED_PIG_OPTIONS.add("-r");
        DISALLOWED_PIG_OPTIONS.add("-dryrun");
        DISALLOWED_PIG_OPTIONS.add("-P");
        DISALLOWED_PIG_OPTIONS.add("-propertyFile");
    }

    public static void main(String[] args) throws Exception {
        run(PigMain.class, args);
    }

    @Override
    protected void run(String[] args) throws Exception {
        System.out.println();
        System.out.println("Oozie Pig action configuration");
        System.out.println("=================================================================");

        // loading action conf prepared by Oozie
        Configuration actionConf = new Configuration(false);

        String actionXml = System.getProperty("oozie.action.conf.xml");

        if (actionXml == null) {
            throw new RuntimeException("Missing Java System Property [oozie.action.conf.xml]");
        }
        if (!new File(actionXml).exists()) {
            throw new RuntimeException("Action Configuration XML file [" + actionXml + "] does not exist");
        }

        actionConf.addResource(new Path("file:///", actionXml));
        setYarnTag(actionConf);
        setApplicationTags(actionConf, TEZ_APPLICATION_TAGS);

        Properties pigProperties = new Properties();
        for (Map.Entry<String, String> entry : actionConf) {
            pigProperties.setProperty(entry.getKey(), entry.getValue());
        }

        // propagate delegation related props from launcher job to Pig job
        String jobTokenFile = getFilePathFromEnv("HADOOP_TOKEN_FILE_LOCATION");
        if (jobTokenFile != null) {
            pigProperties.setProperty("mapreduce.job.credentials.binary", jobTokenFile);
            pigProperties.setProperty("tez.credentials.path", jobTokenFile);
            System.out.println("------------------------");
            System.out.println("Setting env property for mapreduce.job.credentials.binary to:"
                    + jobTokenFile);
            System.out.println("------------------------");
            System.setProperty("mapreduce.job.credentials.binary", jobTokenFile);
        }
        else {
            System.out.println("Non-kerberos execution");
        }

        //setting oozie workflow id as caller context id for pig
        String callerId = "oozie:" + System.getProperty(LauncherAM.OOZIE_JOB_ID);
        pigProperties.setProperty("pig.log.trace.id", callerId);

        createFileWithContentIfNotExists("pig.properties", pigProperties);
        logMasking("pig.properties:",
                (Iterable<Map.Entry<String, String>>)(Iterable<?>) pigProperties.entrySet());

        List<String> arguments = new ArrayList<>();
        String script = actionConf.get(PigActionExecutor.PIG_SCRIPT);

        if (script == null) {
            throw new RuntimeException("Action Configuration does not have [oozie.pig.script] property");
        }

        if (!new File(script).exists()) {
            throw new RuntimeException("Error: Pig script file [" + script + "] does not exist");
        }

        printScript(script, "");

        arguments.add("-file");
        arguments.add(script);
        String[] params = ActionUtils.getStrings(actionConf, PigActionExecutor.PIG_PARAMS);
        for (String param : params) {
            arguments.add("-param");
            arguments.add(param);
        }

        String hadoopJobId = System.getProperty("oozie.launcher.job.id");
        if (hadoopJobId == null) {
            throw new RuntimeException("Launcher Hadoop Job ID system property not set");
        }

        String logFile = new File("pig-oozie-" + hadoopJobId + ".log").getAbsolutePath();

        String pigLogLevel = actionConf.get("oozie.pig.log.level", "INFO");
        String rootLogLevel = actionConf.get("oozie.action." + LauncherAMUtils.ROOT_LOGGER_LEVEL, "INFO");

        // append required PIG properties to the default hadoop log4j file
        log4jProperties.setProperty("log4j.rootLogger", rootLogLevel + ", A, B");
        log4jProperties.setProperty("log4j.logger.org.apache.pig", pigLogLevel + ", A, B");
        log4jProperties.setProperty("log4j.additivity.org.apache.pig", "false");
        log4jProperties.setProperty("log4j.appender.A", "org.apache.log4j.ConsoleAppender");
        log4jProperties.setProperty("log4j.appender.A.layout", "org.apache.log4j.PatternLayout");
        log4jProperties.setProperty("log4j.appender.A.layout.ConversionPattern", "%d [%t] %-5p %c %x - %m%n");
        log4jProperties.setProperty("log4j.appender.B", "org.apache.log4j.FileAppender");
        log4jProperties.setProperty("log4j.appender.B.file", logFile);
        log4jProperties.setProperty("log4j.appender.B.layout", "org.apache.log4j.PatternLayout");
        log4jProperties.setProperty("log4j.appender.B.layout.ConversionPattern", "%d [%t] %-5p %c %x - %m%n");
        log4jProperties.setProperty("log4j.logger.org.apache.hadoop.yarn.client.api.impl.YarnClientImpl", "INFO, B");

        String localProps = new File("piglog4j.properties").getAbsolutePath();
        createFileWithContentIfNotExists(localProps, log4jProperties);

        arguments.add("-log4jconf");
        arguments.add(localProps);

        // print out current directory
        File localDir = new File(localProps).getParentFile();
        System.out.println("Current (local) dir = " + localDir.getAbsolutePath());

        String pigLog = "pig-" + hadoopJobId + ".log";
        arguments.add("-logfile");
        arguments.add(pigLog);

        String[] pigArgs = ActionUtils.getStrings(actionConf, PigActionExecutor.PIG_ARGS);
        for (String pigArg : pigArgs) {
            if (DISALLOWED_PIG_OPTIONS.contains(pigArg)) {
                throw new RuntimeException("Error: Pig argument " + pigArg + " is not supported");
            }
            arguments.add(pigArg);
        }

        if (actionConf.getBoolean(LOG_EXPANDED_PIG_SCRIPT, true)
                // To avoid Pig running the embedded scripts on dryrun
                && ScriptEngine.getSupportedScriptLang(script) == null) {
            logExpandedScript(script, arguments);
        }

        System.out.println("Pig command arguments :");
        for (String arg : arguments) {
            System.out.println("             " + arg);
        }

        LauncherMain.killChildYarnJobs(actionConf);

        System.out.println("=================================================================");
        System.out.println();
        System.out.println(">>> Invoking Pig command line now >>>");
        System.out.println();
        System.out.flush();

        System.out.println();
        runPigJob(new String[] { "-version" }, null, true, false);
        System.out.println();
        System.out.flush();
        boolean hasStats = Boolean.parseBoolean(actionConf.get(EXTERNAL_STATS_WRITE));
        runPigJob(arguments.toArray(new String[arguments.size()]), pigLog, false, hasStats);

        System.out.println();
        System.out.println("<<< Invocation of Pig command completed <<<");
        System.out.println();

        // For embedded python, pig stats are not supported. So retrieving hadoop Ids here
        File file = new File(System.getProperty(EXTERNAL_CHILD_IDS));
        if (!file.exists()) {
            writeExternalChildIDs(logFile, PIG_JOB_IDS_PATTERNS, "Pig");
        }
    }

    /**
     * Logs the expanded
     *
     * @param script
     * @param arguments
     */
    private void logExpandedScript(String script, List<String> arguments) {
        List<String> dryrunArgs = new ArrayList<>();
        dryrunArgs.addAll(arguments);
        dryrunArgs.add("-dryrun");
        try {
            PigRunner.run(dryrunArgs.toArray(new String[dryrunArgs.size()]), null);
            printScript(script + ".expanded", "Expanded");
        }
        catch (Exception e) {
            System.out.println("Failure while expanding pig script");
            e.printStackTrace(System.out);
        }
    }

    private void printScript(String name, String type) {
        FileInputStream fin = null;
        InputStreamReader inReader = null;
        BufferedReader bufReader = null;
        try {
            File script = new File(name);
            if (!script.exists()) {
                return;
            }
            System.out.println("-----------------------------------------------------------");
            System.out.println(type + " Pig script [" + name + "] content: ");
            System.out.println("-----------------------------------------------------------");
            fin = new FileInputStream(script);
            inReader = new InputStreamReader(fin, "UTF-8");
            bufReader = new BufferedReader(inReader);
            String line = bufReader.readLine();
            while (line != null) {
                System.out.println(line);
                line = bufReader.readLine();
            }
            bufReader.close();
        }
        catch (RuntimeException e) {
            throw e;
        }
        catch (Exception e) {
            System.out.println("Unable to read " + name);
            e.printStackTrace(System.out);
        }
        finally {
            IOUtils.closeQuietly(fin);
            IOUtils.closeQuietly(inReader);
            IOUtils.closeQuietly(bufReader);
        }
        System.out.println();
        System.out.flush();
        System.out.println("-----------------------------------------------------------");
        System.out.println();
    }
    private void handleError(String pigLog) throws Exception {
        System.err.println();
        System.err.println("Pig logfile dump:");
        System.err.println();
        try {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(pigLog), "UTF-8"))) {
                String line = reader.readLine();
                while (line != null) {
                    System.err.println(line);
                    line = reader.readLine();
                }
            }
        }
        catch (FileNotFoundException e) {
            System.err.println("pig log file: " + pigLog + "  not found.");
        }
    }

    /**
     * Runs the pig script using PigRunner. Embedded pig within python is also supported.
     *
     * @param args pig command line arguments
     * @param pigLog pig log file
     * @param resetSecurityManager specify if need to reset security manager
     * @param retrieveStats specify if stats are to be retrieved
     * @throws Exception in case of error during Pig execution
     */
    protected void runPigJob(String[] args, String pigLog, boolean resetSecurityManager, boolean retrieveStats) throws Exception {
        PigStats stats = PigRunner.run(args, null);
        String jobIds = getHadoopJobIds(stats);
        if (jobIds != null && !jobIds.isEmpty()) {
            System.out.println("Hadoop Job IDs executed by Pig: " + jobIds);
            File f = new File(System.getProperty(EXTERNAL_CHILD_IDS));
            writeExternalData(jobIds, f);
        }

        if (!stats.isSuccessful()) {
            if (pigLog != null) {
                handleError(pigLog);
            }
            throw new LauncherMainException(PigRunner.ReturnCode.FAILURE);
        }
        else {
            // If pig command is ran with just the "version" option, then
            // return
            if (resetSecurityManager) {
                return;
            }
            // Retrieve stats only if user has specified in workflow
            // configuration
            if (retrieveStats) {
                ActionStats pigStats;
                String JSONString;
                try {
                    pigStats = new OoziePigStats(stats);
                    JSONString = pigStats.toJSON();
                } catch (UnsupportedOperationException uoe) {
                    throw new UnsupportedOperationException(
                            "Pig stats are not supported for this type of operation", uoe);
                }
                File f = new File(System.getProperty(EXTERNAL_ACTION_STATS));
                writeExternalData(JSONString, f);
            }
        }
    }

    // write external data(stats, hadoopIds) to the file which will be read by the LauncherAMUtils
    private static void writeExternalData(String data, File f) throws IOException {
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f), "UTF-8"));
            out.write(data);
        }
        finally {
            if (out != null) {
                out.close();
            }
        }
    }

    /**
     * Get Hadoop Ids through PigStats API
     *
     * @param pigStats stats object obtained through PigStats API
     * @return comma-separated String
     */
    protected String getHadoopJobIds(PigStats pigStats) {
        StringBuilder sb = new StringBuilder(STRING_BUFFER_SIZE);
        String separator = ",";
        // Collect Hadoop Ids through JobGraph API of Pig and store them as
        // comma separated string
        try {
            PigStats.JobGraph jobGraph = pigStats.getJobGraph();
            for (JobStats jobStats : jobGraph) {
                String hadoopJobId = jobStats.getJobId();
                if (StringUtils.isEmpty(hadoopJobId) || hadoopJobId.trim().equalsIgnoreCase("NULL")) {
                    continue;
                }
                if (sb.length() > 0) {
                    sb.append(separator);
                }
                sb.append(hadoopJobId);
            }
        }
        // Return null if Pig API's are not supported
        catch (UnsupportedOperationException uoe) {
            return null;
        }
        return sb.toString();
    }

}
