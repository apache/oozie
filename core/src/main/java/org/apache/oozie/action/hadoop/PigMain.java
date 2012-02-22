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

import org.apache.pig.Main;
import org.apache.pig.PigRunner;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;
import java.net.URL;
import java.util.regex.Pattern;

public class PigMain extends LauncherMain {
    private static final Set<String> DISALLOWED_PIG_OPTIONS = new HashSet<String>();
    public static final String ACTION_PREFIX = "oozie.action.";
    public static final String EXTERNAL_CHILD_IDS = ACTION_PREFIX + "externalChildIDs.properties";
    public static final String EXTERNAL_ACTION_STATS = ACTION_PREFIX + "stats.properties";
    public static final String EXTERNAL_STATS_WRITE = ACTION_PREFIX + "external.stats.write";
    public static final int STRING_BUFFER_SIZE = 100;

    private static final Pattern[] PIG_JOB_IDS_PATTERNS = {
      Pattern.compile("HadoopJobId: (job_\\S*)")
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
        DISALLOWED_PIG_OPTIONS.add("-x");
        DISALLOWED_PIG_OPTIONS.add("-exectype");
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

        Properties pigProperties = new Properties();
        for (Map.Entry<String, String> entry : actionConf) {
            pigProperties.setProperty(entry.getKey(), entry.getValue());
        }

        // propagate delegation related props from launcher job to Pig job
        if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
            pigProperties.setProperty("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
            System.out.println("------------------------");
            System.out.println("Setting env property for mapreduce.job.credentials.binary to:"
                    + System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
            System.out.println("------------------------");
            System.setProperty("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
        }
        else {
            System.out.println("Non-kerberoes execution");
        }

        OutputStream os = new FileOutputStream("pig.properties");
        pigProperties.store(os, "");
        os.close();

        logMasking("pig.properties:", Arrays.asList("password"), pigProperties.entrySet());

        List<String> arguments = new ArrayList<String>();
        String script = actionConf.get("oozie.pig.script");

        if (script == null) {
            throw new RuntimeException("Action Configuration does not have [oozie.pig.script] property");
        }

        if (!new File(script).exists()) {
            throw new RuntimeException("Error: Pig script file [" + script + "] does not exist");
        }

        System.out.println("Pig script [" + script + "] content: ");
        System.out.println("------------------------");
        BufferedReader br = new BufferedReader(new FileReader(script));
        String line = br.readLine();
        while (line != null) {
            System.out.println(line);
            line = br.readLine();
        }
        br.close();
        System.out.println("------------------------");
        System.out.println();

        arguments.add("-file");
        arguments.add(script);
        String[] params = MapReduceMain.getStrings(actionConf, "oozie.pig.params");
        for (String param : params) {
            arguments.add("-param");
            arguments.add(param);
        }

        String hadoopJobId = System.getProperty("oozie.launcher.job.id");
        if (hadoopJobId == null) {
            throw new RuntimeException("Launcher Hadoop Job ID system property not set");
        }

        String logFile = new File("pig-oozie-" + hadoopJobId + ".log").getAbsolutePath();

        URL log4jFile = Thread.currentThread().getContextClassLoader().getResource("log4j.properties");
        if (log4jFile != null) {

            String pigLogLevel = actionConf.get("oozie.pig.log.level", "INFO");

            // append required PIG properties to the default hadoop log4j file
            Properties hadoopProps = new Properties();
            hadoopProps.load(log4jFile.openStream());
            hadoopProps.setProperty("log4j.logger.org.apache.pig", pigLogLevel + ", A, B");
            hadoopProps.setProperty("log4j.appender.A", "org.apache.log4j.ConsoleAppender");
            hadoopProps.setProperty("log4j.appender.A.layout", "org.apache.log4j.PatternLayout");
            hadoopProps.setProperty("log4j.appender.A.layout.ConversionPattern", "%-4r [%t] %-5p %c %x - %m%n");
            hadoopProps.setProperty("log4j.appender.B", "org.apache.log4j.FileAppender");
            hadoopProps.setProperty("log4j.appender.B.file", logFile);
            hadoopProps.setProperty("log4j.appender.B.layout", "org.apache.log4j.PatternLayout");
            hadoopProps.setProperty("log4j.appender.B.layout.ConversionPattern", "%-4r [%t] %-5p %c %x - %m%n");

            String localProps = new File("piglog4j.properties").getAbsolutePath();
            OutputStream os1 = new FileOutputStream(localProps);
            hadoopProps.store(os1, "");
            os1.close();

            arguments.add("-log4jconf");
            arguments.add(localProps);

            // print out current directory
            File localDir = new File(localProps).getParentFile();
            System.out.println("Current (local) dir = " + localDir.getAbsolutePath());
        }
        else {
            System.out.println("log4jfile is null");
        }

        String pigLog = "pig-" + hadoopJobId + ".log";
        arguments.add("-logfile");
        arguments.add(pigLog);

        String[] pigArgs = MapReduceMain.getStrings(actionConf, "oozie.pig.args");
        for (String pigArg : pigArgs) {
            if (DISALLOWED_PIG_OPTIONS.contains(pigArg)) {
                throw new RuntimeException("Error: Pig argument " + pigArg + " is not supported");
            }
            arguments.add(pigArg);
        }

        System.out.println("Pig command arguments :");
        for (String arg : arguments) {
            System.out.println("             " + arg);
        }

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

        // For embedded python or for version of pig lower than 0.8, pig stats are not supported.
        // So retrieving hadoop Ids here
        File file = new File(System.getProperty(EXTERNAL_CHILD_IDS));
        if (!file.exists()) {
            Properties props = getHadoopJobIds(logFile, PIG_JOB_IDS_PATTERNS);
            writeExternalData(props.getProperty(HADOOP_JOBS), file);
            System.out.println(" Hadoop Job IDs executed by Pig: " + props.getProperty(HADOOP_JOBS));
            System.out.println();
        }
    }



    private void handleError(String pigLog) throws Exception {
        System.err.println();
        System.err.println("Pig logfile dump:");
        System.err.println();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(pigLog));
            String line = reader.readLine();
            while (line != null) {
                System.err.println(line);
                line = reader.readLine();
            }
            reader.close();
        }
        catch (FileNotFoundException e) {
            System.err.println("pig log file: " + pigLog + "  not found.");
        }
    }

    /**
     * Runs the pig script using PigRunner API if version 0.8 or above. Embedded
     * pig within python is also supported.
     *
     * @param args pig command line arguments
     * @param pigLog pig log file
     * @param resetSecurityManager specify if need to reset security manager
     * @param retrieveStats specify if stats are to be retrieved
     * @throws Exception
     */
    protected void runPigJob(String[] args, String pigLog, boolean resetSecurityManager, boolean retrieveStats) throws Exception {
        // running as from the command line
        boolean pigRunnerExists = true;
        Class klass;
        try {
            klass = Class.forName("org.apache.pig.PigRunner");
        }
        catch (ClassNotFoundException ex) {
            pigRunnerExists = false;
        }

        if (pigRunnerExists) {
            System.out.println("Run pig script using PigRunner.run() for Pig version 0.8+");
            PigStats stats = PigRunner.run(args, null);
            // isSuccessful is the API from 0.9 supported by both PigStats and
            // EmbeddedPigStats
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
                String jobIds = getHadoopJobIds(stats);
                if (jobIds != null) {
                    System.out.println(" Hadoop Job IDs executed by Pig: " + jobIds);
                    File f = new File(System.getProperty(EXTERNAL_CHILD_IDS));
                    writeExternalData(jobIds, f);
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
        else {
            try {
                System.out.println("Run pig script using Main.main() for Pig version before 0.8");
                Main.main(args);
            }
            catch (SecurityException ex) {
                if (resetSecurityManager) {
                    LauncherSecurityManager.reset();
                }
                else {
                    if (LauncherSecurityManager.getExitInvoked()) {
                        if (LauncherSecurityManager.getExitCode() != 0) {
                            if (pigLog != null) {
                                handleError(pigLog);
                            }
                            throw ex;
                        }
                    }
                }
            }
        }
    }

    // write external data(stats, hadoopIds) to the file which will be read by the LauncherMapper
    private static void writeExternalData(String data, File f) throws IOException {
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new FileWriter(f));
            out.write(data);
        }
        finally {
            if (out != null) {
                out.close();
            }
        }
    }

    public static void setPigScript(Configuration conf, String script, String[] params, String[] args) {
        conf.set("oozie.pig.script", script);
        MapReduceMain.setStrings(conf, "oozie.pig.params", params);
        MapReduceMain.setStrings(conf, "oozie.pig.args", args);
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
