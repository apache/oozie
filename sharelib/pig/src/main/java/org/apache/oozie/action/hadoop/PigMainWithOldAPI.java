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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;
import java.net.URL;

public class PigMainWithOldAPI extends LauncherMain {
    private static final Set<String> DISALLOWED_PIG_OPTIONS = new HashSet<String>();

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
        run(PigMainWithOldAPI.class, args);
    }

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

        //propagate delegation related props from launcher job to Pig job
        String jobTokenFile = getFilePathFromEnv("HADOOP_TOKEN_FILE_LOCATION");
        if (jobTokenFile != null) {
            pigProperties.setProperty("mapreduce.job.credentials.binary", jobTokenFile);
            System.out.println("------------------------");
            System.out.println("Setting env property for mapreduce.job.credentials.binary to:"
                    + jobTokenFile);
            System.out.println("------------------------");
            System.setProperty("mapreduce.job.credentials.binary", jobTokenFile);
        }
        else {
            System.out.println("Non-kerberoes execution");
        }

        OutputStream os = new FileOutputStream("pig.properties");
        pigProperties.store(os, "");
        os.close();

        System.out.println();
        System.out.println("pig.properties content:");
        System.out.println("------------------------");
        pigProperties.store(System.out, "");
        System.out.flush();
        System.out.println("------------------------");
        System.out.println();

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

        try {
            System.out.println();
            runPigJob(new String[] { "-version" });
        }
        catch (SecurityException ex) {
            LauncherSecurityManager.reset();
        }
        System.out.println();
        System.out.flush();
        try {
            runPigJob(arguments.toArray(new String[arguments.size()]));
        }
        catch (SecurityException ex) {
            if (LauncherSecurityManager.getExitInvoked()) {
                if (LauncherSecurityManager.getExitCode() != 0) {
                    System.err.println();
                    System.err.println("Pig logfile dump:");
                    System.err.println();
                    try {
                        BufferedReader reader = new BufferedReader(new FileReader(pigLog));
                        line = reader.readLine();
                        while (line != null) {
                            System.err.println(line);
                            line = reader.readLine();
                        }
                        reader.close();
                    }
                    catch (FileNotFoundException e) {
                        System.err.println("pig log file: " + pigLog + "  not found.");
                    }
                    throw ex;
                }
            }
        }

        System.out.println();
        System.out.println("<<< Invocation of Pig command completed <<<");
        System.out.println();

        // harvesting and recording Hadoop Job IDs
        Properties jobIds = getHadoopJobIds(logFile);
        File file = new File(System.getProperty(LauncherMapper.ACTION_PREFIX + LauncherMapper.ACTION_DATA_OUTPUT_PROPS));
        os = new FileOutputStream(file);
        jobIds.store(os, "");
        os.close();
        System.out.println(" Hadoop Job IDs executed by Pig: " + jobIds.getProperty(HADOOP_JOBS));
        System.out.println();
    }

    protected void runPigJob(String[] args) throws Exception {
        // running as from the command line
        Main.main(args);
    }

    private static final String JOB_ID_LOG_PREFIX = "HadoopJobId: ";

    protected Properties getHadoopJobIds(String logFile) throws IOException {
        int jobCount = 0;
        Properties props = new Properties();
        StringBuffer sb = new StringBuffer(100);
        if (new File(logFile).exists() == false) {
            System.err.println("pig log file: " + logFile + "  not present. Therefore no Hadoop jobids found");
            props.setProperty(HADOOP_JOBS, "");
        }
        else {
            BufferedReader br = new BufferedReader(new FileReader(logFile));
            String line = br.readLine();
            String separator = "";
            while (line != null) {
                if (line.contains(JOB_ID_LOG_PREFIX)) {
                    int jobIdStarts = line.indexOf(JOB_ID_LOG_PREFIX) + JOB_ID_LOG_PREFIX.length();
                    String jobId = line.substring(jobIdStarts);
                    int jobIdEnds = jobId.indexOf(" ");
                    if (jobIdEnds > -1) {
                        jobId = jobId.substring(0, jobId.indexOf(" "));
                    }
                    sb.append(separator).append(jobId);
                    separator = ",";
                }
                line = br.readLine();
            }
            br.close();
            props.setProperty(HADOOP_JOBS, sb.toString());
        }
        return props;
    }

}
