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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.hive.conf.HiveConf;

import com.google.common.annotations.VisibleForTesting;

public class HiveMain extends LauncherMain {
    @VisibleForTesting
    static final Pattern[] HIVE_JOB_IDS_PATTERNS = {
            Pattern.compile("Ended Job = (job_\\S*)"),
            Pattern.compile("Submitted application (application[0-9_]*)")
    };
    private static final Set<String> DISALLOWED_HIVE_OPTIONS = new HashSet<String>();

    public static final String HIVE_L4J_PROPS = "hive-log4j.properties";
    public static final String HIVE_EXEC_L4J_PROPS = "hive-exec-log4j.properties";
    public static final String HIVE_SITE_CONF = "hive-site.xml";

    static {
        DISALLOWED_HIVE_OPTIONS.add("-d");
        DISALLOWED_HIVE_OPTIONS.add("--define");
        DISALLOWED_HIVE_OPTIONS.add("-e");
        DISALLOWED_HIVE_OPTIONS.add("-f");
        DISALLOWED_HIVE_OPTIONS.add("-H");
        DISALLOWED_HIVE_OPTIONS.add("--help");
        DISALLOWED_HIVE_OPTIONS.add("--hiveconf");
        DISALLOWED_HIVE_OPTIONS.add("--hivevar");
        DISALLOWED_HIVE_OPTIONS.add("-s");
        DISALLOWED_HIVE_OPTIONS.add("--silent");
        DISALLOWED_HIVE_OPTIONS.add("-D");
    }

    public static void main(String[] args) throws Exception {
        run(HiveMain.class, args);
    }

    private static Configuration initActionConf() throws java.io.IOException {
        // Loading action conf prepared by Oozie
        Configuration hiveConf = new Configuration(false);

        String actionXml = System.getProperty("oozie.action.conf.xml");

        if (actionXml == null) {
            throw new RuntimeException("Missing Java System Property [oozie.action.conf.xml]");
        }
        if (!new File(actionXml).exists()) {
            throw new RuntimeException("Action Configuration XML file [" + actionXml + "] does not exist");
        } else {
            System.out.println("Using action configuration file " + actionXml);
        }

        hiveConf.addResource(new Path("file:///", actionXml));

        setYarnTag(hiveConf);
        setApplicationTags(hiveConf, TEZ_APPLICATION_TAGS);

        // Propagate delegation related props from launcher job to Hive job
        String delegationToken = getFilePathFromEnv("HADOOP_TOKEN_FILE_LOCATION");
        if (delegationToken != null) {
            hiveConf.set("mapreduce.job.credentials.binary", delegationToken);
            hiveConf.set("tez.credentials.path", delegationToken);
            System.out.println("------------------------");
            System.out.println("Setting env property for mapreduce.job.credentials.binary to: " + delegationToken);
            System.out.println("------------------------");
            System.setProperty("mapreduce.job.credentials.binary", delegationToken);
            System.out.println("------------------------");
            System.out.println("Setting env property for tez.credentials.path to: " + delegationToken);
            System.out.println("------------------------");
            System.setProperty("tez.credentials.path", delegationToken);
        } else {
            System.out.println("Non-Kerberos execution");
        }

        // Have to explicitly unset this property or Hive will not set it.
        // Reset if Oozie-defaults are set. Otherwise, honour user-selected job-name.
        if (hiveConf.get("mapred.job.name", "").startsWith("oozie:action:")) {
            hiveConf.set("mapred.job.name", "");
        }

        // See https://issues.apache.org/jira/browse/HIVE-1411
        hiveConf.set("datanucleus.plugin.pluginRegistryBundleCheck", "LOG");

        // to force hive to use the jobclient to submit the job, never using HADOOPBIN (to do localmode)
        hiveConf.setBoolean("hive.exec.mode.local.auto", false);

        String pwd = new java.io.File("").getCanonicalPath();
        hiveConf.set("hive.querylog.location", pwd + File.separator + "hivetmp" + File.separator + "querylog");
        hiveConf.set("hive.exec.local.scratchdir", pwd + File.separator + "hivetmp" + File.separator + "scratchdir");

        return hiveConf;
    }

    private String setUpHiveLog4J(Configuration hiveConf) throws IOException {
        //Logfile to capture job IDs
        String hadoopJobId = System.getProperty("oozie.launcher.job.id");
        if (hadoopJobId == null) {
            throw new RuntimeException("Launcher Hadoop Job ID system property not set");
        }

        String logFile = new File("hive-oozie-" + hadoopJobId + ".log").getAbsolutePath();

        String logLevel = hiveConf.get("oozie.hive.log.level", "INFO");
        String rootLogLevel = hiveConf.get("oozie.action." + LauncherAMUtils.ROOT_LOGGER_LEVEL, "INFO");

        log4jProperties.setProperty("log4j.rootLogger", rootLogLevel + ", A");
        log4jProperties.setProperty("log4j.logger.org.apache.hadoop.hive", logLevel + ", A");
        log4jProperties.setProperty("log4j.additivity.org.apache.hadoop.hive", "false");
        log4jProperties.setProperty("log4j.logger.hive", logLevel + ", A");
        log4jProperties.setProperty("log4j.additivity.hive", "false");
        log4jProperties.setProperty("log4j.logger.DataNucleus", logLevel + ", A");
        log4jProperties.setProperty("log4j.additivity.DataNucleus", "false");
        log4jProperties.setProperty("log4j.logger.DataStore", logLevel + ", A");
        log4jProperties.setProperty("log4j.additivity.DataStore", "false");
        log4jProperties.setProperty("log4j.logger.JPOX", logLevel + ", A");
        log4jProperties.setProperty("log4j.additivity.JPOX", "false");
        log4jProperties.setProperty("log4j.appender.A", "org.apache.log4j.ConsoleAppender");
        log4jProperties.setProperty("log4j.appender.A.layout", "org.apache.log4j.PatternLayout");
        log4jProperties.setProperty("log4j.appender.A.layout.ConversionPattern", "%d [%t] %-5p %c %x - %m%n");

        log4jProperties.setProperty("log4j.appender.jobid", "org.apache.log4j.FileAppender");
        log4jProperties.setProperty("log4j.appender.jobid.file", logFile);
        log4jProperties.setProperty("log4j.appender.jobid.layout", "org.apache.log4j.PatternLayout");
        log4jProperties.setProperty("log4j.appender.jobid.layout.ConversionPattern", "%d [%t] %-5p %c %x - %m%n");
        log4jProperties.setProperty("log4j.logger.org.apache.hadoop.hive.ql.exec", "INFO, jobid");
        log4jProperties.setProperty("log4j.additivity.org.apache.hadoop.hive.ql.exec", "false");
        log4jProperties.setProperty("log4j.logger.SessionState", "INFO, jobid");
        log4jProperties.setProperty("log4j.additivity.SessionState", "false");
        log4jProperties.setProperty("log4j.logger.org.apache.hadoop.yarn.client.api.impl.YarnClientImpl", "INFO, jobid");
        log4jProperties.setProperty("log4j.additivity.org.apache.hadoop.yarn.client.api.impl.YarnClientImpl", "false");

        createFileWithContentIfNotExists(new File(HIVE_L4J_PROPS).getAbsolutePath(), log4jProperties);
        createFileWithContentIfNotExists(new File(HIVE_EXEC_L4J_PROPS).getAbsolutePath(), log4jProperties);

        return logFile;
    }

    public static Configuration setUpHiveSite() throws Exception {
        Configuration hiveConf = initActionConf();

        // Write the action configuration out to hive-site.xml
        URL hiveSiteURL = createFileWithContentIfNotExists(HIVE_SITE_CONF, hiveConf);
        logMasking("Hive Configuration Properties:", hiveConf);

        // Reset the hiveSiteURL static variable as we just created hive-site.xml.
        // If prepare block had a drop partition it would have been initialized to null.
        HiveConf.setHiveSiteLocation(hiveSiteURL);
        return hiveConf;
    }

    @Override
    protected void run(String[] args) throws Exception {
        System.out.println();
        System.out.println("Oozie Hive action configuration");
        System.out.println("=================================================================");
        System.out.println();

        Configuration hiveConf = setUpHiveSite();

        List<String> arguments = new ArrayList<String>();

        String logFile = setUpHiveLog4J(hiveConf);
        arguments.add("--hiveconf");
        arguments.add("hive.log4j.file=" + new File(HIVE_L4J_PROPS).getAbsolutePath());
        arguments.add("--hiveconf");
        arguments.add("hive.exec.log4j.file=" + new File(HIVE_EXEC_L4J_PROPS).getAbsolutePath());

        //setting oozie workflow id as caller context id for hive
        String callerId = "oozie:" + System.getProperty(LauncherAM.OOZIE_JOB_ID);
        arguments.add("--hiveconf");
        arguments.add("hive.log.trace.id=" + callerId);

        String scriptPath = hiveConf.get(HiveActionExecutor.HIVE_SCRIPT);
        String query = hiveConf.get(HiveActionExecutor.HIVE_QUERY);
        if (scriptPath != null) {
            if (!new File(scriptPath).exists()) {
                throw new RuntimeException("Hive script file [" + scriptPath + "] does not exist");
            }
            // print out current directory & its contents
            File localDir = new File("dummy").getAbsoluteFile().getParentFile();
            System.out.println("Current (local) dir = " + localDir.getAbsolutePath());
            System.out.println("------------------------");
            String[] files = localDir.list();
            if (files != null) {
                for (String file : files) {
                    System.out.println("  " + file);
                }
            }
            System.out.println("------------------------");
            System.out.println();
            // Prepare the Hive Script
            String script = readStringFromFile(scriptPath);
            System.out.println();
            System.out.println("Script [" + scriptPath + "] content: ");
            System.out.println("------------------------");
            System.out.println(script);
            System.out.println("------------------------");
            System.out.println();
            arguments.add("-f");
            arguments.add(scriptPath);
        } else if (query != null) {
            System.out.println("Query: ");
            System.out.println("------------------------");
            System.out.println(query);
            System.out.println("------------------------");
            System.out.println();
            String filename = createScriptFile(query);
            arguments.add("-f");
            arguments.add(filename);
        } else {
            throw new RuntimeException("Action Configuration does not have ["
                +  HiveActionExecutor.HIVE_SCRIPT + "], or ["
                +  HiveActionExecutor.HIVE_QUERY + "] property");
        }

        // Pass any parameters to Hive via arguments
        String[] params = ActionUtils.getStrings(hiveConf, HiveActionExecutor.HIVE_PARAMS);
        if (params.length > 0) {
            System.out.println("Parameters:");
            System.out.println("------------------------");
            for (String param : params) {
                System.out.println("  " + param);

                int idx = param.indexOf('=');
                if (idx == -1) {
                    throw new RuntimeException("Parameter expression must contain an assignment: " + param);
                } else if (idx == 0) {
                    throw new RuntimeException("Parameter value not specified: " + param);
                }
                arguments.add("--hivevar");
                arguments.add(param);
            }
            System.out.println("------------------------");
            System.out.println();
        }

        String[] hiveArgs = ActionUtils.getStrings(hiveConf, HiveActionExecutor.HIVE_ARGS);
        for (String hiveArg : hiveArgs) {
            if (DISALLOWED_HIVE_OPTIONS.contains(hiveArg)) {
                throw new RuntimeException("Error: Hive argument " + hiveArg + " is not supported");
            }
            arguments.add(hiveArg);
        }

        System.out.println("Hive command arguments :");
        for (String arg : arguments) {
            System.out.println("             " + arg);
        }
        System.out.println();

        LauncherMain.killChildYarnJobs(hiveConf);

        System.out.println("=================================================================");
        System.out.println();
        System.out.println(">>> Invoking Hive command line now >>>");
        System.out.println();
        System.out.flush();

        try {
            runHive(arguments.toArray(new String[arguments.size()]));
        }
        finally {
            System.out.println("\n<<< Invocation of Hive command completed <<<\n");
            writeExternalChildIDs(logFile, HIVE_JOB_IDS_PATTERNS, "Hive");
        }
    }

    private String createScriptFile(String query) throws IOException {
        String filename = "oozie-hive-query-" + System.currentTimeMillis() + ".hql";
        File f = new File(filename);
        FileUtils.writeStringToFile(f, query, "UTF-8");
        return filename;
    }

    private void runHive(String[] args) throws Exception {
        CliDriver.main(args);
    }

    private static String readStringFromFile(String filePath) throws IOException {
        String line;
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), "UTF-8"));
            StringBuilder sb = new StringBuilder();
            String sep = System.getProperty("line.separator");
            while ((line = br.readLine()) != null) {
                sb.append(line).append(sep);
            }
            return sb.toString();
        }
        finally {
            if (br != null) {
                br.close();
            }
        }
     }
}
