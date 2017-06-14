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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.TeeOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hive.beeline.BeeLine;

import com.google.common.annotations.VisibleForTesting;

public class Hive2Main extends LauncherMain {
    @VisibleForTesting
    static final Pattern[] HIVE2_JOB_IDS_PATTERNS = {
            Pattern.compile("Ended Job = (job_\\S*)"),
            Pattern.compile("Submitted application (application[0-9_]*)"),
            Pattern.compile("Running with YARN Application = (application[0-9_]*)")
    };
    private static final Set<String> DISALLOWED_BEELINE_OPTIONS = new HashSet<>();

    static {
        DISALLOWED_BEELINE_OPTIONS.add("-u");
        DISALLOWED_BEELINE_OPTIONS.add("-n");
        DISALLOWED_BEELINE_OPTIONS.add("-p");
        DISALLOWED_BEELINE_OPTIONS.add("-d");
        DISALLOWED_BEELINE_OPTIONS.add("-e");
        DISALLOWED_BEELINE_OPTIONS.add("-f");
        DISALLOWED_BEELINE_OPTIONS.add("-a");
        DISALLOWED_BEELINE_OPTIONS.add("--help");
    }

    public static void main(String[] args) throws Exception {
        run(Hive2Main.class, args);
    }

    private static Configuration initActionConf() {
        // Loading action conf prepared by Oozie
        Configuration actionConf = new Configuration(false);

        String actionXml = System.getProperty("oozie.action.conf.xml");

        if (actionXml == null) {
            throw new RuntimeException("Missing Java System Property [oozie.action.conf.xml]");
        }
        if (!new File(actionXml).exists()) {
            throw new RuntimeException("Action Configuration XML file [" + actionXml + "] does not exist");
        } else {
            System.out.println("Using action configuration file " + actionXml);
        }

        actionConf.addResource(new Path("file:///", actionXml));
        setYarnTag(actionConf);

        // Propagate delegation related props from launcher job to Hive job
        String delegationToken = getFilePathFromEnv("HADOOP_TOKEN_FILE_LOCATION");
        if (delegationToken != null) {
            actionConf.set("mapreduce.job.credentials.binary", delegationToken);
            actionConf.set("tez.credentials.path", delegationToken);
            System.out.println("------------------------");
            System.out.println("Setting env property for mapreduce.job.credentials.binary to: " + delegationToken);
            System.out.println("------------------------");
            System.setProperty("mapreduce.job.credentials.binary", delegationToken);
        } else {
            System.out.println("Non-Kerberos execution");
        }

        // See https://issues.apache.org/jira/browse/HIVE-1411
        actionConf.set("datanucleus.plugin.pluginRegistryBundleCheck", "LOG");

        return actionConf;
    }

    @Override
    protected void run(String[] args) throws Exception {
        System.out.println();
        System.out.println("Oozie Hive 2 action configuration");
        System.out.println("=================================================================");
        System.out.println();

        Configuration actionConf = initActionConf();

        //Logfile to capture job IDs
        String hadoopJobId = System.getProperty("oozie.launcher.job.id");
        if (hadoopJobId == null) {
            throw new RuntimeException("Launcher Hadoop Job ID system property not set");
        }
        String logFile = new File("hive2-oozie-" + hadoopJobId + ".log").getAbsolutePath();

        List<String> arguments = new ArrayList<>();
        String jdbcUrl = actionConf.get(Hive2ActionExecutor.HIVE2_JDBC_URL);
        if (jdbcUrl == null) {
            throw new RuntimeException("Action Configuration does not have [" +  Hive2ActionExecutor.HIVE2_JDBC_URL
                    + "] property");
        }
        arguments.add("-u");
        arguments.add(jdbcUrl);

        // Use the user who is running the map task
        String username = actionConf.get("user.name");
        arguments.add("-n");
        arguments.add(username);

        String password = actionConf.get(Hive2ActionExecutor.HIVE2_PASSWORD);
        if (password == null) {
            // Have to pass something or Beeline might interactively prompt, which we don't want
            password = "DUMMY";
        }
        arguments.add("-p");
        arguments.add(password);

        // We always use the same driver
        arguments.add("-d");
        arguments.add("org.apache.hive.jdbc.HiveDriver");

        String scriptPath = actionConf.get(Hive2ActionExecutor.HIVE2_SCRIPT);
        String query = actionConf.get(Hive2ActionExecutor.HIVE2_QUERY);
        if (scriptPath != null) {
            if (!new File(scriptPath).exists()) {
                throw new RuntimeException("Hive 2 script file [" + scriptPath + "] does not exist");
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
                +  Hive2ActionExecutor.HIVE2_SCRIPT + "], or ["
                +  Hive2ActionExecutor.HIVE2_QUERY + "] property");
        }

        // Pass any parameters to Beeline via arguments
        String[] params = ActionUtils.getStrings(actionConf, Hive2ActionExecutor.HIVE2_PARAMS);
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

        // This tells BeeLine to look for a delegation token; otherwise it won't and will fail in secure mode because there are no
        // Kerberos credentials.  In non-secure mode, this argument is ignored so we can simply always pass it.
        arguments.add("-a");
        arguments.add("delegationToken");

        String[] beelineArgs = ActionUtils.getStrings(actionConf, Hive2ActionExecutor.HIVE2_ARGS);
        for (String beelineArg : beelineArgs) {
            if (DISALLOWED_BEELINE_OPTIONS.contains(beelineArg)) {
                throw new RuntimeException("Error: Beeline argument " + beelineArg + " is not supported");
            }
            arguments.add(beelineArg);
        }

        // Propagate MR job tag if defined
        if (actionConf.get(LauncherMain.MAPREDUCE_JOB_TAGS) != null ) {
            arguments.add("--hiveconf");
            arguments.add("mapreduce.job.tags=" + actionConf.get(LauncherMain.MAPREDUCE_JOB_TAGS));
        }
        // Propagate "oozie.*" configs (but not "oozie.launcher.*" nor "oozie.hive2.*")
        for (Map.Entry<String, String> oozieConfig : actionConf.getValByRegex("^oozie\\.(?!launcher|hive2).+").entrySet()) {
            arguments.add("--hiveconf");
            arguments.add(oozieConfig.getKey() + "=" + oozieConfig.getValue());
        }

        System.out.println("Beeline command arguments :");
        for (String arg : arguments) {
            System.out.println("             " + arg);
        }
        System.out.println();

        LauncherMain.killChildYarnJobs(actionConf);

        System.out.println("=================================================================");
        System.out.println();
        System.out.println(">>> Invoking Beeline command line now >>>");
        System.out.println();
        System.out.flush();

        try {
            runBeeline(arguments.toArray(new String[arguments.size()]), logFile);
        }
        finally {
            System.out.println("\n<<< Invocation of Beeline command completed <<<\n");
            writeExternalChildIDs(logFile, HIVE2_JOB_IDS_PATTERNS, "Beeline");
        }
    }

    private String createScriptFile(String query) throws IOException {
        String filename = "oozie-hive2-query-" + System.currentTimeMillis() + ".hql";
        File f = new File(filename);
        FileUtils.writeStringToFile(f, query, "UTF-8");
        return filename;
    }

    private void runBeeline(String[] args, String logFile) throws Exception {
        // We do this instead of calling BeeLine.main so we can duplicate the error stream for harvesting Hadoop child job IDs
        BeeLine beeLine = new BeeLine();
        beeLine.setErrorStream(new PrintStream(new TeeOutputStream(System.err, new FileOutputStream(logFile)), false, "UTF-8"));
        int status = beeLine.begin(args, null);
        beeLine.close();
        if (status != 0) {
            System.exit(status);
        }
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