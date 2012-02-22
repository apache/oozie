/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.action.hadoop;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.PropertyConfigurator;
import org.apache.sqoop.Sqoop;

public class SqoopMain extends LauncherMain {

    private static final String SQOOP_ARGS = "oozie.sqoop.args";

    public static final String SQOOP_SITE_CONF = "sqoop-site.xml";

    private static final Pattern[] SQOOP_JOB_IDS_PATTERNS = {
      Pattern.compile("Job complete: (job_\\S*)"), Pattern.compile("Job (job_\\S*) completed successfully")
    };

    private static final String SQOOP_LOG4J_PROPS = "sqoop-log4j.properties";

    public static void main(String[] args) throws Exception {
        run(SqoopMain.class, args);
    }

    private static Configuration initActionConf() {
        // loading action conf prepared by Oozie
        Configuration sqoopConf = new Configuration(false);

        String actionXml = System.getProperty("oozie.action.conf.xml");

        if (actionXml == null) {
            throw new RuntimeException("Missing Java System Property [oozie.action.conf.xml]");
        }
        if (!new File(actionXml).exists()) {
            throw new RuntimeException("Action Configuration XML file [" + actionXml + "] does not exist");
        }

        sqoopConf.addResource(new Path("file:///", actionXml));

        String delegationToken = System.getenv("HADOOP_TOKEN_FILE_LOCATION");
        if (delegationToken != null) {
            sqoopConf.set("mapreduce.job.credentials.binary", delegationToken);
            System.out.println("------------------------");
            System.out.println("Setting env property for mapreduce.job.credentials.binary to: " + delegationToken);
            System.out.println("------------------------");
            System.setProperty("mapreduce.job.credentials.binary", delegationToken);
        } else {
            System.out.println("Non-Kerberos execution");
        }

        return sqoopConf;
    }

    public static Configuration setUpSqoopSite() throws Exception {
        Configuration sqoopConf = initActionConf();

        // Write the action configuration out to sqoop-site.xml
        OutputStream os = new FileOutputStream(SQOOP_SITE_CONF);
        try {
            sqoopConf.writeXml(os);
        }
        finally {
            os.close();
        }
      
        System.out.println();
        System.out.println("Sqoop Configuration Properties:");
        System.out.println("------------------------");
        for (Map.Entry<String, String> entry : sqoopConf) {
            System.out.println(entry.getKey() + "=" + entry.getValue());
        }
        System.out.flush();
        System.out.println("------------------------");
        System.out.println();
        return sqoopConf;
    }

    public static String setUpSqoopLog4J(Configuration sqoopConf) throws IOException {
        //Logfile to capture job IDs
        String hadoopJobId = System.getProperty("oozie.launcher.job.id");
        if (hadoopJobId == null) {
            throw new RuntimeException("Launcher Hadoop Job ID system property not set");
        }

        String logFile = new File("sqoop-oozie-" + hadoopJobId + ".log").getAbsolutePath();

        Properties hadoopProps = new Properties();

        // Preparing log4j configuration
        URL log4jFile = Thread.currentThread().getContextClassLoader().getResource("log4j.properties");
        if (log4jFile != null) {
            // getting hadoop log4j configuration
            hadoopProps.load(log4jFile.openStream());
        }

        String logLevel = sqoopConf.get("oozie.sqoop.log.level", "INFO");

        hadoopProps.setProperty("log4j.logger.org.apache.sqoop", logLevel + ", A");
        hadoopProps.setProperty("log4j.appender.A", "org.apache.log4j.ConsoleAppender");
        hadoopProps.setProperty("log4j.appender.A.layout", "org.apache.log4j.PatternLayout");
        hadoopProps.setProperty("log4j.appender.A.layout.ConversionPattern", "%-4r [%t] %-5p %c %x - %m%n");

        hadoopProps.setProperty("log4j.appender.jobid", "org.apache.log4j.FileAppender");
        hadoopProps.setProperty("log4j.appender.jobid.file", logFile);
        hadoopProps.setProperty("log4j.appender.jobid.layout", "org.apache.log4j.PatternLayout");
        hadoopProps.setProperty("log4j.appender.jobid.layout.ConversionPattern", "%-4r [%t] %-5p %c %x - %m%n");
        hadoopProps.setProperty("log4j.logger.org.apache.hadoop.mapred", "INFO, jobid");
        hadoopProps.setProperty("log4j.logger.org.apache.hadoop.mapreduce.Job", "INFO, jobid");

        String localProps = new File(SQOOP_LOG4J_PROPS).getAbsolutePath();
        OutputStream os1 = new FileOutputStream(localProps);
        try {
            hadoopProps.store(os1, "");
        }
        finally {
            os1.close();
        }
      
        PropertyConfigurator.configure(SQOOP_LOG4J_PROPS);

        return logFile;
    }

    protected void run(String[] args) throws Exception {
        System.out.println();
        System.out.println("Oozie Sqoop action configuration");
        System.out.println("=================================================================");

        Configuration sqoopConf = setUpSqoopSite();
        String logFile = setUpSqoopLog4J(sqoopConf);

        String[] sqoopArgs = MapReduceMain.getStrings(sqoopConf, SQOOP_ARGS);
        if (sqoopArgs == null) {
            throw new RuntimeException("Action Configuration does not have [" + SQOOP_ARGS + "] property");
        }

        System.out.println("Sqoop command arguments :");
        for (String arg : sqoopArgs) {
            System.out.println("             " + arg);
        }

        System.out.println("=================================================================");
        System.out.println();
        System.out.println(">>> Invoking Sqoop command line now >>>");
        System.out.println();
        System.out.flush();

        try {
            runSqoopJob(sqoopArgs);
        }
        catch (SecurityException ex) {
            if (LauncherSecurityManager.getExitInvoked()) {
                if (LauncherSecurityManager.getExitCode() != 0) {
                    throw ex;
                }
            }
        }

        System.out.println();
        System.out.println("<<< Invocation of Sqoop command completed <<<");
        System.out.println();

        // harvesting and recording Hadoop Job IDs
        Properties jobIds = getHadoopJobIds(logFile, SQOOP_JOB_IDS_PATTERNS);

        File file = new File(System.getProperty("oozie.action.output.properties"));
        OutputStream os = new FileOutputStream(file);
        try {
            jobIds.store(os, "");
        }
        finally {
            os.close();
        }
        System.out.println(" Hadoop Job IDs executed by Sqoop: " + jobIds.getProperty(HADOOP_JOBS));
        System.out.println();
    }

    protected void runSqoopJob(String[] args) throws Exception {
        // running as from the command line
        Sqoop.main(args);
    }

    public static void setSqoopCommand(Configuration conf, String[] args) {
        MapReduceMain.setStrings(conf, SQOOP_ARGS, args);
    }
}
