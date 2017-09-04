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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.PropertyConfigurator;
import org.apache.sqoop.Sqoop;

import com.google.common.annotations.VisibleForTesting;

public class SqoopMain extends LauncherMain {

    public static final String SQOOP_SITE_CONF = "sqoop-site.xml";

    @VisibleForTesting
    static final Pattern[] SQOOP_JOB_IDS_PATTERNS = {
            Pattern.compile("Job complete: (job_\\S*)"),
            Pattern.compile("Job (job_\\S*) has completed successfully"),
            Pattern.compile("Submitted application (application[0-9_]*)")
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
        setYarnTag(sqoopConf);

        String delegationToken = getFilePathFromEnv("HADOOP_TOKEN_FILE_LOCATION");
        if (delegationToken != null) {
            sqoopConf.setBoolean("sqoop.hbase.security.token.skip", true);
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
        createFileWithContentIfNotExists(SQOOP_SITE_CONF, sqoopConf);
        logMasking("Sqoop Configuration Properties:", sqoopConf);
        return sqoopConf;
    }

    private String setUpSqoopLog4J(Configuration sqoopConf) throws IOException {
        //Logfile to capture job IDs
        String hadoopJobId = System.getProperty("oozie.launcher.job.id");
        if (hadoopJobId == null) {
            throw new RuntimeException("Launcher Hadoop Job ID system property not set");
        }

        String logFile = new File("sqoop-oozie-" + hadoopJobId + ".log").getAbsolutePath();

        String logLevel = sqoopConf.get("oozie.sqoop.log.level", "INFO");
        String rootLogLevel = sqoopConf.get("oozie.action." + LauncherAMUtils.ROOT_LOGGER_LEVEL, "INFO");

        log4jProperties.setProperty("log4j.rootLogger", rootLogLevel + ", A");
        log4jProperties.setProperty("log4j.logger.org.apache.sqoop", logLevel + ", A");
        log4jProperties.setProperty("log4j.additivity.org.apache.sqoop", "false");
        log4jProperties.setProperty("log4j.appender.A", "org.apache.log4j.ConsoleAppender");
        log4jProperties.setProperty("log4j.appender.A.layout", "org.apache.log4j.PatternLayout");
        log4jProperties.setProperty("log4j.appender.A.layout.ConversionPattern", "%d [%t] %-5p %c %x - %m%n");

        log4jProperties.setProperty("log4j.appender.jobid", "org.apache.log4j.FileAppender");
        log4jProperties.setProperty("log4j.appender.jobid.file", logFile);
        log4jProperties.setProperty("log4j.appender.jobid.layout", "org.apache.log4j.PatternLayout");
        log4jProperties.setProperty("log4j.appender.jobid.layout.ConversionPattern", "%d [%t] %-5p %c %x - %m%n");
        log4jProperties.setProperty("log4j.logger.org.apache.hadoop.mapred", "INFO, jobid, A");
        log4jProperties.setProperty("log4j.logger.org.apache.hadoop.mapreduce.Job", "INFO, jobid, A");
        log4jProperties.setProperty("log4j.logger.org.apache.hadoop.yarn.client.api.impl.YarnClientImpl", "INFO, jobid");

        String localProps = new File(SQOOP_LOG4J_PROPS).getAbsolutePath();
        createFileWithContentIfNotExists(localProps, log4jProperties);

        PropertyConfigurator.configure(SQOOP_LOG4J_PROPS);

        return logFile;
    }

    @Override
    protected void run(String[] args) throws Exception {
        System.out.println();
        System.out.println("Oozie Sqoop action configuration");
        System.out.println("=================================================================");

        Configuration sqoopConf = setUpSqoopSite();
        String logFile = setUpSqoopLog4J(sqoopConf);

        String[] sqoopArgs = ActionUtils.getStrings(sqoopConf, SqoopActionExecutor.SQOOP_ARGS);
        if (sqoopArgs == null) {
            throw new RuntimeException("Action Configuration does not have [" + SqoopActionExecutor.SQOOP_ARGS + "] property");
        }

        printArgs("Sqoop command arguments :", sqoopArgs);
        LauncherMain.killChildYarnJobs(sqoopConf);

        System.out.println("=================================================================");
        System.out.println();
        System.out.println(">>> Invoking Sqoop command line now >>>");
        System.out.println();
        System.out.flush();

        try {
            runSqoopJob(sqoopArgs);
        }
        finally {
            System.out.println("\n<<< Invocation of Sqoop command completed <<<\n");
            writeExternalChildIDs(logFile, SQOOP_JOB_IDS_PATTERNS, "Sqoop");
        }
    }

    protected void runSqoopJob(String[] args) throws Exception {
        // running as from the command line
        Sqoop.main(args);
    }
}
