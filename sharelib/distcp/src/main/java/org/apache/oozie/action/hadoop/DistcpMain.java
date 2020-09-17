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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

import com.google.common.annotations.VisibleForTesting;

public class DistcpMain extends JavaMain {

    private Constructor<?> construct;
    private Object[] constArgs;
    private static final String DISTCP_LOG4J_PROPS = "distcp-log4j.properties";

    @VisibleForTesting
    static final Pattern[] DISTCP_JOB_IDS_PATTERNS = {
            Pattern.compile("Job complete: (job_\\S*)"),
            Pattern.compile("Job (job_\\S*) completed successfully"),
            Pattern.compile("Submitted application (application[0-9_]*)")
    };
    public static void main(String[] args) throws Exception {
        run(DistcpMain.class, args);
    }

    @Override
    protected void run(String[] args) throws Exception {

        Configuration actionConf = loadActionConf();
        LauncherMain.killChildYarnJobs(actionConf);
        String logFile = setUpDistcpLog4J(actionConf);
        Class<?> klass = actionConf.getClass(LauncherAMUtils.CONF_OOZIE_ACTION_MAIN_CLASS,
                org.apache.hadoop.tools.DistCp.class);
        System.out.println("Main class        : " + klass.getName());
        System.out.println("Arguments         :");
        for (String arg : args) {
            System.out.println("                    " + arg);
        }

        // propagate delegation related props from launcher job to MR job
        if (getFilePathFromEnv("HADOOP_TOKEN_FILE_LOCATION") != null) {
            actionConf.set("mapreduce.job.credentials.binary", getFilePathFromEnv("HADOOP_TOKEN_FILE_LOCATION"));
        }

        getConstructorAndArgs(klass, actionConf);
        if (construct == null) {
            throw new RuntimeException("Distcp constructor was not found, unable to instantiate");
        }
        if (constArgs == null) {
            throw new RuntimeException("Arguments for distcp constructor is null, unable to instantiate");
        }
        try {
            Tool distcp = (Tool) construct.newInstance(constArgs);
            int i = ToolRunner.run(distcp, args);
            if (i != 0) {
                throw new RuntimeException("Returned value from distcp is non-zero (" + i + ")");
            }
        }
        catch (InvocationTargetException ex) {
            throw new JavaMainException(ex.getCause());
        }
        finally {
            System.out.println("\n<<< Invocation of DistCp command completed <<<\n");
            writeExternalChildIDs(logFile, DISTCP_JOB_IDS_PATTERNS, "Distcp");
        }
    }

    protected void getConstructorAndArgs(Class<?> klass, Configuration actionConf) throws Exception {
        Constructor<?>[] allConstructors = klass.getConstructors();
        for (Constructor<?> cstruct : allConstructors) {
            Class<?>[] pType = cstruct.getParameterTypes();
            construct = cstruct;
            if (pType.length == 1 && pType[0].equals(Class.forName("org.apache.hadoop.conf.Configuration"))) {
                System.out.println("found Distcp v1 Constructor");
                System.out.println("                    " + cstruct.toString());
                constArgs = new Object[1];
                constArgs[0] = actionConf;
                break;
            }
            else if (pType.length == 2 && pType[0].equals(Class.forName("org.apache.hadoop.conf.Configuration"))) {
                // 2nd argument is org.apache.hadoop.tools.DistCpOptions
                System.out.println("found Distcp v2 Constructor");
                System.out.println("                    " + cstruct.toString());
                constArgs = new Object[2];
                constArgs[0] = actionConf;
                constArgs[1] = null;
                break;
            }
        }
    }

    private String setUpDistcpLog4J(Configuration distcpConf) throws IOException {
        // Logfile to capture job IDs
        String hadoopJobId = System.getProperty("oozie.launcher.job.id");
        if (hadoopJobId == null) {
            throw new RuntimeException("Launcher Hadoop Job ID system,property not set");
        }

        String logFile = new File("distcp-oozie-" + hadoopJobId + ".log").getAbsolutePath();

        String logLevel = distcpConf.get("oozie.distcp.log.level", "INFO");
        String rootLogLevel = distcpConf.get("oozie.action." + LauncherAMUtils.ROOT_LOGGER_LEVEL, "INFO");

        log4jProperties.setProperty("log4j.rootLogger", rootLogLevel + ", A");
        log4jProperties.setProperty("log4j.logger.org.apache.hadoop.tools", logLevel + ", A, jobid");
        log4jProperties.setProperty("log4j.additivity.org.apache.hadoop.tools", "false");
        log4jProperties.setProperty("log4j.appender.A", "org.apache.log4j.ConsoleAppender");
        log4jProperties.setProperty("log4j.appender.A.layout", "org.apache.log4j.PatternLayout");
        log4jProperties.setProperty("log4j.appender.A.layout.ConversionPattern", "%-4r [%t] %-5p %c %x - %m%n");

        log4jProperties.setProperty("log4j.appender.jobid", "org.apache.log4j.FileAppender");
        log4jProperties.setProperty("log4j.appender.jobid.file", logFile);
        log4jProperties.setProperty("log4j.appender.jobid.layout", "org.apache.log4j.PatternLayout");
        log4jProperties.setProperty("log4j.appender.jobid.layout.ConversionPattern", "%-4r [%t] %-5p %c %x - %m%n");
        log4jProperties.setProperty("log4j.logger.org.apache.hadoop.mapred", "INFO, jobid");
        log4jProperties.setProperty("log4j.logger.org.apache.hadoop.mapreduce.Job", "INFO, jobid");
        log4jProperties.setProperty("log4j.logger.org.apache.hadoop.yarn.client.api.impl.YarnClientImpl", "INFO, jobid");

        String localProps = new File(DISTCP_LOG4J_PROPS).getAbsolutePath();
        createFileWithContentIfNotExists(localProps, log4jProperties);
        PropertyConfigurator.configure(DISTCP_LOG4J_PROPS);

        return logFile;
    }
}
