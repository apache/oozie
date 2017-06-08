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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.LinkedHashMap;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.deploy.SparkSubmit;

import com.google.common.annotations.VisibleForTesting;

public class SparkMain extends LauncherMain {

    @VisibleForTesting
    static final Pattern[] SPARK_JOB_IDS_PATTERNS = {
            Pattern.compile("Submitted application (application[0-9_]*)") };
    @VisibleForTesting
    static final Pattern SPARK_ASSEMBLY_JAR_PATTERN = Pattern
            .compile("^spark-assembly((?:(-|_|(\\d+\\.))\\d+(?:\\.\\d+)*))*\\.jar$");
    @VisibleForTesting
    static final Pattern SPARK_YARN_JAR_PATTERN = Pattern
            .compile("^spark-yarn((?:(-|_|(\\d+\\.))\\d+(?:\\.\\d+)*))*\\.jar$");
    static final String HIVE_SITE_CONF = "hive-site.xml";
    static final String SPARK_LOG4J_PROPS = "spark-log4j.properties";

    private static final String CONF_OOZIE_SPARK_SETUP_HADOOP_CONF_DIR = "oozie.action.spark.setup.hadoop.conf.dir";
    private static final Pattern[] PYSPARK_DEP_FILE_PATTERN = { Pattern.compile("py4\\S*src.zip"),
            Pattern.compile("pyspark.zip") };

    public static void main(final String[] args) throws Exception {
        run(SparkMain.class, args);
    }

    @Override
    protected void run(final String[] args) throws Exception {
        final Configuration actionConf = loadActionConf();
        prepareHadoopConfig(actionConf);

        setYarnTag(actionConf);
        LauncherMainHadoopUtils.killChildYarnJobs(actionConf);
        final String logFile = setUpSparkLog4J(actionConf);
        setHiveSite(actionConf);

        final SparkArgsExtractor sparkArgsExtractor = new SparkArgsExtractor(actionConf);
        final List<String> sparkArgs = sparkArgsExtractor.extract(args);

        if (sparkArgsExtractor.isPySpark()){
            createPySparkLibFolder();
        }

        System.out.println("Spark Action Main class        : " + SparkSubmit.class.getName());
        System.out.println();
        System.out.println("Oozie Spark action configuration");
        System.out.println("=================================================================");
        System.out.println();

        final PasswordMasker passwordMasker = new PasswordMasker();
        for (final String arg : sparkArgs) {
            System.out.println("                    " + passwordMasker.maskPasswordsIfNecessary(arg));
        }
        System.out.println();

        try {
            runSpark(sparkArgs.toArray(new String[sparkArgs.size()]));
        }
        finally {
            System.out.println("\n<<< Invocation of Spark command completed <<<\n");
            writeExternalChildIDs(logFile, SPARK_JOB_IDS_PATTERNS, "Spark");
        }
    }

    private void prepareHadoopConfig(final Configuration actionConf) throws IOException {
        // Copying oozie.action.conf.xml into hadoop configuration *-site files.
        if (actionConf.getBoolean(CONF_OOZIE_SPARK_SETUP_HADOOP_CONF_DIR, false)) {
            final String actionXml = System.getProperty("oozie.action.conf.xml");
            if (actionXml != null) {
                final File currentDir = new File(actionXml).getParentFile();
                writeHadoopConfig(actionXml, currentDir);
            }
        }
    }

    /**
     * SparkActionExecutor sets the SPARK_HOME environment variable to the local directory.
     * Spark is looking for the pyspark.zip and py4j-VERSION-src.zip files in the python/lib folder under SPARK_HOME.
     * This function creates the subfolders and copies the zips from the local folder.
     * @throws OozieActionConfiguratorException  if the zip files are missing
     * @throws IOException if there is an error during file copy
     */
    private void createPySparkLibFolder() throws OozieActionConfiguratorException, IOException {
        final File pythonLibDir = new File("python/lib");
        if(!pythonLibDir.exists()){
            pythonLibDir.mkdirs();
            System.out.println("PySpark lib folder " + pythonLibDir.getAbsolutePath() + " folder created.");
        }

        for(final Pattern fileNamePattern : PYSPARK_DEP_FILE_PATTERN) {
            final File file = getMatchingPyFile(fileNamePattern);
            final File destination = new File(pythonLibDir, file.getName());
            FileUtils.copyFile(file, destination);
            System.out.println("Copied " + file + " to " + destination.getAbsolutePath());
        }
    }

    /**
     * Searches for a file in the current directory that matches the given pattern.
     * If there are multiple files matching the pattern returns one of them.
     * @param fileNamePattern the pattern to look for
     * @return the file if there is one
     * @throws OozieActionConfiguratorException if there is are no files matching the pattern
     */
    private File getMatchingPyFile(final Pattern fileNamePattern) throws OozieActionConfiguratorException {
        final File f = getMatchingFile(fileNamePattern);
        if (f != null) {
            return f;
        }
        throw new OozieActionConfiguratorException("Missing py4j and/or pyspark zip files. Please add them to "
                + "the lib folder or to the Spark sharelib.");
    }

    /**
     * Searches for a file in the current directory that matches the given
     * pattern. If there are multiple files matching the pattern returns one of
     * them.
     *
     * @param fileNamePattern the pattern to look for
     * @return the file if there is one else it returns null
     */
    static File getMatchingFile(final Pattern fileNamePattern) {
        final File localDir = new File(".");

        final String[] localFileNames = localDir.list();
        if (localFileNames == null) {
            return null;
        }

        for (final String fileName : localFileNames){
            if (fileNamePattern.matcher(fileName).find()){
                return new File(fileName);
            }
        }
        return null;
    }

    private void runSpark(final String[] args) throws Exception {
        System.out.println("=================================================================");
        System.out.println();
        System.out.println(">>> Invoking Spark class now >>>");
        System.out.println();
        System.out.flush();
        SparkSubmit.main(args);
    }

    private String setUpSparkLog4J(final Configuration actionConf) throws IOException {
        // Logfile to capture job IDs
        final String hadoopJobId = System.getProperty("oozie.launcher.job.id");
        if (hadoopJobId == null) {
            throw new RuntimeException("Launcher Hadoop Job ID system,property not set");
        }
        final String logFile = new File("spark-oozie-" + hadoopJobId + ".log").getAbsolutePath();
        Properties hadoopProps = new Properties();

        // Preparing log4j configuration
        URL log4jFile = Thread.currentThread().getContextClassLoader().getResource("log4j.properties");
        if (log4jFile != null) {
            // getting hadoop log4j configuration
            hadoopProps.load(log4jFile.openStream());
        }

        final String logLevel = actionConf.get("oozie.spark.log.level", "INFO");
        final String rootLogLevel = actionConf.get("oozie.action." + LauncherMapper.ROOT_LOGGER_LEVEL, "INFO");

        hadoopProps.setProperty("log4j.rootLogger", rootLogLevel + ", A");
        hadoopProps.setProperty("log4j.logger.org.apache.spark", logLevel + ", A, jobid");
        hadoopProps.setProperty("log4j.additivity.org.apache.spark", "false");
        hadoopProps.setProperty("log4j.appender.A", "org.apache.log4j.ConsoleAppender");
        hadoopProps.setProperty("log4j.appender.A.layout", "org.apache.log4j.PatternLayout");
        hadoopProps.setProperty("log4j.appender.A.layout.ConversionPattern", "%d [%t] %-5p %c %x - %m%n");
        hadoopProps.setProperty("log4j.appender.jobid", "org.apache.log4j.FileAppender");
        hadoopProps.setProperty("log4j.appender.jobid.file", logFile);
        hadoopProps.setProperty("log4j.appender.jobid.layout", "org.apache.log4j.PatternLayout");
        hadoopProps.setProperty("log4j.appender.jobid.layout.ConversionPattern", "%d [%t] %-5p %c %x - %m%n");
        hadoopProps.setProperty("log4j.logger.org.apache.hadoop.mapred", "INFO, jobid");
        hadoopProps.setProperty("log4j.logger.org.apache.hadoop.mapreduce.Job", "INFO, jobid");
        hadoopProps.setProperty("log4j.logger.org.apache.hadoop.yarn.client.api.impl.YarnClientImpl", "INFO, jobid");

        final String localProps = new File(SPARK_LOG4J_PROPS).getAbsolutePath();
        try (OutputStream os1 = new FileOutputStream(localProps)) {
            hadoopProps.store(os1, "");
        }
        PropertyConfigurator.configure(SPARK_LOG4J_PROPS);
        return logFile;
    }

    /**
     * Convert URIs into the default format which Spark expects
     * Also filters out duplicate entries
     * @param files
     * @return
     * @throws IOException
     * @throws URISyntaxException
     */
    static Map<String, URI> fixFsDefaultUrisAndFilterDuplicates(final URI[] files) throws IOException, URISyntaxException {
        final Map<String, URI> map= new LinkedHashMap<>();
        if (files == null) {
            return map;
        }
        final FileSystem fs = FileSystem.get(new Configuration(true));
        for (int i = 0; i < files.length; i++) {
            final URI fileUri = files[i];
            final Path p = new Path(fileUri);
            map.put(p.getName(), HadoopUriFinder.getFixedUri(fs, fileUri));
        }
        return map;
    }

    /**
     * Sets up hive-site.xml
     *
     * @param hiveConf
     * @throws IOException
     */
    private void setHiveSite(final Configuration hiveConf) throws IOException {
        // See https://issues.apache.org/jira/browse/HIVE-1411
        hiveConf.set("datanucleus.plugin.pluginRegistryBundleCheck", "LOG");

        // To ensure that the logs go into container attempt tmp directory
        // When unset, default is
        // System.getProperty("java.io.tmpdir") + File.separator +
        // System.getProperty("user.name")
        hiveConf.unset("hive.querylog.location");
        hiveConf.unset("hive.exec.local.scratchdir");

        // Write the action configuration out to hive-site.xml
        OutputStream os = null;
        try {
            os = new FileOutputStream(HIVE_SITE_CONF);
            hiveConf.writeXml(os);
        }
        finally {
            if (os != null) {
                os.close();
            }
        }
        // Reset the hiveSiteURL static variable as we just created
        // hive-site.xml.
        // If prepare block had a drop partition it would have been initialized
        // to null.
        try {
            Field declaredField = HiveConf.class.getDeclaredField("hiveSiteURL");
            declaredField.setAccessible(true);
            declaredField.set(null, HiveConf.class.getClassLoader().getResource("hive-site.xml"));
        }
        catch (Throwable ignore) {
        }
    }
}