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
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.deploy.SparkSubmit;

import com.google.common.annotations.VisibleForTesting;

public class SparkMain extends LauncherMain {
    private static final String MASTER_OPTION = "--master";
    private static final String MODE_OPTION = "--deploy-mode";
    private static final String JOB_NAME_OPTION = "--name";
    private static final String CLASS_NAME_OPTION = "--class";
    private static final String VERBOSE_OPTION = "--verbose";
    private static final String DRIVER_CLASSPATH_OPTION = "--driver-class-path";
    private static final String EXECUTOR_CLASSPATH = "spark.executor.extraClassPath=";
    private static final String DRIVER_CLASSPATH = "spark.driver.extraClassPath=";
    private static final String EXECUTOR_EXTRA_JAVA_OPTIONS = "spark.executor.extraJavaOptions=";
    private static final String DRIVER_EXTRA_JAVA_OPTIONS = "spark.driver.extraJavaOptions=";
    private static final String LOG4J_CONFIGURATION_JAVA_OPTION = "-Dlog4j.configuration=";
    private static final String HIVE_SECURITY_TOKEN = "spark.yarn.security.tokens.hive.enabled";
    private static final String HBASE_SECURITY_TOKEN = "spark.yarn.security.tokens.hbase.enabled";
    private static final String CONF_OOZIE_SPARK_SETUP_HADOOP_CONF_DIR = "oozie.action.spark.setup.hadoop.conf.dir";
    private static final String PWD = "$PWD" + File.separator + "*";
    private static final Pattern[] PYSPARK_DEP_FILE_PATTERN = { Pattern.compile("py4\\S*src.zip"),
            Pattern.compile("pyspark.zip") };
    private static final Pattern SPARK_DEFAULTS_FILE_PATTERN = Pattern.compile("spark-defaults.conf");
    private static final String SPARK_LOG4J_PROPS = "spark-log4j.properties";
    @VisibleForTesting
    static final Pattern[] SPARK_JOB_IDS_PATTERNS = {
            Pattern.compile("Submitted application (application[0-9_]*)") };
    public static final Pattern SPARK_ASSEMBLY_JAR_PATTERN = Pattern
            .compile("^spark-assembly((?:(-|_|(\\d+\\.))\\d+(?:\\.\\d+)*))*\\.jar$");
    public static final Pattern SPARK_YARN_JAR_PATTERN = Pattern
            .compile("^spark-yarn((?:(-|_|(\\d+\\.))\\d+(?:\\.\\d+)*))*\\.jar$");
    private static final Pattern SPARK_VERSION_1 = Pattern.compile("^1.*");
    private static final String SPARK_YARN_JAR = "spark.yarn.jar";
    private static final String SPARK_YARN_JARS = "spark.yarn.jars";
    public static void main(String[] args) throws Exception {
        run(SparkMain.class, args);
    }

    @Override
    protected void run(String[] args) throws Exception {
        boolean isPyspark = false;
        Configuration actionConf = loadActionConf();
        prepareHadoopConfig(actionConf);

        setYarnTag(actionConf);
        LauncherMainHadoopUtils.killChildYarnJobs(actionConf);
        String logFile = setUpSparkLog4J(actionConf);
        List<String> sparkArgs = new ArrayList<String>();

        sparkArgs.add(MASTER_OPTION);
        String master = actionConf.get(SparkActionExecutor.SPARK_MASTER);
        sparkArgs.add(master);

        // In local mode, everything runs here in the Launcher Job.
        // In yarn-client mode, the driver runs here in the Launcher Job and the
        // executor in Yarn.
        // In yarn-cluster mode, the driver and executor run in Yarn.
        String sparkDeployMode = actionConf.get(SparkActionExecutor.SPARK_MODE);
        if (sparkDeployMode != null) {
            sparkArgs.add(MODE_OPTION);
            sparkArgs.add(sparkDeployMode);
        }
        boolean yarnClusterMode = master.equals("yarn-cluster")
                || (master.equals("yarn") && sparkDeployMode != null && sparkDeployMode.equals("cluster"));
        boolean yarnClientMode = master.equals("yarn-client")
                || (master.equals("yarn") && sparkDeployMode != null && sparkDeployMode.equals("client"));

        sparkArgs.add(JOB_NAME_OPTION);
        sparkArgs.add(actionConf.get(SparkActionExecutor.SPARK_JOB_NAME));

        String className = actionConf.get(SparkActionExecutor.SPARK_CLASS);
        if (className != null) {
            sparkArgs.add(CLASS_NAME_OPTION);
            sparkArgs.add(className);
        }

        appendOoziePropertiesToSparkConf(sparkArgs, actionConf);

        String jarPath = actionConf.get(SparkActionExecutor.SPARK_JAR);
        if(jarPath!=null && jarPath.endsWith(".py")){
            isPyspark = true;
        }
        boolean addedHiveSecurityToken = false;
        boolean addedHBaseSecurityToken = false;
        boolean addedLog4jDriverSettings = false;
        boolean addedLog4jExecutorSettings = false;
        StringBuilder driverClassPath = new StringBuilder();
        StringBuilder executorClassPath = new StringBuilder();
        String sparkOpts = actionConf.get(SparkActionExecutor.SPARK_OPTS);
        if (StringUtils.isNotEmpty(sparkOpts)) {
            List<String> sparkOptions = splitSparkOpts(sparkOpts);
            for (int i = 0; i < sparkOptions.size(); i++) {
                String opt = sparkOptions.get(i);
                boolean addToSparkArgs = true;
                if (yarnClusterMode || yarnClientMode) {
                    if (opt.startsWith(EXECUTOR_CLASSPATH)) {
                        appendWithPathSeparator(opt.substring(EXECUTOR_CLASSPATH.length()), executorClassPath);
                        addToSparkArgs = false;
                    }
                    if (opt.startsWith(DRIVER_CLASSPATH)) {
                        appendWithPathSeparator(opt.substring(DRIVER_CLASSPATH.length()), driverClassPath);
                        addToSparkArgs = false;
                    }
                    if (opt.equals(DRIVER_CLASSPATH_OPTION)) {
                        // we need the next element after this option
                        appendWithPathSeparator(sparkOptions.get(i + 1), driverClassPath);
                        // increase i to skip the next element.
                        i++;
                        addToSparkArgs = false;
                    }
                }
                if (opt.startsWith(HIVE_SECURITY_TOKEN)) {
                    addedHiveSecurityToken = true;
                }
                if (opt.startsWith(HBASE_SECURITY_TOKEN)) {
                    addedHBaseSecurityToken = true;
                }
                if (opt.startsWith(EXECUTOR_EXTRA_JAVA_OPTIONS) || opt.startsWith(DRIVER_EXTRA_JAVA_OPTIONS)) {
                    if(!opt.contains(LOG4J_CONFIGURATION_JAVA_OPTION)) {
                        opt += " " + LOG4J_CONFIGURATION_JAVA_OPTION + SPARK_LOG4J_PROPS;
                    }else{
                        System.out.println("Warning: Spark Log4J settings are overwritten." +
                                " Child job IDs may not be available");
                    }
                    if(opt.startsWith(EXECUTOR_EXTRA_JAVA_OPTIONS)) {
                        addedLog4jExecutorSettings = true;
                    }else{
                        addedLog4jDriverSettings = true;
                    }
                }
                if(addToSparkArgs) {
                    sparkArgs.add(opt);
                }
            }
        }

        if ((yarnClusterMode || yarnClientMode)) {
            // Include the current working directory (of executor container)
            // in executor classpath, because it will contain localized
            // files
            appendWithPathSeparator(PWD, executorClassPath);
            appendWithPathSeparator(PWD, driverClassPath);

            sparkArgs.add("--conf");
            sparkArgs.add(EXECUTOR_CLASSPATH + executorClassPath.toString());

            sparkArgs.add("--conf");
            sparkArgs.add(DRIVER_CLASSPATH + driverClassPath.toString());
        }

        if (actionConf.get(MAPREDUCE_JOB_TAGS) != null) {
            sparkArgs.add("--conf");
            sparkArgs.add("spark.yarn.tags=" + actionConf.get(MAPREDUCE_JOB_TAGS));
        }

        if (!addedHiveSecurityToken) {
            sparkArgs.add("--conf");
            sparkArgs.add(HIVE_SECURITY_TOKEN + "=false");
        }
        if (!addedHBaseSecurityToken) {
            sparkArgs.add("--conf");
            sparkArgs.add(HBASE_SECURITY_TOKEN + "=false");
        }
        if(!addedLog4jExecutorSettings) {
            sparkArgs.add("--conf");
            sparkArgs.add(EXECUTOR_EXTRA_JAVA_OPTIONS + LOG4J_CONFIGURATION_JAVA_OPTION + SPARK_LOG4J_PROPS);
        }
        if(!addedLog4jDriverSettings) {
            sparkArgs.add("--conf");
            sparkArgs.add(DRIVER_EXTRA_JAVA_OPTIONS + LOG4J_CONFIGURATION_JAVA_OPTION + SPARK_LOG4J_PROPS);
        }
        File defaultConfFile = getMatchingFile(SPARK_DEFAULTS_FILE_PATTERN);
        if (defaultConfFile != null) {
            sparkArgs.add("--properties-file");
            sparkArgs.add(SPARK_DEFAULTS_FILE_PATTERN.toString());
        }

        if ((yarnClusterMode || yarnClientMode)) {
            LinkedList<URI> fixedUris = fixFsDefaultUris(DistributedCache.getCacheFiles(actionConf));
            JarFilter jarfilter = new JarFilter(fixedUris, jarPath);
            jarfilter.filter();
            jarPath = jarfilter.getApplicationJar();
            fixedUris.add(new Path(SPARK_LOG4J_PROPS).toUri());
            String cachedFiles = StringUtils.join(fixedUris, ",");
            if (cachedFiles != null && !cachedFiles.isEmpty()) {
                sparkArgs.add("--files");
                sparkArgs.add(cachedFiles);
            }
            fixedUris = fixFsDefaultUris(DistributedCache.getCacheArchives(actionConf));
            String cachedArchives = StringUtils.join(fixedUris, ",");
            if (cachedArchives != null && !cachedArchives.isEmpty()) {
                sparkArgs.add("--archives");
                sparkArgs.add(cachedArchives);
            }
            setSparkYarnJarsConf(sparkArgs, jarfilter.getSparkYarnJar(), jarfilter.getSparkVersion());
        }

        if (!sparkArgs.contains(VERBOSE_OPTION)) {
            sparkArgs.add(VERBOSE_OPTION);
        }

        sparkArgs.add(jarPath);
        for (String arg : args) {
            sparkArgs.add(arg);
        }
        if (isPyspark){
            createPySparkLibFolder();
        }


        System.out.println("Spark Action Main class        : " + SparkSubmit.class.getName());
        System.out.println();
        System.out.println("Oozie Spark action configuration");
        System.out.println("=================================================================");
        System.out.println();
        for (String arg : sparkArgs) {
            System.out.println("                    " + arg);
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

    private void prepareHadoopConfig(Configuration actionConf) throws IOException {
        // Copying oozie.action.conf.xml into hadoop configuration *-site files.
        if (actionConf.getBoolean(CONF_OOZIE_SPARK_SETUP_HADOOP_CONF_DIR, false)) {
            String actionXml = System.getProperty("oozie.action.conf.xml");
            if (actionXml != null) {
                File currentDir = new File(actionXml).getParentFile();
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
        File pythonLibDir = new File("python/lib");
        if(!pythonLibDir.exists()){
            pythonLibDir.mkdirs();
            System.out.println("PySpark lib folder " + pythonLibDir.getAbsolutePath() + " folder created.");
        }

        for(Pattern fileNamePattern : PYSPARK_DEP_FILE_PATTERN) {
            File file = getMatchingPyFile(fileNamePattern);
            File destination = new File(pythonLibDir, file.getName());
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
    private File getMatchingPyFile(Pattern fileNamePattern) throws OozieActionConfiguratorException {
        File f = getMatchingFile(fileNamePattern);
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
    private static File getMatchingFile(Pattern fileNamePattern) throws OozieActionConfiguratorException {
        File localDir = new File(".");
        for(String fileName : localDir.list()){
            if(fileNamePattern.matcher(fileName).find()){
                return new File(fileName);
            }
        }
        return null;
    }

    private void runSpark(String[] args) throws Exception {
        System.out.println("=================================================================");
        System.out.println();
        System.out.println(">>> Invoking Spark class now >>>");
        System.out.println();
        System.out.flush();
        SparkSubmit.main(args);
    }

    /**
     * Converts the options to be Spark-compatible.
     * <ul>
     *     <li>Parameters are separated by whitespace and can be groupped using double quotes</li>
     *     <li>Quotes should be removed</li>
     *     <li>Adjacent whitespace separators are treated as one</li>
     * </ul>
     * @param sparkOpts the options for Spark
     * @return the options parsed into a list
     */
    static List<String> splitSparkOpts(String sparkOpts){
        List<String> result = new ArrayList<String>();
        StringBuilder currentWord = new StringBuilder();
        boolean insideQuote = false;
        for (int i = 0; i < sparkOpts.length(); i++) {
            char c = sparkOpts.charAt(i);
            if (c == '"') {
                insideQuote = !insideQuote;
            } else if (Character.isWhitespace(c) && !insideQuote) {
                if (currentWord.length() > 0) {
                    result.add(currentWord.toString());
                    currentWord.setLength(0);
                }
            } else {
                currentWord.append(c);
            }
        }
        if(currentWord.length()>0) {
            result.add(currentWord.toString());
        }
        return result;
    }

    public static String setUpSparkLog4J(Configuration distcpConf) throws IOException {
        // Logfile to capture job IDs
        String hadoopJobId = System.getProperty("oozie.launcher.job.id");
        if (hadoopJobId == null) {
            throw new RuntimeException("Launcher Hadoop Job ID system,property not set");
        }
        String logFile = new File("spark-oozie-" + hadoopJobId + ".log").getAbsolutePath();
        Properties hadoopProps = new Properties();

        // Preparing log4j configuration
        URL log4jFile = Thread.currentThread().getContextClassLoader().getResource("log4j.properties");
        if (log4jFile != null) {
            // getting hadoop log4j configuration
            hadoopProps.load(log4jFile.openStream());
        }

        String logLevel = distcpConf.get("oozie.spark.log.level", "INFO");
        String rootLogLevel = distcpConf.get("oozie.action." + LauncherMapper.ROOT_LOGGER_LEVEL, "INFO");

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

        String localProps = new File(SPARK_LOG4J_PROPS).getAbsolutePath();
        OutputStream os1 = new FileOutputStream(localProps);
        try {
            hadoopProps.store(os1, "");
        }
        finally {
            os1.close();
        }
        PropertyConfigurator.configure(SPARK_LOG4J_PROPS);
        return logFile;
    }

    /**
     * Convert URIs into the default format which Spark expects
     *
     * @param files
     * @return
     * @throws IOException
     * @throws URISyntaxException
     */
    private LinkedList<URI> fixFsDefaultUris(URI[] files) throws IOException, URISyntaxException {
        if (files == null) {
            return null;
        }
        LinkedList<URI> listUris = new LinkedList<URI>();
        FileSystem fs = FileSystem.get(new Configuration(true));
        for (int i = 0; i < files.length; i++) {
            URI fileUri = files[i];
            listUris.add(getFixedUri(fs, fileUri));
        }
        return listUris;
    }

    /**
     * Sets spark.yarn.jars for Spark 2.X. Sets spark.yarn.jar for Spark 1.X.
     *
     * @param sparkArgs
     * @param sparkYarnJar
     * @param sparkVersion
     */
    private void setSparkYarnJarsConf(List<String> sparkArgs, String sparkYarnJar, String sparkVersion) {
        if (SPARK_VERSION_1.matcher(sparkVersion).find()) {
            // In Spark 1.X.X, set spark.yarn.jar to avoid
            // multiple distribution
            sparkArgs.add("--conf");
            sparkArgs.add(SPARK_YARN_JAR + "=" + sparkYarnJar);
                }
        else {
            // In Spark 2.X.X, set spark.yarn.jars
            sparkArgs.add("--conf");
            sparkArgs.add(SPARK_YARN_JARS + "=" + sparkYarnJar);
        }
    }

    private static String getJarVersion(File jarFile) throws IOException {
        @SuppressWarnings("resource")
        Manifest manifest = new JarFile(jarFile).getManifest();
        return manifest.getMainAttributes().getValue("Specification-Version");
    }

    /*
     * Get properties that needs to be passed to Spark as Spark configuration from actionConf.
     */
    @VisibleForTesting
    protected void appendOoziePropertiesToSparkConf(List<String> sparkArgs, Configuration actionConf) {
        for (Map.Entry<String, String> oozieConfig : actionConf
                .getValByRegex("^oozie\\.(?!launcher|spark).+").entrySet()) {
            sparkArgs.add("--conf");
            sparkArgs.add(String.format("spark.%s=%s", oozieConfig.getKey(), oozieConfig.getValue()));
        }
    }

    private void appendWithPathSeparator(String what, StringBuilder to){
        if(to.length() > 0){
            to.append(File.pathSeparator);
        }
        to.append(what);
    }

    private static URI getFixedUri(URI fileUri) throws URISyntaxException, IOException {
        FileSystem fs = FileSystem.get(new Configuration(true));
        return getFixedUri(fs, fileUri);
    }

    /**
     * Spark compares URIs based on scheme, host and port. Here we convert URIs
     * into the default format so that Spark won't think those belong to
     * different file system. This will avoid an extra copy of files which
     * already exists on same hdfs.
     *
     * @param fs
     * @param fileUri
     * @return fixed uri
     * @throws URISyntaxException
     */
    private static URI getFixedUri(FileSystem fs, URI fileUri) throws URISyntaxException {
        if (fs.getUri().getScheme().equals(fileUri.getScheme())
                && (fs.getUri().getHost().equals(fileUri.getHost()) || fileUri.getHost() == null)
                && (fs.getUri().getPort() == -1 || fileUri.getPort() == -1
                        || fs.getUri().getPort() == fileUri.getPort())) {
            return new URI(fs.getUri().getScheme(), fileUri.getUserInfo(), fs.getUri().getHost(), fs.getUri().getPort(),
                    fileUri.getPath(), fileUri.getQuery(), fileUri.getFragment());
        }
        return fileUri;
    }

    /**
     * This class is used for filtering out unwanted jars.
     */
    static class JarFilter {
        private String sparkVersion = "1.X.X";
        private String sparkYarnJar;
        private String applicationJar;
        private LinkedList<URI> listUris = null;

        /**
         * @param listUris List of URIs to be filtered
         * @param jarPath Application jar
         * @throws IOException
         * @throws URISyntaxException
         */
        public JarFilter(LinkedList<URI> listUris, String jarPath) throws URISyntaxException, IOException {
            this.listUris = listUris;
            applicationJar = jarPath;
            Path p = new Path(jarPath);
            if (p.isAbsolute()) {
                applicationJar = getFixedUri(p.toUri()).toString();
            }
        }

        /**
         * Filters out the Spark yarn jar and application jar. Also records
         * spark yarn jar's version.
         *
         * @throws OozieActionConfiguratorException
         */
        public void filter() throws OozieActionConfiguratorException {
            Iterator<URI> iterator = listUris.iterator();
            File matchedFile = null;
            Path applJarPath = new Path(applicationJar);
            while (iterator.hasNext()) {
                URI uri = iterator.next();
                Path p = new Path(uri);
                if (SPARK_YARN_JAR_PATTERN.matcher(p.getName()).find()) {
                    matchedFile = getMatchingFile(SPARK_YARN_JAR_PATTERN);
                }
                else if (SPARK_ASSEMBLY_JAR_PATTERN.matcher(p.getName()).find()) {
                    matchedFile = getMatchingFile(SPARK_ASSEMBLY_JAR_PATTERN);
                }
                if (matchedFile != null) {
                    sparkYarnJar = uri.toString();
                    try {
                        sparkVersion = getJarVersion(matchedFile);
                        System.out.println("Spark Version " + sparkVersion);
                    }
                    catch (IOException io) {
                        System.out.println(
                                "Unable to open " + matchedFile.getPath() + ". Default Spark Version " + sparkVersion);
                    }
                    iterator.remove();
                    matchedFile = null;
                }
                // Here we skip the application jar, because
                // (if uris are same,) it will get distributed multiple times
                // - one time with --files and another time as application jar.
                if (isApplicationJar(p.getName(), uri, applJarPath)) {
                    String fragment = uri.getFragment();
                    applicationJar = fragment != null && fragment.length() > 0 ? fragment : uri.toString();
                    iterator.remove();
                }
            }
        }

        /**
         * Checks if a file is application jar
         *
         * @param fileName fileName name of the file
         * @param fileUri fileUri URI of the file
         * @param applJarPath Path of application jar
         * @return true if fileName or fileUri is the application jar
         */
        private boolean isApplicationJar(String fileName, URI fileUri, Path applJarPath) {
            return (fileName.equals(applicationJar) || fileUri.toString().equals(applicationJar)
                    || applJarPath.getName().equals(fileName)
                    || applicationJar.equals(fileUri.getFragment()));
        }

        public String getApplicationJar() {
            return applicationJar;
        }

        public String getSparkYarnJar() {
            return sparkYarnJar;
        }

        public String getSparkVersion() {
            return sparkVersion;
        }

    }
}
