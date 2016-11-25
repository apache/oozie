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

package org.apache.oozie.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.yarn.api.protocolrecords.ApplicationsRequestScope;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.oozie.action.hadoop.YarnJobActions;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.MiniHCatServer.RUNMODE;
import org.apache.oozie.test.hive.MiniHS2;
import org.apache.oozie.util.ClasspathUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;

/**
 * Base JUnit <code>TestCase</code> subclass used by all Oozie testcases.
 * <p/>
 * This class provides the following functionality:
 * <p/>
 * <ul>
 *   <li>Creates a unique test working directory per test method.</li>
 *   <li>Resets changed system properties to their original values after every test.</li>
 *   <li>WaitFor that supports a predicate,to wait for a condition. It has timeout.</li>
 * </ul>
 * <p/>
 * The base directory for the test working directory must be specified via
 * the system property <code>oozie.test.dir</code>, there default value is '/tmp'.
 * <p/>
 * From within testcases, system properties must be changed using the {@link #setSystemProperty} method.
 */
public abstract class XTestCase extends TestCase {
    private static EnumSet<YarnApplicationState> YARN_TERMINAL_STATES =
            EnumSet.of(YarnApplicationState.FAILED, YarnApplicationState.KILLED, YarnApplicationState.FINISHED);
    protected static final File OOZIE_SRC_DIR = new TestPropertiesLoader().loadTestPropertiesOrThrow();
    private final TestCaseDirectories testCaseDirectories = new TestCaseDirectories();
    private final TestSystemProperties testSystemProperties = new TestSystemProperties();

    private final TestConfigurations testConfigurations = new TestConfigurations();
    private String testCaseDir;

    private String testCaseConfDir;
    private String hadoopVersion;
    protected XLog log = new XLog(LogFactory.getLog(getClass()));

    static final String OOZIE_TEST_PROPERTIES = "oozie.test.properties";
    protected static final String SYSTEM_LINE_SEPARATOR = System.getProperty("line.separator");
    protected static float WAITFOR_RATIO = Float.parseFloat(System.getProperty("oozie.test.waitfor.ratio", "1"));
    protected static final String LOCAL_ACTIVE_MQ_BROKER = "vm://localhost?broker.persistent=false";
    protected static final String ACTIVE_MQ_CONN_FACTORY = "org.apache.activemq.jndi.ActiveMQInitialContextFactory";

    /**
     * Initialize the test working directory. <p/> If it does not exist it creates it, if it already exists it deletes
     * all its contents. <p/> The test working directory it is not deleted after the test runs. <p/> It will also cleanup the
     * database tables. <p/>
     *
     * @throws Exception if the test workflow working directory could not be created or there was a problem cleaning the database
     */
    @Override
    protected void setUp() throws Exception {
        setUp(true);
    }

    /**
     * Like {@link #setUp()} but allows skipping cleaning up the database tables.  Most tests should use the other method, unless
     * they specifically don't want to (or can't) clean up the database tables.
     *
     * @param cleanUpDBTables true if should cleanup the database tables, false if not
     * @throws Exception if the test workflow working directory could not be created or there was a problem cleaning the database
     */
    protected void setUp(final boolean cleanUpDBTables) throws Exception {
        RUNNING_TESTCASES.incrementAndGet();
        super.setUp();

        testCaseDirectories.createTestDirOrError();

        hadoopVersion = System.getProperty(TestConstants.HADOOP_VERSION, "0.20.0");
        testCaseDir = testCaseDirectories.createTestCaseDir(this, true);

        setupOozieHome();

        testCaseConfDir = createTestCaseSubDir("conf");

        final InputStream oozieSiteSourceStream = loadTestOozieSiteOrError();

        setupOozieSiteConfiguration(oozieSiteSourceStream);

        final File hadoopConfDir = copyAndGetHadoopConfig();

        testSystemProperties.setupSystemProperties(testCaseDir);
        if (testSystemProperties.isEmbeddedHadoop()) {
            setUpEmbeddedHadoop(testCaseDir);
        }
        if (testSystemProperties.isEmbeddedHadoop2()) {
            setUpEmbeddedHadoop2();
        }

        if (yarnCluster != null) {
            try (final OutputStream os = new FileOutputStream(new File(hadoopConfDir, "core-site.xml"))) {
                final Configuration conf = testConfigurations.createJobConfFromYarnCluster(yarnCluster.getConfig());
                conf.writeXml(os);
            }
        }

        if (System.getProperty("oozie.test.metastore.server", "false").equals("true")) {
            setupHCatalogServer();
        }

        if (System.getProperty("oozie.test.hive.server.2", "false").equals("true")) {
            setupHiveServer2();
        }

        // Cleanup any leftover database data to make sure we start each test with an empty database
        if (cleanUpDBTables) {
            cleanUpDBTables();
        }
    }

    private InputStream loadTestOozieSiteOrError() throws IOException {
        final String oozieTestDB = System.getProperty("oozie.test.db", "hsqldb");
        final String defaultOozieSize =
                new File(OOZIE_SRC_DIR, "core/src/test/resources/" + oozieTestDB + "-oozie-site.xml").getAbsolutePath();
        final String customOozieSite = System.getProperty("oozie.test.config.file", defaultOozieSize);
        File source = new File(customOozieSite);
        if (!source.isAbsolute()) {
            source = new File(OOZIE_SRC_DIR, customOozieSite);
        }
        source = source.getAbsoluteFile();
        InputStream oozieSiteSourceStream = null;
        if (source.exists()) {
            oozieSiteSourceStream = new FileInputStream(source);
        }
        else {
            // If we can't find it, try using the class loader (useful if we're using XTestCase from outside core)
            final URL sourceURL = getClass().getClassLoader().getResource(oozieTestDB + "-oozie-site.xml");
            if (sourceURL != null) {
                oozieSiteSourceStream = sourceURL.openStream();
            }
            else {
                // If we still can't find it, then exit
                System.err.println();
                System.err.println(XLog.format("Custom configuration file for testing does not exist [{0}]",
                        source.getAbsolutePath()));
                System.err.println();
                System.exit(-1);
            }
        }
        return oozieSiteSourceStream;
    }

    private void setupOozieHome() throws ServiceException {
        setSystemProperty(Services.OOZIE_HOME_DIR, testCaseDir);
        Services.setOozieHome();
    }

    private void setupOozieSiteConfiguration(final InputStream oozieSiteSourceStream) throws IOException {
        // Copy the specified oozie-site file from oozieSiteSourceStream to the test case dir as oozie-site.xml
        final Configuration oozieSiteConf = new Configuration(false);
        oozieSiteConf.addResource(oozieSiteSourceStream);
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        final InputStream inputStream = classLoader.getResourceAsStream(ConfigurationService.DEFAULT_CONFIG_FILE);
        final XConfiguration configuration = new XConfiguration(inputStream);
        final String classes = configuration.get(Services.CONF_SERVICE_CLASSES);
        // Disable sharelib service as it cannot find the sharelib jars
        // as maven has target/classes in classpath and not the jar because test phase is before package phase
        oozieSiteConf.set(Services.CONF_SERVICE_CLASSES, classes.replaceAll("org.apache.oozie.service.ShareLibService,", ""));
        // Make sure to create the Oozie DB during unit tests
        oozieSiteConf.set(JPAService.CONF_CREATE_DB_SCHEMA, "true");
        final File target = new File(testCaseConfDir, "oozie-site.xml");
        oozieSiteConf.writeXml(new FileOutputStream(target));
    }

    private File copyAndGetHadoopConfig() throws IOException {
        final File hadoopConfDir = new File(testCaseConfDir, "hadoop-conf");
        hadoopConfDir.mkdir();
        final File actionConfDir = new File(testCaseConfDir, "action-conf");
        actionConfDir.mkdir();
        final File source = new File(OOZIE_SRC_DIR, "core/src/test/resources/hadoop-config.xml");
        InputStream hadoopConfigResourceStream = null;
        if (!source.exists()) {
            // If we can't find it, try using the class loader (useful if we're using XTestCase from outside core)
            final URL sourceURL = getClass().getClassLoader().getResource("hadoop-config.xml");
            if (sourceURL != null) {
                hadoopConfigResourceStream = sourceURL.openStream();
            }
            else {
                // If we still can't find it, then exit
                System.err.println();
                System.err.println(XLog.format("hadoop-config.xml configuration file for testing does not exist [{0}]",
                        source.getAbsolutePath()));
                System.err.println();
                System.exit(-1);
            }
        } else {
            hadoopConfigResourceStream = new FileInputStream(source);
        }
        final File target = new File(hadoopConfDir, "hadoop-site.xml");
        IOUtils.copyStream(hadoopConfigResourceStream, new FileOutputStream(target));
        return hadoopConfDir;
    }

    /**
     * Clean up the test case.
     */
    @Override
    protected void tearDown() throws Exception {
        tearDownHiveServer2();
        tearDownHCatalogServer();
        testSystemProperties.resetSystemProperties();
        testCaseDir = null;
        super.tearDown();
        RUNNING_TESTCASES.decrementAndGet();
        LAST_TESTCASE_FINISHED.set(System.currentTimeMillis());
    }

    /**
     * Return the test working directory. The directory name is the full class name of the test plus the test method
     * name.
     *
     * @return the test working directory path, it is always an absolute path.
     */
    protected String getTestCaseDir() {
        return testCaseDir;
    }

    /**
     * Return the URI for a test file. The returned value is the testDir + concatenated URI.
     *
     * @return the test working directory path, it is always an absolute path and appends the relative path. The
     * reason for the manual parsing instead of an actual File.toURI is because Oozie tests use tokens ${}
     * frequently. Something like URI("c:/temp/${HOUR}").toString() will generate escaped values that will break tests
     */
    protected String getTestCaseFileUri(final String relativeUri) {
        String uri = new File(testCaseDir).toURI().toString();

        // truncates '/' if the testCaseDir was provided with a fullpath ended with separator
        if (uri.endsWith("/")) {
            uri = uri.substring(0, uri.length() - 1);
        }

        return uri + "/" + relativeUri;
    }

    /**
     * Return the Oozie configuration directory for the testcase.
     *
     * @return the Oozie configuration directory for the testcase.
     */
    protected String getTestCaseConfDir() {
        return testCaseConfDir;
    }

    public String getHadoopVersion() {
        return hadoopVersion;
    }

    /**
     *  Return the user Id use to run Oozie during the test cases.
     *
     * @return Oozie's user Id for running the test cases.
     */
    public static String getOozieUser() {
        return System.getProperty(TestConstants.TEST_OOZIE_USER_PROP, System.getProperty("user.name"));
    }

    /**
     * Return the defaul test user Id. The user belongs to the test group.
     *
     * @return the user Id.
     */
    protected static String getTestUser() {
        return System.getProperty(TestConstants.TEST_USER1_PROP, "test");
    }

    /**
     * Return an alternate test user Id that belongs
     to the test group.
     *
     * @return the user Id.
     */
    protected static String getTestUser2() {
        return System.getProperty(TestConstants.TEST_USER2_PROP, "test2");
    }

    /**
     * Return an alternate test user Id that does not belong
     * to the test group.
     *
     * @return the user Id.
     */
    protected static String getTestUser3() {
        return System.getProperty(TestConstants.TEST_USER3_PROP, "test3");
    }

    /**
     * Return the test group.
     *
     * @return the test group.
     */
    protected static String getTestGroup() {
        return System.getProperty(TestConstants.TEST_GROUP_PROP, "testg");
    }

    /**
     * Return the alternate test group.
     *
     * @return the test group.
     */
    protected static String getTestGroup2() {
        return System.getProperty(TestConstants.TEST_GROUP_PROP, "testg2");
    }

    protected void delete(final File file) throws IOException {
        testCaseDirectories.delete(file);
    }

    /**
     * Create a Test case sub directory.
     *
     * @param subDirNames a list of progressively deeper directory names
     * @return the absolute path to the created directory.
     */
    protected String createTestCaseSubDir(final String... subDirNames) {
        return testCaseDirectories.createTestCaseSubdir(testCaseDir, subDirNames);
    }
    /**
     * Set a system property for the duration of the method test case.
     * <p/>
     * After the test method ends the original value is restored.
     *
     * @param name system property name.
     * @param value value to set.
     */
    protected void setSystemProperty(final String name, final String value) {
        testSystemProperties.setSystemProperty(name, value);
    }

    /**
     * A predicate 'closure' used by {@link XTestCase#waitFor} method.
     */
    public interface Predicate {

        /**
         * Perform a predicate evaluation.
         *
         * @return the boolean result of the evaluation.
         * @throws Exception thrown if the predicate evaluation could not evaluate.
         */
        boolean evaluate() throws Exception;
    }

    /**
     * Wait for a condition, expressed via a {@link Predicate} to become true.
     *
     * @param timeout maximum time in milliseconds to wait for the predicate to become true.
     * @param predicate predicate waiting on.
     * @return the waited time.
     */
    protected long waitFor(final int timeout, final Predicate predicate) {
        ParamChecker.notNull(predicate, "predicate");
        final XLog log = new XLog(LogFactory.getLog(getClass()));
        final long started = System.currentTimeMillis();
        final long mustEnd = System.currentTimeMillis() + (long) (WAITFOR_RATIO * timeout);
        long lastEcho = 0;
        try {
            long waiting = mustEnd - System.currentTimeMillis();
            log.info("Waiting up to [{0}] msec", waiting);
            boolean eval;
            while (!(eval = predicate.evaluate()) && System.currentTimeMillis() < mustEnd) {
                if ((System.currentTimeMillis() - lastEcho) > 1000) {
                    waiting = mustEnd - System.currentTimeMillis();
                    log.info("Waiting up to [{0}] msec", waiting);
                    lastEcho = System.currentTimeMillis();
                }
                Thread.sleep(1000);
            }
            if (!eval) {
                log.info("Waiting timed out after [{0}] msec", timeout);
            }
            return System.currentTimeMillis() - started;
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Wait the specified amount of time; the timeout will be scaled by the oozie.test.waitfor.ratio property.
     *
     * @param sleepTime time in milliseconds to wait
     */
    protected void sleep(final int sleepTime) {
        waitFor(sleepTime, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return false;
            }
        });
    }

    /**
     * Return the Hadoop Job Tracker to use for testing. </p> The value is taken from the Java sytem property
     * {@link TestConstants#OOZIE_TEST_JOB_TRACKER}, if this property is not set, the assumed value is 'locahost:9001'.
     *
     * @return the job tracker URI.
     */
    protected String getResourceManagerUri() {
        return System.getProperty(TestConstants.OOZIE_TEST_JOB_TRACKER, "localhost:9001");
    }

    /**
     * Return the Hadoop Name Node to use for testing. </p> The value is taken from the Java sytem property
     * {@link TestConstants#OOZIE_TEST_NAME_NODE}, if this property is not set, the assumed value is 'locahost:9000'.
     *
     * @return the name node URI.
     */
    protected String getNameNodeUri() {
        return System.getProperty(TestConstants.OOZIE_TEST_NAME_NODE, "hdfs://localhost:9000");
    }

    /**
     * Return the second Hadoop Name Node to use for testing. </p> The value is taken from the Java sytem property
     * {@link TestConstants#OOZIE_TEST_NAME_NODE2}, if this property is not set, the assumed value is 'locahost:9100'.
     *
     * @return the second name node URI.
     */
    protected String getNameNode2Uri() {
        return System.getProperty(TestConstants.OOZIE_TEST_NAME_NODE2, "hdfs://localhost:9100");
    }

    protected String getKeytabFile() {
        final String defaultFile = new File(System.getProperty("user.home"), "oozie.keytab").getAbsolutePath();
        return System.getProperty("oozie.test.kerberos.keytab.file", defaultFile);
    }

    String getRealm() {
        return System.getProperty("oozie.test.kerberos.realm", "LOCALHOST");
    }

    protected String getOoziePrincipal() {
        return System.getProperty("oozie.test.kerberos.oozie.principal",
                getOozieUser() + "/localhost") + "@" + getRealm();
    }

    protected MiniHCatServer getHCatalogServer() {
        return hcatServer;
    }

    /**
     * Cleans up all database tables.  Tests won't typically need to call this directly because {@link #setUp()} will automatically
     * call it before each test.
     *
     * @throws Exception
     */
    protected final void cleanUpDBTables() throws Exception {
        new TestDbCleaner().cleanDbTables();
    }

    private static MiniDFSCluster dfsCluster = null;
    private static MiniDFSCluster dfsCluster2 = null;
    private static MiniMRYarnCluster yarnCluster = null;
    private static MiniHCatServer hcatServer = null;
    private static MiniHS2 hiveserver2 = null;
    private static HiveConf hs2Config = null;

    private void setUpEmbeddedHadoop(final String testCaseDir) throws Exception {
        if (dfsCluster == null && yarnCluster == null) {
            if (System.getProperty("hadoop.log.dir") == null) {
                System.setProperty("hadoop.log.dir", testCaseDir);
            }
            // Tell the ClasspathUtils that we're using a mini cluster
            ClasspathUtils.setUsingMiniYarnCluster(true);
            final int dataNodes = 2;
            final String oozieUser = getOozieUser();
            final JobConf dfsConfig = testConfigurations.createDFSConfig(getOozieUser(), getTestGroup());

            setupUgi(oozieUser);

            try {
                setupDfsCluster(dataNodes, dfsConfig);

                createHdfsPathsAndSetupPermissions();

                setupYarnCluster(dfsConfig);

                final JobConf jobConf = new JobConf(yarnCluster.getConfig());
                System.setProperty(TestConstants.OOZIE_TEST_JOB_TRACKER, jobConf.get("mapred.job.tracker"));
                final String rmAddress = jobConf.get("yarn.resourcemanager.address");
                if (rmAddress != null) {
                    System.setProperty(TestConstants.OOZIE_TEST_JOB_TRACKER, rmAddress);
                }
                System.setProperty(TestConstants.OOZIE_TEST_NAME_NODE, dfsCluster.getFileSystem().getUri().toString());
                ProxyUsers.refreshSuperUserGroupsConfiguration(dfsConfig);
            } catch (final Exception ex) {
                shutdownMiniCluster();
                throw ex;
            }
            new MiniClusterShutdownMonitor().start();
        }
    }

    private void setupDfsCluster(final int dataNodes, final JobConf dfsConfig) throws IOException {
        dfsCluster = new MiniDFSCluster.Builder(dfsConfig)
                .numDataNodes(dataNodes)
                .format(true)
                .racks(null)
                .build();
    }

    private void setupYarnCluster(final JobConf dfsConfig) {
        final Configuration yarnConfig = testConfigurations.createYarnConfig(dfsConfig);
        yarnCluster = new MiniMRYarnCluster(this.getClass().getName());
        yarnCluster.init(yarnConfig);
        yarnCluster.start();
    }

    private void setupUgi(final String oozieUser) {
        final String[] userGroups = new String[]{getTestGroup(), getTestGroup2()};

        UserGroupInformation.createUserForTesting(oozieUser, userGroups);
        UserGroupInformation.createUserForTesting(getTestUser(), userGroups);
        UserGroupInformation.createUserForTesting(getTestUser2(), userGroups);
        UserGroupInformation.createUserForTesting(getTestUser3(), new String[]{"users"});
    }

    private void createHdfsPathsAndSetupPermissions() throws IOException {
        final FileSystem fileSystem = dfsCluster.getFileSystem();
        fileSystem.mkdirs(new Path("target/test-data"));
        fileSystem.mkdirs(new Path("target/test-data" + "/minicluster/mapred"));
        fileSystem.mkdirs(new Path("/user"));
        fileSystem.mkdirs(new Path("/tmp"));
        fileSystem.mkdirs(new Path("/hadoop/mapred/system"));

        fileSystem.setPermission(new Path("target/test-data"), FsPermission.valueOf("-rwxrwxrwx"));
        fileSystem.setPermission(new Path("target/test-data" + "/minicluster"), FsPermission.valueOf("-rwxrwxrwx"));
        fileSystem.setPermission(new Path("target/test-data" + "/minicluster/mapred"), FsPermission.valueOf("-rwxrwxrwx"));
        fileSystem.setPermission(new Path("/user"), FsPermission.valueOf("-rwxrwxrwx"));
        fileSystem.setPermission(new Path("/tmp"), FsPermission.valueOf("-rwxrwxrwx"));
        fileSystem.setPermission(new Path("/hadoop/mapred/system"), FsPermission.valueOf("-rwx------"));
    }

    private void setUpEmbeddedHadoop2() throws Exception {
        if (dfsCluster != null && dfsCluster2 == null) {
            // Trick dfs location for MiniDFSCluster since it doesn't accept location as input)
            final String testBuildDataSaved = System.getProperty("test.build.data", "build/test/data");
            try {
                System.setProperty("test.build.data", FilenameUtils.concat(testBuildDataSaved, "2"));
                // Only DFS cluster is created based upon current need
                dfsCluster2 = new MiniDFSCluster(testConfigurations.createDFSConfig(getOozieUser(), getTestGroup()), 2, true, null);
                final FileSystem fileSystem = dfsCluster2.getFileSystem();
                fileSystem.mkdirs(new Path("target/test-data"));
                fileSystem.mkdirs(new Path("/user"));
                fileSystem.mkdirs(new Path("/tmp"));
                fileSystem.setPermission(new Path("target/test-data"), FsPermission.valueOf("-rwxrwxrwx"));
                fileSystem.setPermission(new Path("/user"), FsPermission.valueOf("-rwxrwxrwx"));
                fileSystem.setPermission(new Path("/tmp"), FsPermission.valueOf("-rwxrwxrwx"));
                System.setProperty(TestConstants.OOZIE_TEST_NAME_NODE2, fileSystem.getConf().get("fs.default.name"));
            } catch (final Exception ex) {
                shutdownMiniCluster2();
                throw ex;
            } finally {
                // Restore previus value
                System.setProperty("test.build.data", testBuildDataSaved);
            }
        }
    }

    protected void setupHCatalogServer() throws Exception {
        if (hcatServer == null) {
            hcatServer = new MiniHCatServer(RUNMODE.SERVER, createJobConf());
            hcatServer.start();
            log.info("Metastore server started at " + hcatServer.getMetastoreURI());
        }
    }

    private void tearDownHCatalogServer() throws Exception {
        // TODO: This doesn't properly shutdown the metastore.  For now, just keep the current one running once it's been started
    }

    protected void setupHiveServer2() throws Exception {
        if (hiveserver2 == null) {
            setSystemProperty("test.tmp.dir", getTestCaseDir());
            // We cache the HS2 config because it's expensive to build
            if (hs2Config == null) {
                // Make HS2 use our Mini cluster by copying all configs to HiveConf; also had to hack MiniHS2
                hs2Config = new HiveConf();
                final Configuration jobConf = createJobConf();
                for (final Map.Entry<String, String> pair : jobConf) {
                    hs2Config.set(pair.getKey(), pair.getValue());
                }
            }
            hiveserver2 = new MiniHS2(hs2Config, dfsCluster.getFileSystem());
            hiveserver2.start(new HashMap<String, String>());
            log.info("Hive Server 2 started at " + hiveserver2.getJdbcURL());
        }
    }

    private void tearDownHiveServer2() {
        if (hiveserver2 != null && hiveserver2.isStarted()) {
            hiveserver2.stop();
            hiveserver2 = null;
            log.info("Hive Server 2 shutdown");
        }
    }

    protected String getHiveServer2JdbcURL() {
        return hiveserver2.getJdbcURL();
    }

    protected String getHiveServer2JdbcURL(final String dbName) {
        return hiveserver2.getJdbcURL(dbName);
    }

    private static void shutdownMiniCluster() {
        try {
            if (yarnCluster != null) {
                final YarnJobActions yarnJobActions =
                        new YarnJobActions.Builder(yarnCluster.getConfig(), ApplicationsRequestScope.ALL)
                                .build();
                final Set<ApplicationId> allYarnJobs = yarnJobActions.getYarnJobs();

                yarnJobActions.killSelectedYarnJobs(allYarnJobs);

                yarnCluster.stop();
            }
        } catch (final Exception ex) {
            System.out.println(ex.getMessage());
        }

        try {
            if (dfsCluster != null) {
                dfsCluster.shutdown();
            }
        } catch (final Exception ex) {
            System.out.println(ex.getMessage());
        }
        // This is tied to the MiniCluster because it inherits configs from there
        hs2Config = null;
    }

    private static void shutdownMiniCluster2() {
        try {
            if (dfsCluster2 != null) {
                dfsCluster2.shutdown();
            }
        } catch (final Exception ex) {
            System.out.println(ex);
        }
    }

    private static final AtomicLong LAST_TESTCASE_FINISHED = new AtomicLong();
    private static final AtomicInteger RUNNING_TESTCASES = new AtomicInteger();

    private static class MiniClusterShutdownMonitor extends Thread {

        MiniClusterShutdownMonitor() {
            setDaemon(true);
        }

        public void run() {
            final long shutdownWait = Long.parseLong(System.getProperty(TestConstants.TEST_MINICLUSTER_MONITOR_SHUTDOWN_WAIT, "10")) * 1000;
            LAST_TESTCASE_FINISHED.set(System.currentTimeMillis());
            while (true) {
                if (RUNNING_TESTCASES.get() == 0) {
                    if (System.currentTimeMillis() - LAST_TESTCASE_FINISHED.get() > shutdownWait) {
                        break;
                    }
                }
                try {
                    Thread.sleep(1000);
                } catch (final InterruptedException ex) {
                    break;
                }
            }
            shutdownMiniCluster();
            shutdownMiniCluster2();
        }
    }

    /**
     * Returns a jobconf preconfigured to talk with the test cluster/minicluster.
     * @return a jobconf preconfigured to talk with the test cluster/minicluster.
     */
    protected JobConf createJobConf() throws IOException {
        if (yarnCluster != null) {
            return testConfigurations.createJobConfFromYarnCluster(yarnCluster.getConfig());
        } else {
            return testConfigurations.createPristineJobConf(getResourceManagerUri(), getNameNodeUri());
        }
    }

    /**
     * A 'closure' used by {@link XTestCase#executeWhileResourceManagerIsShutdown} method.
     */
    public interface ShutdownResourceManagerExecutable {

        /**
         * Execute some code
         *
         * @throws Exception thrown if the executed code throws an exception.
         */
        void execute() throws Exception;
    }

    /**
     * Execute some code, expressed via a {@link ShutdownResourceManagerExecutable}, while the ResourceManager is shutdown. Once the code has
     * finished, the ResourceManager is restarted (even if an exception occurs).
     *
     * @param executable The ShutdownResourceManagerExecutable to execute while the ResourceManager is shutdown
     */
    protected void executeWhileResourceManagerIsShutdown(final ShutdownResourceManagerExecutable executable) {
        for (int i=0; i<yarnCluster.getNumOfResourceManager();i++){
            yarnCluster.getResourceManager(i).stop();
        }
        try {
            executable.execute();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        } finally {
            for (int i=0; i<yarnCluster.getNumOfResourceManager();i++){
                yarnCluster.getResourceManager(i).start();
            }
        }
    }

    protected Services setupServicesForHCatalog() throws ServiceException {
        final Services services = new Services();

        setConfigurationForHCatalogImpl(services);

        return services;
    }

    private void setConfigurationForHCatalogImpl(final Services services) {
        testConfigurations.setConfigurationForHCatalog(services);

        setSystemProperty("java.naming.factory.initial", "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
        setSystemProperty("java.naming.provider.url", "vm://localhost?broker.persistent=false");
    }

    protected Services setupServicesForHCatalog(final Services services) throws ServiceException {
        setConfigurationForHCatalogImpl(services);

        return services;
    }

    private YarnApplicationState waitUntilYarnAppState(final String externalId, final EnumSet<YarnApplicationState> acceptedStates)
            throws HadoopAccessorException, IOException, YarnException {
        final ApplicationId appId = ConverterUtils.toApplicationId(externalId);
        final MutableObject<YarnApplicationState> finalState = new MutableObject<YarnApplicationState>();

        final JobConf jobConf = Services.get().get(HadoopAccessorService.class).createJobConf(getResourceManagerUri());
        final YarnClient yarnClient = Services.get().get(HadoopAccessorService.class).createYarnClient(getTestUser(), jobConf);

        try {
            waitFor(60 * 1000, new Predicate() {
                @Override
                public boolean evaluate() throws Exception {
                     final YarnApplicationState state = yarnClient.getApplicationReport(appId).getYarnApplicationState();
                     finalState.setValue(state);

                     return acceptedStates.contains(state);
                }
            });
        } finally {
            if (yarnClient != null) {
                yarnClient.close();
            }
        }

        log.info("Final state is: {0}", finalState.getValue());
        return finalState.getValue();
    }

    protected void waitUntilYarnAppDoneAndAssertSuccess(final String externalId) throws HadoopAccessorException, IOException, YarnException {
        final YarnApplicationState state = waitUntilYarnAppState(externalId, YARN_TERMINAL_STATES);
        assertEquals("YARN App state", YarnApplicationState.FINISHED, state);
    }

    protected void waitUntilYarnAppKilledAndAssertSuccess(final String externalId) throws HadoopAccessorException, IOException, YarnException {
        final YarnApplicationState state = waitUntilYarnAppState(externalId, YARN_TERMINAL_STATES);
        assertEquals("YARN App state", YarnApplicationState.KILLED, state);
    }

    protected YarnApplicationState getYarnApplicationState(final String externalId) throws HadoopAccessorException, IOException, YarnException {
        final ApplicationId appId = ConverterUtils.toApplicationId(externalId);
        YarnApplicationState state = null;
        final JobConf jobConf = Services.get().get(HadoopAccessorService.class).createJobConf(getResourceManagerUri());
        // This is needed here because we need a mutable final YarnClient
        final MutableObject<YarnClient> yarnClientMO = new MutableObject<YarnClient>(null);
        try {
            yarnClientMO.setValue(Services.get().get(HadoopAccessorService.class).createYarnClient(getTestUser(), jobConf));
            state = yarnClientMO.getValue().getApplicationReport(appId).getYarnApplicationState();
        } finally {
            if (yarnClientMO.getValue() != null) {
                yarnClientMO.getValue().close();
            }
        }

        return state;
    }

    protected class TestLogAppender extends AppenderSkeleton {
        private final List<LoggingEvent> log = new ArrayList<LoggingEvent>();

        @Override
        public boolean requiresLayout() {
            return false;
        }

        @Override
        protected void append(final LoggingEvent loggingEvent) {
            log.add(loggingEvent);
        }

        @Override
        public void close() {
        }

        public List<LoggingEvent> getLog() {
            return new ArrayList<LoggingEvent>(log);
        }
    }

    protected TestLogAppender getTestLogAppender() {
        return new TestLogAppender();
    }
}