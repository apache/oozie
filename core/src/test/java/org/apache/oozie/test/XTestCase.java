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
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.persistence.EntityManager;
import javax.persistence.FlushModeType;
import javax.persistence.PersistenceException;
import javax.persistence.TypedQuery;

import junit.framework.TestCase;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.SLAEventBean;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.hadoop.LauncherMain;
import org.apache.oozie.dependency.FSURIHandler;
import org.apache.oozie.dependency.HCatURIHandler;
import org.apache.oozie.service.CallableQueueService;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.HCatAccessorService;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.JMSAccessorService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.PartitionDependencyManagerService;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.StoreService;
import org.apache.oozie.service.URIHandlerService;
import org.apache.oozie.sla.SLARegistrationBean;
import org.apache.oozie.sla.SLASummaryBean;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.test.MiniHCatServer.RUNMODE;
import org.apache.oozie.test.hive.MiniHS2;
import org.apache.oozie.util.ClasspathUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.ZKUtils;
import org.apache.openjpa.persistence.ArgumentException;
import org.apache.openjpa.persistence.RollbackException;

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
    private static EnumSet<YarnApplicationState> YARN_TERMINAL_STATES = EnumSet.of(YarnApplicationState.FAILED,
            YarnApplicationState.KILLED, YarnApplicationState.FINISHED);
    private static final int DEFAULT_YARN_TIMEOUT = 60_000;
    private Map<String, String> sysProps;
    private String testCaseDir;
    private String testCaseConfDir;
    private String hadoopVersion;
    protected XLog log = new XLog(LogFactory.getLog(getClass()));

    protected static File OOZIE_SRC_DIR = null;
    private static final String OOZIE_TEST_PROPERTIES = "oozie.test.properties";
    protected static final String SYSTEM_LINE_SEPARATOR = System.getProperty("line.separator");

    public static float WAITFOR_RATIO = Float.parseFloat(System.getProperty("oozie.test.waitfor.ratio", "1"));
    protected static final String localActiveMQBroker = "vm://localhost?broker.persistent=false&broker.useJmx=false";
    protected static final String ActiveMQConnFactory = "org.apache.activemq.jndi.ActiveMQInitialContextFactory";

    static {
        try {
            OOZIE_SRC_DIR = new File("core").getAbsoluteFile();
            if (!OOZIE_SRC_DIR.exists()) {
                OOZIE_SRC_DIR = OOZIE_SRC_DIR.getParentFile().getParentFile();
                OOZIE_SRC_DIR = new File(OOZIE_SRC_DIR, "core");
            }
            if (!OOZIE_SRC_DIR.exists()) {
                OOZIE_SRC_DIR = OOZIE_SRC_DIR.getParentFile().getParentFile();
                OOZIE_SRC_DIR = new File(OOZIE_SRC_DIR, "core");
            }
            if (!OOZIE_SRC_DIR.exists()) {
                // We're probably being run from outside of Oozie (e.g. MiniOozie), so just use a dummy location here.
                // Anything that uses this location should have a fallback anyway.
                OOZIE_SRC_DIR = new File(".");
            } else {
                OOZIE_SRC_DIR = OOZIE_SRC_DIR.getParentFile();
            }

            String testPropsFile = System.getProperty(OOZIE_TEST_PROPERTIES, "test.properties");
            File file = new File(testPropsFile).isAbsolute()
                        ? new File(testPropsFile) : new File(OOZIE_SRC_DIR, testPropsFile);
            if (file.exists()) {
                System.out.println();
                System.out.println("*********************************************************************************");
                System.out.println("Loading test system properties from: " + file.getAbsolutePath());
                System.out.println();
                Properties props = new Properties();
                props.load(new FileReader(file));
                for (Map.Entry entry : props.entrySet()) {
                    if (!System.getProperties().containsKey(entry.getKey())) {
                        System.setProperty((String) entry.getKey(), (String) entry.getValue());
                        System.out.println(entry.getKey() + " = " + entry.getValue());
                    }
                    else {
                        System.out.println(entry.getKey() + " IGNORED, using command line value = " +
                                           System.getProperty((String) entry.getKey()));
                    }
                }
                System.out.println("*********************************************************************************");
                System.out.println();
            }
            else {
                if (System.getProperty(OOZIE_TEST_PROPERTIES) != null) {
                    System.err.println();
                    System.err.println("ERROR: Specified test file does not exist: "  +
                                       System.getProperty(OOZIE_TEST_PROPERTIES));
                    System.err.println();
                    System.exit(-1);
                }
            }
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }

    }

    /**
     * System property to specify the parent directory for the 'oozietests' directory to be used as base for all test
     * working directories. </p> If this property is not set, the assumed value is '/tmp'.
     */
    public static final String OOZIE_TEST_DIR = "oozie.test.dir";

    /**
     * System property to specify the Hadoop Job Tracker to use for testing. </p> If this property is not set, the
     * assumed value is 'locahost:9001'.
     */
    public static final String OOZIE_TEST_JOB_TRACKER = "oozie.test.job.tracker";

    /**
     * System property to specify the Hadoop Name Node to use for testing. </p> If this property is not set, the assumed
     * value is 'locahost:9000'.
     */
    public static final String OOZIE_TEST_NAME_NODE = "oozie.test.name.node";

    /**
     * System property to specify the second Hadoop Name Node to use for testing. </p> If this property is not set, the assumed
     * value is 'locahost:9100'.
     */
    public static final String OOZIE_TEST_NAME_NODE2 = "oozie.test.name.node2";

    /**
     * System property to specify the Hadoop Version to use for testing. </p> If this property is not set, the assumed
     * value is "0.20.0"
     */
    public static final String HADOOP_VERSION = "hadoop.version";

    /**
     * System property that specifies the user that test oozie instance runs as.
     * The value of this property defaults to the "${user.name} system property.
     */
    public static final String TEST_OOZIE_USER_PROP = "oozie.test.user.oozie";

    /**
     * System property that specifies the default test user name used by
     * the tests. The defalt value of this property is <tt>test</tt>.
     */
    public static final String TEST_USER1_PROP = "oozie.test.user.test";

    /**
     * System property that specifies an auxilliary test user name used by the
     * tests. The default value of this property is <tt>test2</tt>.
     */
    public static final String TEST_USER2_PROP = "oozie.test.user.test2";

    /**
     * System property that specifies another auxilliary test user name used by
     * the tests. The default value of this property is <tt>test3</tt>.
     */
    public static final String TEST_USER3_PROP = "oozie.test.user.test3";

    /**
     * System property that specifies the test groiup used by the tests.
     * The default value of this property is <tt>testg</tt>.
     */
    public static final String TEST_GROUP_PROP = "oozie.test.group";

    /**
     * System property that specifies the test admin user used in the tests.
     * The default value of this property is myAdmin
     */
    public static final String TEST_ADMIN_PROP = "oozie.test.admin.user";

    /**
     * System property that specifies the test groiup used by the tests.
     * The default value of this property is <tt>testg</tt>.
     */
    public static final String TEST_GROUP_PROP2 = "oozie.test.group2";

    /**
     * System property that specifies the wait time, in seconds, between testcases before
     * triggering a shutdown. The default value is 10 sec.
     */
    public static final String TEST_MINICLUSTER_MONITOR_SHUTDOWN_WAIT = "oozie.test.minicluster.monitor.shutdown.wait";

    /**
     * Name of the shell command
     */
    protected static final String SHELL_COMMAND_NAME = (Shell.WINDOWS)? "cmd": "bash";

    /**
     * Extension for shell script files
     */
    protected static final String SHELL_COMMAND_SCRIPTFILE_EXTENSION = (Shell.WINDOWS)? "cmd": "sh";

    /**
     * Option for shell command to pass script files
     */
    protected static final String SHELL_COMMAND_SCRIPTFILE_OPTION = (Shell.WINDOWS) ? "/c" : "-c";

    /**
     * Minimal set of require Services for cleaning up the database ({@link JPAService} and {@link StoreService})
     */
    private static final String MINIMAL_SERVICES_FOR_DB_CLEANUP = JPAService.class.getName() + "," + StoreService.class.getName();

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
    protected  void setUp(boolean cleanUpDBTables) throws Exception {
        RUNNING_TESTCASES.incrementAndGet();
        super.setUp();
        // if for some reason the tearDown didn`t run, check and delete the files
        deleteCreatedFiles();
        String baseDir = System.getProperty(OOZIE_TEST_DIR, new File("target/test-data").getAbsolutePath());
        String msg = null;
        File f = new File(baseDir);
        if (!f.isAbsolute()) {
            msg = XLog.format("System property [{0}]=[{1}] must be set to an absolute path", OOZIE_TEST_DIR, baseDir);
        }
        else {
            if (baseDir.length() < 4) {
                msg = XLog.format("System property [{0}]=[{1}] path must be at least 4 chars", OOZIE_TEST_DIR, baseDir);
            }
        }
        if (msg != null) {
            System.err.println();
            System.err.println(msg);
            System.exit(-1);
        }
        f.mkdirs();
        if (!f.exists() || !f.isDirectory()) {
            System.err.println();
            System.err.println(XLog.format("Could not create test dir [{0}]",  baseDir));
            System.exit(-1);
        }
        hadoopVersion = System.getProperty(HADOOP_VERSION, "0.20.0");
        testCaseDir = createTestCaseDir(this, true);

        //setting up Oozie HOME and Oozie conf directory
        setSystemProperty(Services.OOZIE_HOME_DIR, testCaseDir);
        Services.setOozieHome();
        testCaseConfDir = createTestCaseSubDir("conf");

        // load test Oozie site
        final String oozieTestDB = System.getProperty("oozie.test.db", "hsqldb");
        final String oozieSiteFileName = oozieTestDB + "-oozie-site.xml";
        final String defaultOozieSite =
            new File(OOZIE_SRC_DIR, "core/src/test/resources/" + oozieSiteFileName).getAbsolutePath();
        final String customOozieSite = System.getProperty("oozie.test.config.file", defaultOozieSite);
        File source = new File(customOozieSite);
        if(!source.isAbsolute()) {
            source = new File(OOZIE_SRC_DIR, customOozieSite);
        }
        source = source.getAbsoluteFile();
        InputStream oozieSiteSourceStream = null;
        if (source.exists()) {
            log.info("Reading Oozie test resource from file. [source.name={0}]", source.getName());
            oozieSiteSourceStream = new FileInputStream(source);
        }
        else {
            // If we can't find it, try using the class loader (useful if we're using XTestCase from outside core)
            log.info("Oozie test resource file doesn't exist. [source.name={0}]", source.getName());
            final String testResourceName;
            if (customOozieSite.lastIndexOf(Path.SEPARATOR) > -1) {
                final String customOozieSiteFileName = customOozieSite.substring(customOozieSite.lastIndexOf(Path.SEPARATOR) + 1);
                if (customOozieSiteFileName.equals(oozieSiteFileName)) {
                    testResourceName = oozieSiteFileName;
                }
                else {
                    testResourceName = customOozieSiteFileName;
                }
            }
            else {
                testResourceName = oozieSiteFileName;
            }
            log.info("Reading Oozie test resource from classpath. [testResourceName={0};source.name={1}]",
                    testResourceName, source.getName());
            final URL sourceURL = getClass().getClassLoader().getResource(testResourceName);
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
        // Copy the specified oozie-site file from oozieSiteSourceStream to the test case dir as oozie-site.xml
        Configuration oozieSiteConf = new Configuration(false);
        oozieSiteConf.addResource(oozieSiteSourceStream);
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream(ConfigurationService.DEFAULT_CONFIG_FILE);
        XConfiguration configuration = new XConfiguration(inputStream);
        String classes = configuration.get(Services.CONF_SERVICE_CLASSES);
        // Disable sharelib service as it cannot find the sharelib jars
        // as maven has target/classes in classpath and not the jar because test phase is before package phase
        oozieSiteConf.set(Services.CONF_SERVICE_CLASSES, classes.replaceAll("org.apache.oozie.service.ShareLibService,",""));
        // Make sure to create the Oozie DB during unit tests
        oozieSiteConf.set(JPAService.CONF_CREATE_DB_SCHEMA, "true");
        // Make sure thread pools shut down in a timely manner
        oozieSiteConf.set(CallableQueueService.CONF_QUEUE_AWAIT_TERMINATION_TIMEOUT_SECONDS, "1");
        File target = new File(testCaseConfDir, "oozie-site.xml");
        oozieSiteConf.writeXml(new FileOutputStream(target));

        File hadoopConfDir = new File(testCaseConfDir, "hadoop-conf");
        hadoopConfDir.mkdir();
        File actionConfDir = new File(testCaseConfDir, "action-conf");
        actionConfDir.mkdir();
        source = new File(OOZIE_SRC_DIR, "core/src/test/resources/hadoop-config.xml");
        InputStream hadoopConfigResourceStream = null;
        if (!source.exists()) {
            // If we can't find it, try using the class loader (useful if we're using XTestCase from outside core)
            URL sourceURL = getClass().getClassLoader().getResource("hadoop-config.xml");
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
        target = new File(hadoopConfDir, "hadoop-site.xml");
        IOUtils.copyStream(hadoopConfigResourceStream, new FileOutputStream(target));

        if (System.getProperty("oozielocal.log") == null) {
            setSystemProperty("oozielocal.log", "/tmp/oozielocal.log");
        }
        if (System.getProperty("oozie.test.hadoop.security", "simple").equals("kerberos")) {
            System.setProperty("oozie.service.HadoopAccessorService.kerberos.enabled", "true");
        }
        if (System.getProperty("oozie.test.hadoop.minicluster", "true").equals("true")) {
            setUpEmbeddedHadoop(getTestCaseDir());
            // Second cluster is not necessary without the first one
            if (System.getProperty("oozie.test.hadoop.minicluster2", "false").equals("true")) {
                setUpEmbeddedHadoop2();
            }
        }

        if (System.getProperty("oozie.test.db.host") == null) {
           System.setProperty("oozie.test.db.host", "localhost");
        }
        setSystemProperty(ConfigurationService.OOZIE_DATA_DIR, testCaseDir);

        setSystemProperty(HadoopAccessorService.SUPPORTED_FILESYSTEMS,"*");

        if (System.getProperty(ZKUtils.OOZIE_INSTANCE_ID) == null) {
            System.setProperty(ZKUtils.OOZIE_INSTANCE_ID, ZKXTestCase.ZK_ID);
        }

        if (mrCluster != null) {
            OutputStream os = new FileOutputStream(new File(hadoopConfDir, "core-site.xml"));
            Configuration conf = createJobConfFromMRCluster();
            conf.writeXml(os);
            os.close();
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

    /**
     * Clean up the test case.
     */
    @Override
    protected void tearDown() throws Exception {
        tearDownHiveServer2();
        tearDownHCatalogServer();
        resetSystemProperties();
        sysProps = null;
        testCaseDir = null;
        deleteCreatedFiles();
        super.tearDown();
        RUNNING_TESTCASES.decrementAndGet();
        LAST_TESTCASE_FINISHED.set(System.currentTimeMillis());
    }

    /**
     * Delete the created files
     */
    protected void deleteCreatedFiles() {
        for (File f : getFilesToDelete()) {
            if (f.exists()){
                f.delete();
            }
        }
    }

    /**
     * Provides a list of files needed to be deleted
     * @return propagation-conf.xml, log4j.properties, .log files from the working directory
     */
    protected List<File> getFilesToDelete() {
        File root = new File(".");
        File [] filesToDelete = root.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".log") || name.endsWith("log4j.properties") || name.equals(LauncherMain.PROPAGATION_CONF_XML);
            }
        });
        return new ArrayList<>(Arrays.asList(filesToDelete));
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
    protected String getTestCaseFileUri(String relativeUri) {
        String uri = new File(testCaseDir).toURI().toString();

        // truncates '/' if the testCaseDir was provided with a fullpath ended with separator
        if (uri.endsWith("/")){
            uri = uri.substring(0, uri.length() -1);
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
        return System.getProperty(TEST_OOZIE_USER_PROP, System.getProperty("user.name"));
    }

    /**
     * Return the defaul test user Id. The user belongs to the test group.
     *
     * @return the user Id.
     */
    protected static String getTestUser() {
        return System.getProperty(TEST_USER1_PROP, "test");
    }

    /**
     * Return an alternate test user Id that belongs
       to the test group.
     *
     * @return the user Id.
     */
    protected static String getTestUser2() {
        return System.getProperty(TEST_USER2_PROP, "test2");
    }

    /**
     * Return an alternate test user Id that does not belong
     * to the test group.
     *
     * @return the user Id.
     */
    protected static String getTestUser3() {
        return System.getProperty(TEST_USER3_PROP, "test3");
    }

    /**
     * Return the test group.
     *
     * @return the test group.
     */
    protected static String getTestGroup() {
        return System.getProperty(TEST_GROUP_PROP, "testg");
    }

    /**
     * Return Admin user
     * @return the admin user
     */
    protected static String getAdminUser(){
        return System.getProperty(TEST_ADMIN_PROP,"myAdmin");
    }

    /**
     * Return the alternate test group.
     *
     * @return the test group.
     */
    protected static String getTestGroup2() {
        return System.getProperty(TEST_GROUP_PROP, "testg2");
    }

    /**
     * Return the test working directory.
     * <p/>
     * It returns <code>${oozie.test.dir}/oozietests/TESTCLASSNAME/TESTMETHODNAME/UNIQUE_ID</code>.
     *
     * @param testCase testcase instance to obtain the working directory.
     * @return the test working directory.
     */
    private String getTestCaseDirInternal(TestCase testCase) {
        ParamChecker.notNull(testCase, "testCase");
        File dir = new File(System.getProperty(OOZIE_TEST_DIR, "target/test-data"));
        dir = new File(dir, "oozietests").getAbsoluteFile();
        dir = new File(dir, testCase.getClass().getName());
        dir = new File(dir, testCase.getName());
        dir = new File(dir, UUID.randomUUID().toString());
        return dir.getAbsolutePath();
    }

    protected void delete(File file) throws IOException {
        ParamChecker.notNull(file, "file");
        if (file.getAbsolutePath().length() < 5) {
            throw new RuntimeException(XLog.format("path [{0}] is too short, not deleting", file.getAbsolutePath()));
        }
        if (file.exists()) {
            if (file.isDirectory()) {
                File[] children = file.listFiles();
                if (children != null) {
                    for (File child : children) {
                        delete(child);
                    }
                }
            }
            if (!file.delete()) {
                throw new RuntimeException(XLog.format("could not delete path [{0}]", file.getAbsolutePath()));
            }
        }
        else {
            // With a dangling symlink, exists() doesn't return true so try to delete it anyway; we fail silently in case the file
            // truely doesn't exist
            file.delete();
        }
    }

    /**
     * Create the test working directory.
     *
     * @param testCase testcase instance to obtain the working directory.
     * @param cleanup indicates if the directory should be cleaned up if it exists.
     * @return return the path of the test working directory, it is always an absolute path.
     * @throws Exception if the test working directory could not be created or cleaned up.
     */
    private String createTestCaseDir(TestCase testCase, boolean cleanup) throws Exception {
        String testCaseDir = getTestCaseDirInternal(testCase);
        System.out.println();
        System.out.println(XLog.format("Setting testcase work dir[{0}]", testCaseDir));
        if (cleanup) {
            delete(new File(testCaseDir));
        }
        File dir = new File(testCaseDir);
        if (!dir.mkdirs()) {
            throw new RuntimeException(XLog.format("Could not create testcase dir[{0}]", testCaseDir));
        }
        return testCaseDir;
    }

    /**
     * Create a Test case sub directory.
     *
     * @param subDirNames a list of progressively deeper directory names
     * @return the absolute path to the created directory.
     */
    protected String createTestCaseSubDir(String... subDirNames) {
        ParamChecker.notNull(subDirNames, "subDirName");
        if (subDirNames.length == 0) {
            throw new RuntimeException(XLog.format("Could not create testcase subdir ''; it already exists"));
        }

        File dir = new File(testCaseDir);
        for (int i = 0; i < subDirNames.length; i++) {
            ParamChecker.notNull(subDirNames[i], "subDirName[" + i + "]");
            dir = new File(dir, subDirNames[i]);
        }

        if (!dir.mkdirs()) {
            throw new RuntimeException(XLog.format("Could not create testcase subdir[{0}]", dir));
        }
        return dir.getAbsolutePath();
    }

    /**
     * Set a system property for the duration of the method test case.
     * <p/>
     * After the test method ends the original value is restored.
     *
     * @param name system property name.
     * @param value value to set.
     */
    protected void setSystemProperty(String name, String value) {
        if (sysProps == null) {
            sysProps = new HashMap<String, String>();
        }
        if (!sysProps.containsKey(name)) {
            String currentValue = System.getProperty(name);
            sysProps.put(name, currentValue);
        }
        if (value != null) {
            System.setProperty(name, value);
        }
        else {
            System.getProperties().remove(name);
        }
    }

    /**
     * Reset changed system properties to their original values. <p/> Called from {@link #tearDown}.
     */
    private void resetSystemProperties() {
        if (sysProps != null) {
            for (Map.Entry<String, String> entry : sysProps.entrySet()) {
                if (entry.getValue() != null) {
                    System.setProperty(entry.getKey(), entry.getValue());
                }
                else {
                    System.getProperties().remove(entry.getKey());
                }
            }
            sysProps.clear();
        }
    }

    /**
     * A predicate 'closure' used by {@link XTestCase#waitFor} method.
     */
    public static interface Predicate {

        /**
         * Perform a predicate evaluation.
         *
         * @return the boolean result of the evaluation.
         * @throws Exception thrown if the predicate evaluation could not evaluate.
         */
        public boolean evaluate() throws Exception;
    }

    /**
     * Wait for a condition, expressed via a {@link Predicate} to become true.
     *
     * @param timeout maximum time in milliseconds to wait for the predicate to become true.
     * @param predicate predicate waiting on.
     * @return the waited time.
     */
    protected long waitFor(int timeout, Predicate predicate) {
        ParamChecker.notNull(predicate, "predicate");
        XLog log = new XLog(LogFactory.getLog(getClass()));
        long started = System.currentTimeMillis();
        long mustEnd = System.currentTimeMillis() + (long)(WAITFOR_RATIO * timeout);
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
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Wait the specified amount of time; the timeout will be scaled by the oozie.test.waitfor.ratio property.
     *
     * @param sleepTime time in milliseconds to wait
     */
    protected void sleep(int sleepTime) {
        waitFor(sleepTime, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return false;
            }
        });
    }

    /**
     * Return the Hadoop Job Tracker to use for testing. </p> The value is taken from the Java sytem property {@link
     * #OOZIE_TEST_JOB_TRACKER}, if this property is not set, the assumed value is 'locahost:9001'.
     *
     * @return the job tracker URI.
     */
    protected String getJobTrackerUri() {
        return System.getProperty(OOZIE_TEST_JOB_TRACKER, "localhost:9001");
    }

    /**
     * Return the Hadoop Name Node to use for testing. </p> The value is taken from the Java sytem property {@link
     * #OOZIE_TEST_NAME_NODE}, if this property is not set, the assumed value is 'locahost:9000'.
     *
     * @return the name node URI.
     */
    protected String getNameNodeUri() {
        return System.getProperty(OOZIE_TEST_NAME_NODE, "hdfs://localhost:9000");
    }

    /**
     * Return the second Hadoop Name Node to use for testing. </p> The value is taken from the Java sytem property {@link
     * #OOZIE_TEST_NAME_NODE2}, if this property is not set, the assumed value is 'locahost:9100'.
     *
     * @return the second name node URI.
     */
    protected String getNameNode2Uri() {
        return System.getProperty(OOZIE_TEST_NAME_NODE2, "hdfs://localhost:9100");
    }

    public String getKeytabFile() {
        String defaultFile = new File(System.getProperty("user.home"), "oozie.keytab").getAbsolutePath();
        return System.getProperty("oozie.test.kerberos.keytab.file", defaultFile);
    }

    public String getRealm() {
        return System.getProperty("oozie.test.kerberos.realm", "LOCALHOST");
    }

    public String getOoziePrincipal() {
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
        // If the Services are already loaded, then a test is likely calling this for something specific and we shouldn't mess with
        // the Services; so just cleanup the database
        if (Services.get() != null) {
            cleanUpDBTablesInternal();
        }
        else {
            // Otherwise, this is probably being called during setup() and we should just load the minimal set of required Services
            // needed to cleanup the database and shut them down when done; the test will likely start its own Services later and
            // we don't want to interfere
            try {
                Services services = new Services();
                services.getConf().set(Services.CONF_SERVICE_CLASSES, MINIMAL_SERVICES_FOR_DB_CLEANUP);
                services.init();
                cleanUpDBTablesInternal();
            }
            finally {
                if (Services.get() != null) {
                    Services.get().destroy();
                }
            }
        }
    }

    private void cleanUpDBTablesInternal() throws StoreException {
        EntityManager entityManager = Services.get().get(JPAService.class).getEntityManager();
        entityManager.setFlushMode(FlushModeType.COMMIT);
        entityManager.getTransaction().begin();

        final int wfjSize = getCountAndRemoveAll(entityManager, "GET_WORKFLOWS", WorkflowJobBean.class);
        final int wfaSize = getCountAndRemoveAll(entityManager, "GET_ACTIONS", WorkflowActionBean.class);
        final int cojSize = getCountAndRemoveAll(entityManager, "GET_COORD_JOBS", CoordinatorJobBean.class);
        final int coaSize = getCountAndRemoveAll(entityManager, "GET_COORD_ACTIONS", CoordinatorActionBean.class);
        final int bjSize = getCountAndRemoveAll(entityManager, "GET_BUNDLE_JOBS", BundleJobBean.class);
        final int baSize = getCountAndRemoveAll(entityManager, "GET_BUNDLE_ACTIONS", BundleActionBean.class);
        final int slaSize = getCountAndRemoveAll(entityManager, "GET_SLA_EVENTS", SLAEventBean.class);
        final int slaRegSize = getCountAndRemoveAll(entityManager, "GET_SLA_REGISTRATIONS", SLARegistrationBean.class);
        final int ssSize = getCountAndRemoveAll(entityManager, "GET_SLA_SUMMARY_ALL", SLASummaryBean.class);

        try {
            if (entityManager.getTransaction().isActive()) {
                entityManager.getTransaction().commit();
            }

            if (entityManager.isOpen()) {
                entityManager.close();
            }
        }
        catch (final RollbackException e) {
            log.warn("Cannot commit current transaction. [e.message={0}]", e.getMessage());
        }

        log.info(wfjSize + " entries in WF_JOBS removed from DB!");
        log.info(wfaSize + " entries in WF_ACTIONS removed from DB!");
        log.info(cojSize + " entries in COORD_JOBS removed from DB!");
        log.info(coaSize + " entries in COORD_ACTIONS removed from DB!");
        log.info(bjSize + " entries in BUNDLE_JOBS removed from DB!");
        log.info(baSize + " entries in BUNDLE_ACTIONS removed from DB!");
        log.info(slaSize + " entries in SLA_EVENTS removed from DB!");
        log.info(slaRegSize + " entries in SLA_REGISTRATION removed from DB!");
        log.info(ssSize + " entries in SLA_SUMMARY removed from DB!");
    }

    private <E> int getCountAndRemoveAll(final EntityManager entityManager,
                                                        final String queryName,
                                                        final Class<E> entityClass) {
        try {
            final TypedQuery<E> getAllQuery = entityManager.createNamedQuery(queryName, entityClass);
            final List<E> allEntities = getAllQuery.getResultList();
            final int entitiesCount = allEntities.size();

            for (final E w : allEntities) {
                entityManager.remove(w);
            }

            return entitiesCount;
        } catch (final RollbackException e) {
            log.warn("Cannot get count or remove all entities. [queryName={0};entityClass.name={1}]",
                    queryName, entityClass.getName());
            return 0;
        } catch (final PersistenceException | ArgumentException e) {
            log.warn("Cannot get count or remove all entities. [queryName={0};entityClass.name={1}]",
                    queryName, entityClass.getName());
            return 0;
        }
    }

    private static MiniDFSCluster dfsCluster = null;
    private static MiniDFSCluster dfsCluster2 = null;
    // TODO: OYA: replace with MiniYarnCluster or MiniMRYarnCluster
    private static MiniMRCluster mrCluster = null;
    private static MiniHCatServer hcatServer = null;
    private static MiniHS2 hiveserver2 = null;
    private static HiveConf hs2Config = null;

    private void setUpEmbeddedHadoop(String testCaseDir) throws Exception {
        if (dfsCluster == null && mrCluster == null) {
            if (System.getProperty("hadoop.log.dir") == null) {
                System.setProperty("hadoop.log.dir", testCaseDir);
            }
            // Tell the ClasspathUtils that we're using a mini cluster
            ClasspathUtils.setUsingMiniYarnCluster(true);
            int taskTrackers = 2;
            int dataNodes = 2;
            String oozieUser = getOozieUser();
            JobConf conf = createDFSConfig();
            String[] userGroups = new String[] { getTestGroup(), getTestGroup2() };
            UserGroupInformation.createUserForTesting(oozieUser, userGroups);
            UserGroupInformation.createUserForTesting(getTestUser(), userGroups);
            UserGroupInformation.createUserForTesting(getTestUser2(), userGroups);
            UserGroupInformation.createUserForTesting(getTestUser3(), new String[] { "users" } );

            try {
                dfsCluster = new MiniDFSCluster(conf, dataNodes, true, null);
                FileSystem fileSystem = dfsCluster.getFileSystem();
                fileSystem.mkdirs(new Path("target/test-data"));
                fileSystem.mkdirs(new Path("target/test-data"+"/minicluster/mapred"));
                fileSystem.mkdirs(new Path("/user"));
                fileSystem.mkdirs(new Path("/tmp"));
                fileSystem.mkdirs(new Path("/hadoop/mapred/system"));
                fileSystem.setPermission(new Path("target/test-data"), FsPermission.valueOf("-rwxrwxrwx"));
                fileSystem.setPermission(new Path("target/test-data"+"/minicluster"), FsPermission.valueOf("-rwxrwxrwx"));
                fileSystem.setPermission(new Path("target/test-data"+"/minicluster/mapred"), FsPermission.valueOf("-rwxrwxrwx"));
                fileSystem.setPermission(new Path("/user"), FsPermission.valueOf("-rwxrwxrwx"));
                fileSystem.setPermission(new Path("/tmp"), FsPermission.valueOf("-rwxrwxrwx"));
                fileSystem.setPermission(new Path("/hadoop/mapred/system"), FsPermission.valueOf("-rwx------"));
                String nnURI = fileSystem.getUri().toString();
                int numDirs = 1;
                String[] racks = null;
                String[] hosts = null;
                mrCluster = new MiniMRCluster(0, 0, taskTrackers, nnURI, numDirs, racks, hosts, null, conf);
                JobConf jobConf = mrCluster.createJobConf();
                System.setProperty(OOZIE_TEST_JOB_TRACKER, jobConf.get("mapred.job.tracker"));
                String rmAddress = jobConf.get("yarn.resourcemanager.address");
                if (rmAddress != null) {
                    System.setProperty(OOZIE_TEST_JOB_TRACKER, rmAddress);
                }
                System.setProperty(OOZIE_TEST_NAME_NODE, jobConf.get("fs.default.name"));
                ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
            }
            catch (Exception ex) {
                shutdownMiniCluster();
                throw ex;
            }
            new MiniClusterShutdownMonitor().start();
        }
    }

    private void setUpEmbeddedHadoop2() throws Exception {
        if (dfsCluster != null && dfsCluster2 == null) {
            // Trick dfs location for MiniDFSCluster since it doesn't accept location as input)
            String testBuildDataSaved = System.getProperty("test.build.data", "build/test/data");
            try {
                System.setProperty("test.build.data", FilenameUtils.concat(testBuildDataSaved, "2"));
                // Only DFS cluster is created based upon current need
                dfsCluster2 = new MiniDFSCluster(createDFSConfig(), 2, true, null);
                FileSystem fileSystem = dfsCluster2.getFileSystem();
                fileSystem.mkdirs(new Path("target/test-data"));
                fileSystem.mkdirs(new Path("/user"));
                fileSystem.mkdirs(new Path("/tmp"));
                fileSystem.setPermission(new Path("target/test-data"), FsPermission.valueOf("-rwxrwxrwx"));
                fileSystem.setPermission(new Path("/user"), FsPermission.valueOf("-rwxrwxrwx"));
                fileSystem.setPermission(new Path("/tmp"), FsPermission.valueOf("-rwxrwxrwx"));
                System.setProperty(OOZIE_TEST_NAME_NODE2, fileSystem.getConf().get("fs.default.name"));
            }
            catch (Exception ex) {
                shutdownMiniCluster2();
                throw ex;
            }
            finally {
                // Restore previus value
                System.setProperty("test.build.data", testBuildDataSaved);
            }
        }
    }

    private JobConf createDFSConfig() throws UnknownHostException {
      JobConf conf = new JobConf();
      conf.set("dfs.block.access.token.enable", "false");
      conf.set("dfs.permissions", "true");
      conf.set("hadoop.security.authentication", "simple");
      conf.setBoolean("dfs.namenode.acls.enabled", true);

      //Doing this because Hadoop 1.x does not support '*' if the value is '*,127.0.0.1'
      StringBuilder sb = new StringBuilder();
      sb.append("127.0.0.1,localhost");
      for (InetAddress i : InetAddress.getAllByName(InetAddress.getLocalHost().getHostName())) {
          sb.append(",").append(i.getCanonicalHostName());
      }
      conf.set("hadoop.proxyuser." + getOozieUser() + ".hosts", sb.toString());
      conf.set("hadoop.proxyuser." + getOozieUser() + ".groups", getTestGroup());
      conf.set("mapred.tasktracker.map.tasks.maximum", "4");
      conf.set("mapred.tasktracker.reduce.tasks.maximum", "4");
      conf.set("hadoop.tmp.dir", "target/test-data"+"/minicluster");

      // Default value is 90 - if you have low disk space, tests will fail.
      conf.set("yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage", "99");
      configureYarnACL(conf);

      return conf;
    }

    /*
     * Sets up YARN ACL - necessary for testing application ACLs
     *
     * If we don't configure queue ACLs, then it's always possible for any
     * user to kill a running application. This is not desired, therefore we
     * explicitly define what users have the permission to kill applications
     * submitted to a given queue.
     */
    private void configureYarnACL(JobConf conf) {
        conf.set("yarn.acl.enable", "true");
        conf.set("yarn.admin.acl", getOozieUser());

        String schedClass = conf.get("yarn.resourcemanager.scheduler.class");

        if (schedClass.contains(FairScheduler.class.getName())) {
            conf.set("yarn.scheduler.fair.allocation.file", "fair-scheduler-alloc.xml");
        }
        else {
            conf.set("yarn.scheduler.capacity.root.acl_administer_queue", getOozieUser());
            conf.set("yarn.scheduler.capacity.root.default.acl_administer_queue", getOozieUser());

            // Scheduler properties required for YARN CapacityScheduler to work
            conf.set("yarn.scheduler.capacity.root.queues", "default,default1");
            conf.set("yarn.scheduler.capacity.root.default.capacity", "50");
            conf.set("yarn.scheduler.capacity.root.default1.capacity", "50");
            // Required to prevent deadlocks with YARN CapacityScheduler
            conf.set("yarn.scheduler.capacity.maximum-am-resource-percent", "0.5");
        }
    }

    protected void setupHCatalogServer() throws Exception {
        if (hcatServer == null) {
            hcatServer = new MiniHCatServer(RUNMODE.SERVER, createJobConf());
            hcatServer.start();
            log.info("Metastore server started at " + hcatServer.getMetastoreURI());
        }
    }

    protected void tearDownHCatalogServer() throws Exception {
        // TODO: This doesn't properly shutdown the metastore.  For now, just keep the current one running once it's been started
//        if (hcatServer != null) {
//            hcatServer.shutdown();
//            hcatServer = null;
//            log.info("Metastore server shutdown");
//        }
    }

    protected void setupHiveServer2() throws Exception {
        if (hiveserver2 == null) {
            setSystemProperty("test.tmp.dir", getTestCaseDir());
            // We cache the HS2 config because it's expensive to build
            if (hs2Config == null) {
                // Make HS2 use our Mini cluster by copying all configs to HiveConf; also had to hack MiniHS2
                hs2Config = new HiveConf();
                Configuration jobConf = createJobConf();
                for (Map.Entry<String, String> pair : jobConf) {
                    hs2Config.set(pair.getKey(), pair.getValue());
                }
            }
            hiveserver2 = new MiniHS2(hs2Config, dfsCluster.getFileSystem());
            hiveserver2.start(new HashMap<String, String>());
            log.info("Hive Server 2 started at " + hiveserver2.getJdbcURL());
        }
    }

    protected void tearDownHiveServer2() {
        if (hiveserver2 != null && hiveserver2.isStarted()) {
            hiveserver2.stop();
            hiveserver2 = null;
            log.info("Hive Server 2 shutdown");
        }
    }

    protected String getHiveServer2JdbcURL() {
        return hiveserver2.getJdbcURL();
    }

    protected String getHiveServer2JdbcURL(String dbName) {
        return hiveserver2.getJdbcURL(dbName);
    }

    private static void shutdownMiniCluster() {
        try {
            if (mrCluster != null) {
                mrCluster.shutdown();
            }
        }
        catch (Exception ex) {
            System.out.println(ex);
        }
        try {
            if (dfsCluster != null) {
                dfsCluster.shutdown();
            }
        }
        catch (Exception ex) {
            System.out.println(ex);
        }
        // This is tied to the MiniCluster because it inherits configs from there
        hs2Config = null;
    }

    private static void shutdownMiniCluster2() {
        try {
            if (dfsCluster2 != null) {
                dfsCluster2.shutdown();
            }
        }
        catch (Exception ex) {
            System.out.println(ex);
        }
    }
    private static final AtomicLong LAST_TESTCASE_FINISHED = new AtomicLong();
    private static final AtomicInteger RUNNING_TESTCASES = new AtomicInteger();

    private static class MiniClusterShutdownMonitor extends Thread {

        public MiniClusterShutdownMonitor() {
            setDaemon(true);
        }

        public void run() {
            long shutdownWait = Long.parseLong(System.getProperty(TEST_MINICLUSTER_MONITOR_SHUTDOWN_WAIT, "10")) * 1000;
            LAST_TESTCASE_FINISHED.set(System.currentTimeMillis());
            while (true) {
                if (RUNNING_TESTCASES.get() == 0) {
                    if (System.currentTimeMillis() - LAST_TESTCASE_FINISHED.get() > shutdownWait) {
                        break;
                    }
                }
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException ex) {
                    break;
                }
            }
            shutdownMiniCluster();
            shutdownMiniCluster2();
        }
    }

    @SuppressWarnings("deprecation")
    private JobConf createJobConfFromMRCluster() {
        JobConf jobConf = new JobConf();
        JobConf jobConfMR = mrCluster.createJobConf();
        for ( Entry<String, String> entry : jobConfMR) {
            // MiniMRClientClusterFactory sets the job jar in Hadoop 2.0 causing tests to fail
            // TODO call conf.unset after moving completely to Hadoop 2.x
            if (!(entry.getKey().equals("mapreduce.job.jar") || entry.getKey().equals("mapred.jar"))) {
                jobConf.set(entry.getKey(), entry.getValue());
            }
        }
        return jobConf;
    }

    /**
     * Returns a jobconf preconfigured to talk with the test cluster/minicluster.
     * @return a jobconf preconfigured to talk with the test cluster/minicluster.
     */
    protected JobConf createJobConf() throws IOException {
        JobConf jobConf;
        if (mrCluster != null) {
            jobConf = createJobConfFromMRCluster();
        }
        else {
            jobConf = new JobConf();
            jobConf.set("mapred.job.tracker", getJobTrackerUri());
            jobConf.set("fs.default.name", getNameNodeUri());
        }
        return jobConf;
    }

    /**
     * A 'closure' used by {@link XTestCase#executeWhileJobTrackerIsShutdown} method.
     */
    public static interface ShutdownJobTrackerExecutable {

        /**
         * Execute some code
         *
         * @throws Exception thrown if the executed code throws an exception.
         */
        public void execute() throws Exception;
    }

    /**
     * Execute some code, expressed via a {@link ShutdownJobTrackerExecutable}, while the JobTracker is shutdown. Once the code has
     * finished, the JobTracker is restarted (even if an exception occurs).
     *
     * @param executable The ShutdownJobTrackerExecutable to execute while the JobTracker is shutdown
     */
    protected void executeWhileJobTrackerIsShutdown(ShutdownJobTrackerExecutable executable) {
        mrCluster.stopJobTracker();
        Exception ex = null;
        try {
            executable.execute();
        } catch (Exception e) {
            ex = e;
        } finally {
            mrCluster.startJobTracker();
        }
        if (ex != null) {
            throw new RuntimeException(ex);
        }
    }

    protected Services setupServicesForHCatalog() throws ServiceException {
        Services services = new Services();
        setupServicesForHCataLogImpl(services);
        return services;
    }

    private void setupServicesForHCataLogImpl(Services services) {
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_EXT_CLASSES,
                JMSAccessorService.class.getName() + "," +
                PartitionDependencyManagerService.class.getName() + "," +
                HCatAccessorService.class.getName());
        conf.set(HCatAccessorService.JMS_CONNECTIONS_PROPERTIES,
                "default=java.naming.factory.initial#" + ActiveMQConnFactory + ";" +
                "java.naming.provider.url#" + localActiveMQBroker +
                "connectionFactoryNames#"+ "ConnectionFactory");
        conf.set(URIHandlerService.URI_HANDLERS,
                FSURIHandler.class.getName() + "," + HCatURIHandler.class.getName());
        setSystemProperty("java.naming.factory.initial", "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
        setSystemProperty("java.naming.provider.url", "vm://localhost?broker.persistent=false");
    }

    protected Services setupServicesForHCatalog(Services services) throws ServiceException {
        setupServicesForHCataLogImpl(services);
        return services;
    }

    protected YarnApplicationState waitUntilYarnAppState(String externalId, final EnumSet<YarnApplicationState> acceptedStates,
            int timeoutMs) throws HadoopAccessorException, IOException, YarnException {
        final ApplicationId appId = ConverterUtils.toApplicationId(externalId);
        final MutableObject<YarnApplicationState> finalState = new MutableObject<YarnApplicationState>();

        Configuration conf = Services.get().get(HadoopAccessorService.class).createConfiguration(getJobTrackerUri());
        final YarnClient yarnClient = Services.get().get(HadoopAccessorService.class).createYarnClient(getTestUser(), conf);

        try {
            waitFor(timeoutMs, new Predicate() {
                @Override
                public boolean evaluate() throws Exception {
                     YarnApplicationState state = yarnClient.getApplicationReport(appId).getYarnApplicationState();
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

    protected YarnApplicationState waitUntilYarnAppState(String externalId, final EnumSet<YarnApplicationState> acceptedStates)
            throws HadoopAccessorException, IOException, YarnException {
        return waitUntilYarnAppState(externalId, acceptedStates, DEFAULT_YARN_TIMEOUT);
    }

    protected void waitUntilYarnAppDoneAndAssertSuccess(String externalId)
            throws HadoopAccessorException, IOException, YarnException {
        YarnApplicationState state = waitUntilYarnAppState(externalId, YARN_TERMINAL_STATES);
        assertEquals("YARN App state for app " + externalId, YarnApplicationState.FINISHED, state);
    }

    protected void waitUntilYarnAppDoneAndAssertSuccess(String externalId, int timeout)
            throws HadoopAccessorException, IOException, YarnException {
        YarnApplicationState state = waitUntilYarnAppState(externalId, YARN_TERMINAL_STATES, timeout);
        assertEquals("YARN App state for app " + externalId, YarnApplicationState.FINISHED, state);
    }

    protected void waitUntilYarnAppKilledAndAssertSuccess(String externalId)
            throws HadoopAccessorException, IOException, YarnException {
        YarnApplicationState state = waitUntilYarnAppState(externalId, YARN_TERMINAL_STATES);
        assertEquals("YARN App state for app " + externalId, YarnApplicationState.KILLED, state);
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

    public TestLogAppender getTestLogAppender() {
        return new TestLogAppender();
    }

}
