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
import java.io.OutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import junit.framework.TestCase;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.SLAEventBean;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.ParamChecker;
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
    private Map<String, String> sysProps;
    private String testCaseDir;
    private String testCaseConfDir;
    private String hadoopVersion;
    protected XLog log = new XLog(LogFactory.getLog(getClass()));

    private static File OOZIE_SRC_DIR = null;
    private static final String OOZIE_TEST_PROPERTIES = "oozie.test.properties";

    public static float WAITFOR_RATIO = Float.parseFloat(System.getProperty("oozie.test.waitfor.ratio", "1"));

    static {
        try {
            OOZIE_SRC_DIR = new File("core").getAbsoluteFile();
            if (!OOZIE_SRC_DIR.exists()) {
                OOZIE_SRC_DIR = OOZIE_SRC_DIR.getParentFile().getParentFile();
                OOZIE_SRC_DIR = new File(OOZIE_SRC_DIR, "core");
            }
            if (!OOZIE_SRC_DIR.exists()) {
                System.err.println();
                System.err.println("Could not determine project root directory");
                System.err.println();
                System.exit(-1);
            }
            OOZIE_SRC_DIR = OOZIE_SRC_DIR.getParentFile();

            String testPropsFile = System.getProperty(OOZIE_TEST_PROPERTIES, "test.properties");
           File file = (testPropsFile.startsWith("/"))
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
     * Initialize the test working directory. <p/> If it does not exist it creates it, if it already exists it deletes
     * all its contents. <p/> The test working directory it is not deleted after the test runs. <p/>
     *
     * @throws Exception if the test workflow working directory could not be created.
     */
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        String baseDir = System.getProperty(OOZIE_TEST_DIR, new File("target/test-data").getAbsolutePath());
        String msg = null;
        if (!baseDir.startsWith("/")) {
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
        File f = new File(baseDir);
        f.mkdirs();
        if (!f.exists() || !f.isDirectory()) {
            System.err.println();
            System.err.println(XLog.format("Could not create test dir [{0}]",  baseDir));
            System.exit(-1);
        }
        hadoopVersion = System.getProperty(HADOOP_VERSION, "0.20.0");
        sysProps = new HashMap<String, String>();
        testCaseDir = createTestCaseDir(this, true);

        //setting up Oozie HOME and Oozie conf directory
        setSystemProperty(Services.OOZIE_HOME_DIR, testCaseDir);
        Services.setOozieHome();
        testCaseConfDir = createTestCaseSubDir("conf");

        // load test Oozie site
        String oozieTestDB = System.getProperty("oozie.test.db", "hsqldb");
        String defaultOozieSize =
            new File(OOZIE_SRC_DIR, "core/src/test/resources/" + oozieTestDB + "-oozie-site.xml").getAbsolutePath();
        String customOozieSite = System.getProperty("oozie.test.config.file", defaultOozieSize);
        File source = (customOozieSite.startsWith("/"))
                      ? new File(customOozieSite) : new File(OOZIE_SRC_DIR, customOozieSite);
        source = source.getAbsoluteFile();
        if (!source.exists()) {
            System.err.println();
            System.err.println(XLog.format("Custom configuration file for testing does no exist [{0}]",
                                           source.getAbsolutePath()));
            System.err.println();
            System.exit(-1);
        }
        File target = new File(testCaseConfDir, "oozie-site.xml");
        IOUtils.copyStream(new FileInputStream(source), new FileOutputStream(target));

        File hadoopConfDir = new File(testCaseConfDir, "hadoop-conf");
        hadoopConfDir.mkdir();
        File actionConfDir = new File(testCaseConfDir, "action-conf");
        actionConfDir.mkdir();
        source = new File(OOZIE_SRC_DIR, "core/src/test/resources/hadoop-config.xml");
        target = new File(hadoopConfDir, "hadoop-site.xml");
        IOUtils.copyStream(new FileInputStream(source), new FileOutputStream(target));

        if (System.getProperty("oozielocal.log") == null) {
            setSystemProperty("oozielocal.log", "/tmp/oozielocal.log");
        }
        if (System.getProperty("oozie.test.hadoop.security", "simple").equals("kerberos")) {
            System.setProperty("oozie.service.HadoopAccessorService.kerberos.enabled", "true");
        }
        if (System.getProperty("oozie.test.hadoop.minicluster", "true").equals("true")) {
            setUpEmbeddedHadoop(getTestCaseDir());
        }

        if (System.getProperty("oozie.test.db.host") == null) {
           System.setProperty("oozie.test.db.host", "localhost");
        }
        setSystemProperty(ConfigurationService.OOZIE_DATA_DIR, testCaseDir);

        if (mrCluster != null) {
            OutputStream os = new FileOutputStream(new File(hadoopConfDir, "core-site.xml"));
            Configuration conf = mrCluster.createJobConf();
            conf.writeXml(os);
            os.close();
        }
    }

    /**
     * Clean up the test case.
     */
    @Override
    protected void tearDown() throws Exception {
        resetSystemProperties();
        sysProps = null;
        testCaseDir = null;
        super.tearDown();
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
     * Return the test working directory.
     * <p/>
     * It returns <code>${oozie.test.dir}/oozietests/TESTCLASSNAME/TESTMETHODNAME</code>.
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
     * @param subDirName sub directory name.
     * @return the absolute path to the created directory.
     */
    protected String createTestCaseSubDir(String subDirName) {
        ParamChecker.notNull(subDirName, "subDirName");
        File dir = new File(testCaseDir, subDirName);
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

    //TODO Fix this
    /**
     * Clean up database schema
     *
     * @param conf
     * @throws Exception
     */
    protected void cleanUpDB(Configuration conf) throws Exception {
    }


    /**
     * Clean up tables
     *
     * @throws StoreException
     */
    protected void cleanUpDBTables() throws StoreException {
        CoordinatorStore store = new CoordinatorStore(false);
        EntityManager entityManager = store.getEntityManager();
        store.beginTrx();

        Query q = entityManager.createNamedQuery("GET_WORKFLOWS");
        List<WorkflowJobBean> wfjBeans = q.getResultList();
        int wfjSize = wfjBeans.size();
        for (WorkflowJobBean w : wfjBeans) {
            entityManager.remove(w);
        }

        q = entityManager.createNamedQuery("GET_ACTIONS");
        List<WorkflowActionBean> wfaBeans = q.getResultList();
        int wfaSize = wfaBeans.size();
        for (WorkflowActionBean w : wfaBeans) {
            entityManager.remove(w);
        }

        q = entityManager.createNamedQuery("GET_COORD_JOBS");
        List<CoordinatorJobBean> cojBeans = q.getResultList();
        int cojSize = cojBeans.size();
        for (CoordinatorJobBean w : cojBeans) {
            entityManager.remove(w);
        }

        q = entityManager.createNamedQuery("GET_COORD_ACTIONS");
        List<CoordinatorActionBean> coaBeans = q.getResultList();
        int coaSize = coaBeans.size();
        for (CoordinatorActionBean w : coaBeans) {
            entityManager.remove(w);
        }

        q = entityManager.createNamedQuery("GET_BUNDLE_JOBS");
        List<BundleJobBean> bjBeans = q.getResultList();
        int bjSize = bjBeans.size();
        for (BundleJobBean w : bjBeans) {
            entityManager.remove(w);
        }

        q = entityManager.createNamedQuery("GET_BUNDLE_ACTIONS");
        List<BundleActionBean> baBeans = q.getResultList();
        int baSize = baBeans.size();
        for (BundleActionBean w : baBeans) {
            entityManager.remove(w);
        }

        q = entityManager.createNamedQuery("GET_SLA_EVENTS");
        List<SLAEventBean> slaBeans = q.getResultList();
        int slaSize = slaBeans.size();
        for (SLAEventBean w : slaBeans) {
            entityManager.remove(w);
        }

        store.commitTrx();
        store.closeTrx();
        log.info(wfjSize + " entries in WF_JOBS removed from DB!");
        log.info(wfaSize + " entries in WF_ACTIONS removed from DB!");
        log.info(cojSize + " entries in COORD_JOBS removed from DB!");
        log.info(coaSize + " entries in COORD_ACTIONS removed from DB!");
        log.info(bjSize + " entries in BUNDLE_JOBS removed from DB!");
        log.info(baSize + " entries in BUNDLE_ACTIONS removed from DB!");
        log.info(slaSize + " entries in SLA_EVENTS removed from DB!");
    }

    private static MiniDFSCluster dfsCluster = null;
    private static MiniMRCluster mrCluster = null;

    private void setUpEmbeddedHadoop(String testCaseDir) throws Exception {
        if (dfsCluster == null && mrCluster == null) {
			if (System.getProperty("hadoop.log.dir") == null) {
				System.setProperty("hadoop.log.dir", testCaseDir);
			}
            int taskTrackers = 2;
            int dataNodes = 2;
            String oozieUser = getOozieUser();
            JobConf conf = new JobConf();
            conf.set("dfs.block.access.token.enable", "false");
            conf.set("dfs.permissions", "true");
            conf.set("hadoop.security.authentication", "simple");

            //Doing this because Hadoop 1.x does not support '*' and
            //Hadoop 0.23.x does not process wildcard if the value is
            // '*,127.0.0.1'
            StringBuilder sb = new StringBuilder();
            sb.append("127.0.0.1,localhost");
            for (InetAddress i : InetAddress.getAllByName(InetAddress.getLocalHost().getHostName())) {
                sb.append(",").append(i.getCanonicalHostName());
            }
            conf.set("hadoop.proxyuser." + oozieUser + ".hosts", sb.toString());

            conf.set("hadoop.proxyuser." + oozieUser + ".groups", getTestGroup());
            conf.set("mapred.tasktracker.map.tasks.maximum", "4");
            conf.set("mapred.tasktracker.reduce.tasks.maximum", "4");

            String [] userGroups = new String[] { getTestGroup() };
            UserGroupInformation.createUserForTesting(oozieUser, userGroups);
            UserGroupInformation.createUserForTesting(getTestUser(), userGroups);
            UserGroupInformation.createUserForTesting(getTestUser2(), userGroups);
            UserGroupInformation.createUserForTesting(getTestUser3(), new String[] { "users" } );
            conf.set("hadoop.tmp.dir", "target/test-data"+"/minicluster");

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
            System.setProperty(OOZIE_TEST_NAME_NODE, jobConf.get("fs.default.name"));
            ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
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
                }
            });
        }
    }

}

