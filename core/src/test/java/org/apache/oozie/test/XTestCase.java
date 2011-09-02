/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
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
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.StoreService;
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
        String baseDir = System.getProperty(OOZIE_TEST_DIR, "/tmp");
        hadoopVersion = System.getProperty(HADOOP_VERSION, "0.20.0");
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
            throw new Error(msg);
        }
        sysProps = new HashMap<String, String>();
        testCaseDir = createTestCaseDir(this, true);

        //setting up Oozie HOME and Oozie conf directory
        setSystemProperty(Services.OOZIE_HOME_ENV, testCaseDir);
        testCaseConfDir = createTestCaseSubDir("conf");

        //setting up custom Oozie site for testing if avail
        String customOozieSite = System.getProperty("oozie.test.config.file", "");
        if (!customOozieSite.equals("")) {
            if (!customOozieSite.startsWith("/")) {
                System.err.println();
                System.err.println(XLog.format(
                        "Custom configuration file must be an absolute path [{0}]", customOozieSite));
                System.err.println();
                System.exit(-1);
            }
            File source = new File(customOozieSite);
            if (!source.exists()) {
                System.err.println();
                System.err.println(XLog.format(
                        "Custom configuration file for testing does no exist [{0}]", customOozieSite));
                System.err.println();
                System.exit(-1);
            }
            File target = new File(testCaseConfDir, "oozie-site.xml");
            IOUtils.copyStream(new FileInputStream(source), new FileOutputStream(target));
        }

        if (System.getProperty("oozielocal.log") == null) {
            setSystemProperty("oozielocal.log", "/tmp/oozielocal.log");
        }
        if (System.getProperty("oozie.test.hadoop.security", "simple").equals("kerberos")) {
            System.setProperty("oozie.service.HadoopAccessorService.kerberos.enabled", "true");
        }
        if (System.getProperty("oozie.test.hadoop.minicluster", "true").equals("true")) {
            setUpEmbeddedHadoop();
        }
        if (System.getProperty("hadoop20", "false").equals("true")) {
            System.setProperty("oozie.services.ext", "org.apache.oozie.service.HadoopAccessorService");
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
        File dir = new File(System.getProperty(OOZIE_TEST_DIR, "/tmp"));
        dir = new File(dir, "oozietests");
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
        long mustEnd = System.currentTimeMillis() + timeout;
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

    public String getJobTrackerPrincipal() {
        return System.getProperty("oozie.test.kerberos.jobtracker.principal", "mapred/_HOST") + "@" + getRealm();
    }

    public String getNamenodePrincipal() {
        return System.getProperty("oozie.test.kerberos.namenode.principal", "hdfs/_HOST") + "@" + getRealm();
    }

    public <C extends Configuration> C injectKerberosInfo(C conf) {
        conf.set(WorkflowAppService.HADOOP_JT_KERBEROS_NAME, getJobTrackerPrincipal());
        conf.set(WorkflowAppService.HADOOP_NN_KERBEROS_NAME, getNamenodePrincipal());
        return conf;
    }

    public void injectKerberosInfo(Properties conf) {
        conf.setProperty(WorkflowAppService.HADOOP_JT_KERBEROS_NAME, getJobTrackerPrincipal());
        conf.setProperty(WorkflowAppService.HADOOP_NN_KERBEROS_NAME, getNamenodePrincipal());
    }

    //TODO Fix this
    /**
     * Clean up database schema
     *
     * @param conf
     * @throws Exception
     */
    protected void cleanUpDB(Configuration conf) throws Exception {
//        String dbName = conf.get(DBLiteWorkflowStoreService.CONF_SCHEMA_NAME);
//        Connection conn = getConnection(conf);
//        Statement st = conn.createStatement();
//        try {
//            st.executeUpdate("DROP SCHEMA " + dbName + " CASCADE");
//        }
//        catch (SQLException ex) {
//            log.error("Failed to drop schema:" + dbName, ex);
//            try {
//                st.executeUpdate("DROP DATABASE " + dbName);
//            }
//            catch (SQLException ex1) {
//                log.error("Failed to drop database:" + dbName, ex1);
//            }
//        }
//        st.close();
//        conn.close();
    }


    /**
     * Clean up tables - COORD_JOBS, COORD_ACTIONS
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

        store.commitTrx();
        store.closeTrx();
        log.info(wfjSize + " entries in WF_JOBS removed from DB!");
        log.info(wfaSize + " entries in WF_ACTIONS removed from DB!");
        log.info(cojSize + " entries in COORD_JOBS removed from DB!");
        log.info(coaSize + " entries in COORD_ACTIONS removed from DB!");
    }

    private static MiniDFSCluster dfsCluster = null;
    private static MiniMRCluster mrCluster = null;

    private static void setUpEmbeddedHadoop() throws Exception {
        if (dfsCluster == null && mrCluster == null) {
            if (System.getProperty("hadoop.log.dir") == null) {
                System.setProperty("hadoop.log.dir", "/tmp");
            }
            int taskTrackers = 2;
            int dataNodes = 2;
            String oozieUser = getOozieUser();
            JobConf conf = new JobConf();
            conf.set("dfs.block.access.token.enable", "false");
            conf.set("dfs.permissions", "true");
            conf.set("hadoop.security.authentication", "simple");
            conf.set("hadoop.proxyuser." + oozieUser + ".hosts", "localhost");
            conf.set("hadoop.proxyuser." + oozieUser + ".groups", getTestGroup());
            conf.set("mapred.tasktracker.map.tasks.maximum", "4");
            conf.set("mapred.tasktracker.reduce.tasks.maximum", "4");

            String [] userGroups = new String[] { getTestGroup() };
            UserGroupInformation.createUserForTesting(oozieUser, userGroups);
            UserGroupInformation.createUserForTesting(getTestUser(), userGroups);
            UserGroupInformation.createUserForTesting(getTestUser2(), userGroups);
            UserGroupInformation.createUserForTesting(getTestUser3(), new String[] { "users" } );

            dfsCluster = new MiniDFSCluster(conf, dataNodes, true, null);
            FileSystem fileSystem = dfsCluster.getFileSystem();
            fileSystem.mkdirs(new Path("/tmp"));
            fileSystem.mkdirs(new Path("/user"));
            fileSystem.mkdirs(new Path("/hadoop/mapred/system"));
            fileSystem.setPermission(new Path("/tmp"), FsPermission.valueOf("-rwxrwxrwx"));
            fileSystem.setPermission(new Path("/user"), FsPermission.valueOf("-rwxrwxrwx"));
            fileSystem.setPermission(new Path("/hadoop/mapred/system"), FsPermission.valueOf("-rwx------"));
            String nnURI = fileSystem.getUri().toString();
            int numDirs = 1;
            String[] racks = null;
            String[] hosts = null;
//            UserGroupInformation ugi = null;
            mrCluster = new MiniMRCluster(0, 0, taskTrackers, nnURI, numDirs, racks, hosts, null, conf);
            JobConf jobConf = mrCluster.createJobConf();
            System.setProperty(OOZIE_TEST_JOB_TRACKER, jobConf.get("mapred.job.tracker"));
            System.setProperty(OOZIE_TEST_NAME_NODE, jobConf.get("fs.default.name"));
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

