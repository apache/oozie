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
package org.apache.oozie.test;

import junit.framework.TestCase;
import org.apache.commons.logging.LogFactory;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;
import org.apache.oozie.service.DataSourceService;
import org.apache.oozie.service.DBLiteWorkflowStoreService;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * Base JUnit <code>TestCase</code> subclass used by all Oozie testcases.
 * <p/>
 * This class provides the following functionality:
 * <p/>
 * <ul>
 * <li>Creates a unique test working directory per test method.</li>
 * <li>Resets changed system properties to their original values after every test.</li>
 * <li>WaitFor that supports a predicate,to wait for a condition. It has timeout.</li>
 * </ul>
 * <p/>
 * The base directory for the test working directory must be specified via the system property
 * <code>oozie.test.dir</code>, there default value is '/tmp'.
 * <p/>
 * From within testcases, system properties must be changed using the {@link #setSystemProperty} method.
 */
public abstract class XTestCase extends TestCase {
    private Map<String, String> sysProps;
    private String testCaseDir;
    private String hadoopVersion;

    /**
     * System property to specify the parent directory for the 'oozietests' directory
     * to be used as base for all test working directories.
     * </p>
     * If this property is not set, the assumed value is '/tmp'.
     */
    public static final String OOZIE_TEST_DIR = "oozie.test.dir";

    /**
     * System property to specify the Hadoop Job Tracker to use for testing.
     * </p>
     * If this property is not set, the assumed value is 'locahost:9001'.
     */
    public static final String OOZIE_TEST_JOB_TRACKER = "oozie.test.job.tracker";

    /**
     * System property to specify the Hadoop Name Node to use for testing.
     * </p>
     * If this property is not set, the assumed value is 'locahost:9000'.
     */
    public static final String OOZIE_TEST_NAME_NODE = "oozie.test.name.node";

    /**
     * System property to specify the Hadoop Version to use for testing.
     * </p>
     * If this property is not set, the assumed value is "0.20.0"
     */
    public static final String HADOOP_VERSION = "hadoop.version";

    /**
     * Initialize the test working directory.
     * <p/>
     * If it does not exist it creates it, if it already exists it deletes all its contents.
     * <p/>
     * The test working directory it is not deleted after the test runs.
     * <p/>
     * @throws Exception if the test workflow working directory could not be created.
     */
    protected void setUp() throws Exception {
        super.setUp();
        String baseDir = System.getProperty(OOZIE_TEST_DIR, "/tmp");
        hadoopVersion = System.getProperty(HADOOP_VERSION, "0.20.0");
        String msg = null;
        if (!baseDir.startsWith("/")) {
            msg = XLog.format("System property [{0}]=[{1}] must be set to an absolute path", OOZIE_TEST_DIR, baseDir);
        }
        else if (baseDir.length() < 4) {
            msg = XLog.format("System property [{0}]=[{1}] path must be at least 4 chars", OOZIE_TEST_DIR, baseDir);
        }
        if (msg != null) {
            throw new Error(msg);
        }
        sysProps = new HashMap<String, String>();
        testCaseDir = createTestCaseDir(this, true);
        if (System.getProperty("oozielocal.log") == null) {
            setSystemProperty("oozielocal.log", "/tmp/oozielocal.log");
        }
    }

    /**
     * Clean up the test case.
     */
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

    public String getHadoopVersion() {
        return hadoopVersion;
    }

    /**
     * Return the test working directory.
     * <p/>
     * It returns <code>${oozie.test.dir}/oozietests/TESTCLASSNAME/TESTMETHODNAME</code>.
     * <p/>
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

    private void delete(File file) throws IOException {
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
     * @param cleanup  indicates if the directory should be cleaned up if it exists.
     * @return return the path of the test working directory, it is always an absolute path.
     * @throws Exception if the test working directory could not be created or cleaned up.
     */
    private String createTestCaseDir(TestCase testCase, boolean cleanup) throws Exception {
        String testCaseDir = getTestCaseDirInternal(testCase);
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
     * After the test method ends the orginal value is restored.
     *
     * @param name  system property name.
     * @param value value to set.
     */
    protected void setSystemProperty(String name, String value) {
        ParamChecker.notNull(value, "value");
        if (!sysProps.containsKey(name)) {
            String currentValue = System.getProperty(name);
            sysProps.put(name, currentValue);
        }
        System.setProperty(name, value);
    }

    /**
     * Reset changed system properties to their original values.
     * <p/>
     * Called from {@link #tearDown}.
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
     * Return the Hadoop Job Tracker to use for testing.
     * </p>
     * The value is taken from the Java sytem property {@link #OOZIE_TEST_JOB_TRACKER}, if this property is not set,
     * the assumed value is 'locahost:9001'.
     *
     * @return the job tracker URI.
     */
    protected String getJobTrackerUri() {
        return System.getProperty(OOZIE_TEST_JOB_TRACKER, "localhost:9001");
    }

    /**
     * Return the Hadoop Name Node to use for testing.
     * </p>
     * The value is taken from the Java sytem property {@link #OOZIE_TEST_NAME_NODE}, if this property is not set,
     * the assumed value is 'locahost:9000'.
     *
     * @return the name node URI.
     */
    protected String getNameNodeUri() {
        return System.getProperty(OOZIE_TEST_NAME_NODE, "hdfs://localhost:9000");
    }

    private Connection getConnection(Configuration conf) throws SQLException {
        String driver = conf.get(DataSourceService.CONF_DRIVER, "org.hsqldb.jdbcDriver");
        String url = conf.get(DataSourceService.CONF_URL, "jdbc:hsqldb:mem:testdb");
        String user = conf.get(DataSourceService.CONF_USERNAME, "sa");
        String password = conf.get(DataSourceService.CONF_PASSWORD, "").trim();
        try {
            Class.forName(driver);
        }
        catch (ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        }
        return DriverManager.getConnection(url, user, password);
    }

    //TODO Fix this
    protected void cleanUpDB(Configuration conf) throws Exception {
        String dbName = conf.get(DBLiteWorkflowStoreService.CONF_SCHEMA_NAME);
        Connection conn = getConnection(conf);
        Statement st = conn.createStatement();
        try {
            st.executeUpdate("DROP SCHEMA " + dbName + " CASCADE");
        }
        catch (SQLException ex) {
            try {
                st.executeUpdate("DROP DATABASE " + dbName);
            }
            catch (SQLException ex1) {
                // nop
            }
        }
        st.close();
        conn.close();
    }

}