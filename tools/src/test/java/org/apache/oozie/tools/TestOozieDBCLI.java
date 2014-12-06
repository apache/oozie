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


package org.apache.oozie.tools;

import org.apache.hadoop.fs.FileUtil;
import org.apache.oozie.test.XTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * Test OozieDBCLI for data base derby
 */
public class TestOozieDBCLI extends XTestCase {
    private SecurityManager SECURITY_MANAGER;
    private static String url = "jdbc:derby:target/test-data/oozietests/org.apache.oozie.tools.TestOozieDBCLI/data.db;create=true";
    private String oozieConfig;

    @BeforeClass
    protected void setUp() throws Exception {
        SECURITY_MANAGER = System.getSecurityManager();
        new LauncherSecurityManager();
        // remove an old variant
        FileUtil.fullyDelete(new File("target/test-data/oozietests/org.apache.oozie.tools.TestOozieDBCLI/data.db"));
        this.oozieConfig = System.getProperty("oozie.test.config.file");
        File oozieConfig = new File("src/test/resources/hsqldb-oozie-site.xml");

        System.setProperty("oozie.test.config.file", oozieConfig.getAbsolutePath());
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        Connection conn = DriverManager.getConnection(url, "sa", "");
        conn.close();

        super.setUp(false);

    }

    @AfterClass
    protected void tearDown() throws Exception {
        System.setSecurityManager(SECURITY_MANAGER);
        if(oozieConfig!=null){
            System.setProperty("oozie.test.config.file", oozieConfig);
        }else{
            System.getProperties().remove("oozie.test.config.file");
        }
        super.tearDown();

    }

    private void execSQL(String sql) throws Exception {
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        Connection conn = DriverManager.getConnection(url, "sa", "");

        Statement st = conn.createStatement();
        st.executeUpdate(sql);

        st.close();
        conn.close();
    }

    public void testOozieDBCLI() throws Exception {
        // test script for create database
        File createSql = new File(getTestCaseConfDir() + File.separator + "out.sql");
        String[] argsCreate = { "create", "-sqlfile", createSql.getAbsolutePath(), "-run" };
        int result = execOozieDBCLICommands(argsCreate);
        assertEquals(0, result);
        assertTrue(createSql.exists());

        ByteArrayOutputStream data = new ByteArrayOutputStream();
        PrintStream oldOut = System.out;
        try {
            // show versions
            System.setOut(new PrintStream(data));
            String[] argsVersion = { "version" };
            assertEquals(0, execOozieDBCLICommands(argsVersion));
            assertTrue(data.toString().contains("db.version: "+ OozieDBCLI.DB_VERSION_FOR_5_0));
            // show help information
            data.reset();
            String[] argsHelp = { "help" };
            assertEquals(0, execOozieDBCLICommands(argsHelp));
            assertTrue(data.toString().contains("ooziedb.sh create <OPTIONS> : create Oozie DB schema"));
            assertTrue(data.toString().contains("ooziedb.sh upgrade <OPTIONS> : upgrade Oozie DB"));
            assertTrue(data.toString().contains("ooziedb.sh postupgrade <OPTIONS> : post upgrade Oozie DB"));
            // try run invalid command
            data.reset();
            String[] argsInvalidCommand = { "invalidCommand" };
            assertEquals(1, execOozieDBCLICommands(argsInvalidCommand));

        }
        finally {
            System.setOut(oldOut);
        }
        // generate an upgrade script
        File upgrade = new File(getTestCaseConfDir() + File.separator + "update.sql");
        execSQL("DROP table OOZIE_SYS");
        execSQL("ALTER TABLE BUNDLE_JOBS ADD COLUMN AUTH_TOKEN CLOB");
        execSQL("ALTER TABLE COORD_JOBS ADD COLUMN AUTH_TOKEN CLOB");
        execSQL("ALTER TABLE WF_JOBS ADD COLUMN AUTH_TOKEN CLOB");

        execSQL("ALTER TABLE WF_JOBS ADD COLUMN BEAN_TYPE VARCHAR(31)");
        execSQL("ALTER TABLE WF_ACTIONS ADD COLUMN BEAN_TYPE VARCHAR(31)");
        execSQL("ALTER TABLE COORD_JOBS ADD COLUMN BEAN_TYPE VARCHAR(31)");
        execSQL("ALTER TABLE COORD_ACTIONS ADD COLUMN BEAN_TYPE VARCHAR(31)");
        execSQL("ALTER TABLE BUNDLE_JOBS ADD COLUMN BEAN_TYPE VARCHAR(31)");
        execSQL("ALTER TABLE BUNDLE_ACTIONS ADD COLUMN BEAN_TYPE VARCHAR(31)");
        String[] argsUpgrade = { "upgrade", "-sqlfile", upgrade.getAbsolutePath(), "-run" };
        assertEquals(0, execOozieDBCLICommands(argsUpgrade));

        assertTrue(upgrade.exists());
        File postUpgrade = new File(getTestCaseConfDir() + File.separator + "postUpdate.sql");
        String[] argsPostUpgrade = { "postupgrade", "-sqlfile", postUpgrade.getAbsolutePath(), "-run" };
        assertEquals(0, execOozieDBCLICommands(argsPostUpgrade));
    }

    private int execOozieDBCLICommands(String[] args) {
        try {
            OozieDBCLI.main(args);

        }
        catch (SecurityException ex) {
            if (LauncherSecurityManager.getExitInvoked()) {
                System.out.println("Intercepting System.exit(" + LauncherSecurityManager.getExitCode() + ")");
                System.err.println("Intercepting System.exit(" + LauncherSecurityManager.getExitCode() + ")");
                return LauncherSecurityManager.getExitCode();

            }
            else {
                throw ex;
            }
        }
        return 1;
    }
}
