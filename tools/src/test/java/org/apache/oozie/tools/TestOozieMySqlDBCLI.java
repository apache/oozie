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

import org.apache.oozie.test.XTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;

/**
 *  Test OozieDBCLI for mysql
 */
public class TestOozieMySqlDBCLI extends XTestCase {
    private SecurityManager SECURITY_MANAGER;
    private static String url = "jdbc:mysql:fake";
    private String oozieConfig;

    @BeforeClass
    protected void setUp() throws Exception {
        SECURITY_MANAGER = System.getSecurityManager();
        DriverManager.registerDriver(new FakeDriver());
        new LauncherSecurityManager();
        this.oozieConfig = System.getProperty("oozie.test.config.file");

      File oozieConfig = new File("src/test/resources/fake-oozie-site.xml");
        System.setProperty("oozie.test.config.file", oozieConfig.getAbsolutePath());
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        Connection conn = DriverManager.getConnection(url, "sa", "");
        conn.close();

        super.setUp(false);

    }

    @AfterClass
    protected void tearDown() throws Exception {
        System.setSecurityManager(SECURITY_MANAGER);
        DriverManager.registerDriver(new FakeDriver());
        if(oozieConfig!=null){
            System.setProperty("oozie.test.config.file", oozieConfig);
        }else{
            System.getProperties().remove("oozie.test.config.file");
        }
        super.tearDown();

    }

    /**
     * Test generate create script
     */
    public void testCreateMysql() throws Exception {
        FakeConnection.SYSTEM_TABLE = false;
        FakeConnection.CREATE = true;

        File createSql = new File(getTestCaseConfDir() + File.separator + "create.sql");
        String[] argsCreate = { "create", "-sqlfile", createSql.getAbsolutePath(), "-run" };
        int result = execOozieDBCLICommands(argsCreate);
        assertEquals(0, result);
        assertTrue(createSql.exists());

    }

  /**
   * Test create upgrade script
   */
    public void testUpdateMysql() throws Exception {
        new LauncherSecurityManager();
        FakeConnection.SYSTEM_TABLE = true;
        FakeConnection.CREATE = false;

        File upgrade = new File(getTestCaseConfDir() + File.separator + "update.sql");
        String[] argsUpgrade = { "upgrade", "-sqlfile", upgrade.getAbsolutePath()};

        assertEquals(0, execOozieDBCLICommands(argsUpgrade));
        assertTrue(upgrade.exists());

        FakeConnection.SYSTEM_TABLE = false;
        upgrade.delete();

        assertEquals(0, execOozieDBCLICommands(argsUpgrade));
        assertTrue(upgrade.exists());

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
