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

import org.apache.oozie.action.hadoop.security.LauncherSecurityManager;
import org.apache.oozie.service.Services;
import org.apache.hadoop.fs.FileUtil;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.db.CompositeIndex;
import org.junit.Assert;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Test OozieDBCLI for data base derby
 */
public class TestOozieDBCLI extends XTestCase {
    private static String url = "jdbc:derby:target/test-data/oozietests/org.apache.oozie.tools.TestOozieDBCLI/data.db;create=true";
    private String oozieConfig;
    private static boolean databaseCreated = false;
    private LauncherSecurityManager launcherSecurityManager;
    @Override
    protected void setUp() throws Exception {
        this.oozieConfig = System.getProperty("oozie.test.config.file");
        File oozieConfig = new File("src/test/resources/hsqldb-oozie-site.xml");
        System.setProperty("oozie.test.config.file", oozieConfig.getAbsolutePath());
        launcherSecurityManager = new LauncherSecurityManager();
        launcherSecurityManager.enable();
        if (!databaseCreated) {
            // remove an old variant
            FileUtil.fullyDelete(new File("target/test-data/oozietests/org.apache.oozie.tools.TestOozieDBCLI/data.db"));

            Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
            Connection conn = getConnection();
            conn.close();
            databaseCreated = true;
        }

        super.setUp(false);
    }

    @Override
    protected void tearDown() throws Exception {
        launcherSecurityManager.disable();
        if(oozieConfig!=null){
            System.setProperty("oozie.test.config.file", oozieConfig);
        }else{
            System.getProperties().remove("oozie.test.config.file");
        }
        super.tearDown();

    }

    private void execSQL(String sql) throws Exception {
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        Connection conn = getConnection();

        Statement st = conn.createStatement();
        st.executeUpdate(sql);

        st.close();
        conn.close();
    }

    public void testServicesDestroy() throws Exception {
        Services services = new Services();
        File runtimeDir = new File(services.getRuntimeDir()).getParentFile();
        final String systemId = services.getSystemId();

        File[] dirsBefore = runtimeDir.listFiles(new FilenameFilter() {
           public boolean accept(final File dir, final String name) {
              return name.contains(systemId) && name.endsWith(".dir");
           }
        });

        OozieDBCLI cli = new OozieDBCLI();
        cli.getJdbcConf();

        File[] dirsAfter = runtimeDir.listFiles(new FilenameFilter() {
           public boolean accept(final File dir, final String name) {
              return name.contains(systemId) && name.endsWith(".dir");
           }
        });

        assertEquals(dirsBefore.length,dirsAfter.length);
    }

    public void testOozieDBCLI() throws Exception {
        // test script for create database
        File createSql = new File(getTestCaseConfDir() + File.separator + "out.sql");
        String[] argsCreate = { "create", "-sqlfile", createSql.getAbsolutePath(), "-run" };
        int result = execOozieDBCLICommands(argsCreate);
        assertEquals(0, result);
        assertTrue(createSql.exists());
        verifyIndexesCreated();

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

        verifyUpgradeScriptContainsIndexStatements(upgrade);
        assertTrue(upgrade.exists());

        File postUpgrade = new File(getTestCaseConfDir() + File.separator + "postUpdate.sql");
        String[] argsPostUpgrade = { "postupgrade", "-sqlfile", postUpgrade.getAbsolutePath(), "-run" };
        assertEquals(0, execOozieDBCLICommands(argsPostUpgrade));
    }

    private void verifyUpgradeScriptContainsIndexStatements(final File upgrade) throws IOException {
        final Charset charset = Charset.defaultCharset();
        final List<String> stringList = Files.readAllLines(upgrade.toPath(), charset);
        final List<String> actualIndexStatements = Arrays.asList(stringList.toArray(new String[]{}));
        final List<String> expectedIndexStatements = CompositeIndex.getIndexStatements();

        for (final String indexStmt : expectedIndexStatements) {
            Assert.assertTrue(actualIndexStatements.contains(indexStmt + ";"));
        }
    }

    private void verifyIndexesCreated() throws SQLException {
        final List<String> indexes = getIndexes();
        for (final String indexStmt : CompositeIndex.getIndexStatements()) {
            final  String index = indexStmt.split(" ")[2];
            Assert.assertTrue(indexes.contains(index));
        }
    }

    private List<String> getIndexes() throws SQLException {
        final List<String> indexes = new ArrayList<>();
        try (final Connection connection = getConnection();
            final ResultSet rs = connection.getMetaData().getTables(null, "SA", "%", null)) {
            while (rs.next()) {
                final String tableName = rs.getString(3);
                indexes.addAll(getIndexesForTable(connection, tableName));
            }
        }
        return indexes;
    }

    private List<String> getIndexesForTable(final Connection connection, final String tableName) throws SQLException {
        final List<String> indexes = new ArrayList<>();
        final DatabaseMetaData metaData = connection.getMetaData();
        try (final ResultSet rs = metaData.getIndexInfo(null, "SA", tableName, false, true)) {
            while (rs.next()) {
                final String indexName = rs.getString(6);
                indexes.add(indexName);
            }
        }
        return indexes;
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, "sa", "");
    }

    private int execOozieDBCLICommands(String[] args) {
        try {
            OozieDBCLI.main(args);

        }
        catch (SecurityException ex) {
            if (launcherSecurityManager.getExitInvoked()) {
                System.out.println("Intercepting System.exit(" + launcherSecurityManager.getExitCode() + ")");
                System.err.println("Intercepting System.exit(" + launcherSecurityManager.getExitCode() + ")");
                return launcherSecurityManager.getExitCode();

            }
            else {
                throw ex;
            }
        }
        return 1;
    }
}
