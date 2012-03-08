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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.BuildInfo;
import org.apache.oozie.cli.CLIParser;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Command line tool to create/upgrade Oozie Database
 */
public class OozieDBCLI {
    public static final String HELP_CMD = "help";
    public static final String VERSION_CMD = "version";
    public static final String CREATE_CMD = "create";
    public static final String UPGRADE_CMD = "upgrade";
    public static final String POST_UPGRADE_CMD = "postupgrade";
    public static final String SQL_FILE_OPT = "sqlfile";
    public static final String RUN_OPT = "run";

    public static final String[] HELP_INFO = {
        "",
        "IMPORTANT: If using an Oracle or MySQL Database, before running this",
        "tool copy the corresponding JDBC driver to the tools libext/ directory"
    };

    private boolean used;

    public static void main(String[] args) {
        System.exit(new OozieDBCLI().run(args));
    }

    public OozieDBCLI() {
        used = false;
    }

    protected Options createUpgradeOptions() {
        Option sqlfile = new Option(SQL_FILE_OPT, true, "Generate SQL script instead creating/upgrading the DB schema");
        Option run = new Option(RUN_OPT, false, "Confirm the DB schema creation/upgrade");
        Options options = new Options();
        options.addOption(sqlfile);
        options.addOption(run);
        return options;
    }

    public synchronized int run(String[] args) {
        if (used) {
            throw new IllegalStateException("CLI instance already used");
        }
        used = true;

        CLIParser parser = new CLIParser("ooziedb.sh", HELP_INFO);
        parser.addCommand(HELP_CMD, "", "display usage", new Options(), false);
        parser.addCommand(VERSION_CMD, "", "show Oozie DB version information", new Options(), false);
        parser.addCommand(CREATE_CMD, "", "create Oozie DB schema", createUpgradeOptions(), false);
        parser.addCommand(UPGRADE_CMD, "", "upgrade Oozie DB", createUpgradeOptions(), false);
        parser.addCommand(POST_UPGRADE_CMD, "", "post upgrade Oozie DB", createUpgradeOptions(), false);

        try {
            System.out.println();
            CLIParser.Command command = parser.parse(args);
            if (command.getName().equals(HELP_CMD)) {
                parser.showHelp();
            }
            else if (command.getName().equals(VERSION_CMD)) {
                showVersion();
            }
            else {
                if (!command.getCommandLine().hasOption(SQL_FILE_OPT) &&
                    !command.getCommandLine().hasOption(RUN_OPT)) {
                    throw new Exception("'-sqlfile <FILE>' or '-run' options must be specified");
                }
                CommandLine commandLine = command.getCommandLine();
                String sqlFile = (commandLine.hasOption(SQL_FILE_OPT))
                                 ? commandLine.getOptionValue(SQL_FILE_OPT)
                                 : File.createTempFile("ooziedb-", ".sql").getAbsolutePath();
                boolean run = commandLine.hasOption(RUN_OPT);
                if (command.getName().equals(CREATE_CMD)) {
                    createDB(sqlFile, run);
                }
                if (command.getName().equals(UPGRADE_CMD)) {
                    upgradeDB(sqlFile, run);
                }
                if (command.getName().equals(POST_UPGRADE_CMD)) {
                    postUpgradeDB(sqlFile, run);
                }
                System.out.println();
                System.out.println("The SQL commands have been written to: " + sqlFile);
                if (!run) {
                    System.out.println();
                    System.out.println("WARN: The SQL commands have NOT been executed, you must use the '-run' option");
                    System.out.println();
                }
            }
            return 0;
        }
        catch (ParseException ex) {
            System.err.println("Invalid sub-command: " + ex.getMessage());
            System.err.println();
            System.err.println(parser.shortHelp());
            return 1;
        }
        catch (Exception ex) {
            System.err.println();
            System.err.println("Error: " + ex.getMessage());
            System.err.println();
            System.err.println("--------------------------------------");
            ex.printStackTrace(System.err);
            System.err.println("--------------------------------------");
            System.err.println();
            return 1;
        }
    }

    private Map<String, String> getJdbcConf() throws Exception {
        Services services = new Services();
        Configuration conf = services.getConf();
        Map<String, String> jdbcConf = new HashMap<String, String>();
        jdbcConf.put("driver", conf.get(JPAService.CONF_DRIVER));
        String url = conf.get(JPAService.CONF_URL);
        jdbcConf.put("url", url);
        jdbcConf.put("user", conf.get(JPAService.CONF_USERNAME));
        jdbcConf.put("password", conf.get(JPAService.CONF_PASSWORD));
        String dbType = url.substring("jdbc:".length());
        if (dbType.indexOf(":") <= 0) {
            throw new RuntimeException("Invalid JDBC URL, missing vendor 'jdbc:[VENDOR]:...'");
        }
        dbType = dbType.substring(0, dbType.indexOf(":"));
        jdbcConf.put("dbtype", dbType);
        return jdbcConf;
    }

    private void createDB(String sqlFile, boolean run) throws Exception {
        validateConnection();
        validateDBSchema(false);
        verifyOozieSysTable(false);
        createUpgradeDB(sqlFile, run, true);
        createOozieSysTable(sqlFile, run);
        System.out.println();
        if (run) {
            System.out.println("Oozie DB has been created for Oozie version '" +
                               BuildInfo.getBuildInfo().getProperty(BuildInfo.BUILD_VERSION) + "'");
        }
        System.out.println();
    }

    private void upgradeDB(String sqlFile, boolean run) throws Exception {
        // placeholder for later versions, to handle upgrades based on the OOZIE_SYS table.
        upgradeDBTo32(sqlFile,  run);
    }

    private void upgradeDBTo32(String sqlFile, boolean run) throws Exception {
        validateConnection();
        validateDBSchema(true);
        verifyOozieSysTable(false);
        verifyDBState();
        createUpgradeDB(sqlFile, run, false);
        createOozieSysTable(sqlFile, run);
        postUpgradeTasks(sqlFile, run, false);
        ddlTweaks(sqlFile, run);
        if (run) {
            System.out.println();
            System.out.println("Oozie DB has been upgraded to Oozie version '" +
                               BuildInfo.getBuildInfo().getProperty(BuildInfo.BUILD_VERSION) + "'");
        }
        System.out.println();
    }

    private void postUpgradeDB(String sqlFile, boolean run) throws Exception {
        postUpgradeDBTo32(sqlFile, run);
    }

    private void postUpgradeDBTo32(String sqlFile, boolean run) throws Exception {
        validateConnection();
        validateDBSchema(true);
        verifyOozieSysTable(true);
        verifyOozieDBVersion();
        verifyDBState();
        postUpgradeTasks(sqlFile, run, true);
        if (run) {
            System.out.println();
            System.out.println("Post ugprade updates have been executed");
        }
        System.out.println();
    }

    private static final String COORD_JOBS_THROTTLING_DEFAULT =
        "update COORD_JOBS set mat_throttling = 12";

    private static final String COORD_JOBS_ADD_APP_NAMESPACE =
        "update COORD_JOBS set app_namespace = 'uri:oozie:coordinator:0.1'";

    private static final String COORD_JOBS_STATUS_1 =
        "update COORD_JOBS set status = 'RUNNING', PENDING = 1 " +
        "where id in ( " +
        "select job_id from COORD_ACTIONS where job_id in ( " +
        "select id from COORD_JOBS where status = 'SUCCEEDED') and(status != 'FAILED' and " +
        "status != 'SUCCEEDED' and status != 'KILLED' and status != 'TIMEDOUT') )";

    private static final String COORD_JOBS_STATUS_2 =
        "update COORD_JOBS set status = 'RUNNING' where status = 'PREMATER'";

    private static final String COORD_ACTIONS_STATUS =
        "update COORD_ACTIONS set status = 'SUSPENDED' " +
        "where id in( " +
        "select A.id from COORD_ACTIONS A, WF_JOBS B where A.external_id = B.id " +
        "and B.status = 'SUSPENDED' and A.status = 'RUNNING' )";

    private String getDBVendor() throws Exception {
        String url = getJdbcConf().get("url");
        String vendor = url.substring("jdbc:".length());
        vendor = vendor.substring(0, vendor.indexOf(":"));
        return vendor;
    }

    private void postUpgradeTasks(String sqlFile, boolean run, boolean force) throws Exception {
        PrintWriter writer = new PrintWriter(new FileWriter(sqlFile, true));
        writer.println();
        boolean skipUpdates = getDBVendor().equals("mysql");
        Connection conn = (run) ? createConnection() : null;
        try {
            System.out.println("Post-upgrade COORD_JOBS new columns default values");
            writer.println(COORD_JOBS_THROTTLING_DEFAULT + ";");
            if (run) {
                conn.setAutoCommit(true);
                Statement st = conn.createStatement();
                st.executeUpdate(COORD_JOBS_THROTTLING_DEFAULT);
                st.close();
            }
            writer.println(COORD_JOBS_ADD_APP_NAMESPACE + ";");
            if (run) {
                Statement st = conn.createStatement();
                st.executeUpdate(COORD_JOBS_ADD_APP_NAMESPACE);
                st.close();
            }
            System.out.println("DONE");
            if (!skipUpdates || force) {
                System.out.println("Post-upgrade COORD_JOBS & COORD_ACTIONS status values");
                writer.println(COORD_JOBS_STATUS_1 + ";");
                writer.println(COORD_JOBS_STATUS_2 + ";");
                writer.println(COORD_ACTIONS_STATUS + ";");
                if (run) {
                    Statement st = conn.createStatement();
                    st.executeUpdate(COORD_JOBS_STATUS_1);
                    st.close();
                    st = conn.createStatement();
                    st.executeUpdate(COORD_JOBS_STATUS_2);
                    st.close();
                    st = conn.createStatement();
                    st.executeUpdate(COORD_ACTIONS_STATUS);
                    st.close();
                }
                System.out.println("DONE");
            }
            else {
                System.out.println("SKIPPING Post-upgrade of COORD_JOBS & COORD_ACTIONS status values,");
                System.out.println("         MySQL 5 does not support the update queries");
                System.out.println();
                System.out.println("         Oozie will be able to run jobs started before the upgrade,");
                System.out.println("         although those jobs may show different status names in their actions");
            }
            writer.close();
        }
        finally {
            if (run) {
                conn.close();
            }
        }
    }

    private void ddlTweaks(String sqlFile, boolean run) throws Exception {
        PrintWriter writer = new PrintWriter(new FileWriter(sqlFile, true));
        writer.println();
        String dbVendor = getDBVendor();
        String ddlQuery = null;
        if (dbVendor.equals("derby")) {
            ddlQuery = "ALTER TABLE WF_ACTIONS ALTER COLUMN execution_path SET DATA TYPE VARCHAR(1024)";
        }
        else
        if (dbVendor.equals("oracle")) {
            ddlQuery = "ALTER TABLE WF_ACTIONS MODIFY (execution_path VARCHAR2(1024))";
        }
        else
        if (dbVendor.equals("mysql")) {
            ddlQuery = "ALTER TABLE WF_ACTIONS MODIFY execution_path VARCHAR(1024)";
        }
        else
        if (dbVendor.equals("postgresql")) {
            ddlQuery = "ALTER TABLE WF_ACTIONS ALTER COLUMN execution_path TYPE VARCHAR(1024)";
        }
        Connection conn = (run) ? createConnection() : null;
        try {
            System.out.println("Table 'WF_ACTIONS' column 'execution_path', length changed to 1024");
            writer.println(ddlQuery + ";");
            if (run) {
                conn.setAutoCommit(true);
                Statement st = conn.createStatement();
                st.executeUpdate(ddlQuery);
                st.close();
            }
            System.out.println("DONE");
            writer.close();
        }
        finally {
            if (run) {
                conn.close();
            }
        }
    }

    private Connection createConnection() throws Exception {
        Map<String, String> conf = getJdbcConf();
        Class.forName(conf.get("driver")).newInstance();
        return DriverManager.getConnection(conf.get("url"), conf.get("user"), conf.get("password"));
    }

    private void validateConnection() throws Exception {
        System.out.println("Validate DB Connection");
        try {
            createConnection().close();
            System.out.println("DONE");
        }
        catch (Exception ex) {
            throw new Exception("Could not connect to the database: " + ex.toString(), ex);
        }
    }

    private static final String WORKFLOW_STATUS_QUERY =
        "select count(*) from WF_JOBS where status IN ('RUNNING', 'SUSPENDED')";

    private void validateDBSchema(boolean exists) throws Exception {
        System.out.println((exists) ? "Check DB schema exists" : "Check DB schema does not exist");
        boolean schemaExists;
        Connection conn = createConnection();
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(WORKFLOW_STATUS_QUERY);
            rs.next();
            rs.close();
            st.close();
            schemaExists = true;
        }
        catch (Exception ex) {
            schemaExists = false;
        }
        finally {
            conn.close();
        }
        if (schemaExists != exists) {
            throw new Exception("DB schema " + ((exists) ? "does not exist" : "exists"));
        }
        System.out.println("DONE");
    }

    private final static String OOZIE_SYS_EXISTS = "select count(*) from OOZIE_SYS";

    private void verifyOozieSysTable(boolean exists) throws Exception {
        System.out.println((exists) ? "Check OOZIE_SYS table exists" : "Check OOZIE_SYS table does not exist");
        boolean tableExists;
        Connection conn = createConnection();
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(OOZIE_SYS_EXISTS);
            rs.next();
            rs.close();
            st.close();
            tableExists = true;
        }
        catch (Exception ex) {
            tableExists = false;
        }
        finally {
            conn.close();
        }
        if (tableExists != exists) {
            throw new Exception("OOZIE SYS table " + ((exists) ? "does not exist" : "exists"));
        }
        System.out.println("DONE");
    }

    private final static String DB_VERSION = "1";
    
    private final static String GET_OOZIE_DB_VERSION = "select data from OOZIE_SYS where name = 'db.version'";

    private void verifyOozieDBVersion() throws Exception {
        System.out.println("Verify Oozie DB version");
        Connection conn = createConnection();
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(GET_OOZIE_DB_VERSION);
            if (rs.next()) {
                String version = rs.getString(1);
                if (!DB_VERSION.equals(version.trim())) {
                    throw new Exception("ERROR: Expected Oozie DB version '" +
                                        DB_VERSION + "', found '" + version.trim() + "'");
                }
            }
            else {
                throw new Exception("ERROR: Could not find Oozie DB 'db.version' in OOZIE_SYS table");
            }
            rs.close();
            st.close();
        }
        catch (Exception ex) {
            throw new Exception("ERROR: Could not query OOZIE_SYS table: " + ex.toString(), ex);
        }
        finally {
            conn.close();
        }
        System.out.println("DONE");
    }

    private final static String CREATE_OOZIE_SYS =
        "create table OOZIE_SYS (name varchar(100), data varchar(100))";

    private final static String SET_DB_VERSION =
        "insert into OOZIE_SYS (name, data) values ('db.version', '" + DB_VERSION + "')";

    private final static String SET_OOZIE_VERSION =
        "insert into OOZIE_SYS (name, data) values ('oozie.version', '" +
        BuildInfo.getBuildInfo().getProperty(BuildInfo.BUILD_VERSION) + "')";

    private void createOozieSysTable(String sqlFile, boolean run) throws Exception {
        PrintWriter writer = new PrintWriter(new FileWriter(sqlFile, true));
        writer.println();
        writer.println(CREATE_OOZIE_SYS);
        writer.println(SET_DB_VERSION);
        writer.println(SET_OOZIE_VERSION);
        writer.close();
        System.out.println("Create OOZIE_SYS table");
        if (run) {
            Connection conn = createConnection();
            try {
                conn.setAutoCommit(true);
                Statement st = conn.createStatement();
                st.executeUpdate(CREATE_OOZIE_SYS);
                st.executeUpdate(SET_DB_VERSION);
                st.executeUpdate(SET_OOZIE_VERSION);
                st.close();
            }
            catch (Exception ex) {
                throw new Exception("Could not create OOZIE_SYS table: " + ex.toString(), ex);
            }
            finally {
                conn.close();
            }
        }
        System.out.println("DONE");
    }

    private final static String GET_OOZIE_SYS_INFO = "select name, data from OOZIE_SYS order by name";

    private void showOozieSysInfo() throws Exception {
        Connection conn = createConnection();
        try {
            System.out.println();
            System.out.println("Oozie DB Version Information");
            System.out.println("--------------------------------------");
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(GET_OOZIE_SYS_INFO);
            while (rs.next()) {
                System.out.println(rs.getString(1) + ": " + rs.getString(2));
            }
            System.out.println("--------------------------------------");
            System.out.println();
            rs.close();
            st.close();
        }
        catch (Exception ex) {
            throw new Exception("ERROR querying OOZIE_SYS table: " + ex.toString(), ex);
        }
        finally {
            conn.close();
        }
    }

    private void verifyDBState() throws Exception {
        System.out.println("Verify there are not active Workflow Jobs");
        Connection conn = createConnection();
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(WORKFLOW_STATUS_QUERY);
            rs.next();
            long activeWorkflows = rs.getLong(1);
            rs.close();
            st.close();
            if (activeWorkflows > 0) {
                throw new Exception("There are [" + activeWorkflows +
                                    "] workflows in RUNNING/SUSPENDED state, they must complete or be killed");
            }
            System.out.println("DONE");
        }
        finally {
            conn.close();
        }
    }

    private String[] createMappingToolArguments(String sqlFile) throws Exception {
        Map<String, String> conf = getJdbcConf();
        List<String> args = new ArrayList<String>();
        args.add("-schemaAction");
        args.add("add");
        args.add("-p");
        args.add("persistence.xml#oozie-" + conf.get("dbtype"));
        args.add("-connectionDriverName");
        args.add(conf.get("driver"));
        args.add("-connectionURL");
        args.add(conf.get("url"));
        args.add("-connectionUserName");
        args.add(conf.get("user"));
        args.add("-connectionPassword");
        args.add(conf.get("password"));
        if (sqlFile != null) {
            args.add("-sqlFile");
            args.add(sqlFile);
        }
        args.add("org.apache.oozie.client.rest.JsonWorkflowJob");
        args.add("org.apache.oozie.WorkflowJobBean");
        args.add("org.apache.oozie.client.rest.JsonWorkflowAction");
        args.add("org.apache.oozie.WorkflowActionBean");
        args.add("org.apache.oozie.client.rest.JsonCoordinatorJob");
        args.add("org.apache.oozie.CoordinatorJobBean");
        args.add("org.apache.oozie.client.rest.JsonCoordinatorAction");
        args.add("org.apache.oozie.CoordinatorActionBean");
        args.add("org.apache.oozie.client.rest.JsonSLAEvent");
        args.add("org.apache.oozie.SLAEventBean");
        args.add("org.apache.oozie.client.rest.JsonBundleJob");
        args.add("org.apache.oozie.BundleJobBean");
        args.add("org.apache.oozie.BundleActionBean");
        return args.toArray(new String[args.size()]);
    }

    private void createUpgradeDB(String sqlFile, boolean run, boolean create) throws Exception {
        System.out.println((create) ? "Create SQL schema" : "Upgrade SQL schema");
        String[] args = createMappingToolArguments(sqlFile);
        org.apache.openjpa.jdbc.meta.MappingTool.main(args);
        if (run) {
            args = createMappingToolArguments(null);
            org.apache.openjpa.jdbc.meta.MappingTool.main(args);
        }
        System.out.println("DONE");
    }

    private void showVersion() throws Exception {
        System.out.println("Oozie DB tool version: "
                           + BuildInfo.getBuildInfo().getProperty(BuildInfo.BUILD_VERSION));
        System.out.println();
        validateConnection();
        validateDBSchema(true);
        try {
            verifyOozieSysTable(true);
        }
        catch (Exception ex) {
            throw new Exception("ERROR: It seems this Oozie DB was never upgraded with the 'ooziedb' tool");
        }
        showOozieSysInfo();
    }

}
