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
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.BuildInfo;
import org.apache.oozie.cli.CLIParser;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
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
    private final static String DB_VERSION_PRE_4_0 = "1";
    private final static String DB_VERSION_FOR_4_0 = "2";
    final static String DB_VERSION_FOR_5_0 = "3";
    private final static String DISCRIMINATOR_COLUMN = "bean_type";
    private final static String TEMP_COLUMN_PREFIX = "temp_";
    private HashMap <String, List<String>> clobColumnMap;

    public static final String[] HELP_INFO = {
        "",
        "IMPORTANT: If using an Oracle, MS SQL or MySQL Database, before running this",
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
        parser.addCommand(HELP_CMD, "", "display usage for all commands or specified command", new Options(), false);
        parser.addCommand(VERSION_CMD, "", "show Oozie DB version information", new Options(), false);
        parser.addCommand(CREATE_CMD, "", "create Oozie DB schema", createUpgradeOptions(), false);
        parser.addCommand(UPGRADE_CMD, "", "upgrade Oozie DB", createUpgradeOptions(), false);
        parser.addCommand(POST_UPGRADE_CMD, "", "post upgrade Oozie DB", createUpgradeOptions(), false);

        try {
            System.out.println();
            CLIParser.Command command = parser.parse(args);
            if (command.getName().equals(HELP_CMD)) {
                parser.showHelp(command.getCommandLine());
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
            System.err.println("Stack trace for the error was (for debug purposes):");
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
        createOozieSysTable(sqlFile, run, DB_VERSION_FOR_5_0);
        System.out.println();
        if (run) {
            System.out.println("Oozie DB has been created for Oozie version '" +
                               BuildInfo.getBuildInfo().getProperty(BuildInfo.BUILD_VERSION) + "'");
        }
        System.out.println();
    }

    private void upgradeDB(String sqlFile, boolean run) throws Exception {
        validateConnection();
        validateDBSchema(true);
        String version = BuildInfo.getBuildInfo().getProperty(BuildInfo.BUILD_VERSION);

        if (!verifyOozieSysTable(false, false)) { // If OOZIE_SYS table doesn't
                                                  // exist (pre 3.2)
            createOozieSysTable(sqlFile, run, DB_VERSION_PRE_4_0);
        }
        String ver = getOozieDBVersion().trim();
        String startingVersion = ver;
        if (ver.equals(DB_VERSION_FOR_5_0)) {
            System.out.println("Oozie DB already upgraded to Oozie version '" + version + "'");
            return;
        }
        createUpgradeDB(sqlFile, run, false);

        while (!ver.equals(DB_VERSION_FOR_5_0)) {
            if (ver.equals(DB_VERSION_PRE_4_0)) {
                System.out.println("Upgrading to db schema for Oozie 4.0");
                upgradeDBTo40(sqlFile, run);
                ver = run ? getOozieDBVersion().trim() : DB_VERSION_FOR_4_0;
            }
            else if (ver.equals(DB_VERSION_FOR_4_0)) {
                System.out.println("Upgrading to db schema for Oozie " + version);
                upgradeDBto50(sqlFile, run, startingVersion);
                ver = run ? getOozieDBVersion().trim() : DB_VERSION_FOR_5_0;
            }
        }

        if (run) {
            System.out.println();
            System.out.println("Oozie DB has been upgraded to Oozie version '" + version + "'");
        }
        System.out.println();
    }

    private void upgradeDBTo40(String sqlFile, boolean run) throws Exception {
        upgradeOozieDBVersion(sqlFile, run, DB_VERSION_FOR_4_0);
        postUpgradeTasksFor40(sqlFile, run);
        ddlTweaks(sqlFile, run);
    }

    private void upgradeDBto50(String sqlFile, boolean run, String startingVersion) throws Exception {
        upgradeOozieDBVersion(sqlFile, run, DB_VERSION_FOR_5_0);
        ddlTweaksFor50(sqlFile, run, startingVersion);
    }

    private final static String UPDATE_OOZIE_VERSION =
            "update OOZIE_SYS set data='" + BuildInfo.getBuildInfo().getProperty(BuildInfo.BUILD_VERSION)
            + "' where name='oozie.version'";

    private void upgradeOozieDBVersion(String sqlFile, boolean run, String version) throws Exception {
        String updateDBVersion = "update OOZIE_SYS set data='" + version + "' where name='db.version'";
        PrintWriter writer = new PrintWriter(new FileWriter(sqlFile, true));
        writer.println();
        writer.println(UPDATE_OOZIE_VERSION);
        writer.println(updateDBVersion);
        writer.close();
        System.out.println("Update db.version in OOZIE_SYS table to " + version);
        if (run) {
            Connection conn = createConnection();
            try {
                conn.setAutoCommit(true);
                Statement st = conn.createStatement();
                st.executeUpdate(updateDBVersion);
                st.executeUpdate(UPDATE_OOZIE_VERSION);
                st.close();
            }
            catch (Exception ex) {
                throw new Exception("Could not upgrade db.version in OOZIE_SYS table: " + ex.toString(), ex);
            }
            finally {
                conn.close();
            }
        }
        System.out.println("DONE");
    }

    private void postUpgradeDB(String sqlFile, boolean run) throws Exception {
        String version = getOozieDBVersion();
        if (getOozieDBVersion().equals(DB_VERSION_FOR_4_0)) {
            postUpgradeDBTo40(sqlFile, run);
        }
        else {
            System.out.println("No Post ugprade updates available for " + version);
        }
    }

    private void postUpgradeDBTo40(String sqlFile, boolean run) throws Exception {
        validateConnection();
        validateDBSchema(true);
        verifyOozieSysTable(true);
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
        "select id from COORD_JOBS where status = 'SUCCEEDED') and (status != 'FAILED' and " +
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

    private final static String UPDATE_DELIMITER_VER_TWO =
            "UPDATE COORD_ACTIONS SET MISSING_DEPENDENCIES = REPLACE(MISSING_DEPENDENCIES,';','!!')";

    private final static String UPDATE_DELIMITER_VER_TWO_MSSQL=
            "UPDATE COORD_ACTIONS SET MISSING_DEPENDENCIES = REPLACE(CAST(MISSING_DEPENDENCIES AS varchar(MAX)),';','!!')";

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
            if (!getDBVendor().equals("derby")) {
                String  updateMissingDependenciesQuery;
                if (getDBVendor().equals("sqlserver")){
                    updateMissingDependenciesQuery = UPDATE_DELIMITER_VER_TWO_MSSQL;
                } else {
                    updateMissingDependenciesQuery = UPDATE_DELIMITER_VER_TWO;
                }

                writer.println(updateMissingDependenciesQuery + ";");
                System.out.println("Post-upgrade MISSING_DEPENDENCIES column");
                if (run) {
                    Statement st = conn.createStatement();
                    st.executeUpdate(updateMissingDependenciesQuery);
                    st.close();
                }
            }
            else {
                System.out.println("Post-upgrade MISSING_DEPENDENCIES column in Derby");
                replaceForDerby(";", "!!");
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

    private void postUpgradeTasksFor40(String sqlFile, boolean run) throws Exception {
        PrintWriter writer = new PrintWriter(new FileWriter(sqlFile, true));
        writer.println();
        Connection conn = (run) ? createConnection() : null;
        try {
            if (!getDBVendor().equals("derby")) {
                String  updateMissingDependenciesQuery;
                if (getDBVendor().equals("sqlserver")){
                    updateMissingDependenciesQuery = UPDATE_DELIMITER_VER_TWO_MSSQL;
                } else {
                    updateMissingDependenciesQuery = UPDATE_DELIMITER_VER_TWO;
                }

                writer.println(updateMissingDependenciesQuery + ";");
                System.out.println("Post-upgrade MISSING_DEPENDENCIES column");
                if (run) {
                    Statement st = conn.createStatement();
                    st.executeUpdate(updateMissingDependenciesQuery);
                    st.close();
                }
            }
            else {
                System.out.println("Post-upgrade MISSING_DEPENDENCIES column in Derby");
                replaceForDerby(";", "!!");
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

    private static final String COORD_ACTION_ID_DEPS = "SELECT ID, MISSING_DEPENDENCIES FROM COORD_ACTIONS";

    private void replaceForDerby(String oldStr, String newStr) throws Exception {
        Connection connRead = createConnection();
        try {
            connRead.setAutoCommit(false);
            Statement st = connRead.createStatement();
            // set fetch size to limit number of rows into memory for large table
            st.setFetchSize(100);
            ResultSet rs = st.executeQuery(COORD_ACTION_ID_DEPS);
            while (rs.next()) {
                String id = rs.getString(1);
                Clob clob = rs.getClob(2);
                String clobStr = clob.getSubString(1, (int) clob.length());
                clob.setString(1, clobStr.replace(oldStr, newStr));
                PreparedStatement prepStmt = connRead
                        .prepareStatement("UPDATE COORD_ACTIONS SET MISSING_DEPENDENCIES=? WHERE ID=?");
                prepStmt.setString(1, clob.getSubString(1, (int) clob.length()));
                prepStmt.setString(2, id);
                prepStmt.execute();
                prepStmt.close();
            }
        }
        finally {
            connRead.commit();
            connRead.close();
        }
    }

    private void convertClobToBlobInOracle(Connection conn) throws Exception {
        if (conn == null) {
            return ;
        }
        System.out.println("Converting clob columns to blob for all tables");
        Statement statement = conn.createStatement();
        CallableStatement tempBlobCall = conn.prepareCall("{call dbms_lob.CREATETEMPORARY(?, TRUE)}");
        tempBlobCall.registerOutParameter(1, java.sql.Types.BLOB);
        CallableStatement dbmsLobCallStmt = conn.prepareCall("{call dbms_lob.CONVERTTOBLOB(?, ?, ?, ?, ?, 0, ?, ?)}");
        dbmsLobCallStmt.registerOutParameter(1, java.sql.Types.BLOB);
        // Lob max size
        dbmsLobCallStmt.setInt(3, Integer.MAX_VALUE);
        dbmsLobCallStmt.registerOutParameter(4, java.sql.Types.INTEGER);
        // dest_offset
        dbmsLobCallStmt.setInt(4, 1);
        // src_offset
        dbmsLobCallStmt.registerOutParameter(5, java.sql.Types.INTEGER);
        dbmsLobCallStmt.setInt(5, 1);
        // blob_csid
        dbmsLobCallStmt.registerOutParameter(6, java.sql.Types.INTEGER);
        // lang_context
        dbmsLobCallStmt.setInt(6, 0);
        // warning
        dbmsLobCallStmt.registerOutParameter(7, java.sql.Types.INTEGER);
        dbmsLobCallStmt.setInt(7, 1);
        for (Map.Entry<String, List<String>> tableClobColumnMap : getTableClobColumnMap().entrySet()) {
            String tableName = tableClobColumnMap.getKey();
            List<String> columnNames = tableClobColumnMap.getValue();
            for (String column : columnNames) {
                statement.executeUpdate(getAddColumnQuery(tableName, TEMP_COLUMN_PREFIX + column, "blob"));
            }
            ResultSet rs = statement.executeQuery(getSelectQuery(tableName, columnNames));
            while (rs.next()) {
                for (int i = 0; i < columnNames.size(); i++) {
                    Clob srcClob = rs.getClob(columnNames.get(i));
                    if (srcClob == null) {
                        continue;
                    }
                    tempBlobCall.execute();
                    Blob destLob = tempBlobCall.getBlob(1);
                    dbmsLobCallStmt.setBlob(1, destLob);
                    dbmsLobCallStmt.setClob(2, srcClob);
                    dbmsLobCallStmt.execute();
                    Blob blob = dbmsLobCallStmt.getBlob(1);
                    PreparedStatement ps = conn.prepareStatement("update " + tableName + " set " + TEMP_COLUMN_PREFIX
                            + columnNames.get(i) + "=? where id = ?");
                    ps.setBlob(1, blob);
                    ps.setString(2, rs.getString(1));
                    ps.executeUpdate();
                    ps.close();
                }
            }
            rs.close();
            for (String column : columnNames) {
                statement.executeUpdate(getDropColumnQuery(tableName, column));
                statement.executeUpdate(getRenameColumnQuery(tableName, TEMP_COLUMN_PREFIX + column, column));
            }
        }
        dbmsLobCallStmt.close();
        tempBlobCall.close();
        System.out.println("Done");
    }

    private void convertClobToBlobInMysql(String sqlFile, Connection conn) throws Exception {
        System.out.println("Converting mediumtext/text columns to mediumblob for all tables");
        PrintWriter writer = new PrintWriter(new FileWriter(sqlFile, true));
        writer.println();
        Statement statement = conn != null ? conn.createStatement() : null;
        for (Map.Entry<String, List<String>> tableClobColumnMap : getTableClobColumnMap().entrySet()) {
            String tableName = tableClobColumnMap.getKey();
            List<String> columnNames = tableClobColumnMap.getValue();
            StringBuilder modifyColumn = new StringBuilder();
            modifyColumn.append(" ALTER TABLE " + tableName);
            for (String column : columnNames) {
                modifyColumn.append(" MODIFY " + column + " mediumblob,");
            }
            modifyColumn.replace(modifyColumn.length() - 1, modifyColumn.length(), "");
            writer.println(modifyColumn.toString() + ";");
            if (statement != null) {
                statement.executeUpdate(modifyColumn.toString());
            }
        }
        writer.close();
        if (statement != null) {
            statement.close();
        }
        System.out.println("Done");
    }

    private void convertClobToBlobInPostgres(String sqlFile, Connection conn, String startingVersion) throws Exception {
        System.out.println("Converting text columns to bytea for all tables");
        PrintWriter writer = new PrintWriter(new FileWriter(sqlFile, true));
        writer.println();
        Statement statement = conn != null ? conn.createStatement() : null;
        for (Map.Entry<String, List<String>> tableClobColumnMap : getTableClobColumnMap().entrySet()) {
            String tableName = tableClobColumnMap.getKey();
            List<String> columnNames = tableClobColumnMap.getValue();
            for (String column : columnNames) {
                if (startingVersion.equals(DB_VERSION_PRE_4_0)
                        && tableName.equals("COORD_ACTIONS") && column.equals("push_missing_dependencies")) {
                    // The push_missing_depdencies column was added in DB_VERSION_FOR_4_0 as TEXT and we're going to convert it to
                    // BYTEA in DB_VERSION_FOR_5_0.  However, if Oozie 5 did the upgrade from DB_VERSION_PRE_4_0 to
                    // DB_VERSION_FOR_4_0 (and is now doing it for DB_VERSION_FOR_5_0 push_missing_depdencies will already be a
                    // BYTEA because Oozie 5 created the column instead of Oozie 4; and the update query below will fail.
                    continue;
                }
                String addQuery = getAddColumnQuery(tableName, TEMP_COLUMN_PREFIX + column, "bytea");
                writer.println(addQuery + ";");
                String updateQuery = "update " + tableName + " set " + TEMP_COLUMN_PREFIX + column + "=(decode(replace("
                        + column + ", E'\\\\', E'\\\\\\\\'), 'escape'))";
                writer.println(updateQuery + ";");
                String dropQuery = getDropColumnQuery(tableName, column);
                writer.println(dropQuery + ";");
                String renameQuery = getRenameColumnQuery(tableName, TEMP_COLUMN_PREFIX + column, column);
                writer.println(renameQuery + ";");
                if (statement != null) {
                    statement.executeUpdate(addQuery);
                    statement.executeUpdate(updateQuery);
                    statement.executeUpdate(dropQuery);
                    statement.executeUpdate(renameQuery);
                }
            }
        }
        writer.close();
        if (statement != null) {
            statement.close();
        }
        System.out.println("DONE");
    }

    private void convertClobToBlobinDerby(Connection conn) throws Exception {
        if (conn == null) {
            return;
        }
        System.out.println("Converting clob columns to blob for all tables");
        Statement statement = conn.createStatement();
        for (Map.Entry<String, List<String>> tableClobColumnMap : getTableClobColumnMap().entrySet()) {
            String tableName = tableClobColumnMap.getKey();
            List<String> columnNames = tableClobColumnMap.getValue();
            for (String column : columnNames) {
                statement.executeUpdate(getAddColumnQuery(tableName, TEMP_COLUMN_PREFIX + column, "blob"));
            }
            ResultSet rs = statement.executeQuery(getSelectQuery(tableName, columnNames));
            while (rs.next()) {
                for (int i = 0; i < columnNames.size(); i++) {
                    Clob confClob = rs.getClob(columnNames.get(i));
                    if (confClob == null) {
                        continue;
                    }
                    PreparedStatement ps = conn.prepareStatement("update " + tableName + " set " + TEMP_COLUMN_PREFIX
                            + columnNames.get(i) + "=? where id = ?");
                    byte[] data = IOUtils.toByteArray(confClob.getCharacterStream(), "UTF-8");
                    ps.setBinaryStream(1, new ByteArrayInputStream(data), data.length);
                    ps.setString(2, rs.getString(1));
                    ps.executeUpdate();
                    ps.close();
                }
            }
            rs.close();
            for (String column : columnNames) {
                statement.executeUpdate(getDropColumnQuery(tableName, column));
                statement.executeUpdate("RENAME COLUMN " + tableName + "." + TEMP_COLUMN_PREFIX + column + " TO "
                        + column);
            }
        }
        statement.close();
        System.out.println("DONE");
    }

    private String getRenameColumnQuery(String tableName, String srcColumn, String destColumn) {
        return new String("ALTER TABLE " + tableName + " RENAME column " + srcColumn + " TO " + destColumn);
    }

    private String getDropColumnQuery(String tableName, String column) {
        return new String("ALTER TABLE " + tableName + " DROP column " + column);
    }

    private String getAddColumnQuery(String tableName, String tempColumn, String type) {
        return new String("ALTER TABLE " + tableName + " ADD " + tempColumn + " " + type);
    }

    private String getSelectQuery(String tableName, List<String> columnNames) {
        StringBuilder selectQuery = new StringBuilder();
        selectQuery.append("SELECT id,");
        for (String column : columnNames) {
            selectQuery.append(column);
            selectQuery.append(",");
        }
        selectQuery.replace(selectQuery.length() - 1, selectQuery.length(), "");
        selectQuery.append(" FROM ");
        selectQuery.append(tableName);
        return selectQuery.toString();
    }

    private void ddlTweaksFor50(String sqlFile, boolean run, String startingVersion) throws Exception {
        String dbVendor = getDBVendor();
        Connection conn = (run) ? createConnection() : null;

        if (dbVendor.equals("oracle")) {
            convertClobToBlobInOracle(conn);
        }
        else if (dbVendor.equals("mysql")) {
            convertClobToBlobInMysql(sqlFile, conn);
        }
        else if (dbVendor.equals("postgresql")) {
            convertClobToBlobInPostgres(sqlFile, conn, startingVersion);
        }
        else if (dbVendor.equals("derby")) {
            convertClobToBlobinDerby(conn);
        }
        System.out.println("Dropping discriminator column");
        PrintWriter writer = new PrintWriter(new FileWriter(sqlFile, true));
        writer.println();
        ArrayList<String> ddlQueries = new ArrayList<String>();
        ddlQueries.add(getDropColumnQuery("WF_JOBS", DISCRIMINATOR_COLUMN));
        ddlQueries.add(getDropColumnQuery("WF_ACTIONS", DISCRIMINATOR_COLUMN));
        ddlQueries.add(getDropColumnQuery("COORD_JOBS", DISCRIMINATOR_COLUMN));
        ddlQueries.add(getDropColumnQuery("COORD_ACTIONS", DISCRIMINATOR_COLUMN));
        ddlQueries.add(getDropColumnQuery("BUNDLE_JOBS", DISCRIMINATOR_COLUMN));
        ddlQueries.add(getDropColumnQuery("BUNDLE_ACTIONS", DISCRIMINATOR_COLUMN));
        Statement stmt = conn != null ? conn.createStatement() : null;
        for (String query : ddlQueries) {
            writer.println(query + ";");
            if (run) {
                stmt.executeUpdate(query);
            }
        }
        System.out.println("DONE");
        writer.close();
        if (run) {
            stmt.close();
            conn.close();
        }
    }

    private Map<String, List<String>> getTableClobColumnMap() {
        if (clobColumnMap != null) {
            return clobColumnMap;
        }
        else {
            clobColumnMap = new HashMap<String, List<String>>();
            clobColumnMap.put("WF_ACTIONS",
                    new ArrayList<String>(Arrays.asList("conf", "sla_xml", "data", "stats", "external_child_ids")));
            clobColumnMap.put("WF_JOBS", new ArrayList<String>(Arrays.asList("proto_action_conf", "sla_xml", "conf")));
            clobColumnMap.put(
                    "COORD_ACTIONS",
                    new ArrayList<String>(Arrays.asList("sla_xml", "created_conf", "run_conf", "action_xml",
                            "missing_dependencies", "push_missing_dependencies")));
            clobColumnMap.put("COORD_JOBS",
                    new ArrayList<String>(Arrays.asList("conf", "job_xml", "orig_job_xml", "sla_xml")));
            clobColumnMap.put("BUNDLE_JOBS", new ArrayList<String>(Arrays.asList("conf", "job_xml", "orig_job_xml")));

        }
        return clobColumnMap;
    }


    private void ddlTweaks(String sqlFile, boolean run) throws Exception {
        PrintWriter writer = new PrintWriter(new FileWriter(sqlFile, true));
        writer.println();
        String dbVendor = getDBVendor();
        ArrayList<String> ddlQueries = new ArrayList<String>();
        if (dbVendor.equals("derby")) {
            ddlQueries.add("ALTER TABLE WF_ACTIONS ALTER COLUMN execution_path SET DATA TYPE VARCHAR(1024)");
            // change wf_action.error_message from clob to varchar(500)
            ddlQueries.add("ALTER TABLE WF_ACTIONS ADD COLUMN error_message_temp VARCHAR(500)");
            ddlQueries.add("UPDATE WF_ACTIONS SET error_message_temp = SUBSTR(error_message,1,500)");
            ddlQueries.add("ALTER TABLE WF_ACTIONS DROP COLUMN error_message");
            ddlQueries.add("RENAME COLUMN WF_ACTIONS.error_message_temp TO error_message");
            // change coord_jobs.frequency from int to varchar(255)
                // Derby doesn't support INTEGER to VARCHAR, so: INTEGER --> CHAR --> VARCHAR
                    // http://java.dzone.com/articles/derby-casting-madness-–-sequel
                // Also, max CHAR length is 254 (so can't use 255)
                // And we have to trim when casting from CHAR to VARCHAR because of the added whitespace in CHAR
            ddlQueries.add("ALTER TABLE COORD_JOBS ADD COLUMN frequency_temp_a CHAR(254)");
            ddlQueries.add("UPDATE COORD_JOBS SET frequency_temp_a=CAST(frequency AS CHAR(254))");
            ddlQueries.add("ALTER TABLE COORD_JOBS ADD COLUMN frequency_temp_b VARCHAR(255)");
            ddlQueries.add("UPDATE COORD_JOBS SET frequency_temp_b=TRIM(CAST(frequency_temp_a AS VARCHAR(255)))");
            ddlQueries.add("ALTER TABLE COORD_JOBS DROP COLUMN frequency_temp_a");
            ddlQueries.add("ALTER TABLE COORD_JOBS DROP COLUMN frequency");
            ddlQueries.add("RENAME COLUMN COORD_JOBS.frequency_temp_b TO frequency");
        }
        else
        if (dbVendor.equals("oracle")) {
            ddlQueries.add("ALTER TABLE WF_ACTIONS MODIFY (execution_path VARCHAR2(1024))");
            // change wf_action.error_message from clob to varchar2(500)
            ddlQueries.add("ALTER TABLE WF_ACTIONS ADD (error_message_temp VARCHAR2(500))");
            ddlQueries.add("UPDATE WF_ACTIONS SET error_message_temp = dbms_lob.substr(error_message,500,1)");
            ddlQueries.add("ALTER TABLE WF_ACTIONS DROP COLUMN error_message");
            ddlQueries.add("ALTER TABLE WF_ACTIONS RENAME COLUMN error_message_temp TO error_message");
            // change coord_jobs.frequency from int to varchar(255)
            ddlQueries.add("ALTER TABLE COORD_JOBS ADD (frequency_temp VARCHAR2(255))");
            ddlQueries.add("UPDATE COORD_JOBS SET frequency_temp = CAST(frequency AS VARCHAR(255))");
            ddlQueries.add("ALTER TABLE COORD_JOBS DROP COLUMN frequency");
            ddlQueries.add("ALTER TABLE COORD_JOBS RENAME COLUMN frequency_temp TO frequency");
        }
        else
        if (dbVendor.equals("mysql")) {
            ddlQueries.add("ALTER TABLE WF_ACTIONS MODIFY execution_path VARCHAR(1024)");
            ddlQueries.add("ALTER TABLE WF_ACTIONS ADD COLUMN error_message_temp VARCHAR(500)");
            ddlQueries.add("UPDATE WF_ACTIONS SET error_message_temp = SUBSTR(error_message,1,500)");
            ddlQueries.add("ALTER TABLE WF_ACTIONS DROP COLUMN error_message");
            ddlQueries.add("ALTER TABLE WF_ACTIONS CHANGE error_message_temp error_message VARCHAR(500)");
            ddlQueries.add("ALTER TABLE COORD_JOBS MODIFY frequency VARCHAR(255)");
        }
        else
        if (dbVendor.equals("postgresql")) {
            ddlQueries.add("ALTER TABLE WF_ACTIONS ALTER COLUMN execution_path TYPE VARCHAR(1024)");
            ddlQueries.add("ALTER TABLE WF_ACTIONS ADD COLUMN error_message_temp VARCHAR(500)");
            ddlQueries.add("UPDATE WF_ACTIONS SET error_message_temp = SUBSTR(error_message,1,500)");
            ddlQueries.add("ALTER TABLE WF_ACTIONS DROP COLUMN error_message");
            ddlQueries.add("ALTER TABLE WF_ACTIONS RENAME error_message_temp TO error_message");
            ddlQueries.add("ALTER TABLE COORD_JOBS ALTER COLUMN frequency TYPE VARCHAR(255)");
        }
        else
        if (dbVendor.equals("sqlserver")) {
            ddlQueries.add("ALTER TABLE WF_ACTIONS ALTER COLUMN execution_path VARCHAR(1024)");
            ddlQueries.add("ALTER TABLE WF_ACTIONS ADD error_message_temp VARCHAR(500)");
            ddlQueries.add("UPDATE WF_ACTIONS SET error_message_temp = SUBSTRING(error_message,1,500)");
            ddlQueries.add("ALTER TABLE WF_ACTIONS DROP COLUMN error_message");
            ddlQueries.add("EXEC sp_rename 'WF_ACTIONS.error_message_temp', 'error_message', 'COLUMN'");
            ddlQueries.add("ALTER TABLE COORD_JOBS ALTER COLUMN frequency VARCHAR(255)");
        }
        Connection conn = (run) ? createConnection() : null;

        try {
            System.out.println("Table 'WF_ACTIONS' column 'execution_path', length changed to 1024");
            System.out.println("Table 'WF_ACTIONS, column 'error_message', changed to varchar/varchar2");
            System.out.println("Table 'COORD_JOB' column 'frequency' changed to varchar/varchar2");
            for(String query : ddlQueries){
                writer.println(query + ";");
                if (run) {
                    conn.setAutoCommit(true);
                    Statement st = conn.createStatement();
                    st.executeUpdate(query);
                    st.close();
                }
            }
            System.out.println("DONE");

            // Drop AUTH_TOKEN from BUNDLE_JOBS, COORD_JOBS, WF_JOBS (OOIZE-1398)
            System.out.println("Post-upgrade BUNDLE_JOBS, COORD_JOBS, WF_JOBS to drop AUTH_TOKEN column");
            for (String sql : DROP_AUTH_TOKEN_QUERIES){
                writer.println(sql + ";");
                if (run) {
                    Statement st = conn.createStatement();
                    st.executeUpdate(sql);
                    st.close();
                }
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

    private final static String[] DROP_AUTH_TOKEN_QUERIES = {"ALTER TABLE BUNDLE_JOBS DROP COLUMN AUTH_TOKEN",
        "ALTER TABLE COORD_JOBS DROP COLUMN AUTH_TOKEN",
        "ALTER TABLE WF_JOBS DROP COLUMN AUTH_TOKEN"};


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

    private boolean verifyOozieSysTable(boolean exists) throws Exception {
        return verifyOozieSysTable(exists, true);
    }

    private boolean verifyOozieSysTable(boolean exists, boolean throwException) throws Exception {
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
        if (throwException && tableExists != exists) {
            throw new Exception("OOZIE SYS table " + ((exists) ? "does not exist" : "exists"));
        }
        System.out.println("DONE");
        return tableExists;
    }

    private final static String GET_OOZIE_DB_VERSION = "select data from OOZIE_SYS where name = 'db.version'";

    private String getOozieDBVersion() throws Exception {
        String version;
        System.out.println("Get Oozie DB version");
        Connection conn = createConnection();
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(GET_OOZIE_DB_VERSION);
            if (rs.next()) {
                version = rs.getString(1);
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
        return version;
    }

    private final static String CREATE_OOZIE_SYS =
        "create table OOZIE_SYS (name varchar(100), data varchar(100))";


    private final static String SET_OOZIE_VERSION =
        "insert into OOZIE_SYS (name, data) values ('oozie.version', '" +
        BuildInfo.getBuildInfo().getProperty(BuildInfo.BUILD_VERSION) + "')";

    private final static String CREATE_OOZIE_SYS_INDEX =
        "create clustered index OOZIE_SYS_PK on OOZIE_SYS (name);";

    private void createOozieSysTable(String sqlFile, boolean run, String version) throws Exception {
        String insertDbVerion = "insert into OOZIE_SYS (name, data) values ('db.version', '" + version + "')";
        // Some databases do not support tables without a clustered index
        // so we need to explicitly create a clustered index for OOZIE_SYS table
        boolean createIndex = getDBVendor().equals("sqlserver");

        PrintWriter writer = new PrintWriter(new FileWriter(sqlFile, true));
        writer.println();
        writer.println(CREATE_OOZIE_SYS);
        if (createIndex){
            writer.println(CREATE_OOZIE_SYS_INDEX);
        }
        writer.println(insertDbVerion);
        writer.println(SET_OOZIE_VERSION);
        writer.close();
        System.out.println("Create OOZIE_SYS table");
        if (run) {
            Connection conn = createConnection();
            try {
                conn.setAutoCommit(true);
                Statement st = conn.createStatement();
                st.executeUpdate(CREATE_OOZIE_SYS);
                if (createIndex){
                    st.executeUpdate(CREATE_OOZIE_SYS_INDEX);
                }
                st.executeUpdate(insertDbVerion);
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
        args.add("-indexes");
        args.add("true");
        args.add("org.apache.oozie.WorkflowJobBean");
        args.add("org.apache.oozie.WorkflowActionBean");
        args.add("org.apache.oozie.CoordinatorJobBean");
        args.add("org.apache.oozie.CoordinatorActionBean");
        args.add("org.apache.oozie.client.rest.JsonSLAEvent");
        args.add("org.apache.oozie.SLAEventBean");
        args.add("org.apache.oozie.sla.SLARegistrationBean");
        args.add("org.apache.oozie.BundleJobBean");
        args.add("org.apache.oozie.BundleActionBean");
        args.add("org.apache.oozie.sla.SLASummaryBean");
        args.add("org.apache.oozie.util.db.ValidateConnectionBean");
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
