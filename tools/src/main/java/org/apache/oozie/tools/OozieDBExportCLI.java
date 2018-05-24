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

import com.google.gson.Gson;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.oozie.cli.CLIParser;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.util.IOUtils;

import javax.persistence.EntityManager;
import javax.persistence.FlushModeType;
import javax.persistence.Query;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * This class provides the following functionality:
 * <ul>
 * <li>Exports the data from the Oozie database to a specified target zip file</li>
 * <li>This class uses the current oozie configuration in oozie-site.xml</li>
 * </ul>
 */

public class OozieDBExportCLI {

    public static final String OOZIEDB_WF_JSON = "ooziedb_wf.json";
    public static final String OOZIEDB_AC_JSON = "ooziedb_ac.json";
    public static final String OOZIEDB_CJ_JSON = "ooziedb_cj.json";
    public static final String OOZIEDB_CA_JSON = "ooziedb_ca.json";
    public static final String OOZIEDB_BNJ_JSON = "ooziedb_bnj.json";
    public static final String OOZIEDB_BNA_JSON = "ooziedb_bna.json";
    public static final String OOZIEDB_SLAREG_JSON = "ooziedb_slareg.json";
    public static final String OOZIEDB_SLASUM_JSON = "ooziedb_slasum.json";
    public static final String OOZIEDB_SYS_INFO_JSON = "ooziedb_sysinfo.json";

    private static final String GET_DB_VERSION = "select name, data from OOZIE_SYS where name = 'db.version'";
    private static final String GET_WORKFLOW_JOBS = "select OBJECT(w) from WorkflowJobBean w";
    private static final String GET_WORKFLOW_ACTIONS = "select OBJECT(a) from WorkflowActionBean a";
    private static final String GET_COORD_JOBS = "select OBJECT(w) from CoordinatorJobBean w";
    private static final String GET_COORD_ACTIONS = "select OBJECT(w) from CoordinatorActionBean w";
    private static final String GET_BUNDLE_JOBS = "select OBJECT(w) from BundleJobBean w";
    private static final String GET_BUNDLE_ACIONS = "select OBJECT(w) from BundleActionBean w";
    private static final String GET_SLA_REGISTRATIONS = "select OBJECT(w) from SLARegistrationBean w";
    private static final String GET_SLA_SUMMARYS = "select OBJECT(w) from SLASummaryBean w";

    private static final int LIMIT = 1000;
    private static final String[] HELP_INFO = {
            "",
            "OozieDBExportCLI dumps Oozie database into a zip file."
    };
    private static final String HELP_CMD = "help";
    private static final String EXPORT_CMD = "export";

    public static void main(String[] args) {

        CLIParser parser = new CLIParser("oozie-setup.sh", HELP_INFO);
        parser.addCommand(HELP_CMD, "", "display usage for all commands or specified command", new Options(), false);
        parser.addCommand(EXPORT_CMD, "",
                "exports the contents of the Oozie database to the specified file",
                new Options(), true);

        try {
            CLIParser.Command command = parser.parse(args);
            if (command.getName().equals(EXPORT_CMD)) {
                Services services = new Services();
                services.getConf().set(Services.CONF_SERVICE_CLASSES, JPAService.class.getName());
                services.getConf().set(Services.CONF_SERVICE_EXT_CLASSES, "");
                services.init();
                queryAllDBTables(command.getCommandLine().getArgs()[0]);
            } else if (command.getName().equals(HELP_CMD)) {
                parser.showHelp(command.getCommandLine());
            }
        } catch (ParseException pex) {
            System.err.println("Invalid sub-command: " + pex.getMessage());
            System.err.println();
            System.err.println(parser.shortHelp());
            System.exit(1);
        } catch (Exception e) {
            System.err.println();
            System.err.println("Error: " + e.getMessage());
            System.err.println();
            System.err.println("Stack trace for the error was (for debug purposes):");
            System.err.println("--------------------------------------");
            e.printStackTrace(System.err);
            System.err.println("--------------------------------------");
            System.err.println();
            System.exit(1);
        } finally {
            if (Services.get() != null) {
                Services.get().destroy();
            }
        }
    }

    private static void queryAllDBTables(String filename) throws StoreException, IOException {

        EntityManager manager = null;
        ZipOutputStream zos = null;
        File file = null;
        try {
            file = new File(filename);
            zos = new ZipOutputStream(new FileOutputStream(file));
            zos.setLevel(1);
            manager = Services.get().get(JPAService.class).getEntityManager();
            manager.setFlushMode(FlushModeType.COMMIT);

            int infoSize = exportTableToJSON(manager.createNativeQuery(GET_DB_VERSION), zos, OOZIEDB_SYS_INFO_JSON);
            System.out.println(infoSize + " rows exported from OOZIE_SYS");

            int wfjSize = exportTableToJSON(manager.createQuery(GET_WORKFLOW_JOBS), zos, OOZIEDB_WF_JSON);
            System.out.println(wfjSize + " rows exported from WF_JOBS");

            int wfaSize = exportTableToJSON(manager.createQuery(GET_WORKFLOW_ACTIONS), zos, OOZIEDB_AC_JSON);
            System.out.println(wfaSize + " rows exported from WF_ACTIONS");

            int cojSize = exportTableToJSON(manager.createQuery(GET_COORD_JOBS), zos, OOZIEDB_CJ_JSON);
            System.out.println(cojSize + " rows exported from COORD_JOBS");

            int coaSize = exportTableToJSON(manager.createQuery(GET_COORD_ACTIONS), zos, OOZIEDB_CA_JSON);
            System.out.println(coaSize + " rows exported from COORD_ACTIONS");

            int bnjSize = exportTableToJSON(manager.createQuery(GET_BUNDLE_JOBS), zos, OOZIEDB_BNJ_JSON);
            System.out.println(bnjSize + " rows exported from BUNDLE_JOBS");

            int bnaSize = exportTableToJSON(manager.createQuery(GET_BUNDLE_ACIONS), zos, OOZIEDB_BNA_JSON);
            System.out.println(bnaSize + " rows exported from BUNDLE_ACTIONS");

            int slaRegSize = exportTableToJSON(manager.createQuery(GET_SLA_REGISTRATIONS), zos, OOZIEDB_SLAREG_JSON);
            System.out.println(slaRegSize + " rows exported from SLA_REGISTRATION");

            int ssSize = exportTableToJSON(manager.createQuery(GET_SLA_SUMMARYS), zos, OOZIEDB_SLASUM_JSON);
            System.out.println(ssSize + " rows exported from SLA_SUMMARY");

        } catch (Exception e){
            System.err.println("Error during dump creation: " + e.getMessage());
            System.err.println();
            e.printStackTrace(System.err);
            System.err.println();
            if (file != null) {
                file.delete();
            }
            System.exit(1);
        } finally {
            IOUtils.closeSafely(zos);
            if (manager != null) {
                manager.close();
            }
        }
    }

    private static int exportTableToJSON(Query query, ZipOutputStream zipOutputStream, String filename) throws IOException {
        Gson gson = new Gson();
        ZipEntry zipEntry = new ZipEntry(filename);
        zipOutputStream.putNextEntry(zipEntry);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(zipOutputStream, "UTF-8"));
        query.setMaxResults(LIMIT);
        int exported = 0;
        List<?> list = query.getResultList();
        while (!list.isEmpty()) {
            query.setFirstResult(exported);
            list = query.getResultList();
            for (Object w : list) {
                exported++;
                gson.toJson(w, writer);
                writer.newLine();
            }
        }
        writer.flush();
        zipOutputStream.closeEntry();
        return exported;
    }
}
