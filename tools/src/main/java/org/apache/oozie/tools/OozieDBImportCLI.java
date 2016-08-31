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
import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.cli.CLIParser;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.SLARegistrationBean;
import org.apache.oozie.sla.SLASummaryBean;
import org.apache.oozie.store.StoreException;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.FlushModeType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.apache.oozie.tools.OozieDBExportCLI.*;

/**
 * This class provides the following functionality:
 * <p/>
 * <ul>
 * <li>Imports the data from json files created by {@link OozieDBExportCLI} the specified target zip file</li>
 * <li>This class uses the current oozie configuration in oozie-site.xml</li>
 * </ul>
 * <p/>
 */


public class OozieDBImportCLI {
    private static final String[] HELP_INFO = {
            "",
            "OozieDBImportCLI reads Oozie database from a zip file."
    };
    private static final String IMPORT_CMD = "import";
    private static final String HELP_CMD = "help";

    public static void main(String[] args) throws ParseException {


        CLIParser parser = new CLIParser("oozie-setup.sh", HELP_INFO);
        parser.addCommand(HELP_CMD, "", "display usage for all commands or specified command", new Options(), false);
        parser.addCommand(IMPORT_CMD, "",
                "imports the contents of the Oozie database from the specified file",
                new Options(), true);

        try {
            CLIParser.Command command = parser.parse(args);
            if (command.getName().equals(IMPORT_CMD)) {
                Services services = new Services();
                services.getConf().set(Services.CONF_SERVICE_CLASSES, JPAService.class.getName());
                services.getConf().set(Services.CONF_SERVICE_EXT_CLASSES, "");
                services.init();
                System.out.println("==========================================================");
                System.out.println(Arrays.toString(command.getCommandLine().getArgs()));
                importAllDBTables(command.getCommandLine().getArgs()[0]);
            } else if (command.getName().equals(HELP_CMD)) {
                parser.showHelp(command.getCommandLine());
            }
        } catch (ParseException pex) {
            System.err.println("Invalid sub-command: " + pex.getMessage());
            System.err.println();
            System.err.println(parser.shortHelp());
            System.exit(1);
        } catch (Throwable e) {
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

    private static void importAllDBTables(String zipFileName) throws StoreException, IOException, JPAExecutorException {

        EntityManager entityManager = null;
        ZipFile zipFile = null;
        try {

            entityManager = Services.get().get(JPAService.class).getEntityManager();
            entityManager.setFlushMode(FlushModeType.COMMIT);
            zipFile = new ZipFile(zipFileName);
            checkDBVersion(entityManager, zipFile);
            importFrom(entityManager, zipFile, "WF_JOBS", WorkflowJobBean.class, OOZIEDB_WF_JSON);
            importFrom(entityManager, zipFile, "WF_ACTIONS", WorkflowActionBean.class, OOZIEDB_AC_JSON);
            importFrom(entityManager, zipFile, "COORD_JOBS", CoordinatorJobBean.class, OOZIEDB_CJ_JSON);
            importFrom(entityManager, zipFile, "COORD_ACTIONS", CoordinatorActionBean.class, OOZIEDB_CA_JSON);
            importFrom(entityManager, zipFile, "BUNDLE_JOBS", BundleJobBean.class, OOZIEDB_BNJ_JSON);
            importFrom(entityManager, zipFile, "BUNDLE_ACTIONS", BundleActionBean.class, OOZIEDB_BNA_JSON);
            importFrom(entityManager, zipFile, "SLA_REGISTRATION", SLARegistrationBean.class, OOZIEDB_SLAREG_JSON);
            importFrom(entityManager, zipFile, "SLA_SUMMARY", SLASummaryBean.class, OOZIEDB_SLASUM_JSON);

        } finally {
            if (entityManager != null) {
                entityManager.close();
            }
            if(zipFile != null){
                zipFile.close();
            }
        }

    }

    private static void checkDBVersion(EntityManager entityManager, ZipFile zipFile) throws IOException {
        try {
            String currentDBVersion = (String) entityManager.createNativeQuery("select data from OOZIE_SYS where name='db.version'").getSingleResult();
            String dumpDBVersion = null;
            ZipEntry entry = zipFile.getEntry(OOZIEDB_SYS_INFO_JSON);
            BufferedReader reader = new BufferedReader(new InputStreamReader(zipFile.getInputStream(entry), "UTF-8"));
            String line;
            Gson gson = new Gson();
            while ((line = reader.readLine()) != null) {
                List<String> info = gson.fromJson(line, List.class);
                if (info.size() > 1 && "db.version".equals(info.get(0))) {
                    dumpDBVersion = info.get(1);
                }
            }
            reader.close();
            if (currentDBVersion.equals(dumpDBVersion)) {
                System.out.println("Loading to Oozie database version " + currentDBVersion);
            } else {
                System.err.println("ERROR Oozie database version mismatch.");
                System.err.println("Oozie DB version:\t" + currentDBVersion);
                System.err.println("Dump DB version:\t" + dumpDBVersion);
                System.exit(1);
            }
        }catch (Exception e){
            System.err.println();
            System.err.println("Error during DB version check: " + e.getMessage());
            System.err.println();
            System.err.println("Stack trace for the error was (for debug purposes):");
            System.err.println("--------------------------------------");
            e.printStackTrace(System.err);
            System.err.println("--------------------------------------");
            System.err.println();
        }
    }

    private static void importFrom(EntityManager entityManager, ZipFile zipFile, String table,
                                   Class<?> clazz, String fileName) throws JPAExecutorException, IOException {
        EntityTransaction transaction = entityManager.getTransaction();
        transaction.begin();
        try {
            int size = importFromJSONtoDB(entityManager, zipFile, fileName, clazz);
            transaction.commit();
            System.out.println(size + " rows imported to " + table);
        } catch (Exception e) {
            if (transaction.isActive()) {
                transaction.rollback();
            }
            throw new RuntimeException("Import failed to table " + table + ".", e);
        }
    }

    private static <E> int importFromJSONtoDB(EntityManager entityManager, ZipFile zipFile, String filename, Class<E> clazz)
            throws JPAExecutorException, IOException {
        int wfjSize = 0;
        Gson gson = new Gson();
        ZipEntry entry = zipFile.getEntry(filename);
        if (entry != null) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(zipFile.getInputStream(entry), "UTF-8"));
            String line;
            while ((line = reader.readLine()) != null) {
                E workflow = gson.fromJson(line, clazz);
                entityManager.persist(workflow);
                wfjSize++;
            }
            reader.close();
        }
        return wfjSize;
    }
}