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

import org.apache.commons.cli.ParseException;
import org.apache.oozie.*;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.SLARegistrationBean;
import org.apache.oozie.sla.SLASummaryBean;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.Query;
import javax.persistence.TypedQuery;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.Permission;
import java.util.List;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import static org.apache.oozie.tools.OozieDBImportCLI.DEFAULT_BATCH_SIZE;
import static org.apache.oozie.tools.OozieDBImportCLI.OOZIE_DB_IMPORT_BATCH_SIZE_KEY;

/**
 * Test Dump and dump reading mechanism
 */
public class TestDBLoadDump extends XTestCase {
    private File validZipDump;
    private File invalidZipDump;

    @Before
    protected void setUp() throws Exception {
        // Use hsqldb-tools-oozie-site.xml with sql.enforce_strict_size=true in these tests
        System.setProperty("oozie.test.db", "hsqldb-tools");
        System.getProperties().remove("oozie.test.config.file");

        super.setUp();

        validZipDump = zipDump("/dumpData/valid", "validZipDumpTest.zip");
        invalidZipDump = zipDump("/dumpData/invalid", "invalidZipDumpTest.zip");

        createOozieSysTable(getEntityManager());

        System.setSecurityManager(new NoExitSecurityManager());
    }

    @After
    protected void tearDown() throws Exception {
        System.setSecurityManager(null);

        dropTable(getEntityManager(), "OOZIE_SYS");

        super.tearDown();
    }

    @Test
    public void testImportedDBIsExportedCorrectly() throws Exception {
        importValidDataToDB();

        final EntityManager entityManager = getEntityManager();
        final TypedQuery<WorkflowJobBean> q = entityManager.createNamedQuery("GET_WORKFLOWS", WorkflowJobBean.class);
        final List<WorkflowJobBean> wfjBeans = q.getResultList();
        final int wfjSize = wfjBeans.size();
        assertEquals(1, wfjSize);
        assertEquals("0000003-160720041037822-oozie-oozi-W", wfjBeans.get(0).getId());
        assertEquals("aggregator-wf", wfjBeans.get(0).getAppName());

        final File newZipDump = new File(getTestCaseDir() + System.getProperty("file.separator") + "newDumpTest.zip");

        exportFromDB(newZipDump);

        assertEquals(validZipDump.length(), newZipDump.length());
        final ZipFile zip = new ZipFile(newZipDump);
        final BufferedReader reader = new BufferedReader(new InputStreamReader(
                zip.getInputStream(zip.getEntry("ooziedb_wf.json"))));
        assertTrue(reader.readLine().contains("0000003-160720041037822-oozie-oozi-W"));
    }

    @Test
    public void testImportToNonEmptyTablesCausesPrematureExit() throws Exception {
        importValidDataToDB();

        assertEquals("One WorkflowJobBean should be inserted.", 1L, getCount(WorkflowJobBean.class));
        assertEquals("Three WorkflowActionBeans should be inserted.", 3L, getCount(WorkflowActionBean.class));
        assertEquals("Three CoordinatorActionBeans should be inserted.", 3L, getCount(CoordinatorActionBean.class));
        assertEquals("Three CoordinatorJobBeans should be inserted.", 3L, getCount(CoordinatorJobBean.class));

        assertEquals("The only WorkflowActionBean should have the right appPath.",
                getFieldValue("executionPath", "0000003-160720041037822-oozie-oozi-W@aggregator").toString(), "/");

        tryImportAndCheckPrematureExit(true);
    }

    @Test
    public void testImportToNonExistingTablesSucceedsOnHsqldb() throws Exception {
        dropTable(getEntityManager(), OozieDBImportCLI.findTableName(getEntityManager(), WorkflowJobBean.class));
        dropTable(getEntityManager(), OozieDBImportCLI.findTableName(getEntityManager(), WorkflowActionBean.class));
        dropTable(getEntityManager(), OozieDBImportCLI.findTableName(getEntityManager(), CoordinatorJobBean.class));
        dropTable(getEntityManager(), OozieDBImportCLI.findTableName(getEntityManager(), CoordinatorActionBean.class));
        dropTable(getEntityManager(), OozieDBImportCLI.findTableName(getEntityManager(), BundleJobBean.class));
        dropTable(getEntityManager(), OozieDBImportCLI.findTableName(getEntityManager(), BundleActionBean.class));
        dropTable(getEntityManager(), OozieDBImportCLI.findTableName(getEntityManager(), SLARegistrationBean.class));
        dropTable(getEntityManager(), OozieDBImportCLI.findTableName(getEntityManager(), SLASummaryBean.class));

        importValidDataToDB();
    }

    @Test
    public void testImportInvalidDataLeavesTablesEmpty() throws Exception {
        tryImportAndCheckPrematureExit(false);

        assertTableEmpty(WorkflowJobBean.class);
        assertTableEmpty(WorkflowActionBean.class);
        assertTableEmpty(CoordinatorJobBean.class);
        assertTableEmpty(CoordinatorActionBean.class);
        assertTableEmpty(BundleJobBean.class);
        assertTableEmpty(BundleActionBean.class);
        assertTableEmpty(SLARegistrationBean.class);
        assertTableEmpty(SLASummaryBean.class);
    }

    @Test
    public void testImportTablesOverflowBatchSize() throws Exception {
        System.setProperty(OOZIE_DB_IMPORT_BATCH_SIZE_KEY, "2");

        testImportedDBIsExportedCorrectly();

        System.setProperty(OOZIE_DB_IMPORT_BATCH_SIZE_KEY, Integer.toString(DEFAULT_BATCH_SIZE));
    }

    private EntityManager getEntityManager() throws ServiceException {
        Services services = Services.get();

        if (services == null){
            final Services s = new Services();
            s.init();
            services = Services.get();
        }

        return services.get(JPAService.class).getEntityManager();
    }

    private File zipDump(final String dumpFolderPath, final String zipDumpFileName) throws IOException {
        final File zipDump = new File(getTestCaseDir() + File.separator + zipDumpFileName);

        try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(zipDump))) {
            zos.setLevel(1);
            final File dumpFolder = new File(getClass().getResource(dumpFolderPath).getPath());

            IOUtils.zipDir(dumpFolder, "", zos);
        }

        return zipDump;
    }

    private void createOozieSysTable(final EntityManager entityManager) throws Exception {
        final String createDB = "create table OOZIE_SYS (name varchar(100), data varchar(100))";
        final String insertDBVersion = "insert into OOZIE_SYS (name, data) values ('db.version', '3')";

        final EntityTransaction tx = entityManager.getTransaction();
        tx.begin();

        entityManager.createNativeQuery(createDB).executeUpdate();

        entityManager.createNativeQuery(insertDBVersion).executeUpdate();

        tx.commit();
    }

    private void dropTable(final EntityManager entityManager, final String tableName) throws ServiceException {
        final String dropTableStmt = String.format("DROP TABLE %s CASCADE", tableName);

        final EntityTransaction tx = entityManager.getTransaction();

        tx.begin();

        entityManager.createNativeQuery(dropTableStmt).executeUpdate();

        tx.commit();
    }

    private void importValidDataToDB() throws ParseException {
        importToDB(validZipDump);
    }

    private void importInvalidDataToDB() throws ParseException {
        importToDB(invalidZipDump);
    }

    private void importToDB(final File zipDump) throws ParseException {
        OozieDBImportCLI.main(new String[]{"import", "--verbose", zipDump.getAbsolutePath()});
    }

    private void exportFromDB(final File zipDump) {
        OozieDBExportCLI.main(new String[]{"export", zipDump.getAbsolutePath()});
    }

    private long getCount(final Class<?> entityClass) throws ServiceException {
        return Long.valueOf(getEntityManager()
                .createQuery(String.format("SELECT COUNT(e) FROM %s e", entityClass.getSimpleName()))
                .getSingleResult()
                .toString());
    }

    private Object getFieldValue(final String fieldName, final String id)
            throws ServiceException {
        final Query fieldValueQuery = getEntityManager().createQuery(
                String.format("SELECT e.%s FROM %s e WHERE e.id = '%s'", fieldName, WorkflowActionBean.class.getSimpleName(), id));
        return fieldValueQuery.getSingleResult();
    }

    private void tryImportAndCheckPrematureExit(final boolean validData) throws ParseException {
        boolean prematureExit = false;

        try {
            if (validData) {
                importValidDataToDB();
            } else {
                importInvalidDataToDB();
            }
        } catch (final ExitException e) {
            prematureExit = (e.status == 1);
        }

        assertTrue("import should have been ended prematurely", prematureExit);
    }

    private void assertTableEmpty(final Class<?> entityClass) throws ServiceException {
        assertEquals(String.format("[%s] table is empty", OozieDBImportCLI.findTableName(getEntityManager(), entityClass)),
                0,
                getCount(entityClass));
    }

    private static class ExitException extends SecurityException {
        private final int status;

        ExitException(final int status) {
            this.status = status;
        }
    }

    private static class NoExitSecurityManager extends SecurityManager {

        @Override
        public void checkPermission(final Permission perm) {

        }

        @Override
        public void checkPermission(final Permission perm, final Object context) {

        }

        @Override
        public void checkExit(final int status) {
            super.checkExit(status);
            throw new ExitException(status);
        }
    }

}
