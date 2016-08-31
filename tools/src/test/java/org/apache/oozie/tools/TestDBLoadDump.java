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

import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.Query;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

/**
 * Test Dump and dump reading mechanism
 */
public class TestDBLoadDump extends XTestCase {
    File zipDump;

    private EntityManager getEntityManager() throws ServiceException {
        Services services = Services.get();
        if(services == null){
            Services s = new Services();
            s.init();
            services = Services.get();
        }
        return services.get(JPAService.class).getEntityManager();
    }
    @BeforeClass
    protected void setUp() throws Exception {
        System.getProperties().remove("oozie.test.config.file");
        super.setUp();
        zipDump = new File(getTestCaseDir() + System.getProperty("file.separator") + "dumpTest.zip");
        ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(zipDump));
        zos.setLevel(1);
        File dumpFolder = new File(getClass().getResource("/dumpData").getPath());
        IOUtils.zipDir(dumpFolder, "", zos);

        createOozieSysTable(getEntityManager());
    }

    @AfterClass
    protected void tearDown() throws Exception {
        dropOozieSysTable(getEntityManager());
        super.tearDown();
    }

    @Test
    public void testLoadDump() throws Exception {
        OozieDBImportCLI.main(new String[]{"import", zipDump.getAbsolutePath()});
        EntityManager entityManager = getEntityManager();
        Query q = entityManager.createNamedQuery("GET_WORKFLOWS");
        List<WorkflowJobBean> wfjBeans = q.getResultList();
        int wfjSize = wfjBeans.size();
        assertEquals(1, wfjSize);
        assertEquals("0000003-160720041037822-oozie-oozi-W", wfjBeans.get(0).getId());
        assertEquals("aggregator-wf", wfjBeans.get(0).getAppName());

        File newZipDump = new File(getTestCaseDir() + System.getProperty("file.separator") + "newDumpTest.zip");
        //export the contents of the database
        OozieDBExportCLI.main(new String[]{"export", newZipDump.getAbsolutePath()});
        assertEquals(zipDump.length(), newZipDump.length());
        ZipFile zip = new ZipFile(newZipDump);
        // check that dump is identical with the original input
        BufferedReader reader = new BufferedReader(new InputStreamReader(
                zip.getInputStream(zip.getEntry("ooziedb_wf.json"))));
        assertTrue(reader.readLine().contains("0000003-160720041037822-oozie-oozi-W"));

    }

    private void createOozieSysTable(EntityManager entityManager) throws Exception {
        String createDB = "create table OOZIE_SYS (name varchar(100), data varchar(100))";
        String insertDbVerion = "insert into OOZIE_SYS (name, data) values ('db.version', '3')";
        EntityTransaction t = entityManager.getTransaction();
        t.begin();
        entityManager.createNativeQuery(createDB).executeUpdate();
        entityManager.createNativeQuery(insertDbVerion).executeUpdate();
        t.commit();
    }

    private void dropOozieSysTable(EntityManager entityManager) {
        String dropDB = "drop table OOZIE_SYS";
        EntityTransaction t = entityManager.getTransaction();
        t.begin();
        entityManager.createNativeQuery(dropDB).executeUpdate();
        t.commit();
    }
}
