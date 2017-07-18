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

package org.apache.oozie.command.coord;

import java.io.File;
import java.io.FileWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.coord.input.logic.TestCoordInputLogicPush;
import org.apache.oozie.coord.input.logic.TestCoordInputLogicPush.TEST_TYPE;
import org.apache.oozie.dependency.ActionDependency;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XHCatTestCase;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.Pair;
import org.apache.oozie.util.XConfiguration;

public class TestCoordActionMissingDependenciesXCommand extends XHCatTestCase {
    private Services services;
    final String TABLE = "table1";
    final String DB_A = "db_a";
    final String DB_B = "db_b";
    final String DB_C = "db_c";
    final String DB_D = "db_d";
    final String DB_E = "db_e";
    final String DB_F = "db_f";

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = super.setupServicesForHCatalog();
        services.init();

    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testCoordActionPullDependencyMissing() throws Exception {

        Configuration conf = new XConfiguration();
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");

        // CASE 1: Failure case i.e. multiple data-in instances
        Reader reader = IOUtils.getResourceAsReader("coord-multiple-output-instance5.xml", -1);
        Writer writer = new FileWriter(new File(getTestCaseDir(), "coordinator.xml"));
        IOUtils.copyCharStream(reader, writer);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("data_set_a", "file://" + getTestCaseDir() + "/input-data/a/${YEAR}/${DAY}");
        conf.set("data_set_b", "file://" + getTestCaseDir() + "/input-data/b/${YEAR}/${DAY}");
        conf.set("data_set_c", "file://" + getTestCaseDir() + "/input-data/c/${YEAR}/${DAY}");
        conf.set("data_set_d", "file://" + getTestCaseDir() + "/input-data/d/${YEAR}/${DAY}");
        conf.set("data_set_e", "file://" + getTestCaseDir() + "/input-data/e/${YEAR}/${DAY}");

        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);
        String jobId = sc.call();
        new CoordMaterializeTransitionXCommand(jobId, 3600).call();
        List<Pair<CoordinatorActionBean, Map<String, ActionDependency>>> data = new CoordActionMissingDependenciesXCommand(
                jobId + "@1").call();

        Map<String, ActionDependency> dependencyMap = data.get(0).getSecond();
        assertEquals(6, dependencyMap.size());
        assertEquals(1, dependencyMap.get("A").getMissingDependencies().size());
        assertEquals(6, dependencyMap.get("B").getMissingDependencies().size());
        assertEquals(1, dependencyMap.get("C").getMissingDependencies().size());
        assertEquals(1, dependencyMap.get("D").getMissingDependencies().size());
        assertEquals(6, dependencyMap.get("E").getMissingDependencies().size());
        createTestCaseSubDir("input-data/a/2009/01/_SUCCESS".split("/"));
        createTestCaseSubDir("input-data/b/2009/01/_SUCCESS".split("/"));
        new CoordActionInputCheckXCommand(jobId + "@1", jobId).call();
        data = new CoordActionMissingDependenciesXCommand(jobId + "@1").call();
        dependencyMap = data.get(0).getSecond();
        assertEquals(5, dependencyMap.size());
        assertNull(dependencyMap.get("A"));
        assertEquals(5, dependencyMap.get("B").getMissingDependencies().size());
        assertTrue(dependencyMap.get("B").getMissingDependencies()
                .contains("file://" + getTestCaseDir() + "/input-data/b/2009/31/_SUCCESS"));
        assertTrue(dependencyMap.get("B").getMissingDependencies()
                .contains("file://" + getTestCaseDir() + "/input-data/b/2009/30/_SUCCESS"));
        assertTrue(dependencyMap.get("B").getMissingDependencies()
                .contains("file://" + getTestCaseDir() + "/input-data/b/2009/29/_SUCCESS"));
        assertTrue(dependencyMap.get("B").getMissingDependencies()
                .contains("file://" + getTestCaseDir() + "/input-data/b/2009/28/_SUCCESS"));
        assertTrue(dependencyMap.get("B").getMissingDependencies()
                .contains("file://" + getTestCaseDir() + "/input-data/b/2009/27/_SUCCESS"));
        assertEquals(1, dependencyMap.get("C").getMissingDependencies().size());
        assertEquals(1, dependencyMap.get("D").getMissingDependencies().size());
        assertEquals(dependencyMap.get("D").getMissingDependencies().get(0),
                "file://" + getTestCaseDir() + "/input-data/d/2009/01");

        assertEquals(6, dependencyMap.get("E").getMissingDependencies().size());

        assertEquals(2, dependencyMap.get("F").getMissingDependencies().size());
        assertEquals(dependencyMap.get("F").getMissingDependencies().get(0),
                "file://" + getTestCaseDir() + "/input-data/e/2009/01/_SUCCESS");
        assertEquals(dependencyMap.get("F").getMissingDependencies().get(1),
                "${coord:latest(0)} -> file://" + getTestCaseDir() + "/input-data/e/${YEAR}/${DAY}");
    }

    public void testCoordActionPushDependencyMissing() throws Exception {
        createTestTables();
        Configuration conf = new XConfiguration();
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");
        Reader reader = IOUtils.getResourceAsReader("coord-multiple-output-instance5.xml", -1);
        Writer writer = new FileWriter(new File(getTestCaseDir(), "coordinator.xml"));
        IOUtils.copyCharStream(reader, writer);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        String datasetSuffix = "/" + TABLE + "/dt=${YEAR}${DAY};country=usa";
        String datasetPrefix = "hcat://" + getMetastoreAuthority() + "/";

        conf.set("data_set_a", datasetPrefix.toString() + DB_A + datasetSuffix);
        conf.set("data_set_b", datasetPrefix.toString() + DB_B + datasetSuffix);
        conf.set("data_set_c", datasetPrefix.toString() + DB_C + datasetSuffix);
        conf.set("data_set_d", datasetPrefix.toString() + DB_D + datasetSuffix);
        conf.set("data_set_e", datasetPrefix.toString() + DB_E + datasetSuffix);

        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);
        String jobId = sc.call();
        new CoordMaterializeTransitionXCommand(jobId, 3600).call();
        List<Pair<CoordinatorActionBean, Map<String, ActionDependency>>> data = new CoordActionMissingDependenciesXCommand(
                jobId + "@1").call();

        assertEquals(datasetPrefix + "db_a/table1/dt=200901;country=usa",
                CoordCommandUtils.getFirstMissingDependency(data.get(0).getFirst()));

        Map<String, ActionDependency> dependencyMap = data.get(0).getSecond();
        assertEquals(6, dependencyMap.size());
        assertEquals(1, dependencyMap.get("A").getMissingDependencies().size());
        assertEquals(6, dependencyMap.get("B").getMissingDependencies().size());
        assertEquals(1, dependencyMap.get("C").getMissingDependencies().size());
        assertEquals("${coord:latestRange(-5,0)} -> " + datasetPrefix.toString() + DB_C + datasetSuffix,
                dependencyMap.get("C").getMissingDependencies().get(0));
        assertEquals(1, dependencyMap.get("D").getMissingDependencies().size());
        assertEquals(6, dependencyMap.get("E").getMissingDependencies().size());

        addPartition(DB_A, TABLE, "dt=200901;country=usa");
        new CoordPushDependencyCheckXCommand(jobId + "@1").call();
        data = new CoordActionMissingDependenciesXCommand(jobId + "@1").call();
        dependencyMap = data.get(0).getSecond();
        assertEquals(5, dependencyMap.size());
        assertNull(dependencyMap.get("A"));

        addPartition(DB_B, TABLE, "dt=200901;country=usa");
        addPartition(DB_B, TABLE, "dt=200931;country=usa");
        addPartition(DB_B, TABLE, "dt=200930;country=usa");
        addPartition(DB_B, TABLE, "dt=200929;country=usa");
        addPartition(DB_B, TABLE, "dt=200928;country=usa");
        addPartition(DB_B, TABLE, "dt=200927;country=usa");

        new CoordPushDependencyCheckXCommand(jobId + "@1").call();
        data = new CoordActionMissingDependenciesXCommand(jobId + "@1").call();
        dependencyMap = data.get(0).getSecond();
        assertEquals(4, dependencyMap.size());
        assertNull(dependencyMap.get("B"));
    }

    public void testCoordActionPullPushDependencyMissing() throws Exception {
        createTestTables();
        Configuration conf = new XConfiguration();
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");
        Reader reader = IOUtils.getResourceAsReader("coord-multiple-output-instance5.xml", -1);
        Writer writer = new FileWriter(new File(getTestCaseDir(), "coordinator.xml"));
        IOUtils.copyCharStream(reader, writer);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        String datasetPrefix = "/" + TABLE + "/dt=${YEAR}${DAY};country=usa";
        String datasetSuffix = "hcat://" + getMetastoreAuthority() + "/";

        conf.set("data_set_a", datasetSuffix.toString() + DB_A + datasetPrefix);
        conf.set("data_set_b", datasetSuffix.toString() + DB_B + datasetPrefix);
        conf.set("data_set_c", datasetSuffix.toString() + DB_C + datasetPrefix);
        conf.set("data_set_d", "file://" + getTestCaseDir() + "/input-data/d/${YEAR}/${DAY}");
        conf.set("data_set_e", "file://" + getTestCaseDir() + "/input-data/e/${YEAR}/${DAY}");

        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);
        String jobId = sc.call();
        new CoordMaterializeTransitionXCommand(jobId, 3600).call();
        List<Pair<CoordinatorActionBean, Map<String, ActionDependency>>> data = new CoordActionMissingDependenciesXCommand(
                jobId + "@1").call();

        assertEquals("file://" + getTestCaseDir() + "/input-data/d/2009/01",
                CoordCommandUtils.getFirstMissingDependency(data.get(0).getFirst()));

        Map<String, ActionDependency> dependencyMap = data.get(0).getSecond();
        assertEquals(6, dependencyMap.size());
        assertEquals(1, dependencyMap.get("A").getMissingDependencies().size());
        assertEquals(6, dependencyMap.get("B").getMissingDependencies().size());
        assertEquals(1, dependencyMap.get("C").getMissingDependencies().size());
        assertEquals(1, dependencyMap.get("D").getMissingDependencies().size());
        assertEquals(6, dependencyMap.get("E").getMissingDependencies().size());

        addPartition(DB_A, TABLE, "dt=200901;country=usa");
        addPartition(DB_B, TABLE, "dt=200901;country=usa");
        addPartition(DB_B, TABLE, "dt=200931;country=usa");
        addPartition(DB_B, TABLE, "dt=200930;country=usa");
        addPartition(DB_B, TABLE, "dt=200929;country=usa");
        addPartition(DB_B, TABLE, "dt=200928;country=usa");
        addPartition(DB_B, TABLE, "dt=200927;country=usa");
        createTestCaseSubDir("input-data/d/2009/01".split("/"));

        new CoordActionInputCheckXCommand(jobId + "@1", jobId).call();
        new CoordPushDependencyCheckXCommand(jobId + "@1").call();
        data = new CoordActionMissingDependenciesXCommand(jobId + "@1").call();
        dependencyMap = data.get(0).getSecond();
        assertEquals(3, dependencyMap.size());
        assertNull(dependencyMap.get("B"));
        assertNull(dependencyMap.get("D"));
    }

    public void testCoordActionInputLogicMissing() throws Exception {
        createTestTables();

        Configuration conf = TestCoordInputLogicPush.getConfForCombine("file://" + getTestCaseDir(),
                "hcat://" + getMetastoreAuthority());
        conf.set("initial_instance_a", "2014-10-07T00:00Z");
        conf.set("initial_instance_b", "2014-10-07T00:00Z");

        String inputLogic =
                // @formatter:off
                "<and name=\"test\">" + "<data-in dataset=\"A\" />" + "<data-in dataset=\"B\" />" + "</and>";
        // @formatter:on
        String jobId = TestCoordInputLogicPush.submitCoord(getTestCaseDir(), "coord-inputlogic-combine.xml", conf,
                inputLogic, TEST_TYPE.CURRENT_SINGLE, TEST_TYPE.CURRENT_SINGLE, TEST_TYPE.CURRENT_RANGE,
                TEST_TYPE.LATEST_RANGE);

        new CoordMaterializeTransitionXCommand(jobId, 3600).call();
        List<Pair<CoordinatorActionBean, Map<String, ActionDependency>>> data = new CoordActionMissingDependenciesXCommand(
                jobId + "@1").call();
        Map<String, ActionDependency> dependencyMap = data.get(0).getSecond();
        assertEquals(6, dependencyMap.size());

        assertNull(CoordCommandUtils.getFirstMissingDependency(data.get(0).getFirst()));

        createTestCaseSubDir("input-data/b/2014/10/08/_SUCCESS".split("/"));
        addPartition(DB_A, TABLE, "dt=20141008;country=usa");

        new CoordActionInputCheckXCommand(jobId + "@1", jobId).call();
        new CoordPushDependencyCheckXCommand(jobId + "@1").call();
        new CoordActionInputCheckXCommand(jobId + "@1", jobId).call();

        data = new CoordActionMissingDependenciesXCommand(jobId + "@1").call();
        dependencyMap = data.get(0).getSecond();
        assertEquals(4, dependencyMap.size());
        assertNull(dependencyMap.get("A"));
        assertNull(dependencyMap.get("B"));

    }

    private void createSingleTestDB(String db) throws Exception {
        dropTable(db, TABLE, true);
        dropDatabase(db, true);
        createDatabase(db);
        createTable(db, TABLE, "dt,country");
    }

    private void createTestTables() throws Exception {
        createSingleTestDB(DB_A);
        createSingleTestDB(DB_B);
        createSingleTestDB(DB_C);
        createSingleTestDB(DB_D);
        createSingleTestDB(DB_E);
        createSingleTestDB(DB_F);

    }

}
