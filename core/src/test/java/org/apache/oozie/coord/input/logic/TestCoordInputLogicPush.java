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

package org.apache.oozie.coord.input.logic;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.Writer;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.coord.CoordActionInputCheckXCommand;
import org.apache.oozie.command.coord.CoordActionStartXCommand;
import org.apache.oozie.command.coord.CoordMaterializeTransitionXCommand;
import org.apache.oozie.command.coord.CoordPushDependencyCheckXCommand;
import org.apache.oozie.command.coord.CoordSubmitXCommand;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor.CoordActionQuery;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XHCatTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.JDOMException;

public class TestCoordInputLogicPush extends XHCatTestCase {

    private Services services;
    private String server;
    private static final String table = "table1";


    public final static long TIME_DAYS = 60 * 60 * 1000 * 24;

    public static enum TEST_TYPE {
        CURRENT_SINGLE, CURRENT_RANGE, LATEST_SINGLE, LATEST_RANGE;
    };

    @Override
    public void setUp() throws Exception {
        super.setUp();
        services = super.setupServicesForHCatalog();
        services.init();
        createTestTable();
        server = getMetastoreAuthority();

    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
        dropTestTable();
    }

    private void createSingleTestTable(String db) throws Exception {
        dropTable(db, table, true);
        dropDatabase(db, true);
        createDatabase(db);
        createTable(db, table, "dt,country");
    }

    private void createTestTable() throws Exception {

        createSingleTestTable("db_a");
        createSingleTestTable("db_b");
        createSingleTestTable("db_c");
        createSingleTestTable("db_d");
        createSingleTestTable("db_e");
        createSingleTestTable("db_f");

    }

    private void dropSingleTestTable(String db) throws Exception {
        dropTable(db, table, false);
        dropDatabase(db, false);
    }

    private void dropTestTable() throws Exception {

        dropSingleTestTable("db_a");
        dropSingleTestTable("db_b");
        dropSingleTestTable("db_c");
        dropSingleTestTable("db_d");
        dropSingleTestTable("db_e");
        dropSingleTestTable("db_f");

    }

    public void testExists() throws Exception {
        Configuration conf = getConf();

        //@formatter:off
        String inputLogic =
        "<or name=\"test\">"+
                  "<data-in dataset=\"B\" />"+
                  "<data-in dataset=\"D\" />"+
         "</or>";
        //@formatter:on
        conf.set("partitionName", "test");
        String jobId = submitCoord("coord-inputlogic-hcat.xml", conf, inputLogic, TEST_TYPE.CURRENT_SINGLE);

        String input = addPartition("db_b", "table1", "dt=20141008;country=usa");

        startCoordAction(jobId);

        CoordinatorActionBean actionBean = CoordActionQueryExecutor.getInstance().get(
                CoordActionQuery.GET_COORD_ACTION, jobId + "@1");
        Configuration runConf = getActionConf(actionBean);
        String dataSets = runConf.get("inputLogicData");
        assertEquals(dataSets.split(",").length, 1);
        checkDataSets(dataSets, input);

    }

    public void testNestedCondition3() throws Exception {
        Configuration conf = getConf();

        //@formatter:off
        String inputLogic =
        "<and name=\"test\">"+
                  "<and>" +
                          "<data-in dataset=\"A\" />"+
                          "<data-in dataset=\"B\" />"+
                   "</and>" +
                   "<and>"+
                          "<data-in dataset=\"C\" />"+
                          "<data-in dataset=\"D\" />"+
                   "</and>"+
                   "<and>"+
                       "<data-in dataset=\"E\" />"+
                       "<data-in dataset=\"F\" />"+
                   "</and>"+
         "</and>";
        //@formatter:on
        conf.set("partitionName", "test");
        final String jobId = submitCoord("coord-inputlogic-hcat.xml", conf, inputLogic, TEST_TYPE.CURRENT_SINGLE);

        String input1 = addPartition("db_a", "table1", "dt=20141008;country=usa");
        String input2 = addPartition("db_b", "table1", "dt=20141008;country=usa");
        String input3 = addPartition("db_c", "table1", "dt=20141008;country=usa");
        String input4 = addPartition("db_d", "table1", "dt=20141008;country=usa");
        String input5 = addPartition("db_e", "table1", "dt=20141008;country=usa");
        String input6 = addPartition("db_f", "table1", "dt=20141008;country=usa");

        startCoordAction(jobId);

        CoordinatorActionBean actionBean = CoordActionQueryExecutor.getInstance().get(
                CoordActionQuery.GET_COORD_ACTION, jobId + "@1");
        Configuration runConf = getActionConf(actionBean);
        String dataSets = runConf.get("inputLogicData");
        assertEquals(dataSets.split(",").length, 6);
        checkDataSets(dataSets, input1, input2, input3, input4, input5, input6);

    }

    public void testNestedConditionWithRange() throws Exception {

        Configuration conf = getConfForCombine();
        Date now = new Date();
        conf.set("start_time", DateUtils.formatDateOozieTZ(now));
        conf.set("end_time", DateUtils.formatDateOozieTZ(new Date(now.getTime() + 10 * TIME_DAYS)));
        conf.set("initial_instance_a", DateUtils.formatDateOozieTZ(new Date(now.getTime() - 5 * TIME_DAYS)));
        conf.set("initial_instance_b", DateUtils.formatDateOozieTZ(new Date(now.getTime() - 5 * TIME_DAYS)));

        //@formatter:off
        String inputLogic =
        "<and name=\"test\" min=\"2\" >"+
                  "<or min=\"2\">" +
                          "<data-in dataset=\"A\" />"+
                          "<data-in dataset=\"B\" />"+
                   "</or>" +
                   "<or min=\"2\">"+
                          "<data-in dataset=\"C\" />"+
                          "<data-in dataset=\"D\" />"+
                   "</or>"+
                   "<and min=\"2\">"+
                       "<data-in dataset=\"A\" />"+
                       "<data-in dataset=\"C\" />"+
                   "</and>"+
         "</and>";
        //@formatter:on
        conf.set("partitionName", "test");
        final String jobId = submitCoord("coord-inputlogic-hcat.xml", conf, inputLogic, TEST_TYPE.CURRENT_RANGE,
                TEST_TYPE.LATEST_RANGE);
        List<String> inputPartition = createPartitionWithTime("db_a", now, 0, 1, 2);
        inputPartition.addAll(createPartitionWithTime("db_c", now, 0, 1, 2));
        startCoordAction(jobId);
        CoordinatorActionBean actionBean = CoordActionQueryExecutor.getInstance().get(
                CoordActionQuery.GET_COORD_ACTION, jobId + "@1");
        Configuration runConf = getActionConf(actionBean);
        String dataSets = runConf.get("inputLogicData");
        assertEquals(dataSets.split(",").length, 12);
        checkDataSets(dataSets, inputPartition.toArray(new String[inputPartition.size()]));

    }


    public void testLatestRange() throws Exception {

        Configuration conf = getConfForCombine();
        Date now = new Date();
        conf.set("start_time", DateUtils.formatDateOozieTZ(now));
        conf.set("end_time", DateUtils.formatDateOozieTZ(new Date(now.getTime() + 10 * TIME_DAYS)));
        conf.set("initial_instance_a", DateUtils.formatDateOozieTZ(new Date(now.getTime() - 5 * TIME_DAYS)));
        conf.set("initial_instance_b", DateUtils.formatDateOozieTZ(new Date(now.getTime() - 5 * TIME_DAYS)));

        String inputLogic =
        //@formatter:off
        "<and name=\"test\">"+
              "<data-in dataset=\"A\" />" +
              "<data-in dataset=\"B\" />" +
         "</and>";
        //@formatter:on
        String jobId = submitCoord("coord-inputlogic-combine.xml", conf, inputLogic, TEST_TYPE.LATEST_RANGE);

        List<String> inputDir = createDirWithTime("input-data/b/", now, 0, 1, 2, 3, 4, 5);
        inputDir.addAll(createPartitionWithTime("db_a", now, 0, 1, 2, 3, 4, 5));

        startCoordAction(jobId);

        CoordinatorActionBean actionBean = CoordActionQueryExecutor.getInstance().get(
                CoordActionQuery.GET_COORD_ACTION, jobId + "@1");

        assertFalse(CoordinatorAction.Status.WAITING.equals(actionBean.getStatus()));
        XConfiguration runConf = new XConfiguration(new StringReader(actionBean.getRunConf()));
        String dataSets = runConf.get("inputLogicData");
        assertEquals(dataSets.split(",").length, 12);
        checkDataSets(dataSets, inputDir.toArray(new String[inputDir.size()]));

    }

    public void testCurrentLatest() throws Exception {

        Configuration conf = getConfForCombine();
        Date now = new Date();
        conf.set("start_time", DateUtils.formatDateOozieTZ(now));
        conf.set("end_time", DateUtils.formatDateOozieTZ(new Date(now.getTime() + 10 * TIME_DAYS)));
        conf.set("initial_instance_a", DateUtils.formatDateOozieTZ(new Date(now.getTime() - 5 * TIME_DAYS)));
        conf.set("initial_instance_b", DateUtils.formatDateOozieTZ(new Date(now.getTime() - 5 * TIME_DAYS)));

        String inputLogic =
//@formatter:off
        "<and name=\"test\">"+
              "<data-in dataset=\"A\"/>" +
              "<data-in dataset=\"B\"/>" +
         "</and>";
        //@formatter:on
        String jobId = submitCoord("coord-inputlogic-combine.xml", conf, inputLogic, TEST_TYPE.LATEST_RANGE,
                TEST_TYPE.CURRENT_RANGE);

        List<String> inputDir = createDirWithTime("input-data/b/", now, 0, 1, 2, 3, 4, 5);
        inputDir.addAll(createPartitionWithTime("db_a", now, 0, 1, 2, 3, 4, 5));

        startCoordAction(jobId);

        CoordinatorActionBean actionBean = CoordActionQueryExecutor.getInstance().get(
                CoordActionQuery.GET_COORD_ACTION, jobId + "@1");

        assertFalse(CoordinatorAction.Status.WAITING.equals(actionBean.getStatus()));
        XConfiguration runConf = new XConfiguration(new StringReader(actionBean.getRunConf()));
        String dataSets = runConf.get("inputLogicData");
        assertEquals(dataSets.split(",").length, 12);
        checkDataSets(dataSets, inputDir.toArray(new String[inputDir.size()]));

    }

    public void testLatestRangeComplex() throws Exception {

        Configuration conf = getConfForCombine();
        Date now = new Date();
        conf.set("start_time", DateUtils.formatDateOozieTZ(now));
        conf.set("end_time", DateUtils.formatDateOozieTZ(new Date(now.getTime() + 10 * TIME_DAYS)));
        conf.set("initial_instance_a", DateUtils.formatDateOozieTZ(new Date(now.getTime() - 5 * TIME_DAYS)));
        conf.set("initial_instance_b", DateUtils.formatDateOozieTZ(new Date(now.getTime() - 5 * TIME_DAYS)));

        String inputLogic =
        //@formatter:off
        "<or name=\"test\">" +
            "<and>"+
                   "<data-in name=\"testA\" dataset=\"A\" />" +
                   "<data-in name=\"testB\" dataset=\"B\" />" +
             "</and>" +
             "<and name=\"test\">"+
                 "<data-in name=\"testC\" dataset=\"C\" />" +
                 "<data-in name=\"testD\" dataset=\"D\" />" +
             "</and>" +
        "</or>";

        //@formatter:on
        String jobId = submitCoord("coord-inputlogic-combine.xml", conf, inputLogic, TEST_TYPE.LATEST_RANGE);
        List<String> inputDir = createDirWithTime("input-data/b/", now, 0, 1, 2, 3, 4, 5);
        inputDir.addAll(createPartitionWithTime("db_a", now, 0, 1, 2, 3, 4, 5));

        startCoordAction(jobId);

        CoordinatorActionBean actionBean = CoordActionQueryExecutor.getInstance().get(
                CoordActionQuery.GET_COORD_ACTION, jobId + "@1");

        assertFalse(CoordinatorAction.Status.WAITING.equals(actionBean.getStatus()));
        XConfiguration runConf = new XConfiguration(new StringReader(actionBean.getRunConf()));
        String dataSets = runConf.get("inputLogicData");
        assertEquals(dataSets.split(",").length, 12);
        checkDataSets(dataSets, inputDir.toArray(new String[inputDir.size()]));

    }

    public void testHcatHdfs() throws Exception {
        Configuration conf = getConfForCombine();
        conf.set("initial_instance_a", "2014-10-07T00:00Z");
        conf.set("initial_instance_b", "2014-10-07T00:00Z");

        String inputLogic =
        //@formatter:off
            "<and name=\"test\">" +
                       "<data-in name=\"testA\" dataset=\"A\" />" +
                       "<data-in name=\"testB\" dataset=\"B\" />" +
            "</and>";
            //@formatter:on
        String jobId = submitCoord("coord-inputlogic-combine.xml", conf, inputLogic, TEST_TYPE.CURRENT_SINGLE);

        String input1 = createTestCaseSubDir("input-data/b/2014/10/08/_SUCCESS".split("/"));
        String input2 = addPartition("db_a", "table1", "dt=20141008;country=usa");

        new CoordMaterializeTransitionXCommand(jobId, 3600).call();
        new CoordPushDependencyCheckXCommand(jobId + "@1").call();
        new CoordActionInputCheckXCommand(jobId + "@1", jobId).call();

        startCoordAction(jobId);

        CoordinatorActionBean actionBean = CoordActionQueryExecutor.getInstance().get(
                CoordActionQuery.GET_COORD_ACTION, jobId + "@1");

        assertFalse(CoordinatorAction.Status.WAITING.equals(actionBean.getStatus()));
        XConfiguration runConf = new XConfiguration(new StringReader(actionBean.getRunConf()));
        String dataSets = runConf.get("inputLogicData");
        assertEquals(dataSets.split(",").length, 2);
        checkDataSets(dataSets, input1, input2);

    }

    public void testHcatHdfsLatest() throws Exception {
        Configuration conf = getConfForCombine();
        Date now = new Date();
        conf.set("start_time", DateUtils.formatDateOozieTZ(now));
        conf.set("end_time", DateUtils.formatDateOozieTZ(new Date(now.getTime() + 10 * TIME_DAYS)));
        conf.set("initial_instance_a", DateUtils.formatDateOozieTZ(new Date(now.getTime() - 5 * TIME_DAYS)));
        conf.set("initial_instance_b", DateUtils.formatDateOozieTZ(new Date(now.getTime() - 5 * TIME_DAYS)));
        conf.set("initial_instance", DateUtils.formatDateOozieTZ(new Date(now.getTime() - 5 * TIME_DAYS)));

        SimpleDateFormat sd = new SimpleDateFormat("yyyy/MM/dd");
        TimeZone tzUTC = TimeZone.getTimeZone("UTC");
        sd.setTimeZone(tzUTC);

        String inputLogic =
        // @formatter:off
            "<and name=\"test\" min = \"1\" >" +
                       "<data-in dataset=\"A\" />" +
                       "<data-in dataset=\"D\" />" +
            "</and>";

        //@formatter:on
        String jobId = submitCoord("coord-inputlogic-combine.xml", conf, inputLogic, TEST_TYPE.LATEST_RANGE);

        String input1 = createTestCaseSubDir(("input-data/d/" + sd.format(now) + "/_SUCCESS").split("/"));
        sd = new SimpleDateFormat("yyyyMMdd");
        String input2 = addPartition("db_a", "table1", "dt=" + sd.format(now) + ";country=usa");

        startCoordAction(jobId);

        CoordinatorActionBean actionBean = CoordActionQueryExecutor.getInstance().get(
                CoordActionQuery.GET_COORD_ACTION, jobId + "@1");

        assertFalse(CoordinatorAction.Status.WAITING.equals(actionBean.getStatus()));
        XConfiguration runConf = new XConfiguration(new StringReader(actionBean.getRunConf()));
        String dataSets = runConf.get("inputLogicData");
        assertEquals(dataSets.split(",").length, 2);
        checkDataSets(dataSets, input1, input2);

    }

    private Configuration getConf() throws Exception {
        Configuration conf = new XConfiguration();
        conf.set("start_time", "2014-10-08T00:00Z");
        conf.set("end_time", "2015-10-08T00:00Z");
        conf.set("initial_instance", "2014-10-08T00:00Z");

        String dataset1 = "hcat://" + getMetastoreAuthority();

        conf.set("data_set", dataset1.toString());
        conf.set("db_a", "db_a");
        conf.set("db_b", "db_b");
        conf.set("db_c", "db_c");
        conf.set("db_d", "db_d");
        conf.set("db_e", "db_e");
        conf.set("db_f", "db_f");
        conf.set("table", table);
        conf.set("wfPath", getWFPath(getTestCaseFileUri("workflow.xml")));
        conf.set("partitionName", "test");

        return conf;
    }

    public Configuration getConfForCombine() throws Exception {
        return getConfForCombine("file://" + getTestCaseDir(), "hcat://" + getMetastoreAuthority());
    }

    public static Configuration getConfForCombine(String testCaseDir, String hcatURL) throws Exception {
        Configuration conf = new XConfiguration();
        conf.set("start_time", "2014-10-08T00:00Z");
        conf.set("end_time", "2015-10-08T00:00Z");
        conf.set("initial_instance", "2014-10-08T00:00Z");

        conf.set("data_set_b", testCaseDir + "/input-data/b");
        conf.set("data_set_d", testCaseDir + "/input-data/d");
        conf.set("data_set_f", testCaseDir + "/input-data/f");

        conf.set("start_time", "2014-10-08T00:00Z");
        conf.set("end_time", "2015-10-08T00:00Z");
        conf.set("initial_instance_a", "2014-10-08T00:00Z");
        conf.set("initial_instance_b", "2014-10-08T00:00Z");
        conf.set("data_set", hcatURL);
        conf.set("db_a", "db_a");
        conf.set("db_b", "db_b");
        conf.set("db_c", "db_c");
        conf.set("db_d", "db_d");
        conf.set("db_e", "db_e");
        conf.set("db_f", "db_f");
        conf.set("table", table);
        conf.set("wfPath", getWFPath(testCaseDir  + "/workflow.xml"));
        conf.set("partitionName", "test");
        return conf;
    }

    public   String  submitCoord(String coordinatorXml, Configuration conf, String inputLogic, TEST_TYPE... testType)
            throws Exception {
        return submitCoord(getTestCaseDir(), coordinatorXml, conf, inputLogic, testType);
    }

    public static String submitCoord(String testCaseDir, String coordinatorXml, Configuration conf, String inputLogic,
            TEST_TYPE... testType) throws Exception {
        String appPath = "file://" + testCaseDir + File.separator + "coordinator.xml";

        String content = IOUtils.getResourceAsString(coordinatorXml, -1);
        content = content.replaceAll("=input-logic=", inputLogic);
        for (int i = 1; i <= 6; i++) {
            if (i - 1 < testType.length) {
                content = content.replaceAll("=data-in-param-" + i + "=", getEnumText(testType[i - 1]));
            }
            else {
                content = content.replaceAll("=data-in-param-" + i + "=", getEnumText(testType[testType.length - 1]));
            }
        }

        Writer writer = new FileWriter(new URI(appPath).getPath());
        IOUtils.copyCharStream(new StringReader(content), writer);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("nameNode", "hdfs://localhost:9000");
        conf.set("queueName", "default");
        conf.set("jobTracker", "localhost:9001");
        conf.set("examplesRoot", "examples");

        String coordId = null;

        try {
            coordId = new CoordSubmitXCommand(conf).call();
        }
        catch (CommandException e) {
            e.printStackTrace();
            fail("should not throw exception " + e.getMessage());
        }
        return coordId;
    }

    public static String getWFPath(String workflowUri) throws Exception {
        String appXml = "<workflow-app xmlns='uri:oozie:workflow:0.1' name='map-reduce-wf'> " + "<start to='end' /> "
                + "<end name='end' /> " + "</workflow-app>";

        writeToFile(appXml, workflowUri);
        return workflowUri;
    }

    private static void writeToFile(String appXml, String appPath) throws IOException {
        File wf = new File(URI.create(appPath));
        PrintWriter out = null;
        try {
            out = new PrintWriter(new FileWriter(wf));
            out.println(appXml);
        }
        catch (IOException iOException) {
            throw iOException;
        }
        finally {
            if (out != null) {
                out.close();
            }
        }
    }

    public void checkDataSets(String dataSets, String... values) {

        Set<String> inputDataSets = new HashSet<String>();
        for (String dataSet : dataSets.split(",")) {
            if (dataSet.indexOf(getTestCaseDir()) >= 0) {
                inputDataSets.add(dataSet.substring(dataSet.indexOf(getTestCaseDir())));
            }
            else {
                inputDataSets.add(dataSet);
            }
        }

        for (String value : values) {
            assertTrue(inputDataSets.contains(value.replace("/_SUCCESS","")));
        }

    }

    private void startCoordAction(final String jobId) throws CommandException, JPAExecutorException {
        new CoordMaterializeTransitionXCommand(jobId, 3600).call();
        new CoordActionInputCheckXCommand(jobId + "@1", jobId).call();
        new CoordPushDependencyCheckXCommand(jobId + "@1").call();
        new CoordActionInputCheckXCommand(jobId + "@1", jobId).call();

        waitFor(50 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordinatorActionBean actionBean = CoordActionQueryExecutor.getInstance().get(
                        CoordActionQuery.GET_COORD_ACTION, jobId + "@1");
                return !actionBean.getStatus().equals(CoordinatorAction.Status.WAITING);
            }
        });

        CoordinatorAction actionBean = CoordActionQueryExecutor.getInstance().get(CoordActionQuery.GET_COORD_ACTION,
                jobId + "@1");
        assertFalse("Action status should not be waiting",
                actionBean.getStatus().equals(CoordinatorAction.Status.WAITING));

        waitFor(50 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordinatorActionBean actionBean = CoordActionQueryExecutor.getInstance().get(
                        CoordActionQuery.GET_COORD_ACTION, jobId + "@1");
                return !actionBean.getStatus().equals(CoordinatorAction.Status.READY);
            }
        });
        CoordinatorJob coordJob = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB, jobId);
        new CoordActionStartXCommand(actionBean.getId(), coordJob.getUser(), coordJob.getAppName(),
                actionBean.getJobId()).call();
    }

    @SuppressWarnings("unchecked")
    public Configuration getActionConf(CoordinatorActionBean actionBean) throws JDOMException {
        Configuration conf = new XConfiguration();
        Element eAction = XmlUtils.parseXml(actionBean.getActionXml());
        Element configElem = eAction.getChild("action", eAction.getNamespace())
                .getChild("workflow", eAction.getNamespace()).getChild("configuration", eAction.getNamespace());
        List<Element> elementList = configElem.getChildren("property", eAction.getNamespace());
        for (Element element : elementList) {
            conf.set(((Element) element.getChildren().get(0)).getText(),
                    ((Element) element.getChildren().get(1)).getText());
        }
        return conf;
    }

    private  static String getEnumText(TEST_TYPE testType) {
        switch (testType) {
            case LATEST_SINGLE:
                return "<instance>\\${coord:latest(0)}</instance>";
            case LATEST_RANGE:
                return "<start-instance>\\${coord:latest(-5)}</start-instance>"
                        + "<end-instance>\\${coord:latest(0)}</end-instance>";
            case CURRENT_SINGLE:
                return "<instance>\\${coord:current(0)}</instance>";
            case CURRENT_RANGE:
                return "<start-instance>\\${coord:current(-5)}</start-instance>"
                        + "<end-instance>\\${coord:current(0)}</end-instance>";
        }
        return "";

    }

    public List<String> createDirWithTime(String dirPrefix, Date date, int... hours) {

        SimpleDateFormat sd = new SimpleDateFormat("yyyy/MM/dd");

        TimeZone tzUTC = TimeZone.getTimeZone("UTC");
        sd.setTimeZone(tzUTC);
        List<String> createdDirPath = new ArrayList<String>();

        for (int hour : hours) {
            createdDirPath
                    .add(createTestCaseSubDir((dirPrefix + sd.format(new Date(date.getTime() - hour * TIME_DAYS)) + "/_SUCCESS")
                            .split("/")));
        }
        return createdDirPath;
    }

    public List<String> createPartitionWithTime(String database, Date date, int... hours) throws Exception {

        List<String> createdPartition = new ArrayList<String>();
        SimpleDateFormat sd = new SimpleDateFormat("yyyyMMdd");
        TimeZone tzUTC = TimeZone.getTimeZone("UTC");
        sd.setTimeZone(tzUTC);
        for (int hour : hours) {
            createdPartition.add(addPartition(database, "table1",
                    "dt=" + sd.format(new Date(date.getTime() - hour * TIME_DAYS)) + ";country=usa"));

        }
        return createdPartition;
    }

    protected String addPartition(String db, String table, String partitionSpec) throws Exception {
        super.addPartition(db, table, partitionSpec);
        return "hcat://" + server + "/" + db + "/" + table + "/" + partitionSpec;
    }

}
