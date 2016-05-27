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
import java.io.Reader;
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
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.coord.CoordActionInputCheckXCommand;
import org.apache.oozie.command.coord.CoordActionStartXCommand;
import org.apache.oozie.command.coord.CoordMaterializeTransitionXCommand;
import org.apache.oozie.command.coord.CoordSubmitXCommand;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor.CoordActionQuery;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.jdom.JDOMException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestCoordinatorInputLogic extends XDataTestCase {
    private Services services;

    @Before
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
    }

    @After
    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }


    @Test(expected = CommandException.class)
    public void testValidateRange() throws Exception {
        Configuration conf = getConf();

        //@formatter:off
        String inputLogic =
        "<combine name=\"test\">"+
                        "<data-in dataset=\"A\" />"+
                        "<data-in dataset=\"b\" />"+
         "</combine>";
        String inputEvent =
        "<data-in name=\"A\" dataset=\"a\">" +
                    "<start-instance>${coord:current(-5)}</start-instance>" +
                    "<end-instance>${coord:current(0)}</end-instance>" +
        "</data-in>" +
        "<data-in name=\"B\" dataset=\"b\">" +
                    "<start-instance>${coord:current(-4)}</start-instance>" +
                    "<end-instance>${coord:current(0)}</end-instance>" +
       "</data-in>";
        //@formatter:on
        conf.set("partitionName", "test");
        try {
            _testCoordSubmit("coord-inputlogic.xml", conf, inputLogic, inputEvent, true);
            fail();
        }
        catch (CommandException e) {
            assertEquals(e.getErrorCode(), ErrorCode.E0803);
        }
    }

    public void testDryRun() throws Exception {
        Configuration conf = getConf();

        //@formatter:off
        String inputLogic =
        "<or name=\"test\">"+
                "<and>"+
                    "<or>"+
                        "<data-in dataset=\"A\" />"+
                        "<data-in dataset=\"B\" />"+
                    "</or>"+
                    "<or>"+
                        "<data-in dataset=\"C\" />"+
                        "<data-in dataset=\"D\" />"+
                    "</or>"+
                "</and>"+
                "<and>"+
                    "<data-in dataset=\"A\" />"+
                    "<data-in dataset=\"B\" />"+
                "</and>"+
         "</or>";
        //@formatter:on
        conf.set("partitionName", "test");
        _testCoordSubmit("coord-inputlogic.xml", conf, inputLogic, "", true);

    }

    public void testNestedCondition() throws Exception {
        Configuration conf = getConf();

        //@formatter:off
        String inputLogic =
                "<or name=\"test\">"+
                        "<and>"+
                            "<or>"+
                                "<data-in dataset=\"A\" />"+
                                "<data-in dataset=\"B\" />"+
                            "</or>"+
                        "<or>"+
                            "<data-in dataset=\"C\" />"+
                            "<data-in dataset=\"D\" />"+
                        "</or>"+
                        "</and>"+
                            "<and>"+
                                "<data-in dataset=\"A\" />"+
                                "<data-in dataset=\"B\" />"+
                             "</and>"+
                        "</or>";
        //@formatter:on
        conf.set("partitionName", "test");

        final String jobId = _testCoordSubmit("coord-inputlogic.xml", conf, inputLogic);

        new CoordMaterializeTransitionXCommand(jobId, 3600).call();

        new CoordActionInputCheckXCommand(jobId + "@1", jobId).call();
        String input1 = createTestCaseSubDir("input-data/a/2014/10/08/00/_SUCCESS".split("/"));
        String input2 = createTestCaseSubDir("input-data/b/2014/10/08/00/_SUCCESS".split("/"));

        startCoordAction(jobId);

        CoordinatorActionBean actionBean = CoordActionQueryExecutor.getInstance().get(
                CoordActionQuery.GET_COORD_ACTION, jobId + "@1");
        XConfiguration runConf = new XConfiguration(new StringReader(actionBean.getRunConf()));
        String dataSets = runConf.get("inputLogicData");
        assertEquals(dataSets.split(",").length, 2);
        checkDataSets(dataSets, input1, input2);

    }

    public void testNestedCondition1() throws Exception {
        Configuration conf = getConf();

        //@formatter:off
        String inputLogic =
        "<and name=\"test\">"+
              "<or>"+
                  "<and>" +
                          "<data-in dataset=\"A\"/>"+
                          "<data-in dataset=\"B\"/>"+
                   "</and>" +
                   "<and>"+
                          "<data-in dataset=\"C\"/>"+
                          "<data-in dataset=\"D\"/>"+
                   "</and>"+
             "</or>"+
             "<and>"+
                 "<data-in dataset=\"E\"/>"+
                 "<data-in dataset=\"F\"/>"+
             "</and>"+
         "</and>";
        //@formatter:on
        conf.set("partitionName", "test");
        conf.set("A_done_flag", "done");
        final String jobId = _testCoordSubmit("coord-inputlogic.xml", conf, inputLogic);

        String input1 = createTestCaseSubDir("input-data/a/2014/10/08/00/done".split("/"));
        String input2 = createTestCaseSubDir("input-data/b/2014/10/08/00/_SUCCESS".split("/"));
        String input3 = createTestCaseSubDir("input-data/e/2014/10/08/00/_SUCCESS".split("/"));
        String input4 = createTestCaseSubDir("input-data/f/2014/10/08/00/_SUCCESS".split("/"));

        startCoordAction(jobId);

        CoordinatorActionBean actionBean = CoordActionQueryExecutor.getInstance().get(
                CoordActionQuery.GET_COORD_ACTION, jobId + "@1");
        XConfiguration runConf = new XConfiguration(new StringReader(actionBean.getRunConf()));
        String dataSets = runConf.get("inputLogicData");
        assertEquals(dataSets.split(",").length, 4);
        checkDataSets(dataSets, input1.replace("/done", ""), input2, input3, input4);

    }

    public void testNestedCondition2() throws Exception {
        Configuration conf = getConf();

        //@formatter:off
        String inputLogic =
        "<or name=\"${partitionName}\">"+
                  "<and>" +
                          "<data-in dataset=\"A\" />"+
                          "<data-in dataset=\"B\" />"+
                          "<data-in dataset=\"C\" />"+
                          "<data-in dataset=\"D\" />"+
                   "</and>" +
                   "<and>"+
                          "<data-in dataset=\"E\" />"+
                          "<data-in dataset=\"F\" />"+
                   "</and>"+
         "</or>";
        //@formatter:on
        conf.set("partitionName", "test");
        conf.set("A_done_flag", "done");
        final String jobId = _testCoordSubmit("coord-inputlogic.xml", conf, inputLogic);

        String input1 = createTestCaseSubDir("input-data/a/2014/10/08/00/done".split("/"));
        String input2 = createTestCaseSubDir("input-data/b/2014/10/08/00/_SUCCESS".split("/"));
        String input3 = createTestCaseSubDir("input-data/c/2014/10/08/00/_SUCCESS".split("/"));
        String input4 = createTestCaseSubDir("input-data/e/2014/10/08/00/_SUCCESS".split("/"));
        String input5 = createTestCaseSubDir("input-data/f/2014/10/08/00/_SUCCESS".split("/"));

        startCoordAction(jobId);

        CoordinatorActionBean actionBean = CoordActionQueryExecutor.getInstance().get(
                CoordActionQuery.GET_COORD_ACTION, jobId + "@1");
        XConfiguration runConf = new XConfiguration(new StringReader(actionBean.getRunConf()));
        String dataSets = runConf.get("inputLogicData");
        assertEquals(dataSets.split(",").length, 2);
        checkDataSets(dataSets, input4, input5);
        checkDataSetsForFalse(dataSets, input1, input2, input3);

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
        final String jobId = _testCoordSubmit("coord-inputlogic.xml", conf, inputLogic);

        String input1 = createTestCaseSubDir("input-data/a/2014/10/08/00/_SUCCESS".split("/"));
        String input2 = createTestCaseSubDir("input-data/b/2014/10/08/00/_SUCCESS".split("/"));
        String input3 = createTestCaseSubDir("input-data/c/2014/10/08/00/_SUCCESS".split("/"));
        String input4 = createTestCaseSubDir("input-data/d/2014/10/08/00/_SUCCESS".split("/"));
        String input5 = createTestCaseSubDir("input-data/e/2014/10/08/00/_SUCCESS".split("/"));
        String input6 = createTestCaseSubDir("input-data/f/2014/10/08/00/_SUCCESS".split("/"));

        startCoordAction(jobId);

        CoordinatorActionBean actionBean = CoordActionQueryExecutor.getInstance().get(
                CoordActionQuery.GET_COORD_ACTION, jobId + "@1");
        XConfiguration runConf = new XConfiguration(new StringReader(actionBean.getRunConf()));
        String dataSets = runConf.get("inputLogicData");
        assertEquals(dataSets.split(",").length, 6);
        checkDataSets(dataSets, input1, input2, input3, input4, input5, input6);

    }

    public void testSimpleOr() throws Exception {
        Configuration conf = getConf();
        //@formatter:off
        String inputLogic =
        "<or name=\"test\">"+
                  "<data-in dataset=\"A\" />"+
                  "<data-in dataset=\"B\" />"+
         "</or>";
        //@formatter:on
        conf.set("partitionName", "test");
        String jobId = _testCoordSubmit("coord-inputlogic.xml", conf, inputLogic);

        String input1 = createTestCaseSubDir("input-data/a/2014/10/08/00/_SUCCESS".split("/"));

        startCoordAction(jobId);

        CoordinatorActionBean actionBean = CoordActionQueryExecutor.getInstance().get(
                CoordActionQuery.GET_COORD_ACTION, jobId + "@1");
        XConfiguration runConf = new XConfiguration(new StringReader(actionBean.getRunConf()));
        String dataSets = runConf.get("inputLogicData");
        assertEquals(dataSets.split(",").length, 1);
        checkDataSets(dataSets, input1);
    }

    public void testSimpleOr1() throws Exception {
        Configuration conf = getConf();
        //@formatter:off
        String inputLogic =
        "<or name=\"test\">"+
                  "<and>" +
                          "<data-in dataset=\"C\" />"+
                          "<data-in dataset=\"D\" />"+
                   "</and>" +
                   "<or>"+
                          "<data-in dataset=\"A\" />"+
                          "<data-in dataset=\"B\" />"+
                   "</or>"+
          "</or>";

        String jobId = _testCoordSubmit("coord-inputlogic.xml", conf, inputLogic);

        new CoordMaterializeTransitionXCommand(jobId, 3600).call();
        CoordinatorActionBean actionBean = CoordActionQueryExecutor.getInstance().get(
                CoordActionQuery.GET_COORD_ACTION, jobId + "@1");
        assertEquals(actionBean.getStatus(), CoordinatorAction.Status.WAITING);

        new CoordActionInputCheckXCommand(jobId + "@1", jobId).call();
        String input1=createTestCaseSubDir("input-data/b/2014/10/08/00/_SUCCESS".split("/"));
        startCoordAction(jobId);
        actionBean = CoordActionQueryExecutor.getInstance().get(CoordActionQuery.GET_COORD_ACTION, jobId + "@1");
        XConfiguration runConf = new XConfiguration(new StringReader(actionBean.getRunConf()));
        String dataSets = runConf.get("inputLogicData");
        assertEquals(dataSets.split(",").length, 1);
        checkDataSets(dataSets, input1);

    }

    public void testOrWithMin() throws Exception {
        Configuration conf = getConf();
        //@formatter:off
        String inputLogic =
        "<or name=\"test\">"+
                   "<data-in dataset=\"A\" min=\"3\"/>"+
                   "<data-in dataset=\"B\" min=\"3\"/>"+
        "</or>";
        //@formatter:on
        conf.set("initial_instance_a", "2014-10-07T00:00Z");
        conf.set("initial_instance_b", "2014-10-07T00:00Z");

        String jobId = _testCoordSubmit("coord-inputlogic-range.xml", conf, inputLogic, getInputEventForRange());

        String input1 = createTestCaseSubDir("input-data/a/2014/10/08/00/_SUCCESS".split("/"));
        String input2 = createTestCaseSubDir("input-data/a/2014/10/07/23/_SUCCESS".split("/"));
        String input3 = createTestCaseSubDir("input-data/b/2014/10/07/21/_SUCCESS".split("/"));
        String input4 = createTestCaseSubDir("input-data/b/2014/10/07/20/_SUCCESS".split("/"));
        String input5 = createTestCaseSubDir("input-data/b/2014/10/07/19/_SUCCESS".split("/"));

        startCoordAction(jobId);

        CoordinatorActionBean actionBean = CoordActionQueryExecutor.getInstance().get(
                CoordActionQuery.GET_COORD_ACTION, jobId + "@1");

        assertFalse(CoordinatorAction.Status.WAITING.equals(actionBean.getStatus()));
        XConfiguration runConf = new XConfiguration(new StringReader(actionBean.getRunConf()));
        String dataSets = runConf.get("inputLogicData");
        assertEquals(dataSets.split(",").length, 3);
        checkDataSets(dataSets, input3, input4, input5);
    }

    public void testAndWithMin() throws Exception {
        Configuration conf = getConf();
        //@formatter:off
        String inputLogic =
        "<and name=\"test\">"+
                   "<data-in dataset=\"A\" min=\"2\"/>"+
                   "<data-in dataset=\"B\" min=\"3\"/>"+
                   "<data-in dataset=\"C\" min=\"0\"/>"+

        "</and>";
        //@formatter:on
        conf.set("initial_instance_a", "2014-10-07T00:00Z");
        conf.set("initial_instance_b", "2014-10-07T00:00Z");

        String jobId = _testCoordSubmit("coord-inputlogic-range.xml", conf, inputLogic, getInputEventForRange());

        String input1 = createTestCaseSubDir("input-data/a/2014/10/08/00/_SUCCESS".split("/"));
        String input2 = createTestCaseSubDir("input-data/a/2014/10/07/23/_SUCCESS".split("/"));
        String input3 = createTestCaseSubDir("input-data/b/2014/10/07/21/_SUCCESS".split("/"));
        String input4 = createTestCaseSubDir("input-data/b/2014/10/07/20/_SUCCESS".split("/"));
        String input5 = createTestCaseSubDir("input-data/b/2014/10/07/19/_SUCCESS".split("/"));

        startCoordAction(jobId);

        CoordinatorActionBean actionBean = CoordActionQueryExecutor.getInstance().get(
                CoordActionQuery.GET_COORD_ACTION, jobId + "@1");

        assertFalse(CoordinatorAction.Status.WAITING.equals(actionBean.getStatus()));
        XConfiguration runConf = new XConfiguration(new StringReader(actionBean.getRunConf()));
        String dataSets = runConf.get("inputLogicData");
        assertEquals(dataSets.split(",").length, 5);
        checkDataSets(dataSets, input1, input2, input3, input4, input5, input5);
    }

    public void testMultipleInstance() throws Exception {
        Configuration conf = getConf();
        Date now = new Date();
        //@formatter:off
        String inputLogic =
        "<and name=\"test\">"+
                   "<data-in dataset=\"A\" min=\"2\"/>"+
                   "<data-in dataset=\"B\"/>"+

        "</and>";
        String event =
                "<data-in name=\"A\" dataset=\"a\">" +
                        "<instance>${coord:current(-5)}</instance>" +
                        "<instance>${coord:latest(-1)}</instance>" +
                        "<instance>${coord:futureRange(0,2,10)}</instance>" +
                 "</data-in>" +
                 "<data-in name=\"B\" dataset=\"b\">" +
                     "<instance>${coord:latest(0)}</instance>" +
                     "<instance>${coord:latestRange(-3,0)}</instance>" +
                 "</data-in>" ;

        //@formatter:on
        conf.set("start_time", DateUtils.formatDateOozieTZ(now));
        conf.set("end_time", DateUtils.formatDateOozieTZ(new Date(now.getTime() + 3 * 60 * 60 * 1000)));
        // 5 hour before
        conf.set("initial_instance_a", DateUtils.formatDateOozieTZ(new Date(now.getTime() - 5 * 60 * 60 * 1000)));
        // 5 hour before
        conf.set("initial_instance_b", DateUtils.formatDateOozieTZ(new Date(now.getTime() - 5 * 60 * 60 * 1000)));

        String jobId = _testCoordSubmit("coord-inputlogic-range.xml", conf, inputLogic, event);

        List<String> inputDir = createDirWithTime("input-data/a/", now, 3, 5, 0, -1, -2);
        inputDir.addAll(createDirWithTime("input-data/b/", now, 0, 1));

        startCoordActionForWaiting(jobId);

        CoordinatorActionBean actionBean = CoordActionQueryExecutor.getInstance().get(
                CoordActionQuery.GET_COORD_ACTION, jobId + "@1");

        assertTrue(CoordinatorAction.Status.WAITING.equals(actionBean.getStatus()));

        inputDir.addAll(createDirWithTime("input-data/b/", now, 2, 3));

        new CoordActionInputCheckXCommand(jobId + "@1", jobId).call();
        actionBean = CoordActionQueryExecutor.getInstance().get(CoordActionQuery.GET_COORD_ACTION, jobId + "@1");

        assertFalse(CoordinatorAction.Status.WAITING.equals(actionBean.getStatus()));

        XConfiguration runConf = new XConfiguration(new StringReader(actionBean.getRunConf()));
        String dataSets = runConf.get("inputLogicData");
        assertEquals(dataSets.split(",").length, 10);
        checkDataSets(dataSets, inputDir.toArray(new String[inputDir.size()]));
    }

    public void testAnd() throws Exception {
        Configuration conf = getConf();
        //@formatter:off
        String inputLogic =
        "<and name=\"test\">"+
                  "<data-in dataset=\"A\"/>"+
                  "<data-in dataset=\"B\"/>"+
         "</and>";
        //@formatter:on
        String jobId = _testCoordSubmit("coord-inputlogic.xml", conf, inputLogic);

        String input1 = createTestCaseSubDir("input-data/a/2014/10/08/00/_SUCCESS".split("/"));
        String input2 = createTestCaseSubDir("input-data/b/2014/10/08/00/_SUCCESS".split("/"));

        startCoordAction(jobId);

        CoordinatorActionBean actionBean = CoordActionQueryExecutor.getInstance().get(
                CoordActionQuery.GET_COORD_ACTION, jobId + "@1");

        assertFalse(CoordinatorAction.Status.WAITING.equals(actionBean.getStatus()));
        XConfiguration runConf = new XConfiguration(new StringReader(actionBean.getRunConf()));
        String dataSets = runConf.get("inputLogicData");
        assertEquals(dataSets.split(",").length, 2);
        checkDataSets(dataSets, input1, input2);

    }

    public void testCombine() throws Exception {

        Configuration conf = getConf();
        //@formatter:off
        String inputLogic =
        "<combine name=\"test\">"+
                   "<data-in dataset=\"A\" />"+
                   "<data-in dataset=\"B\" />"+
        "</combine>";
        //@formatter:on
        conf.set("initial_instance_a", "2014-10-07T00:00Z");
        conf.set("initial_instance_b", "2014-10-07T00:00Z");

        String jobId = _testCoordSubmit("coord-inputlogic-range.xml", conf, inputLogic, getInputEventForRange());

        String input1 = createTestCaseSubDir("input-data/a/2014/10/08/00/_SUCCESS".split("/"));
        String input2 = createTestCaseSubDir("input-data/a/2014/10/07/23/_SUCCESS".split("/"));
        String input3 = createTestCaseSubDir("input-data/a/2014/10/07/22/_SUCCESS".split("/"));
        String input4 = createTestCaseSubDir("input-data/b/2014/10/07/21/_SUCCESS".split("/"));
        String input5 = createTestCaseSubDir("input-data/b/2014/10/07/20/_SUCCESS".split("/"));
        String input6 = createTestCaseSubDir("input-data/b/2014/10/07/19/_SUCCESS".split("/"));

        startCoordAction(jobId);

        CoordinatorActionBean actionBean = CoordActionQueryExecutor.getInstance().get(
                CoordActionQuery.GET_COORD_ACTION, jobId + "@1");

        assertFalse(CoordinatorAction.Status.WAITING.equals(actionBean.getStatus()));
        XConfiguration runConf = new XConfiguration(new StringReader(actionBean.getRunConf()));
        String dataSets = runConf.get("inputLogicData");
        assertEquals(dataSets.split(",").length, 6);
        checkDataSets(dataSets, input1, input2, input3, input4, input5, input6);
    }

    public void testCombineNegative() throws Exception {
        Configuration conf = getConf();
        //@formatter:off
         String inputLogic =
         "<combine name=\"test\">"+
                    "<data-in dataset=\"A\" />"+
                    "<data-in dataset=\"B\" />"+
         "</combine>";
         //@formatter:on
        conf.set("initial_instance_a", "2014-10-07T00:00Z");
        conf.set("initial_instance_b", "2014-10-07T00:00Z");

        final String jobId = _testCoordSubmit("coord-inputlogic-range.xml", conf, inputLogic, getInputEventForRange());

        createTestCaseSubDir("input-data/a/2014/10/08/00/_SUCCESS".split("/"));
        createTestCaseSubDir("input-data/a/2014/10/07/23/_SUCCESS".split("/"));
        createTestCaseSubDir("input-data/b/2014/10/07/21/_SUCCESS".split("/"));
        createTestCaseSubDir("input-data/b/2014/10/07/20/_SUCCESS".split("/"));

        new CoordMaterializeTransitionXCommand(jobId, 3600).call();

        new CoordActionInputCheckXCommand(jobId + "@1", jobId).call();
        waitFor(5 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordinatorActionBean actionBean = CoordActionQueryExecutor.getInstance().get(
                        CoordActionQuery.GET_COORD_ACTION, jobId + "@1");
                return !actionBean.getStatus().equals(CoordinatorAction.Status.WAITING);
            }
        });

        CoordinatorAction actionBean = CoordActionQueryExecutor.getInstance().get(CoordActionQuery.GET_COORD_ACTION,
                jobId + "@1");
        assertEquals(actionBean.getStatus(), CoordinatorAction.Status.WAITING);

    }

    public void testSingeSetWithMin() throws Exception {
        Configuration conf = getConf();
        //@formatter:off
        String inputLogic =
        "<or name=\"test\">"+
                     "<data-in dataset=\"A\" min=\"3\" />"+
        "</or>";
        //@formatter:on

        conf.set("initial_instance_a", "2014-10-07T00:00Z");
        conf.set("initial_instance_b", "2014-10-07T00:00Z");

        String jobId = _testCoordSubmit("coord-inputlogic-range.xml", conf, inputLogic, getInputEventForRange());

        String input1 = createTestCaseSubDir("input-data/a/2014/10/08/00/_SUCCESS".split("/"));
        String input2 = createTestCaseSubDir("input-data/a/2014/10/07/23/_SUCCESS".split("/"));
        // dataset with gap
        String input3 = createTestCaseSubDir("input-data/a/2014/10/07/19/_SUCCESS".split("/"));

        startCoordAction(jobId);

        CoordinatorActionBean actionBean = CoordActionQueryExecutor.getInstance().get(
                CoordActionQuery.GET_COORD_ACTION, jobId + "@1");

        assertFalse(CoordinatorAction.Status.WAITING.equals(actionBean.getStatus()));
        XConfiguration runConf = new XConfiguration(new StringReader(actionBean.getRunConf()));
        String dataSets = runConf.get("inputLogicData");
        assertEquals(dataSets.split(",").length, 3);
        checkDataSets(dataSets, input1, input2, input3);
    }

    public void testCombineWithMin() throws Exception {
        Configuration conf = getConf();
        String inputLogic =
        //@formatter:off
        "<combine name=\"test\" min=\"4\">"+
                   "<data-in dataset=\"A\" />"+
                   "<data-in dataset=\"B\" />"+
        "</combine>";
        //@formatter:on
        conf.set("initial_instance_a", "2014-10-07T00:00Z");
        conf.set("initial_instance_b", "2014-10-07T00:00Z");

        final String jobId = _testCoordSubmit("coord-inputlogic-range.xml", conf, inputLogic, getInputEventForRange());
        new CoordMaterializeTransitionXCommand(jobId, 3600).call();

        String input1 = createTestCaseSubDir("input-data/a/2014/10/08/00/_SUCCESS".split("/"));
        String input2 = createTestCaseSubDir("input-data/a/2014/10/07/23/_SUCCESS".split("/"));
        String input3 = createTestCaseSubDir("input-data/a/2014/10/07/22/_SUCCESS".split("/"));
        String input4 = createTestCaseSubDir("input-data/b/2014/10/07/21/_SUCCESS".split("/"));
        String input5 = createTestCaseSubDir("input-data/b/2014/10/07/20/_SUCCESS".split("/"));

        startCoordAction(jobId);

        CoordinatorActionBean actionBean = CoordActionQueryExecutor.getInstance().get(
                CoordActionQuery.GET_COORD_ACTION, jobId + "@1");

        assertFalse(CoordinatorAction.Status.WAITING.equals(actionBean.getStatus()));
        XConfiguration runConf = new XConfiguration(new StringReader(actionBean.getRunConf()));
        String dataSets = runConf.get("inputLogicData");
        assertEquals(dataSets.split(",").length, 5);
        checkDataSets(dataSets, input1, input2, input3, input4, input5);

    }

    public void testMinWait() throws Exception {
        Configuration conf = getConf();
        Date now = new Date();
        String inputLogic =
        //@formatter:off
        "<combine name=\"test\" min= \"4\" wait=\"1\">"+
                   "<data-in dataset=\"A\" />"+
                   "<data-in dataset=\"B\" />"+
        "</combine>";
        //@formatter:on
        conf.set("start_time", DateUtils.formatDateOozieTZ(now));
        conf.set("end_time", DateUtils.formatDateOozieTZ(new Date(now.getTime() + 3 * 60 * 60 * 1000)));
        // 5 hour before
        conf.set("initial_instance_a", DateUtils.formatDateOozieTZ(new Date(now.getTime() - 5 * 60 * 60 * 1000)));
        // 5 hour before
        conf.set("initial_instance_b", DateUtils.formatDateOozieTZ(new Date(now.getTime() - 5 * 60 * 60 * 1000)));

        String jobId = _testCoordSubmit("coord-inputlogic-range.xml", conf, inputLogic, getInputEventForRange());

        new CoordMaterializeTransitionXCommand(jobId, 3600).call();

        List<String> inputDir = createDirWithTime("input-data/b/", now, 0, 1, 2, 3, 4);

        startCoordActionForWaiting(jobId);
        // wait for 1 min
        sleep(60 * 1000);
        new CoordActionInputCheckXCommand(jobId + "@1", jobId).call();

        CoordinatorActionBean actionBean = CoordActionQueryExecutor.getInstance().get(
                CoordActionQuery.GET_COORD_ACTION, jobId + "@1");

        assertFalse(CoordinatorAction.Status.WAITING.equals(actionBean.getStatus()));
        XConfiguration runConf = new XConfiguration(new StringReader(actionBean.getRunConf()));
        String dataSets = runConf.get("inputLogicData");
        assertEquals(dataSets.split(",").length, 5);
        checkDataSets(dataSets, inputDir.toArray(new String[inputDir.size()]));
    }

    public void testWait() throws Exception {
        Configuration conf = getConf();
        Date now = new Date();
        String inputLogic =
        //@formatter:off
        "<combine name=\"test\" wait=\"1\">"+
                   "<data-in dataset=\"A\" />"+
                   "<data-in dataset=\"B\" />"+
        "</combine>";
        //@formatter:on
        conf.set("start_time", DateUtils.formatDateOozieTZ(now));
        conf.set("end_time", DateUtils.formatDateOozieTZ(new Date(now.getTime() + 3 * 60 * 60 * 1000)));
        conf.set("initial_instance_a", DateUtils.formatDateOozieTZ(new Date(now.getTime() - 5 * 60 * 60 * 1000)));
        conf.set("initial_instance_b", DateUtils.formatDateOozieTZ(new Date(now.getTime() - 5 * 60 * 60 * 1000)));

        String jobId = _testCoordSubmit("coord-inputlogic-range.xml", conf, inputLogic, getInputEventForRange());

        new CoordMaterializeTransitionXCommand(jobId, 3600).call();

        List<String> inputDir = createDirWithTime("input-data/b/", now, 0, 1, 2, 3, 4);

        startCoordActionForWaiting(jobId);
        // wait for 1 min
        sleep(60 * 1000);

        inputDir.addAll(createDirWithTime("input-data/b/", now, 5));
        new CoordActionInputCheckXCommand(jobId + "@1", jobId).call();

        CoordinatorActionBean actionBean = CoordActionQueryExecutor.getInstance().get(
                CoordActionQuery.GET_COORD_ACTION, jobId + "@1");

        assertFalse(CoordinatorAction.Status.WAITING.equals(actionBean.getStatus()));
        XConfiguration runConf = new XConfiguration(new StringReader(actionBean.getRunConf()));
        String dataSets = runConf.get("inputLogicData");
        assertEquals(dataSets.split(",").length, 6);
        checkDataSets(dataSets, inputDir.toArray(new String[inputDir.size()]));
    }

    public void testWaitFail() throws Exception {
        Configuration conf = getConf();
        Date now = new Date();
        String inputLogic =
        //@formatter:off
                "<or name=\"test\" min=\"${min}\" wait=\"${wait}\">"+
                           "<data-in dataset=\"${dataA}\" />"+
                           "<data-in dataset=\"${dataB}\" />"+
                "</or>";
        //@formatter:on
        conf.set("start_time", DateUtils.formatDateOozieTZ(now));
        conf.set("min", "4");
        conf.set("wait", "180");
        conf.set("dataA", "A");
        conf.set("dataB", "B");
        conf.set("end_time", DateUtils.formatDateOozieTZ(new Date(now.getTime() + 3 * 60 * 60 * 1000)));
        conf.set("initial_instance_a", DateUtils.formatDateOozieTZ(new Date(now.getTime() - 5 * 60 * 60 * 1000)));
        conf.set("initial_instance_b", DateUtils.formatDateOozieTZ(new Date(now.getTime() - 5 * 60 * 60 * 1000)));

        String jobId = _testCoordSubmit("coord-inputlogic-range.xml", conf, inputLogic, getInputEventForRange());

        createDirWithTime("input-data/b/", now, 0, 1, 2, 3, 4);

        startCoordActionForWaiting(jobId);
        new CoordActionInputCheckXCommand(jobId + "@1", jobId).call();

        CoordinatorActionBean actionBean = CoordActionQueryExecutor.getInstance().get(
                CoordActionQuery.GET_COORD_ACTION, jobId + "@1");

        assertTrue(CoordinatorAction.Status.WAITING.equals(actionBean.getStatus()));
    }

    public void testLatest() throws Exception {

        Configuration conf = getConf();
        conf.set("initial_instance_a", "2014-10-07T00:00Z");
        conf.set("initial_instance_b", "2014-10-07T00:00Z");

        String inputLogic = "<data-in name=\"test\" dataset=\"A\"/>";
        String jobId = _testCoordSubmit("coord-inputlogic-latest.xml", conf, inputLogic);

        String input1 = createTestCaseSubDir("input-data/a/2014/10/08/00/_SUCCESS".split("/"));

        startCoordAction(jobId);

        CoordinatorActionBean actionBean = CoordActionQueryExecutor.getInstance().get(
                CoordActionQuery.GET_COORD_ACTION, jobId + "@1");

        assertFalse(CoordinatorAction.Status.WAITING.equals(actionBean.getStatus()));
        XConfiguration runConf = new XConfiguration(new StringReader(actionBean.getRunConf()));
        String dataSets = runConf.get("inputLogicData");
        assertEquals(dataSets.split(",").length, 1);
        checkDataSets(dataSets, input1);

    }

    public void testLatestRange() throws Exception {

        Configuration conf = getConf();
        Date now = new Date();
        conf.set("start_time", DateUtils.formatDateOozieTZ(now));
        conf.set("end_time", DateUtils.formatDateOozieTZ(new Date(now.getTime() + 3 * 60 * 60 * 1000)));
        conf.set("initial_instance_a", DateUtils.formatDateOozieTZ(new Date(now.getTime() - 5 * 60 * 60 * 1000)));
        conf.set("initial_instance_b", DateUtils.formatDateOozieTZ(new Date(now.getTime() - 5 * 60 * 60 * 1000)));

        String inputLogic =
        //@formatter:off
              "<data-in name=\"test\" dataset=\"A\" min =\"2\" />";
        //@formatter:on
        String jobId = _testCoordSubmit("coord-inputlogic-range-latest.xml", conf, inputLogic);

        createDirWithTime("input-data/a/", now, 0, 1);

        startCoordAction(jobId);

        CoordinatorActionBean actionBean = CoordActionQueryExecutor.getInstance().get(
                CoordActionQuery.GET_COORD_ACTION, jobId + "@1");

        assertFalse(CoordinatorAction.Status.WAITING.equals(actionBean.getStatus()));
        XConfiguration runConf = new XConfiguration(new StringReader(actionBean.getRunConf()));
        String dataSets = runConf.get("inputLogicData");
        assertEquals(dataSets.split(",").length, 2);

    }

     //TODO combine support for unresolved
     // public void testLatestWithCombine() throws Exception {
     // Configuration conf = getConf();
     // conf.set("input_check", "combine(\"A\", \"B\")");
     // conf.set("initial_instance_a", "2014-10-07T00:00Z");
     // conf.set("initial_instance_b", "2014-10-07T00:00Z");
     //
     // String jobId = _testCoordSubmit("coord-inputlogic-range-latest.xml", conf);
     //
     // new CoordMaterializeTransitionXCommand(jobId, 3600).call();
     // CoordinatorActionBean actionBean = CoordActionQueryExecutor.getInstance().get(
     // CoordActionQuery.GET_COORD_ACTION, jobId + "@1");
     // sleep(2000);
     //
     // new CoordActionInputCheckXCommand(jobId + "@1", jobId).call();
     // assertEquals(actionBean.getStatus(), CoordinatorAction.Status.WAITING);
     // createTestCaseSubDir("input-data/a/2014/10/08/00/_SUCCESS".split("/"));
     // createTestCaseSubDir("input-data/a/2014/10/07/23/_SUCCESS".split("/"));
     // createTestCaseSubDir("input-data/a/2014/10/07/22/_SUCCESS".split("/"));
     // createTestCaseSubDir("input-data/b/2014/10/07/21/_SUCCESS".split("/"));
     //
     // new CoordActionInputCheckXCommand(jobId + "@1", jobId).call();
     //
     // actionBean = CoordActionQueryExecutor.getInstance().get(CoordActionQuery.GET_COORD_ACTION, jobId + "@1");
     // assertEquals(actionBean.getStatus(), CoordinatorAction.Status.WAITING);
     //
     // createTestCaseSubDir("input-data/b/2014/10/07/20/_SUCCESS".split("/"));
     // new CoordActionInputCheckXCommand(jobId + "@1", jobId).call();
     //
     // actionBean = CoordActionQueryExecutor.getInstance().get(CoordActionQuery.GET_COORD_ACTION, jobId + "@1");
     // assertFalse(CoordinatorAction.Status.WAITING.equals(actionBean.getStatus()));
     //
     // }
    public void testCoordWithoutInputCheck() throws Exception {
        Configuration conf = new XConfiguration();
        String jobId = setupCoord(conf, "coord-multiple-input-instance3.xml");
        sleep(1000);
        new CoordMaterializeTransitionXCommand(jobId, 3600).call();
        new CoordActionInputCheckXCommand(jobId + "@1", jobId).call();

        CoordinatorAction actionBean = CoordActionQueryExecutor.getInstance().get(CoordActionQuery.GET_COORD_ACTION,
                jobId + "@1");

        assertEquals(actionBean.getMissingDependencies(), "!!${coord:latest(0)}#${coord:latest(-1)}");

    }

    private String _testCoordSubmit(String coordinatorXml, Configuration conf, String inputLogic) throws Exception {
        return _testCoordSubmit(coordinatorXml, conf, inputLogic, "", false);
    }

    private String _testCoordSubmit(String coordinatorXml, Configuration conf, String inputLogic, String inputEvent)
            throws Exception {
        return _testCoordSubmit(coordinatorXml, conf, inputLogic, inputEvent, false);
    }

    private String _testCoordSubmit(String coordinatorXml, Configuration conf, String inputLogic, String inputEvent,
            boolean dryRun) throws Exception {
        String appPath = "file://" + getTestCaseDir() + File.separator + "coordinator.xml";

        String content = IOUtils.getResourceAsString(coordinatorXml, -1);
        content = content.replace("=input-logic=", inputLogic);
        content = content.replace("=input-events=", inputEvent);

        Writer writer = new FileWriter(new URI(appPath).getPath());
        IOUtils.copyCharStream(new StringReader(content), writer);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("nameNode", "hdfs://localhost:9000");
        conf.set("queueName", "default");
        conf.set("jobTracker", "localhost:9001");
        conf.set("examplesRoot", "examples");
        if (conf.get("A_done_flag") == null) {
            conf.set("A_done_flag", "_SUCCESS");
        }

        return new CoordSubmitXCommand(dryRun, conf).call();
    }

    private Configuration getConf() throws Exception {
        Configuration conf = new XConfiguration();
        conf.set("data_set_a", "file://" + getTestCaseDir() + "/input-data/a");
        conf.set("data_set_b", "file://" + getTestCaseDir() + "/input-data/b");
        conf.set("data_set_c", "file://" + getTestCaseDir() + "/input-data/c");
        conf.set("data_set_d", "file://" + getTestCaseDir() + "/input-data/d");
        conf.set("data_set_e", "file://" + getTestCaseDir() + "/input-data/e");
        conf.set("data_set_f", "file://" + getTestCaseDir() + "/input-data/f");
        conf.set("partitionName", "test");

        conf.set("start_time", "2014-10-08T00:00Z");
        conf.set("end_time", "2015-10-08T00:00Z");
        conf.set("initial_instance_a", "2014-10-08T00:00Z");
        conf.set("initial_instance_b", "2014-10-08T00:00Z");
        conf.set("wfPath", getWFPath());
        return conf;

    }

    public String getWFPath() throws Exception {
        String workflowUri = getTestCaseFileUri("workflow.xml");
        String appXml = "<workflow-app xmlns='uri:oozie:workflow:0.1' name='map-reduce-wf'> " + "<start to='end' /> "
                + "<end name='end' /> " + "</workflow-app>";

        writeToFile(appXml, workflowUri);
        return workflowUri;

    }

    private void writeToFile(String appXml, String appPath) throws IOException {
        File wf = new File(URI.create(appPath));
        PrintWriter out = null;
        try {
            out = new PrintWriter(new FileWriter(wf));
            out.println(appXml);
        }
        catch (IOException iex) {
            throw iex;
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
            inputDataSets.add(dataSet.substring(dataSet.indexOf(getTestCaseDir())));
        }

        for (String value : values) {
            assertTrue(inputDataSets.contains(value.replace("/_SUCCESS","")));
        }
    }

    public void checkDataSetsForFalse(String dataSets, String... values) {

        Set<String> inputDataSets = new HashSet<String>();
        for (String dataSet : dataSets.split(",")) {
            inputDataSets.add(dataSet.substring(dataSet.indexOf(getTestCaseDir())));
        }

        for (String value : values) {
            assertFalse(inputDataSets.contains(value));
        }

    }

    private void startCoordAction(final String jobId) throws CommandException, JPAExecutorException {
        startCoordAction(jobId, CoordinatorAction.Status.WAITING);

    }

    private void startCoordAction(final String jobId, final CoordinatorAction.Status coordActionStatus)
            throws CommandException, JPAExecutorException {
        new CoordMaterializeTransitionXCommand(jobId, 3600).call();

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
        assertFalse(actionBean.getStatus().equals(coordActionStatus));

        CoordinatorJob coordJob = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB, jobId);

        new CoordActionStartXCommand(actionBean.getId(), coordJob.getUser(), coordJob.getAppName(),
                actionBean.getJobId()).call();
    }

    private void startCoordActionForWaiting(final String jobId) throws CommandException, JPAExecutorException,
            JDOMException {
        new CoordMaterializeTransitionXCommand(jobId, 3600).call();

        new CoordActionInputCheckXCommand(jobId + "@1", jobId).call();
        waitFor(5 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordinatorActionBean actionBean = CoordActionQueryExecutor.getInstance().get(
                        CoordActionQuery.GET_COORD_ACTION, jobId + "@1");
                return !actionBean.getStatus().equals(CoordinatorAction.Status.WAITING);
            }
        });

        CoordinatorActionBean actionBean = CoordActionQueryExecutor.getInstance().get(
                CoordActionQuery.GET_COORD_ACTION, jobId + "@1");
        assertTrue("should be waiting", actionBean.getStatus().equals(CoordinatorAction.Status.WAITING));
    }

    private String setupCoord(Configuration conf, String coordFile) throws CommandException, IOException {
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");
        Reader reader = IOUtils.getResourceAsReader(coordFile, -1);
        Writer writer = new FileWriter(appPathFile);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);
        IOUtils.copyCharStream(reader, writer);
        sc = new CoordSubmitXCommand(conf);
        return sc.call();

    }

    private String getInputEventForRange() {
        //@formatter:off
        return
                "<data-in name=\"A\" dataset=\"a\">" +
                    "<start-instance>${coord:current(-5)}</start-instance>" +
                    "<end-instance>${coord:current(0)}</end-instance>" +
                "</data-in>" +
                "<data-in name=\"B\" dataset=\"b\">" +
                    "<start-instance>${coord:current(-5)}</start-instance>" +
                    "<end-instance>${coord:current(0)}</end-instance>" +
                "</data-in>" +
                "<data-in name=\"C\" dataset=\"c\">" +
                    "<start-instance>${coord:current(-5)}</start-instance> " +
                    "<end-instance>${coord:current(0)}</end-instance>" +
                "</data-in>" +
                "<data-in name=\"D\" dataset=\"d\">" +
                    "<start-instance>${coord:current(-5)}</start-instance>" +
                    "<end-instance>${coord:current(0)}</end-instance>" +
                "</data-in>" +
                "<data-in name=\"E\" dataset=\"e\">" +
                    "<start-instance>${coord:current(-5)}</start-instance>" +
                    "<end-instance>${coord:current(0)}</end-instance>" +
                "</data-in>" +
                "<data-in name=\"F\" dataset=\"f\">" +
                    "<start-instance>${coord:current(-5)}</start-instance> " +
                    "<end-instance>${coord:current(0)}</end-instance>" +
                "</data-in>";
        //@formatter:on
    }

    public List<String> createDirWithTime(String dirPrefix, Date date, int... hours) {

        SimpleDateFormat sd = new SimpleDateFormat("yyyy/MM/dd/HH");

        TimeZone tzUTC = TimeZone.getTimeZone("UTC");
        sd.setTimeZone(tzUTC);
        List<String> createdDirPath = new ArrayList<String>();

        for (int hour : hours) {
            createdDirPath.add(createTestCaseSubDir((dirPrefix
                    + sd.format(new Date(date.getTime() - hour * 60 * 60 * 1000)) + "/_SUCCESS").split("/")));
        }
        return createdDirPath;
    }

}
