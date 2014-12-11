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
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.Namespace;

public class TestCoordUpdateXCommand extends XDataTestCase {
    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        LocalOozie.start();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
        LocalOozie.stop();
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

    // test conf change
    public void testConfChange() throws Exception {
        Configuration conf = new XConfiguration();
        String jobId = setupCoord(conf, "coord-multiple-input-instance3.xml");
        String addedProperty = "jobrerun";
        XConfiguration xConf = new XConfiguration();
        assertNull(xConf.get(addedProperty));
        conf.set(addedProperty, "true");
        CoordinatorJobBean job = getCoordJobs(jobId);
        xConf = new XConfiguration(new StringReader(job.getConf()));
        CoordUpdateXCommand update = new CoordUpdateXCommand(false, conf, jobId);
        String diff = update.call();
        job = getCoordJobs(jobId);
        xConf = new XConfiguration(new StringReader(job.getConf()));
        assertEquals(xConf.get(addedProperty), "true");
        assertTrue(diff.contains("+    <name>jobrerun</name>"));
        assertTrue(diff.contains("+    <value>true</value>"));
    }

    // test definition change
    public void testDefinitionChange() throws Exception {
        Configuration conf = new XConfiguration();
        File appPathFile1 = new File(getTestCaseDir(), "coordinator.xml");
        String jobId = setupCoord(conf, "coord-multiple-input-instance3.xml");
        CoordinatorJobBean job = getCoordJobs(jobId);
        Element processedJobXml = XmlUtils.parseXml(job.getJobXml());
        Namespace namespace = processedJobXml.getNamespace();
        String text = ((Element) processedJobXml.getChild("input-events", namespace).getChild("data-in", namespace)
                .getChildren("instance", namespace).get(0)).getText();
        assertEquals(text, "${coord:latest(0)}");
        Reader reader = IOUtils.getResourceAsReader("coord-multiple-input-instance4.xml", -1);
        Writer writer = new FileWriter(appPathFile1);
        IOUtils.copyCharStream(reader, writer);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile1.toURI().toString());
        job = getCoordJobs(jobId);
        CoordUpdateXCommand update = new CoordUpdateXCommand(false, conf, jobId);
        update.call();
        job = getCoordJobs(jobId);
        processedJobXml = XmlUtils.parseXml(job.getJobXml());
        namespace = processedJobXml.getNamespace();
        text = ((Element) processedJobXml.getChild("input-events", namespace).getChild("data-in", namespace)
                .getChildren("instance", namespace).get(0)).getText();
        assertEquals(text, "${coord:future(0, 1)}");
    }

    // test fail... error in coord definition
    public void testCoordDefinitionChangeError() throws Exception {
        Configuration conf = new XConfiguration();
        File appPathFile1 = new File(getTestCaseDir(), "coordinator.xml");
        String jobId = setupCoord(conf, "coord-multiple-input-instance3.xml");

        CoordinatorJobBean job = getCoordJobs(jobId);
        Element processedJobXml = XmlUtils.parseXml(job.getJobXml());
        Namespace namespace = processedJobXml.getNamespace();
        String text = ((Element) processedJobXml.getChild("input-events", namespace).getChild("data-in", namespace)
                .getChildren("instance", namespace).get(0)).getText();
        assertEquals(text, "${coord:latest(0)}");
        Reader reader = IOUtils.getResourceAsReader("coord-multiple-input-instance1.xml", -1);
        Writer writer = new FileWriter(appPathFile1);
        IOUtils.copyCharStream(reader, writer);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile1.toURI().toString());
        job = getCoordJobs(jobId);
        CoordUpdateXCommand update = new CoordUpdateXCommand(false, conf, jobId);
        try {
            update.call();
            fail(" should not come here");
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("E1021: Coord Action Input Check Error"));
        }
    }

    // test fail... trying to set unsupported field.
    public void testCoordDefUnsupportedChange() throws Exception {
        final XConfiguration conf = new XConfiguration();
        conf.set("start", "2009-02-01T01:00Z");
        conf.set("end", "2012-02-03T23:59Z");
        conf.set("unit", "UTC");
        conf.set("name", "NAME");
        conf.set("throttle", "12");
        conf.set("concurrency", "12");
        conf.set("execution", "FIFO");
        conf.set("timeout", "10");
        String jobId = setupCoord(conf, "coord-update-test.xml");

        Configuration newConf = new XConfiguration(conf.toProperties());
        newConf.set("start", "2010-02-01T01:00Z");

        try {
            new CoordUpdateXCommand(false, newConf, jobId).call();
            fail(" should not come here");
        }
        catch (XException e) {
            assertTrue(e.getMessage().contains("Start time can't be changed"));
        }

        newConf = new XConfiguration(conf.toProperties());
        newConf.set("end", "2015-02-03T23:59Z");
        try {
            new CoordUpdateXCommand(false, newConf, jobId).call();
            fail(" should not come here");
        }
        catch (XException e) {
            assertTrue(e.getMessage().contains("End time can't be changed"));
        }
        newConf = new XConfiguration(conf.toProperties());
        newConf.set("name", "test");
        try {
            new CoordUpdateXCommand(false, newConf, jobId).call();
            fail(" should not come here");
        }
        catch (XException e) {
            assertTrue(e.getMessage().contains("Coord name can't be changed"));
        }

        newConf = new XConfiguration(conf.toProperties());
        newConf.set("unit", "America/New_York");
        try {
            new CoordUpdateXCommand(false, newConf, jobId).call();
            fail(" should not come here");
        }
        catch (XException e) {
            assertTrue(e.getMessage().contains("TimeZone can't be changed"));
        }
    }

    // Test update control param.
    public void testUpdateControl() throws Exception {
        final XConfiguration conf = new XConfiguration();
        conf.set("start", "2009-02-01T01:00Z");
        conf.set("end", "2012-02-03T23:59Z");
        conf.set("unit", "UTC");
        conf.set("name", "NAME");
        conf.set("throttle", "12");
        conf.set("concurrency", "12");
        conf.set("execution", "FIFO");
        conf.set("timeout", "7");
        String jobId = setupCoord(conf, "coord-update-test.xml");

        CoordinatorJobBean job = getCoordJobs(jobId);
        assertEquals(12, job.getMatThrottling());
        assertEquals(12, job.getConcurrency());
        assertEquals(7, job.getTimeout());
        assertEquals("FIFO", job.getExecution());

        Configuration newConf = new XConfiguration(conf.toProperties());
        newConf.set("throttle", "8");
        new CoordUpdateXCommand(false, newConf, jobId).call();
        job = getCoordJobs(jobId);
        assertEquals(8, job.getMatThrottling());

        newConf = new XConfiguration(conf.toProperties());
        newConf.set("concurrency", "5");
        new CoordUpdateXCommand(false, newConf, jobId).call();
        job = getCoordJobs(jobId);
        assertEquals(5, job.getConcurrency());

        newConf = new XConfiguration(conf.toProperties());
        newConf.set("timeout", "10");
        new CoordUpdateXCommand(false, newConf, jobId).call();
        job = getCoordJobs(jobId);
        assertEquals(10, job.getTimeout());

        newConf = new XConfiguration(conf.toProperties());
        newConf.set("execution", "LIFO");
        new CoordUpdateXCommand(false, newConf, jobId).call();
        job = getCoordJobs(jobId);
        assertEquals("LIFO", job.getExecution());

    }

    // test coord re-run with refresh. will use the updated coord definition.
    public void testReRunRefresh() throws Exception {
        Configuration conf = new XConfiguration();
        File appPathFile1 = new File(getTestCaseDir(), "coordinator.xml");
        String jobId = setupCoord(conf, "coord-multiple-input-instance3.xml");
        sleep(1000);
        final int actionNum = 1;
        final String actionId = jobId + "@" + actionNum;
        final OozieClient coordClient = LocalOozie.getCoordClient();
        waitFor(120 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                CoordinatorAction bean = coordClient.getCoordActionInfo(actionId);
                return (bean.getStatus() == CoordinatorAction.Status.WAITING || bean.getStatus() == CoordinatorAction.Status.SUBMITTED);
            }
        });
        CoordinatorAction bean = coordClient.getCoordActionInfo(actionId);
        assertEquals(bean.getMissingDependencies(), "!!${coord:latest(0)}#${coord:latest(-1)}");
        CoordinatorJobBean job = getCoordJobs(jobId);
        Reader reader = IOUtils.getResourceAsReader("coord-multiple-input-instance4.xml", -1);
        Writer writer = new FileWriter(appPathFile1);
        IOUtils.copyCharStream(reader, writer);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile1.toURI().toString());
        new CoordUpdateXCommand(false, conf, jobId).call();
        job = getCoordJobs(jobId);
        Element processedJobXml = XmlUtils.parseXml(job.getJobXml());
        Namespace namespace = processedJobXml.getNamespace();
        String text = ((Element) processedJobXml.getChild("input-events", namespace).getChild("data-in", namespace)
                .getChildren("instance", namespace).get(0)).getText();
        assertEquals(text, "${coord:future(0, 1)}");
        new CoordActionsKillXCommand(jobId, RestConstants.JOB_COORD_SCOPE_ACTION, Integer.toString(actionNum)).call();
        coordClient.reRunCoord(jobId, RestConstants.JOB_COORD_SCOPE_ACTION, Integer.toString(actionNum), true,
                true);
        bean = coordClient.getCoordActionInfo(actionId);
        sleep(1000);
        assertEquals(bean.getMissingDependencies(), "!!${coord:future(0, 1)}");
    }

    private CoordinatorJobBean getCoordJobs(String jobId) {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            CoordinatorJobBean job = jpaService.execute(new CoordJobGetJPAExecutor(jobId));
            return job;
        }
        catch (JPAExecutorException e) {
            fail("Job ID " + jobId + " was not stored properly in db");
        }
        return null;
    }
}
