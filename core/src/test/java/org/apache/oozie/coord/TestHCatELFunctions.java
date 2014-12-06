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

package org.apache.oozie.coord;

import java.io.ByteArrayOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.DagELFunctions;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.dependency.FSURIHandler;
import org.apache.oozie.dependency.HCatURIHandler;
import org.apache.oozie.service.ELService;
import org.apache.oozie.service.HCatAccessorService;
import org.apache.oozie.service.LiteWorkflowStoreService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.URIHandlerService;
import org.apache.oozie.test.XHCatTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.workflow.lite.EndNodeDef;
import org.apache.oozie.workflow.lite.LiteWorkflowApp;
import org.apache.oozie.workflow.lite.LiteWorkflowInstance;
import org.apache.oozie.workflow.lite.StartNodeDef;
import org.junit.Test;

public class TestHCatELFunctions extends XHCatTestCase {
    ELEvaluator eval = null;
    SyncCoordAction appInst = null;
    SyncCoordDataset ds = null;
    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.getConf().set(URIHandlerService.URI_HANDLERS,
                FSURIHandler.class.getName() + "," + HCatURIHandler.class.getName());
        services.setService(HCatAccessorService.class);
        services.init();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    @Test
    public void testHCatExists() throws Exception {
        dropTable("db1", "table1", true);
        dropDatabase("db1", true);
        createDatabase("db1");
        createTable("db1", "table1", "year,month,dt,country");
        addPartition("db1", "table1", "year=2012;month=12;dt=02;country=us");

        Configuration protoConf = new Configuration();
        protoConf.set(OozieClient.USER_NAME, getTestUser());
        protoConf.set("hadoop.job.ugi", getTestUser() + "," + "group");
        Configuration conf = new XConfiguration();
        conf.set(OozieClient.APP_PATH, "appPath");
        conf.set(OozieClient.USER_NAME, getTestUser());

        conf.set("test.dir", getTestCaseDir());
        conf.set("partition1", getHCatURI("db1", "table1", "dt=02").toString());
        conf.set("partition2", getHCatURI("db1", "table1", "dt=05").toString());

        LiteWorkflowApp def = new LiteWorkflowApp("name", "<workflow-app/>", new StartNodeDef(
                LiteWorkflowStoreService.LiteControlNodeHandler.class, "end")).addNode(new EndNodeDef("end",
                LiteWorkflowStoreService.LiteControlNodeHandler.class));
        LiteWorkflowInstance job = new LiteWorkflowInstance(def, conf, "wfId");

        WorkflowJobBean wf = new WorkflowJobBean();
        wf.setId(job.getId());
        wf.setAppName("name");
        wf.setAppPath("appPath");
        wf.setUser(getTestUser());
        wf.setGroup("group");
        wf.setWorkflowInstance(job);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        protoConf.writeXml(baos);
        wf.setProtoActionConf(baos.toString());

        WorkflowActionBean action = new WorkflowActionBean();
        action.setId("actionId");
        action.setName("actionName");
        ELEvaluator eval = Services.get().get(ELService.class).createEvaluator("workflow");
        DagELFunctions.configureEvaluator(eval, wf, action);

        assertEquals(true, (boolean) eval.evaluate("${hcat:exists(wf:conf('partition1'))}", Boolean.class));
        assertEquals(false, (boolean) eval.evaluate("${hcat:exists(wf:conf('partition2'))}", Boolean.class));

        dropTable("db1", "table1", true);
        dropDatabase("db1", true);
    }

    /**
     * Test HCat databaseIn and databaseOut EL functions (phase 1) which echo
     * back the EL function itself
     *
     * @throws Exception
     */
    @Test
    public void testDatabasePh1() throws Exception {
        init("coord-job-submit-data");
        /*
         * databaseIn
         */
        String expr = "${coord:databaseIn('ABC')}";
        // +ve test
        eval.setVariable("oozie.dataname.ABC", "data-in");
        assertEquals("${coord:databaseIn('ABC')}", CoordELFunctions.evalAndWrap(eval, expr));
        // -ve test
        expr = "${coord:databaseIn('ABCD')}";
        try {
            assertEquals("${coord:databaseIn('ABCD')}", CoordELFunctions.evalAndWrap(eval, expr));
            fail("should throw exception because Data-in ABCD is not defiend");
        }
        catch (Exception ex) {
        }
        /*
         * databaseOut
         */
        expr = "${coord:databaseOut('ABC')}";
        // +ve test
        eval.setVariable("oozie.dataname.ABC", "data-out");
        assertEquals("${coord:databaseOut('ABC')}", CoordELFunctions.evalAndWrap(eval, expr));
        // -ve test
        expr = "${coord:databaseOut('ABCD')}";
        try {
            assertEquals("${coord:databaseOut('ABCD')}", CoordELFunctions.evalAndWrap(eval, expr));
            fail("should throw exception because Data-out ABCD is not defiend");
        }
        catch (Exception ex) {
        }
        init("coord-sla-submit");
        eval.setVariable("oozie.dataname.ABCD", "data-out");
        assertEquals("${coord:databaseOut('ABCD')}", CoordELFunctions.evalAndWrap(eval, expr));
    }

    /**
     * Test HCat tableIn and tableOut EL functions (phase 1) which echo back the
     * EL function itself
     *
     * @throws Exception
     */
    @Test
    public void testTablePh1() throws Exception {
        init("coord-job-submit-data");
        /*
         * tableIn
         */
        String expr = "${coord:tableIn('ABC')}";
        // +ve test
        eval.setVariable("oozie.dataname.ABC", "data-in");
        assertEquals("${coord:tableIn('ABC')}", CoordELFunctions.evalAndWrap(eval, expr));
        // -ve test
        expr = "${coord:tableIn('ABCD')}";
        try {
            assertEquals("${coord:tableIn('ABCD')}", CoordELFunctions.evalAndWrap(eval, expr));
            fail("should throw exception because Data-in ABCD is not defiend");
        }
        catch (Exception ex) {
        }
        /*
         * tableOut
         */
        expr = "${coord:tableOut('ABC')}";
        // +ve test
        eval.setVariable("oozie.dataname.ABC", "data-out");
        assertEquals("${coord:tableOut('ABC')}", CoordELFunctions.evalAndWrap(eval, expr));
        // -ve test
        expr = "${coord:tableOut('ABCD')}";
        try {
            assertEquals("${coord:tableOut('ABCD')}", CoordELFunctions.evalAndWrap(eval, expr));
            fail("should throw exception because Data-out ABCD is not defiend");
        }
        catch (Exception ex) {
        }

        init("coord-sla-submit");
        expr = "${coord:tableOut('ABC')}";
        eval.setVariable("oozie.dataname.ABC", "data-out");
        assertEquals("${coord:tableOut('ABC')}", CoordELFunctions.evalAndWrap(eval, expr));
    }

    /**
     * Test HCat dataInPartitionPigFilter EL function (phase 1) which echo back
     * the EL function itself
     *
     * @throws Exception
     */
    @Test
    public void testdataInPartitionFilterPh1() throws Exception {
        init("coord-job-submit-data");
        String expr = "${coord:dataInPartitionFilter('ABC', 'pig')}";
        // +ve test
        eval.setVariable("oozie.dataname.ABC", "data-in");
        assertEquals("${coord:dataInPartitionFilter('ABC', 'pig')}", CoordELFunctions.evalAndWrap(eval, expr));
        // -ve test
        expr = "${coord:dataInPartitionFilter('ABCD')}";
        eval.setVariable("oozie.dataname.ABCD", "data-in");
        try {
            assertEquals("${coord:dataInPartitionFilter('ABCD')}", CoordELFunctions.evalAndWrap(eval, expr));
            fail("should throw exception because dataInPartitionFilter() requires 2 parameters");
        }
        catch (Exception ex) {
        }
    }

    /**
     * Test HCat dataInPartitionMin EL function (phase 1) which echo back the EL
     * function itself
     *
     * @throws Exception
     */
    @Test
    public void testDataInPartitionMinPh1() throws Exception {
        init("coord-job-submit-data");
        String expr = "${coord:dataInPartitionMin('ABC', 'mypartition')}";
        // +ve test
        eval.setVariable("oozie.dataname.ABC", "data-in");
        assertEquals("${coord:dataInPartitionMin('ABC', 'mypartition')}", CoordELFunctions.evalAndWrap(eval, expr));
        // -ve test
        expr = "${coord:dataInPartitionMin('ABCD')}";
        eval.setVariable("oozie.dataname.ABCD", "data-in");
        try {
            assertEquals("${coord:dataInPartitionMin('ABCD')}", CoordELFunctions.evalAndWrap(eval, expr));
            fail("should throw exception because EL function requires 2 parameters");
        }
        catch (Exception ex) {
        }
    }

    /**
     * Test HCat dataInPartitionMax EL function (phase 1) which echo back the EL
     * function itself
     *
     * @throws Exception
     */
    @Test
    public void testDataInPartitionMaxPh1() throws Exception {
        init("coord-job-submit-data");
        String expr = "${coord:dataInPartitionMax('ABC', 'mypartition')}";
        // +ve test
        eval.setVariable("oozie.dataname.ABC", "data-in");
        assertEquals("${coord:dataInPartitionMax('ABC', 'mypartition')}", CoordELFunctions.evalAndWrap(eval, expr));
        // -ve test
        expr = "${coord:dataInPartitionMax('ABCD')}";
        eval.setVariable("oozie.dataname.ABCD", "data-in");
        try {
            assertEquals("${coord:dataInPartitionMax('ABCD')}", CoordELFunctions.evalAndWrap(eval, expr));
            fail("should throw exception because EL function requires 2 parameters");
        }
        catch (Exception ex) {
        }
    }

    /**
     * Test HCat dataInPartition EL function (phase 1) which echo back the EL
     * function itself
     *
     * @throws Exception
     */
    @Test
    public void testDataInPartitionsPh1() throws Exception {
        init("coord-job-submit-data");
        String expr = "${coord:dataInPartitions('ABC', 'hive-export')}";
        // +ve test
        eval.setVariable("oozie.dataname.ABC", "data-in");
        assertEquals("${coord:dataInPartitions('ABC', 'hive-export')}", CoordELFunctions.evalAndWrap(eval, expr));
        // -ve test
        expr = "${coord:dataInPartitions('ABCD', 'hive-export')}";
        try {
            CoordELFunctions.evalAndWrap(eval, expr);
            fail("should throw exception because Data-in is not defined");
        }
        catch (Exception ex) {
        }
        // -ve test
        expr = "${coord:dataInPartitions('ABCD')}";
        eval.setVariable("oozie.dataname.ABCD", "data-in");
        try {
            CoordELFunctions.evalAndWrap(eval, expr);
            fail("should throw exception because EL function requires 2 parameters");
        }
        catch (Exception ex) {
        }
    }

    /**
     * Test HCat dataOutPartition EL function (phase 1) which echo back the EL
     * function itself
     *
     * @throws Exception
     */
    @Test
    public void testDataOutPartitionsPh1() throws Exception {
        init("coord-job-submit-data");
        String expr = "${coord:dataOutPartitions('ABC')}";
        // +ve test
        eval.setVariable("oozie.dataname.ABC", "data-out");
        assertEquals("${coord:dataOutPartitions('ABC')}", CoordELFunctions.evalAndWrap(eval, expr));
        // -ve test
        expr = "${coord:dataOutPartitions('ABCD')}";
        try {
            assertEquals("${coord:dataOutPartitions('ABCD')}", CoordELFunctions.evalAndWrap(eval, expr));
            fail("should throw exception because Data-in is not defiend");
        }
        catch (Exception ex) {
        }
        init("coord-sla-submit");
        eval.setVariable("oozie.dataname.ABCD", "data-out");
        assertEquals("${coord:dataOutPartitions('ABCD')}", CoordELFunctions.evalAndWrap(eval, expr));
    }

    /**
     * Test HCat dataOutPartitionValue EL function (phase 1) which echo back the
     * EL function itself
     *
     * @throws Exception
     */
    @Test
    public void testDataOutPartitionValuePh1() throws Exception {
        init("coord-job-submit-data");
        String expr = "${coord:dataOutPartitionValue('ABC', 'mypartition')}";
        // +ve test
        eval.setVariable("oozie.dataname.ABC", "data-out");
        assertEquals("${coord:dataOutPartitionValue('ABC', 'mypartition')}", CoordELFunctions.evalAndWrap(eval, expr));
        // -ve test
        expr = "${coord:dataOutPartitionValue('ABCD')}";
        eval.setVariable("oozie.dataname.ABCD", "data-out");
        try {
            assertEquals("${coord:dataOutPartitionValue('ABCD')}", CoordELFunctions.evalAndWrap(eval, expr));
            fail("should throw exception because EL function requires 2 parameters");
        }
        catch (Exception ex) {
        }
    }

    /**
     * Test databaseIn and databaseOut EL functions (phase 3) which returns the
     * DB name from URI
     *
     * @throws Exception
     */
    @Test
    public void testDatabase() throws Exception {
        init("coord-action-start", "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12;region=us");
        eval.setVariable(".datain.ABC", "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12;region=us");
        eval.setVariable(".datain.ABC.unresolved", Boolean.FALSE);
        String expr = "${coord:databaseIn('ABC')}";
        assertEquals("mydb", CoordELFunctions.evalAndWrap(eval, expr));

        eval.setVariable(".dataout.ABC", "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12;region=us");
        eval.setVariable(".dataout.ABC.unresolved", Boolean.FALSE);
        expr = "${coord:databaseOut('ABC')}";
        assertEquals("mydb", CoordELFunctions.evalAndWrap(eval, expr));
    }

    /**
     * Test HCat tableIn and tableOut EL functions (phase 3) which returns the HCat table from
     * URI
     *
     * @throws Exception
     */
    @Test
    public void testTable() throws Exception {
        init("coord-action-start", "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12;region=us");
        eval.setVariable(".datain.ABC", "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12;region=us");
        eval.setVariable(".datain.ABC.unresolved", Boolean.FALSE);
        String expr = "${coord:tableIn('ABC')}";
        assertEquals("clicks", CoordELFunctions.evalAndWrap(eval, expr));

        eval.setVariable(".dataout.ABC", "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12;region=us");
        eval.setVariable(".dataout.ABC.unresolved", Boolean.FALSE);
        expr = "${coord:tableOut('ABC')}";
        assertEquals("clicks", CoordELFunctions.evalAndWrap(eval, expr));

        init("coord-sla-create", "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12;region=us");
        eval.setVariable(".dataout.ABC", "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12;region=us");
        eval.setVariable(".dataout.ABC.unresolved", Boolean.FALSE);
        expr = "${coord:tableOut('ABC')}";
        assertEquals("clicks", CoordELFunctions.evalAndWrap(eval, expr));
    }

    /**
     * Test dataInPartitionPigFilter EL function (phase 3) which returns the
     * partition filter to be used as input to load data from
     *
     * @throws Exception
     */
    @Test
    public void testdataInPartitionFilter() throws Exception {
        init("coord-action-start");
        eval.setVariable(".datain.ABC", "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12;region=us");
        eval.setVariable(".datain.ABC.unresolved", Boolean.FALSE);
        /*
         * type=pig
         */
        String expr = "${coord:dataInPartitionFilter('ABC', 'pig')}";
        String res = CoordELFunctions.evalAndWrap(eval, expr);
        assertTrue(res.equals("(datastamp=='12' AND region=='us')") || res.equals("(region=='us' AND datastamp=='12')"));

        eval.setVariable(".datain.ABC", "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12;region=us,"
                + "hcat://hcat.server.com:5080/mydb/clicks/datastamp=13;region=us");
        eval.setVariable(".datain.ABC.unresolved", Boolean.FALSE);
        expr = "${coord:dataInPartitionFilter('ABC', 'pig')}";
        res = CoordELFunctions.evalAndWrap(eval, expr);
        assertTrue(res.equals("(datastamp=='12' AND region=='us') OR (datastamp=='13' AND region=='us')")
                || res.equals("(datastamp=='12' AND region=='us') OR (region=='us' AND datastamp=='13')")
                || res.equals("(region=='us' AND datastamp=='12') OR (datastamp=='13' AND region=='us')")
                || res.equals("(region=='us' AND datastamp=='12') OR (region=='us' AND datastamp=='13')"));

        /*
         * type=java
         */
        eval.setVariable(".datain.ABC", "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12;region=us");
        eval.setVariable(".datain.ABC.unresolved", Boolean.FALSE);
        expr = "${coord:dataInPartitionFilter('ABC', 'java')}";
        res = CoordELFunctions.evalAndWrap(eval, expr);
        assertTrue(res.equals("(datastamp='12' AND region='us')") || res.equals("(region='us' AND datastamp='12')"));
    }

    /**
     * Test dataOutPartitionsPig EL function (phase 3) which returns the partition
     * to be used as output to store data into
     *
     * @throws Exception
     */
    @Test
    public void testDataOutPartitions() throws Exception {
        init("coord-action-start");
        String expr = "${coord:dataOutPartitions('ABC')}";
        eval.setVariable(".dataout.ABC", "hcat://hcat.server.com:5080/mydb/clicks/datastamp=20120230;region=us");
        eval.setVariable(".dataout.ABC.unresolved", Boolean.FALSE);
        String res = CoordELFunctions.evalAndWrap(eval, expr);
        assertTrue(res.equals("'datastamp=20120230,region=us'"));

        init("coord-sla-create");
        eval.setVariable(".dataout.ABC", "hcat://hcat.server.com:5080/mydb/clicks/datastamp=20130230;region=euro");
        eval.setVariable(".dataout.ABC.unresolved", Boolean.FALSE);
        res = CoordELFunctions.evalAndWrap(eval, expr);
        assertTrue(res.equals("'datastamp=20130230,region=us'") || res.equals("'datastamp=20130230,region=euro'"));
    }

    /**
     * Test dataOutPartitionValue EL function (phase 3) which returns the value
     * of a particular partition to be used as output to store data into
     *
     * @throws Exception
     */
    @Test
    public void testDataOutPartitionValue() throws Exception {
        init("coord-action-start");
        String expr = "${coord:dataOutPartitionValue('ABC','datastamp')}";
        eval.setVariable(".dataout.ABC", "hcat://hcat.server.com:5080/mydb/clicks/datastamp=20120230;region=US");
        eval.setVariable(".dataout.ABC.unresolved", Boolean.FALSE);
        String res = CoordELFunctions.evalAndWrap(eval, expr);
        assertTrue(res.equals("20120230"));

        expr = "${coord:dataOutPartitionValue('ABC','region')}";
        res = CoordELFunctions.evalAndWrap(eval, expr);
        assertTrue(res.equals("US"));

        init("coord-sla-create");
        eval.setVariable(".dataout.ABC", "hcat://hcat.server.com:5080/mydb/clicks/datastamp=20120230;region=US");
        eval.setVariable(".dataout.ABC.unresolved", Boolean.FALSE);
        expr = "${coord:dataOutPartitionValue('ABC','datastamp')}";
        res = CoordELFunctions.evalAndWrap(eval, expr);
        assertTrue(res.equals("20120230"));
        expr = "${coord:dataOutPartitionValue('ABC','region')}";
        res = CoordELFunctions.evalAndWrap(eval, expr);
        assertTrue(res.equals("US"));
    }

    /**
     * Test dataInPartitionMin EL function (phase 3) which returns the minimum
     * value of a partition to be used as minimum in a range of input data to
     * load
     *
     * @throws Exception
     */
    @Test
    public void testDataInPartitionMin() throws Exception {
        init("coord-action-start");
        eval.setVariable(".datain.ABC", "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12;region=us,"
                + "hcat://hcat.server.com:5080/mydb/clicks/datastamp=13;region=us,"
                + "hcat://hcat.server.com:5080/mydb/clicks/datastamp=10;region=us");
        eval.setVariable(".datain.ABC.unresolved", Boolean.FALSE);
        String expr = "${coord:dataInPartitionMin('ABC','datastamp')}";
        String res = CoordELFunctions.evalAndWrap(eval, expr);
        assertTrue(res.equals("10"));
    }

    /**
     * Test dataInPartitionMax EL function (phase 3) which returns the maximum
     * value of a partition to be used as maximum in a range of input data to
     * load
     *
     * @throws Exception
     */
    @Test
    public void testDataInPartitionMax() throws Exception {
        init("coord-action-start");
        eval.setVariable(".datain.ABC", "hcat://hcat.server.com:5080/mydb/clicks/datastamp=20;region=us,"
                + "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12;region=us,"
                + "hcat://hcat.server.com:5080/mydb/clicks/datastamp=13;region=us,"
                + "hcat://hcat.server.com:5080/mydb/clicks/datastamp=10;region=us");
        eval.setVariable(".datain.ABC.unresolved", Boolean.FALSE);
        String expr = "${coord:dataInPartitionMax('ABC','datastamp')}";
        String res = CoordELFunctions.evalAndWrap(eval, expr);
        assertTrue(res.equals("20"));
    }

    /**
     * Test dataInPartitions EL function (phase 3) which returns the complete partition value string of a single partition
     * in case of hive-export type.
     *
     * @throws Exception
     */
    @Test
    public void testDataInPartitions() throws Exception {
        init("coord-action-start");
        String expr = "${coord:dataInPartitions('ABC', 'hive-export')}";
        eval.setVariable(".datain.ABC", "hcat://hcat.server.com:5080/mydb/clicks/datastamp=20120230;region=us");
        eval.setVariable(".datain.ABC.unresolved", Boolean.FALSE);
        String res = CoordELFunctions.evalAndWrap(eval, expr);
        assertTrue(res.equals("datastamp='20120230',region='us'") || res.equals("region='us',datastamp='20120230'"));
        // -ve test; execute EL function with any other type than hive-export
        try {
            expr = "${coord:dataInPartitions('ABC', 'invalid-type')}";
            eval.setVariable(".datain.ABC", "hcat://hcat.server.com:5080/mydb/clicks/datastamp=20120230;region=us");
            eval.setVariable(".datain.ABC.unresolved", Boolean.FALSE);
            res = CoordELFunctions.evalAndWrap(eval, expr);
            fail("EL function should throw exception because of invalid type");
        } catch (Exception e) {
        }
    }

    private void init(String tag) throws Exception {
        init(tag, "hdfs://localhost:9000/user/" + getTestUser() + "/US/${YEAR}/${MONTH}/${DAY}");
    }

    private void init(String tag, String uriTemplate) throws Exception {
        eval = Services.get().get(ELService.class).createEvaluator(tag);
        eval.setVariable(OozieClient.USER_NAME, getTestUser());
        eval.setVariable(OozieClient.GROUP_NAME, getTestGroup());
        appInst = new SyncCoordAction();
        ds = new SyncCoordDataset();
        ds.setFrequency(1);
        ds.setInitInstance(DateUtils.parseDateOozieTZ("2009-09-01T23:59Z"));
        ds.setTimeUnit(TimeUnit.DAY);
        ds.setTimeZone(DateUtils.getTimeZone("America/Los_Angeles"));
        ds.setName("test");
        ds.setUriTemplate(uriTemplate);
        ds.setType("SYNC");
        ds.setDoneFlag("");
        appInst.setActualTime(DateUtils.parseDateOozieTZ("2009-09-10T23:59Z"));
        appInst.setNominalTime(DateUtils.parseDateOozieTZ("2009-09-09T23:59Z"));
        appInst.setTimeZone(DateUtils.getTimeZone("America/Los_Angeles"));
        appInst.setActionId("00000-oozie-C@1");
        appInst.setName("mycoordinator-app");
        CoordELFunctions.configureEvaluator(eval, ds, appInst);
    }

}
