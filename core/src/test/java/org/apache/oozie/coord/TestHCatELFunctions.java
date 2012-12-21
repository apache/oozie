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

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.service.ELService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ELEvaluator;
import org.junit.Test;

public class TestHCatELFunctions extends XTestCase {
    ELEvaluator eval = null;
    SyncCoordAction appInst = null;
    SyncCoordDataset ds = null;
    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    /**
     * Test HCat database EL function (phase 1) which echo back the EL function
     * itself
     *
     * @throws Exception
     */
    @Test
    public void testDatabasePh1() throws Exception {
        init("coord-job-submit-data");
        String expr = "${coord:database('ABC', 'input')}";
        // +ve test
        eval.setVariable("oozie.dataname.ABC", "data-in");
        assertEquals("${coord:database('ABC', 'input')}", CoordELFunctions.evalAndWrap(eval, expr));
        // -ve test
        expr = "${coord:database('ABCD', 'input')}";
        try {
            assertEquals("${coord:database('ABCD', 'input')}", CoordELFunctions.evalAndWrap(eval, expr));
            fail("should throw exception beacuse Data in is not defiend");
        }
        catch (Exception ex) {
        }
        expr = "${coord:database('ABC', 'output')}";
        eval.setVariable("oozie.dataname.ABC", "data-out");
        assertEquals("${coord:database('ABC', 'output')}", CoordELFunctions.evalAndWrap(eval, expr));
    }

    /**
     * Test HCat table EL function (phase 1) which echo back the EL function
     * itself
     *
     * @throws Exception
     */
    @Test
    public void testTablePh1() throws Exception {
        init("coord-job-submit-data");
        String expr = "${coord:table('ABC', 'input')}";
        // +ve test
        eval.setVariable("oozie.dataname.ABC", "data-in");
        assertEquals("${coord:table('ABC', 'input')}", CoordELFunctions.evalAndWrap(eval, expr));
        // -ve test
        expr = "${coord:table('ABCD', 'input')}";
        try {
            assertEquals("${coord:table('ABCD', 'input')}", CoordELFunctions.evalAndWrap(eval, expr));
            fail("should throw exception beacuse Data in is not defiend");
        }
        catch (Exception ex) {
        }
        expr = "${coord:table('ABC', 'output')}";
        // +ve test
        eval.setVariable("oozie.dataname.ABC", "data-out");
        assertEquals("${coord:table('ABC', 'output')}", CoordELFunctions.evalAndWrap(eval, expr));
    }

    /**
     * Test HCat dataInPartitionPigFilter EL function (phase 1) which echo back the
     * EL function itself
     *
     * @throws Exception
     */
    @Test
    public void testdataInPartitionPigFilterPh1() throws Exception {
        init("coord-job-submit-data");
        String expr = "${coord:dataInPartitionPigFilter('ABC')}";
        // +ve test
        eval.setVariable("oozie.dataname.ABC", "data-in");
        assertEquals("${coord:dataInPartitionPigFilter('ABC')}", CoordELFunctions.evalAndWrap(eval, expr));
        // -ve test
        expr = "${coord:dataInPartitionPigFilter('ABCD')}";
        try {
            assertEquals("${coord:dataInPartitionPigFilter('ABCD')}", CoordELFunctions.evalAndWrap(eval, expr));
            fail("should throw exception beacuse Data in is not defiend");
        }
        catch (Exception ex) {
        }
    }

    /**
     * Test HCat dataInPartitionMin EL function (phase 1) which echo back the
     * EL function itself
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
     * Test HCat dataInPartitionMax EL function (phase 1) which echo back the
     * EL function itself
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
     * Test HCat dataOutPartition EL function (phase 1) which echo back the
     * EL function itself
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
            fail("should throw exception beacuse Data in is not defiend");
        }
        catch (Exception ex) {
        }
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
     * Test database EL function (phase 3) which returns the DB name from URI
     *
     * @throws Exception
     */
    @Test
    public void testDatabase() throws Exception {
        init("coord-action-start", "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12&region=us");
        eval.setVariable(".datain.ABC", "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12&region=us");
        eval.setVariable(".datain.ABC.unresolved", Boolean.FALSE);
        String expr = "${coord:database('ABC', 'input')}";
        assertEquals("mydb", CoordELFunctions.evalAndWrap(eval, expr));

        eval.setVariable(".dataout.ABC", "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12&region=us");
        eval.setVariable(".dataout.ABC.unresolved", Boolean.FALSE);
        expr = "${coord:database('ABC', 'output')}";
        assertEquals("mydb", CoordELFunctions.evalAndWrap(eval, expr));
    }

    /**
     * Test HCat table EL function (phase 3) which returns the HCat table from
     * URI
     *
     * @throws Exception
     */
    @Test
    public void testTable() throws Exception {
        init("coord-action-start", "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12&region=us");
        eval.setVariable(".datain.ABC", "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12&region=us");
        eval.setVariable(".datain.ABC.unresolved", Boolean.FALSE);
        String expr = "${coord:table('ABC', 'input')}";
        assertEquals("clicks", CoordELFunctions.evalAndWrap(eval, expr));

        eval.setVariable(".dataout.ABC", "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12&region=us");
        eval.setVariable(".dataout.ABC.unresolved", Boolean.FALSE);
        expr = "${coord:table('ABC', 'output')}";
        assertEquals("clicks", CoordELFunctions.evalAndWrap(eval, expr));
    }

    /**
     * Test dataInPartitionPigFilter EL function (phase 3) which returns the
     * partition filter to be used as input to load data from
     *
     * @throws Exception
     */
    @Test
    public void testdataInPartitionPigFilter() throws Exception {
        init("coord-action-start");
        eval.setVariable(".datain.ABC", "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12&region=us");
        eval.setVariable(".datain.ABC.unresolved", Boolean.FALSE);
        String expr = "${coord:dataInPartitionPigFilter('ABC')}";
        String res = CoordELFunctions.evalAndWrap(eval, expr);
        assertTrue(res.equals("(datastamp=='12' AND region=='us')") || res.equals("(region=='us' AND datastamp=='12')"));

        eval.setVariable(".datain.ABC", "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12&region=us,"
                + "hcat://hcat.server.com:5080/mydb/clicks/datastamp=13&region=us");
        eval.setVariable(".datain.ABC.unresolved", Boolean.FALSE);
        expr = "${coord:dataInPartitionPigFilter('ABC')}";
        res = CoordELFunctions.evalAndWrap(eval, expr);
        assertTrue(res.equals("(datastamp=='12' AND region=='us') OR (datastamp=='13' AND region=='us')")
                || res.equals("(datastamp=='12' AND region=='us') OR (region=='us' AND datastamp=='13')")
                || res.equals("(region=='us' AND datastamp=='12') OR (datastamp=='13' AND region=='us')")
                || res.equals("(region=='us' AND datastamp=='12') OR (region=='us' AND datastamp=='13')"));
    }

    /**
     * Test dataOutPartition EL function (phase 3) which returns the partition
     * to be used as output to store data into
     *
     * @throws Exception
     */
    @Test
    public void testDataOutPartitions() throws Exception {
        init("coord-action-start");
        String expr = "${coord:dataOutPartitions('ABC')}";
        eval.setVariable(".dataout.ABC", "hcat://hcat.server.com:5080/mydb/clicks/datastamp=20120230&region=us");
        eval.setVariable(".dataout.ABC.unresolved", Boolean.FALSE);
        String res = CoordELFunctions.evalAndWrap(eval, expr);
        assertTrue(res.equals("'datastamp=20120230,region=us'")
                || res.equals("'region=us,datastamp=20120230'"));
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
        eval.setVariable(".dataout.ABC", "hcat://hcat.server.com:5080/mydb/clicks/datastamp=20120230&region=US");
        eval.setVariable(".dataout.ABC.unresolved", Boolean.FALSE);
        String res = CoordELFunctions.evalAndWrap(eval, expr);
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
        eval.setVariable(".datain.ABC", "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12&region=us,"
                + "hcat://hcat.server.com:5080/mydb/clicks/datastamp=13&region=us,"
                + "hcat://hcat.server.com:5080/mydb/clicks/datastamp=10&region=us");
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
        eval.setVariable(".datain.ABC", "hcat://hcat.server.com:5080/mydb/clicks/datastamp=20&region=us,"
                + "hcat://hcat.server.com:5080/mydb/clicks/datastamp=12&region=us,"
                + "hcat://hcat.server.com:5080/mydb/clicks/datastamp=13&region=us,"
                + "hcat://hcat.server.com:5080/mydb/clicks/datastamp=10&region=us");
        eval.setVariable(".datain.ABC.unresolved", Boolean.FALSE);
        String expr = "${coord:dataInPartitionMax('ABC','datastamp')}";
        String res = CoordELFunctions.evalAndWrap(eval, expr);
        assertTrue(res.equals("20"));
    }

    private void init(String tag) throws Exception {
        init(tag, "hdfs://localhost:9000/user/" + getTestUser() + "/US/${YEAR}/${MONTH}/${DAY}");
    }

    private void init(String tag, String uriTemplate) throws Exception {
        eval = Services.get().get(ELService.class).createEvaluator(tag);
        eval.setVariable(OozieClient.USER_NAME, "test_user");
        eval.setVariable(OozieClient.GROUP_NAME, "test_group");
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