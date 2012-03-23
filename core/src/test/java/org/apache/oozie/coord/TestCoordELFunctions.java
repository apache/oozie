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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.service.ELService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ELEvaluator;

public class TestCoordELFunctions extends XTestCase {
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

    /*
     * public void testSetup() throws Exception { services = new Services();
     * services.init(); }
     */

    public void testURIVars() throws Exception {
        init("coord-job-submit-freq");
        String expr = "${YEAR}";
        try {
            assertEquals(expr, CoordELFunctions.evalAndWrap(eval, expr));
            fail("should throw exception beacuse coord-job-submit-freq doesn't resolve YEAR/MONTH/DAY");
        }
        catch (Exception ex) {
        }
        init("coord-job-submit-nofuncs");
        assertEquals(expr, CoordELFunctions.evalAndWrap(eval, expr));
        expr = "${MONTH}";
        assertEquals(expr, CoordELFunctions.evalAndWrap(eval, expr));
        expr = "${DAY}";
        assertEquals(expr, CoordELFunctions.evalAndWrap(eval, expr));
        expr = "${HOUR}";
        assertEquals(expr, CoordELFunctions.evalAndWrap(eval, expr));
        expr = "${MINUTE}";
        assertEquals(expr, CoordELFunctions.evalAndWrap(eval, expr));
    }

    public void testDay() throws Exception {
        init("coord-job-submit-freq");
        String expr = "${coord:days(1)}";
        assertEquals("1", CoordELFunctions.evalAndWrap(eval, expr));
        assertEquals(TimeUnit.DAY, (TimeUnit) eval.getVariable("timeunit"));

        expr = "${coord:days(256)}";
        assertEquals("256", CoordELFunctions.evalAndWrap(eval, expr));
        assertEquals(TimeUnit.DAY, (TimeUnit) eval.getVariable("timeunit"));
    }

    public void testMonth() throws Exception {
        init("coord-job-submit-freq");
        String expr = "${coord:months(1)}";
        assertEquals("1", CoordELFunctions.evalAndWrap(eval, expr));
        assertEquals(TimeUnit.MONTH, (TimeUnit) eval.getVariable("timeunit"));

        expr = "${coord:months(1) + 7}";
        assertEquals("8", CoordELFunctions.evalAndWrap(eval, expr));
        assertEquals(TimeUnit.MONTH, (TimeUnit) eval.getVariable("timeunit"));

        expr = "${coord:months(256)}";
        assertEquals("256", CoordELFunctions.evalAndWrap(eval, expr));
        assertEquals(TimeUnit.MONTH, (TimeUnit) eval.getVariable("timeunit"));

        expr = "${coord:months(coord:months(7))}";
        assertEquals("7", CoordELFunctions.evalAndWrap(eval, expr));
        assertEquals(TimeUnit.MONTH, (TimeUnit) eval.getVariable("timeunit"));
    }

    public void testHours() throws Exception {
        init("coord-job-submit-freq");
        String expr = "${coord:hours(1)}";
        assertEquals("60", CoordELFunctions.evalAndWrap(eval, expr));
        assertEquals(TimeUnit.MINUTE, (TimeUnit) eval.getVariable("timeunit"));

        expr = "${coord:hours(coord:hours(1))}";
        assertEquals("3600", CoordELFunctions.evalAndWrap(eval, expr));
        assertEquals(TimeUnit.MINUTE, (TimeUnit) eval.getVariable("timeunit"));
    }

    public void testEndOfDays() throws Exception {
        init("coord-job-submit-freq");
        String expr = "${coord:endOfDays(1)}";
        expr = "${coord:endOfDays(3)}";
        assertEquals("3", CoordELFunctions.evalAndWrap(eval, expr));
        assertEquals(TimeUnit.DAY, (TimeUnit) eval.getVariable("timeunit"));
        assertEquals(TimeUnit.END_OF_DAY, (TimeUnit) eval.getVariable("endOfDuration"));
    }

    public void testEndOfMonths() throws Exception {
        init("coord-job-submit-freq");
        String expr = "${coord:endOfMonths(1)}";
        expr = "${coord:endOfMonths(3)}";
        assertEquals("3", CoordELFunctions.evalAndWrap(eval, expr));
        assertEquals(TimeUnit.MONTH, (TimeUnit) eval.getVariable("timeunit"));
        assertEquals(TimeUnit.END_OF_MONTH, (TimeUnit) eval.getVariable("endOfDuration"));
    }

    public void testMinutes() throws Exception {
        init("coord-job-submit-freq");
        String expr = "${coord:minutes(1)}";
        expr = "${coord:minutes(1)}";
        assertEquals("1", CoordELFunctions.evalAndWrap(eval, expr));
        assertEquals(TimeUnit.MINUTE, (TimeUnit) eval.getVariable("timeunit"));

        expr = "${coord:minutes(coord:minutes(1))}";
        assertEquals("1", CoordELFunctions.evalAndWrap(eval, expr));
        assertEquals(TimeUnit.MINUTE, (TimeUnit) eval.getVariable("timeunit"));
    }

    public void testTzOffsetPh1() throws Exception {
        init("coord-job-submit-instances");
        String expr = "${coord:current(-coord:tzOffset())}";
        assertEquals("${coord:current(-3)}", CoordELFunctions.evalAndWrap(eval, expr));
    }

    public void testDataNamesPh1() throws Exception {
        init("coord-job-submit-data");
        String expr = "${coord:dataIn('ABC')}";
        eval.setVariable("oozie.dataname.ABC", "data-in");
        assertEquals("${coord:dataIn('ABC')}", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:dataIn('ABCD')}";
        try {
            assertEquals("${coord:dataIn('ABCD')}", CoordELFunctions.evalAndWrap(eval, expr));
            fail("should throw exception beacuse Data in is not defiend");
        }
        catch (Exception ex) {
        }

        expr = "${coord:dataOut('EFG')}";
        eval.setVariable("oozie.dataname.EFG", "data-out");
        assertEquals("${coord:dataOut('EFG')}", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:dataOut('EFGH')}";
        try {
            assertEquals("${coord:dataOut('EFGH')}", CoordELFunctions.evalAndWrap(eval, expr));
            fail("should throw exception beacuse Data in is not defiend");
        }
        catch (Exception ex) {
        }
    }

    public void testHoursInDay() throws Exception {
        init("coord-action-create");
        String expr = "${coord:hoursInDay(1)}";
        String res = CoordELFunctions.evalAndWrap(eval, expr);
        assertEquals("24", res);
        expr = "${coord:hoursInDay(coord:hoursInDay(1))}";
        res = CoordELFunctions.evalAndWrap(eval, expr);

        SyncCoordAction appInst = new SyncCoordAction();
        SyncCoordDataset ds = new SyncCoordDataset();
        ;
        ds.setFrequency(1);
        ds.setTimeUnit(TimeUnit.DAY);
        ds.setInitInstance(DateUtils.parseDateUTC("2009-01-02T00:00Z"));
        ds.setTimeZone(DateUtils.getTimeZone("UTC"));
        ds.setName("test1");
        ds.setType("SYNC");

        appInst.setActualTime(DateUtils.parseDateUTC("2009-09-10T23:59Z"));
        appInst.setTimeZone(DateUtils.getTimeZone("UTC"));

        ds.setInitInstance(DateUtils.parseDateUTC("2009-01-01T08:00Z"));
        appInst.setNominalTime(DateUtils.parseDateUTC("2009-01-01T08:00Z"));
        appInst.setActualTime(DateUtils.parseDateUTC("2010-10-01T00:00Z"));
        CoordELFunctions.configureEvaluator(eval, ds, appInst);
        expr = "${coord:hoursInDay(0)}";
        assertEquals("24", CoordELFunctions.evalAndWrap(eval, expr));

        ds.setTimeZone(DateUtils.getTimeZone("America/Los_Angeles"));
        CoordELFunctions.configureEvaluator(eval, ds, appInst);
        expr = "${coord:hoursInDay(0)}";
        assertEquals("24", CoordELFunctions.evalAndWrap(eval, expr));
        expr = "${coord:hoursInDay(-2)}";
        assertEquals("24", CoordELFunctions.evalAndWrap(eval, expr));

        appInst.setNominalTime(DateUtils.parseDateUTC("2009-03-08T08:00Z"));
        appInst.setActualTime(DateUtils.parseDateUTC("2010-10-01T00:00Z"));
        ds.setTimeZone(DateUtils.getTimeZone("UTC"));
        CoordELFunctions.configureEvaluator(eval, ds, appInst);
        expr = "${coord:hoursInDay(0)}";
        assertEquals("24", CoordELFunctions.evalAndWrap(eval, expr));

        ds.setTimeZone(DateUtils.getTimeZone("Europe/London"));
        CoordELFunctions.configureEvaluator(eval, ds, appInst);
        expr = "${coord:hoursInDay(0)}";
        assertEquals("24", CoordELFunctions.evalAndWrap(eval, expr));

        appInst.setNominalTime(DateUtils.parseDateUTC("2009-03-08T08:00Z"));
        appInst.setActualTime(DateUtils.parseDateUTC("2010-10-01T00:00Z"));
        ds.setTimeZone(DateUtils.getTimeZone("America/Los_Angeles"));
        CoordELFunctions.configureEvaluator(eval, ds, appInst);
        expr = "${coord:hoursInDay(0)}";
        assertEquals("23", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:hoursInDay(1)}";
        assertEquals("24", CoordELFunctions.evalAndWrap(eval, expr));

        appInst.setNominalTime(DateUtils.parseDateUTC("2009-11-01T08:00Z"));
        appInst.setActualTime(DateUtils.parseDateUTC("2010-10-01T00:00Z"));
        CoordELFunctions.configureEvaluator(eval, ds, appInst);
        expr = "${coord:hoursInDay(0)}";
        assertEquals("25", CoordELFunctions.evalAndWrap(eval, expr));

        appInst.setNominalTime(DateUtils.parseDateUTC("2009-03-08T08:00Z"));
        appInst.setActualTime(DateUtils.parseDateUTC("2010-10-01T00:00Z"));
        ds.setTimeZone(DateUtils.getTimeZone("America/Los_Angeles"));
        ds.setEndOfDuration(TimeUnit.END_OF_DAY);
        CoordELFunctions.configureEvaluator(eval, ds, appInst);
        expr = "${coord:hoursInDay(0)}";
        assertEquals("23", CoordELFunctions.evalAndWrap(eval, expr));
        expr = "${coord:hoursInDay(1)}";
        assertEquals("24", CoordELFunctions.evalAndWrap(eval, expr));
        expr = "${coord:hoursInDay(-1)}";
        assertEquals("24", CoordELFunctions.evalAndWrap(eval, expr));

    }

    public void testDaysInMonth() throws Exception {
        init("coord-action-create");
        String expr = "${coord:daysInMonth(1)}";
        assertEquals("30", CoordELFunctions.evalAndWrap(eval, expr));
        expr = "${coord:daysInMonth(coord:daysInMonth(1))}";
        assertEquals("31", CoordELFunctions.evalAndWrap(eval, expr));

        SyncCoordAction appInst = new SyncCoordAction();
        SyncCoordDataset ds = new SyncCoordDataset();
        ;
        ds.setFrequency(1);
        ds.setTimeUnit(TimeUnit.MONTH);
        ds.setInitInstance(DateUtils.parseDateUTC("2009-01-02T00:00Z"));
        ds.setTimeZone(DateUtils.getTimeZone("UTC"));
        ds.setName("test1");
        ds.setType("SYNC");

        appInst.setActualTime(DateUtils.parseDateUTC("2009-09-10T23:59Z"));
        appInst.setTimeZone(DateUtils.getTimeZone("UTC"));

        ds.setInitInstance(DateUtils.parseDateUTC("2009-01-01T00:00Z"));
        appInst.setNominalTime(DateUtils.parseDateUTC("2009-02-01T11:00Z"));
        appInst.setActualTime(DateUtils.parseDateUTC("2010-10-01T00:00Z"));
        CoordELFunctions.configureEvaluator(eval, ds, appInst);
        expr = "${coord:daysInMonth(0)}";
        assertEquals("28", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:daysInMonth(-1)}";
        assertEquals("31", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:daysInMonth(2)}";
        assertEquals("30", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:daysInMonth(-3)}";
        assertEquals("30", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:daysInMonth(3)}";
        assertEquals("31", CoordELFunctions.evalAndWrap(eval, expr));

        ds.setTimeZone(DateUtils.getTimeZone("America/Los_Angeles"));
        appInst.setNominalTime(DateUtils.parseDateUTC("2009-02-01T11:00Z")); // Feb
        appInst.setActualTime(DateUtils.parseDateUTC("2010-10-01T00:00Z"));
        CoordELFunctions.configureEvaluator(eval, ds, appInst);
        expr = "${coord:daysInMonth(0)}";
        assertEquals("28", CoordELFunctions.evalAndWrap(eval, expr)); // Jan
        // 31

        // End of Month
        ds.setFrequency(1);
        ds.setTimeUnit(TimeUnit.MONTH);
        ds.setEndOfDuration(TimeUnit.END_OF_MONTH);
        ds.setInitInstance(DateUtils.parseDateUTC("2009-01-02T00:00Z"));
        ds.setTimeZone(DateUtils.getTimeZone("UTC"));
        ds.setName("test1");
        ds.setType("SYNC");

        appInst.setActualTime(DateUtils.parseDateUTC("2009-09-10T23:59Z"));
        appInst.setTimeZone(DateUtils.getTimeZone("UTC"));
        // Case 1
        ds.setInitInstance(DateUtils.parseDateUTC("2009-01-01T00:00Z"));
        appInst.setNominalTime(DateUtils.parseDateUTC("2009-02-01T11:00Z"));
        appInst.setActualTime(DateUtils.parseDateUTC("2010-10-01T00:00Z"));
        CoordELFunctions.configureEvaluator(eval, ds, appInst);
        expr = "${coord:daysInMonth(0)}";
        assertEquals("28", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:daysInMonth(-1)}";
        assertEquals("31", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:daysInMonth(2)}";
        assertEquals("30", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:daysInMonth(-3)}";
        assertEquals("30", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:daysInMonth(3)}";
        assertEquals("31", CoordELFunctions.evalAndWrap(eval, expr));

        ds.setTimeZone(DateUtils.getTimeZone("America/Los_Angeles"));
        appInst.setNominalTime(DateUtils.parseDateUTC("2009-02-01T11:00Z")); // Feb
        appInst.setActualTime(DateUtils.parseDateUTC("2010-10-01T00:00Z"));
        CoordELFunctions.configureEvaluator(eval, ds, appInst);
        expr = "${coord:daysInMonth(0)}";
        assertEquals("28", CoordELFunctions.evalAndWrap(eval, expr)); // Jan
        // 31
    }

    public void testTZOffset() throws Exception {
        init("coord-action-create");
        String expr = "${coord:tzOffset()}";
        // eval.setVariable("resolve_tzOffset", "true");
        assertEquals("0", CoordELFunctions.evalAndWrap(eval, expr));

        appInst.setTimeZone(DateUtils.getTimeZone("America/New_York"));
        CoordELFunctions.configureEvaluator(eval, ds, appInst);
        assertEquals("-180", CoordELFunctions.evalAndWrap(eval, expr));
    }

    public void testDateOffset() throws Exception {
        init("coord-job-submit-data");
        String expr = "${coord:dateOffset(\"2009-09-08T23:59Z\", 2, \"DAY\")}";
        init("coord-action-start");
        expr = "${coord:dateOffset(\"2009-09-08T23:59Z\", 2, \"DAY\")}";
        assertEquals("2009-09-10T23:59Z", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:dateOffset(\"2009-09-08T23:59Z\", -1, \"DAY\")}";
        assertEquals("2009-09-07T23:59Z", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:dateOffset(\"2009-09-08T23:59Z\", 1, \"YEAR\")}";
        assertEquals("2010-09-08T23:59Z", CoordELFunctions.evalAndWrap(eval, expr));
    }

    public void testCurrent() throws Exception {
        init("coord-action-create");
        String expr = "${coord:current(-1)}";
        assertEquals("2009-09-08T23:59Z", CoordELFunctions.evalAndWrap(eval, expr));

        appInst.setNominalTime(DateUtils.parseDateUTC("2009-05-30T00:45Z"));
        ds.setFrequency(1);
        ds.setTimeUnit(TimeUnit.DAY);
        ds.setInitInstance(DateUtils.parseDateUTC("2009-01-02T00:00Z"));
        ds.setTimeZone(DateUtils.getTimeZone("America/Los_Angeles"));
        expr = "${coord:current(0)} ${coord:current(1)} ${coord:current(-1)} ${coord:current(-3)}";
        assertEquals("2009-05-29T23:00Z 2009-05-30T23:00Z 2009-05-28T23:00Z 2009-05-26T23:00Z", CoordELFunctions
                .evalAndWrap(eval, expr));

        appInst.setNominalTime(DateUtils.parseDateUTC("2009-05-30T00:45Z"));
        ds.setFrequency(30);
        ds.setTimeUnit(TimeUnit.MINUTE);
        ds.setInitInstance(DateUtils.parseDateUTC("2009-01-08T00:00Z"));
        expr = "${coord:current(0)} ${coord:current(1)} ${coord:current(-1)} ${coord:current(-3)}";
        assertEquals("2009-05-30T00:30Z 2009-05-30T01:00Z 2009-05-30T00:00Z 2009-05-29T23:00Z", eval.evaluate(expr,
                                                                                                              String.class));

        SyncCoordAction appInst = new SyncCoordAction();
        SyncCoordDataset ds = new SyncCoordDataset();
        ;
        ds.setFrequency(1);
        ds.setTimeUnit(TimeUnit.DAY);
        ds.setInitInstance(DateUtils.parseDateUTC("2009-01-02T00:00Z"));
        ds.setTimeZone(DateUtils.getTimeZone("UTC"));
        ds.setName("test1");
        ds.setType("SYNC");

        appInst.setActualTime(DateUtils.parseDateUTC("2009-09-10T23:59Z"));
        appInst.setNominalTime(DateUtils.parseDateUTC("2009-05-30T00:00Z "));
        appInst.setTimeZone(DateUtils.getTimeZone("UTC"));
        CoordELFunctions.configureEvaluator(eval, ds, appInst);

        expr = "${coord:current(0)}";
        assertEquals("2009-05-30T00:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:current(1)}";
        assertEquals("2009-05-31T00:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:current(-1)}";
        assertEquals("2009-05-29T00:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:current(-3)}";
        assertEquals("2009-05-27T00:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        ds.setFrequency(7);
        ds.setInitInstance(DateUtils.parseDateUTC("2009-01-08T00:00Z"));
        CoordELFunctions.configureEvaluator(eval, ds, appInst);

        expr = "${coord:current(0)}";
        assertEquals("2009-05-28T00:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:current(1)}";
        assertEquals("2009-06-04T00:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:current(-1)}";
        assertEquals("2009-05-21T00:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:current(-3)}";
        assertEquals("2009-05-07T00:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        // Spring DST transition
        appInst.setNominalTime(DateUtils.parseDateUTC("2009-03-08T10:45Z"));
        ds.setFrequency(1);
        ds.setTimeUnit(TimeUnit.DAY);
        ds.setInitInstance(DateUtils.parseDateUTC("2009-03-06T10:00Z"));
        ds.setTimeZone(DateUtils.getTimeZone("America/Los_Angeles"));
        expr = "${coord:current(-2)} ${coord:current(-1)} ${coord:current(0)} ${coord:current(1)} ${coord:current(2)}";
        assertEquals("2009-03-06T10:00Z 2009-03-07T10:00Z 2009-03-08T09:00Z 2009-03-09T09:00Z 2009-03-10T09:00Z",
                     CoordELFunctions.evalAndWrap(eval, expr));

        // Winter DST Transition
        appInst.setNominalTime(DateUtils.parseDateUTC("2009-11-01T08:00Z"));

        ds.setFrequency(1);
        ds.setTimeUnit(TimeUnit.DAY);
        ds.setInitInstance(DateUtils.parseDateUTC("2009-10-30T08:00Z"));
        ds.setTimeZone(DateUtils.getTimeZone("America/Los_Angeles"));
        expr = "${coord:current(-2)} ${coord:current(-1)} ${coord:current(0)} ${coord:current(1)} ${coord:current(2)}";
        // System.out.println("AAAAA " + CoordELFunctions.evalAndWrap(eval,
        // expr));
        assertEquals("2009-10-30T08:00Z 2009-10-31T08:00Z 2009-11-01T08:00Z 2009-11-02T09:00Z 2009-11-03T09:00Z",
                     CoordELFunctions.evalAndWrap(eval, expr));

        // EndofDay testing
        ds.setFrequency(1);
        ds.setTimeUnit(TimeUnit.DAY);
        ds.setInitInstance(DateUtils.parseDateUTC("2009-01-02T09:00Z"));
        ds.setTimeZone(DateUtils.getTimeZone("UTC"));
        ds.setName("test1");
        ds.setType("SYNC");
        appInst.setNominalTime(DateUtils.parseDateUTC("2009-05-30T12:00Z "));
        ds.setEndOfDuration(TimeUnit.END_OF_DAY);

        CoordELFunctions.configureEvaluator(eval, ds, appInst);

        expr = "${coord:current(0)}";
        assertEquals("2009-05-30T00:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:current(1)}";
        assertEquals("2009-05-31T00:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        // From Specification
        // Case 1
        ds.setEndOfDuration(TimeUnit.END_OF_DAY);
        ds.setInitInstance(DateUtils.parseDateUTC("2009-01-01T08:00Z"));
        appInst.setNominalTime(DateUtils.parseDateUTC("2009-01-01T08:00Z"));
        CoordELFunctions.configureEvaluator(eval, ds, appInst);

        expr = "${coord:current(0)}";
        assertEquals("", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:current(1)}";
        assertEquals("", CoordELFunctions.evalAndWrap(eval, expr));

        // Case 2
        ds.setEndOfDuration(TimeUnit.END_OF_DAY);
        ds.setTimeZone(DateUtils.getTimeZone("America/Los_Angeles"));
        ds.setInitInstance(DateUtils.parseDateUTC("2009-01-01T08:00Z"));
        appInst.setNominalTime(DateUtils.parseDateUTC("2009-01-01T08:00Z"));
        CoordELFunctions.configureEvaluator(eval, ds, appInst);

        expr = "${coord:current(0)}";
        // assertEquals("2009-01-02T08:00Z", CoordELFunctions.evalAndWrap(eval,
        // expr));
        assertEquals("", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:current(1)}";
        // assertEquals("2009-01-03T08:00Z", CoordELFunctions.evalAndWrap(eval,
        // expr));
        assertEquals("", CoordELFunctions.evalAndWrap(eval, expr));

        // Case 3
        ds.setEndOfDuration(TimeUnit.END_OF_DAY);
        ds.setTimeZone(DateUtils.getTimeZone("America/Los_Angeles"));
        ds.setInitInstance(DateUtils.parseDateUTC("2009-01-01T07:01Z"));
        appInst.setNominalTime(DateUtils.parseDateUTC("2009-01-01T08:01Z"));
        CoordELFunctions.configureEvaluator(eval, ds, appInst);

        expr = "${coord:current(0)}";
        assertEquals("2009-01-01T08:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:current(1)}";
        assertEquals("2009-01-02T08:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        // Case 4
        ds.setEndOfDuration(TimeUnit.END_OF_DAY);
        ds.setTimeZone(DateUtils.getTimeZone("America/Los_Angeles"));
        ds.setInitInstance(DateUtils.parseDateUTC("2009-01-01T7:00Z"));
        appInst.setNominalTime(DateUtils.parseDateUTC("2009-01-01T18:00Z"));
        CoordELFunctions.configureEvaluator(eval, ds, appInst);

        expr = "${coord:current(0)}";
        assertEquals("2009-01-01T08:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:current(1)}";
        assertEquals("2009-01-02T08:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        // Case 5
        ds.setEndOfDuration(TimeUnit.END_OF_DAY);
        ds.setTimeZone(DateUtils.getTimeZone("America/Los_Angeles"));
        ds.setInitInstance(DateUtils.parseDateUTC("2009-03-07T07:00Z"));
        appInst.setNominalTime(DateUtils.parseDateUTC("2009-03-07T09:00Z"));
        CoordELFunctions.configureEvaluator(eval, ds, appInst);

        expr = "${coord:current(0)}";
        assertEquals("2009-03-07T08:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:current(1)}";
        assertEquals("2009-03-08T08:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        // Case 6
        ds.setEndOfDuration(TimeUnit.END_OF_DAY);
        ds.setTimeZone(DateUtils.getTimeZone("America/Los_Angeles"));
        ds.setInitInstance(DateUtils.parseDateUTC("2009-03-08T07:00Z"));
        appInst.setNominalTime(DateUtils.parseDateUTC("2009-03-08T08:00Z"));
        CoordELFunctions.configureEvaluator(eval, ds, appInst);

        expr = "${coord:current(0)}";
        assertEquals("2009-03-08T08:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:current(1)}";
        assertEquals("2009-03-09T07:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        // Case 7
        ds.setEndOfDuration(TimeUnit.END_OF_DAY);
        ds.setTimeZone(DateUtils.getTimeZone("America/Los_Angeles"));
        ds.setInitInstance(DateUtils.parseDateUTC("2009-03-09T07:00Z"));
        appInst.setNominalTime(DateUtils.parseDateUTC("2009-03-10T08:01Z"));
        CoordELFunctions.configureEvaluator(eval, ds, appInst);

        expr = "${coord:current(0)}";
        assertEquals("2009-03-10T07:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:current(1)}";
        assertEquals("2009-03-11T07:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        // Case 8
        ds.setEndOfDuration(TimeUnit.END_OF_DAY);
        ds.setFrequency(2); // Changed
        ds.setTimeZone(DateUtils.getTimeZone("America/Los_Angeles"));
        ds.setInitInstance(DateUtils.parseDateUTC("2009-03-09T07:00Z"));
        appInst.setNominalTime(DateUtils.parseDateUTC("2009-03-10T07:00Z"));
        CoordELFunctions.configureEvaluator(eval, ds, appInst);

        expr = "${coord:current(0)}";
        assertEquals("2009-03-10T07:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:current(1)}";
        assertEquals("2009-03-12T07:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        // Test with EOM

        ds.setTimeUnit(TimeUnit.MONTH);
        // Case 1
        ds.setEndOfDuration(TimeUnit.END_OF_MONTH);
        ds.setFrequency(1);
        ds.setTimeZone(DateUtils.getTimeZone("UTC"));
        ds.setInitInstance(DateUtils.parseDateUTC("2009-01-01T00:00Z"));
        appInst.setNominalTime(DateUtils.parseDateUTC("2009-02-01T00:00Z"));
        CoordELFunctions.configureEvaluator(eval, ds, appInst);

        expr = "${coord:current(0)}";
        assertEquals("2009-02-01T00:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:current(1)}";
        assertEquals("2009-03-01T00:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        // Case 2
        ds.setEndOfDuration(TimeUnit.END_OF_MONTH);
        ds.setTimeZone(DateUtils.getTimeZone("UTC"));
        ds.setInitInstance(DateUtils.parseDateUTC("2009-01-01T08:00Z"));
        appInst.setNominalTime(DateUtils.parseDateUTC("2009-02-01T08:00Z"));
        CoordELFunctions.configureEvaluator(eval, ds, appInst);

        expr = "${coord:current(0)}";
        assertEquals("2009-02-01T00:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:current(1)}";
        assertEquals("2009-03-01T00:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        // Case 3
        ds.setEndOfDuration(TimeUnit.END_OF_MONTH);
        ds.setTimeZone(DateUtils.getTimeZone("UTC"));
        ds.setInitInstance(DateUtils.parseDateUTC("2009-01-31T08:00Z"));
        appInst.setNominalTime(DateUtils.parseDateUTC("2009-02-01T08:00Z"));
        CoordELFunctions.configureEvaluator(eval, ds, appInst);

        expr = "${coord:current(0)}";
        assertEquals("2009-02-01T00:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:current(1)}";
        assertEquals("2009-03-01T00:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        // Case 4
        ds.setEndOfDuration(TimeUnit.END_OF_MONTH);
        ds.setTimeZone(DateUtils.getTimeZone("America/Los_Angeles"));
        ds.setInitInstance(DateUtils.parseDateUTC("2009-01-01T08:00Z"));
        appInst.setNominalTime(DateUtils.parseDateUTC("2009-02-01T08:00Z"));
        CoordELFunctions.configureEvaluator(eval, ds, appInst);

        expr = "${coord:current(0)}";
        assertEquals("2009-02-01T08:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:current(1)}";
        assertEquals("2009-03-01T08:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        // Case 5
        ds.setEndOfDuration(TimeUnit.END_OF_MONTH);
        ds.setTimeZone(DateUtils.getTimeZone("America/Los_Angeles"));
        ds.setInitInstance(DateUtils.parseDateUTC("2009-02-02T08:00Z"));
        appInst.setNominalTime(DateUtils.parseDateUTC("2009-03-02T08:00Z"));
        CoordELFunctions.configureEvaluator(eval, ds, appInst);

        expr = "${coord:current(0)}";
        assertEquals("2009-03-01T08:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:current(1)}";
        assertEquals("2009-04-01T07:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        // Case 6
        ds.setEndOfDuration(TimeUnit.END_OF_MONTH);
        ds.setTimeZone(DateUtils.getTimeZone("America/Los_Angeles"));
        ds.setInitInstance(DateUtils.parseDateUTC("2009-02-01T08:00Z"));
        appInst.setNominalTime(DateUtils.parseDateUTC("2009-03-01T08:00Z"));
        CoordELFunctions.configureEvaluator(eval, ds, appInst);

        expr = "${coord:current(0)}";
        assertEquals("2009-03-01T08:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:current(1)}";
        assertEquals("2009-04-01T07:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        // Case 7
        ds.setEndOfDuration(TimeUnit.END_OF_MONTH);
        ds.setFrequency(3);
        ds.setTimeZone(DateUtils.getTimeZone("America/Los_Angeles"));
        ds.setInitInstance(DateUtils.parseDateUTC("2009-02-01T08:00Z"));
        appInst.setNominalTime(DateUtils.parseDateUTC("2009-03-01T08:00Z"));
        CoordELFunctions.configureEvaluator(eval, ds, appInst);

        expr = "${coord:current(0)}";
        assertEquals("2009-03-01T08:00Z", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:current(1)}";
        assertEquals("2009-06-01T07:00Z", CoordELFunctions.evalAndWrap(eval, expr));
    }

    public void testLatest() throws Exception {
        init("coord-action-start");
        String expr = "${coord:latest(0)}";
        Configuration conf = new Configuration();

        // TODO:Set hadoop properties
        eval.setVariable(CoordELFunctions.CONFIGURATION, conf);
        String testDir = getTestCaseDir();
        // ds.setUriTemplate("file:///tmp/coord/${YEAR}/${MONTH}/${DAY}");
        ds.setUriTemplate("file://" + testDir + "/${YEAR}/${MONTH}/${DAY}");
        createDir(testDir + "/2009/09/10");
        // TODO: Create the directories
        assertEquals("2009-09-10T23:59Z", CoordELFunctions.evalAndWrap(eval, expr));
        createDir(testDir + "/2009/09/09");
        expr = "${coord:latest(-1)}";
        assertEquals("2009-09-09T23:59Z", CoordELFunctions.evalAndWrap(eval, expr));
        createDir(testDir + "/2009/09/08");
        expr = "${coord:latest(-2)}";
        assertEquals("2009-09-08T23:59Z", CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:latest(-100)}";
        assertEquals(expr, CoordELFunctions.evalAndWrap(eval, expr));

        expr = "${coord:latest(1)}";
        try {
            assertEquals(expr, CoordELFunctions.evalAndWrap(eval, expr));
            fail("Should throw exception, because latest for +ve instance is not valid");
        }
        catch (Exception ex) {
        }
        // Add test cases with EOM and EOD option
    }

    public void testPh1Future() throws Exception {
        init("coord-job-submit-instances");
        String expr = "${coord:future(1, 10)}";
        assertEquals(expr, CoordELFunctions.evalAndWrap(eval, expr));
    }

    public void testFormatTime() throws Exception {
        String expr1 = "${coord:formatTime(\"2009-09-08T23:59Z\", \"yyyy\")}";
        String expr2 = "${coord:formatTime(\"2009-09-08T23:59Z\", \"yyyyMMdd_HHmmss\")}";
        init("coord-action-create");
        assertEquals("2009", CoordELFunctions.evalAndWrap(eval, expr1));
        assertEquals("20090908_235900", CoordELFunctions.evalAndWrap(eval, expr2));
        init("coord-action-create-inst");
        assertEquals("2009", CoordELFunctions.evalAndWrap(eval, expr1));
        assertEquals("20090908_235900", CoordELFunctions.evalAndWrap(eval, expr2));
        init("coord-action-start");
        assertEquals("2009", CoordELFunctions.evalAndWrap(eval, expr1));
        assertEquals("20090908_235900", CoordELFunctions.evalAndWrap(eval, expr2));

        String utcDate = "2009-09-08T23:59Z";
        String expr3 = "${coord:formatTime(date, \"yyyy\")}";
        String expr3_eval = "${coord:formatTime('" + utcDate + "' , " + "yyyy)}";
        init("coord-job-submit-instances");
        eval.setVariable("date", utcDate);
        assertEquals(expr3_eval, CoordELFunctions.evalAndWrap(eval, expr3));
        init("coord-job-submit-data");
        eval.setVariable("date", utcDate);
        assertEquals(expr3_eval, CoordELFunctions.evalAndWrap(eval, expr3));
    }

    public void testFuture() throws Exception {
        init("coord-job-submit-instances");
        String expr = "${coord:future(1, 20)}";

        init("coord-action-start");
        Configuration conf = new Configuration();

        // TODO:Set hadoop properties
        eval.setVariable(CoordELFunctions.CONFIGURATION, conf);
        String testDir = getTestCaseDir();
        ds.setUriTemplate("file://" + testDir + "/${YEAR}/${MONTH}/${DAY}");
        createDir(testDir + "/2009/09/10");
        createDir(testDir + "/2009/09/11");
        assertEquals("2009-09-11T23:59Z", CoordELFunctions.evalAndWrap(eval, expr));

        try {
            expr = "${coord:future(-1, 3)}";
            CoordELFunctions.evalAndWrap(eval, expr);
            fail("Should fail for negative instance value");
        }
        catch (Exception ex) {
        }

        expr = "${coord:future(4, 20)}";
        String res = "${coord:future(4, 20)}";
        assertEquals(res, CoordELFunctions.evalAndWrap(eval, expr));
    }

    public void testNominalTime() throws Exception {
        init("coord-job-submit-data");
        String expr = "${coord:nominalTime()}";
        init("coord-action-start");
        expr = "${coord:nominalTime()}";
        assertEquals("2009-09-09T23:59Z", CoordELFunctions.evalAndWrap(eval, expr));
    }

    public void testActualTime() throws Exception {
        init("coord-job-submit-data");
        String expr = "${coord:actualTime()}";
        init("coord-action-start");
        expr = "${coord:actualTime()}";
        assertEquals("2009-09-10T23:59Z", CoordELFunctions.evalAndWrap(eval, expr));
    }

    public void testDataIn() throws Exception {
        init("coord-action-start");
        eval.setVariable(".datain.ABC", "file:///tmp/coord/US/2009/1/30,file:///tmp/coord/US/2009/1/31");
        eval.setVariable(".datain.ABC.unresolved", Boolean.FALSE);
        String expr = "${coord:dataIn('ABC')}";
        assertEquals("file:///tmp/coord/US/2009/1/30,file:///tmp/coord/US/2009/1/31", CoordELFunctions.evalAndWrap(
                eval, expr));
        eval.setVariable(".datain.ABC", "file:///tmp/coord/US/2009/1/30,file:///tmp/coord/US/2009/1/31");
        eval.setVariable(".datain.ABC.unresolved", Boolean.TRUE);
        assertEquals(expr, CoordELFunctions.evalAndWrap(eval, expr));
    }

    public void testDataOut() throws Exception {
        init("coord-action-start");
        eval.setVariable(".dataout.ABC", "file:///tmp/coord/US/2009/1/30,file:///tmp/coord/US/2009/1/31");
        String expr = "${coord:dataOut('ABC')}";
        assertEquals("file:///tmp/coord/US/2009/1/30,file:///tmp/coord/US/2009/1/31", CoordELFunctions.evalAndWrap(
                eval, expr));
    }

    public void testActionId() throws Exception {
        init("coord-action-start");
        String expr = "${coord:actionId()}";
        assertEquals("00000-oozie-C@1", CoordELFunctions.evalAndWrap(eval, expr));
    }

    public void testName() throws Exception {
        init("coord-action-start");
        String expr = "${coord:name()}";
        assertEquals("mycoordinator-app", CoordELFunctions.evalAndWrap(eval, expr));
    }

    public void testConf() throws Exception {
        init("coord-job-submit-freq");
        eval.setVariable("my.test.variable", "test");
        String expr = "${coord:conf('my.test.variable')}";
        assertEquals("test", CoordELFunctions.evalAndWrap(eval, expr));
    }

    public void testUser() throws Exception {
        init("coord-job-submit-freq");
        String expr = "${coord:user()}";
        assertEquals("test_user", CoordELFunctions.evalAndWrap(eval, expr));
        init("coord-job-submit-instances");
         expr = "${coord:user()}";
        assertEquals("test_user", CoordELFunctions.evalAndWrap(eval, expr));
        init("coord-job-submit-data");
         expr = "${coord:user()}";
        assertEquals("test_user", CoordELFunctions.evalAndWrap(eval, expr));
        init("coord-sla-submit");
         expr = "${coord:user()}";
        assertEquals("test_user", CoordELFunctions.evalAndWrap(eval, expr));
        init("coord-action-create");
         expr = "${coord:user()}";
        assertEquals("test_user", CoordELFunctions.evalAndWrap(eval, expr));
        init("coord-action-create-inst");
         expr = "${coord:user()}";
        assertEquals("test_user", CoordELFunctions.evalAndWrap(eval, expr));
        init("coord-sla-create");
         expr = "${coord:user()}";
        assertEquals("test_user", CoordELFunctions.evalAndWrap(eval, expr));
        init("coord-action-start");
         expr = "${coord:user()}";
        assertEquals("test_user", CoordELFunctions.evalAndWrap(eval, expr));
        init("coord-action-create-inst");
         expr = "${coord:user()}";
        assertEquals("test_user", CoordELFunctions.evalAndWrap(eval, expr));
    }

    /*
     * public void testDetach() throws Exception { Services.get().destroy(); }
     */

    private void init(String tag) throws Exception {
        eval = Services.get().get(ELService.class).createEvaluator(tag);
        eval.setVariable(OozieClient.USER_NAME, "test_user");
        eval.setVariable(OozieClient.GROUP_NAME, "test_group");
        appInst = new SyncCoordAction();
        ds = new SyncCoordDataset();
        ds.setFrequency(1);
        ds.setInitInstance(DateUtils.parseDateUTC("2009-09-01T23:59Z"));
        ds.setTimeUnit(TimeUnit.DAY);
        ds.setTimeZone(DateUtils.getTimeZone("America/Los_Angeles"));
        ds.setName("test");
        ds.setUriTemplate("hdfs://localhost:9000/user/" + getTestUser() + "/US/${YEAR}/${MONTH}/${DAY}");
        ds.setType("SYNC");
        ds.setDoneFlag("");
        appInst.setActualTime(DateUtils.parseDateUTC("2009-09-10T23:59Z"));
        appInst.setNominalTime(DateUtils.parseDateUTC("2009-09-09T23:59Z"));
        appInst.setTimeZone(DateUtils.getTimeZone("America/Los_Angeles"));
        appInst.setActionId("00000-oozie-C@1");
        appInst.setName("mycoordinator-app");
        CoordELFunctions.configureEvaluator(eval, ds, appInst);
    }

    private void createDir(String dir) {
        Process pr;
        try {
            pr = Runtime.getRuntime().exec("mkdir -p " + dir + "/_SUCCESS");
            pr.waitFor();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
