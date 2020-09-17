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

package org.apache.oozie.executor.jpa.sla;

import org.apache.oozie.servlet.XServletException;
import org.apache.oozie.util.DateUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.servlet.ServletException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestSLASummaryGetForFilterJPAExecutorFilterCollection {

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private SLASummaryGetForFilterJPAExecutor.FilterCollection filterCollection;

    @Before
    public void setUp() throws Exception {
        filterCollection = new SLASummaryGetForFilterJPAExecutor.FilterCollection();
    }

    @Test
    public void testNullParameterName() throws ServletException, ParseException {
        expectedException.expect(XServletException.class);
        filterCollection.checkAndSetFilterField(null, "value");
    }

    @Test
    public void testInvalidParameterName() throws ServletException, ParseException {
        expectedException.expect(XServletException.class);
        filterCollection.checkAndSetFilterField("app_name_typo", "app_name_value");
    }

    @Test
    public void testMixedCaseParameterName() throws ServletException, ParseException {
        expectedException.expect(XServletException.class);
        filterCollection.checkAndSetFilterField("aPP_name", "app_name_value");
    }

    @Test
    public void testInvalidInteger() throws ParseException, ServletException {
        expectedException.expect(IllegalArgumentException.class);
        filterCollection.checkAndSetFilterField("actual_duration_min", "not a number");
    }

    @Test
    public void testStringParameters() throws ServletException, ParseException {
        checkSetAndAssertFilterField("app_name", "app_name_value");
        checkSetAndAssertFilterField("app_type", "app_type_value");
        checkSetAndAssertFilterField("user_name", "user_name_value");
        checkSetAndAssertFilterField("job_status", "job_status_value");
        checkSetAndAssertFilterField("id", "id_value");
        checkSetAndAssertFilterField("parent_id", "parent_id_value");
        checkSetAndAssertFilterField("bundle", "bundle_value");
    }

    @Test
    public void testIntegerParameters() throws ServletException, ParseException {
        checkSetAndAssertFilterField("actual_duration_min", "100", 100);
        checkSetAndAssertFilterField("actual_duration_max", "200", 200);
        checkSetAndAssertFilterField("expected_duration_min", "300", 300);
        checkSetAndAssertFilterField("expected_duration_max", "400", 400);
    }

    @Test
    public void testTimestampParameters() throws ServletException, ParseException {
        String time1 = "2012-06-03T16:00Z";
        Date date1 = DateUtils.parseDateUTC(time1);
        Timestamp timestamp1 = new Timestamp(date1.getTime());
        String time2 = "2012-08-03T16:00Z";
        Date date2 = DateUtils.parseDateUTC(time2);
        Timestamp timestamp2 = new Timestamp(date2.getTime());
        checkSetAndAssertFilterField("nominal_after", time1, timestamp1);
        checkSetAndAssertFilterField("nominal_before", time2, timestamp2);
        checkSetAndAssertFilterField("created_after", time1, timestamp1);
        checkSetAndAssertFilterField("created_before", time2, timestamp2);
        checkSetAndAssertFilterField("expectedstart_after", time1, timestamp1);
        checkSetAndAssertFilterField("expectedstart_before", time2, timestamp2);
        checkSetAndAssertFilterField("expectedend_after", time1, timestamp1);
        checkSetAndAssertFilterField("expectedend_before", time2, timestamp2);
        checkSetAndAssertFilterField("actualstart_after", time1, timestamp1);
        checkSetAndAssertFilterField("actualstart_before", time2, timestamp2);
        checkSetAndAssertFilterField("actualend_after", time1, timestamp1);
        checkSetAndAssertFilterField("actualend_before", time2, timestamp2);
    }

    @Test
    public void testDeprecatedParameter() throws ServletException, ParseException {
        String time1 = "2012-06-03T16:00Z";
        Date date1 = DateUtils.parseDateUTC(time1);
        Timestamp timestamp1 = new Timestamp(date1.getTime());
        String time2 = "2012-08-03T16:00Z";
        Date date2 = DateUtils.parseDateUTC(time2);
        Timestamp timestamp2 = new Timestamp(date2.getTime());
        checkSetAndAssertFilterField("nominal_start", time1, "nominal_after", timestamp1);
        checkSetAndAssertFilterField("nominal_end", time2, "nominal_before", timestamp2);
    }

    @Test
    public void testInvalidSingleSetSLAStatus() throws ParseException, ServletException {
        expectedException.expect(IllegalArgumentException.class);
        filterCollection.checkAndSetFilterField("sla_status", "IN_PROCESS_TYPO");
    }

    @Test
    public void testSingleSetSLAStatus() throws ServletException, ParseException {
        checkSetAndAssertFilterField("sla_status", Collections.singletonList("IN_PROCESS"),
                Collections.singletonList("IN_PROCESS"));
    }

    @Test
    public void testMultipleSetSLAStatusSingleSet() throws ServletException, ParseException {
        checkSetAndAssertFilterField("sla_status", Collections.singletonList("MET,MISS"), Arrays.asList("MET", "MISS"));
    }

    @Test
    public void testMultipleSetSLAStatusMultiSet() throws ServletException, ParseException {
        checkSetAndAssertFilterField("sla_status", Arrays.asList("MET", "MISS"), Arrays.asList("MISS"));
    }

    @Test
    public void testSingleSetEventStatus() throws ServletException, ParseException {
        checkSetAndAssertFilterField("event_status", Collections.singletonList("START_MET,DURATION_MET"),
                Arrays.asList("START_MET", "DURATION_MET"));
    }

    @Test
    public void testMultipleSetEventStatusSingleSet() throws ServletException, ParseException {
        checkSetAndAssertFilterField("event_status", Collections.singletonList("START_MET,DURATION_MISS"),
                Arrays.asList("START_MET", "DURATION_MISS"));
    }

    @Test
    public void testMultipleSetEventStatusMultiSet() throws ServletException, ParseException {
        checkSetAndAssertFilterField("event_status", Arrays.asList("START_MET", "DURATION_MISS"),
                Collections.singletonList("DURATION_MISS"));
    }

    @Test
    public void testInvalidIntegerInterval() throws ServletException, ParseException {
        filterCollection.checkAndSetFilterField("actual_duration_min", "200");
        expectedException.expect(XServletException.class);
        filterCollection.checkAndSetFilterField("actual_duration_max", "100");
    }

    @Test
    public void testInvalidTimeInterval() throws ServletException, ParseException {
        String time1 = "2018-09-12T16:00Z";
        String time2 = "2018-09-13T16:00Z";
        filterCollection.checkAndSetFilterField("nominal_after", time2);
        expectedException.expect(XServletException.class);
        filterCollection.checkAndSetFilterField("nominal_before", time1);
    }

    private void checkSetAndAssertFilterField(String fieldName, String fieldValue) throws ParseException, ServletException {
        checkSetAndAssertFilterField(fieldName, fieldValue, fieldName, fieldValue);
    }

    private void checkSetAndAssertFilterField(String fieldName, String fieldValue, Object expectedValue) throws ParseException,
            ServletException {
        checkSetAndAssertFilterField(fieldName, fieldValue, fieldName, expectedValue);
    }

    private void checkSetAndAssertFilterField(String fieldName, String fieldValue, String expectedFieldName, Object expectedValue)
            throws ServletException, ParseException {
        filterCollection.checkAndSetFilterField(fieldName, fieldValue);
        assertEquals("Invalid parameter value", expectedValue, filterCollection.getFilterField(expectedFieldName));
    }

    private void checkSetAndAssertFilterField(String fieldName, List<String> fieldValues, List<String> expectedFieldValues)
    throws ServletException, ParseException {
        for (String fieldValue : fieldValues) {
            filterCollection.checkAndSetFilterField(fieldName, fieldValue);
        }
        List<String> filterFieldReadAfterSet = (List<String>)filterCollection.getFilterField(fieldName);
        String assertMessage = String.format("incorrect %s items", fieldName);
        assertEquals(assertMessage, expectedFieldValues, filterFieldReadAfterSet);
    }
}