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

import org.apache.oozie.util.DateUtils;
import org.junit.Test;

import java.text.ParseException;
import java.util.Calendar;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;

public class TestDaylightOffsetCalculator {
    private final static TimeZone TZ_DST = TimeZone.getTimeZone("America/Los_Angeles");
    private final static TimeZone TZ_STANDARD = TimeZone.getTimeZone("Asia/Kolkata");

    @Test
    public void testCalculateBetweenNonDSTStartAndEndGivesZeroOffset() throws ParseException {
        final DaylightOffsetCalculator nonDSTStartAndEndCalculator = new DaylightOffsetCalculator(
                DateUtils.parseDateOozieTZ("2013-01-18T00:00Z"),
                DateUtils.parseDateOozieTZ("2014-01-18T00:00Z"));

        final Calendar target = Calendar.getInstance();
        target.setTime(DateUtils.parseDateOozieTZ("2014-01-18T00:00Z"));

        assertEquals(nonDSTStartAndEndCalculator.calculate(TZ_DST, target), target);
    }

    @Test
    public void testCalculateBetweenDSTStartAndStandardEndGivesPositiveOffset() throws ParseException {
        final DaylightOffsetCalculator dstStartAndStandardEndCalculator = new DaylightOffsetCalculator(
                DateUtils.parseDateOozieTZ("2013-07-18T00:00Z"),
                DateUtils.parseDateOozieTZ("2014-01-18T00:00Z"));

        final Calendar standardEnd = Calendar.getInstance();
        standardEnd.setTime(DateUtils.parseDateOozieTZ("2014-01-18T00:00Z"));

        final Calendar standardEndPlusOneHour = (Calendar) standardEnd.clone();
        standardEndPlusOneHour.add(Calendar.HOUR, 1);

        assertEquals(dstStartAndStandardEndCalculator.calculate(TZ_DST, standardEnd), standardEndPlusOneHour);
    }

    @Test
    public void testCalculateBetweenStandardStartAndDSTEndGivesNegativeOffset() throws ParseException {
        final DaylightOffsetCalculator standardStartAndDSTEndCalculator = new DaylightOffsetCalculator(
                DateUtils.parseDateOozieTZ("2013-01-18T00:00Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:00Z"));

        final Calendar standardEnd = Calendar.getInstance();
        standardEnd.setTime(DateUtils.parseDateOozieTZ("2013-07-18T00:00Z"));

        final Calendar standardEndMinusOneHour = (Calendar) standardEnd.clone();
        standardEndMinusOneHour.add(Calendar.HOUR, -1);

        assertEquals(standardStartAndDSTEndCalculator.calculate(TZ_DST, standardEnd), standardEndMinusOneHour);
    }

    @Test
    public void testCalculateGivenStandardTZGivesZeroOffset() throws ParseException {
        final DaylightOffsetCalculator standardStartAndDSTEndCalculator = new DaylightOffsetCalculator(
                DateUtils.parseDateOozieTZ("2013-01-18T00:00Z"),
                DateUtils.parseDateOozieTZ("2013-07-18T00:00Z"));

        final Calendar dstStart = Calendar.getInstance();
        dstStart.setTime(DateUtils.parseDateOozieTZ("2013-01-18T00:00Z"));

        final Calendar standardEnd = Calendar.getInstance();
        standardEnd.setTime(DateUtils.parseDateOozieTZ("2013-07-18T00:00Z"));

        assertEquals(standardStartAndDSTEndCalculator.calculate(TZ_STANDARD, standardEnd), standardEnd);
    }

    @Test
    public void testGetDSTOffsetOfNonDaylightChangeGivesZeroOffset() throws ParseException {
        final long dstOffset = DaylightOffsetCalculator.getDSTOffset(TZ_DST,
                DateUtils.parseDateOozieTZ("2013-07-18T00:00Z"),
                DateUtils.parseDateOozieTZ("2013-07-19T00:00Z"));

        assertEquals("non-daylight change should give zero DST offset", 0L, dstOffset);
    }

    @Test
    public void testGetDSTOffsetOfDaylightChangeFromDSTToStandardGivesPositiveOffset() throws ParseException {
        final long dstOffset = DaylightOffsetCalculator.getDSTOffset(TZ_DST,
                DateUtils.parseDateOozieTZ("2013-07-18T00:00Z"),
                DateUtils.parseDateOozieTZ("2014-01-19T00:00Z"));

        assertEquals("daylight change from DST to standard should give positive DST offset", 3_600_000L, dstOffset);
    }

    @Test
    public void testGetDSTOffsetOfDaylightChangeFromStandardToDSTGivesNegativeOffset() throws ParseException {
        final long dstOffset = DaylightOffsetCalculator.getDSTOffset(TZ_DST,
                DateUtils.parseDateOozieTZ("2013-01-18T00:00Z"),
                DateUtils.parseDateOozieTZ("2013-07-19T00:00Z"));

        assertEquals("daylight change from standard to DST should give negative DST offset", -3_600_000L, dstOffset);
    }

    @Test
    public void testGetDSTOffsetGivenStandardTZGivesZeroOffset() throws ParseException {
        final long dstOffset = DaylightOffsetCalculator.getDSTOffset(TZ_STANDARD,
                DateUtils.parseDateOozieTZ("2013-01-18T00:00Z"),
                DateUtils.parseDateOozieTZ("2013-07-19T00:00Z"));

        assertEquals("daylight change from standard to DST should give negative DST offset", 0L, dstOffset);
    }
}