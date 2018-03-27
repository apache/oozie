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

package org.apache.oozie.util;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;

import org.apache.commons.logging.LogFactory;
import org.apache.log4j.LogManager;
import org.apache.oozie.test.XTestCase;

public class TestOozieRollingPolicy extends XTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        LogFactory.getFactory().release();
        LogManager.resetConfiguration();
    }

    @Override
    protected void tearDown() throws Exception {
        LogFactory.getFactory().release();
        LogManager.resetConfiguration();
        super.tearDown();
    }

    public void testDeletingOldFiles() throws Exception {
        _testDeletingOldFiles("oozie.log");
    }

    public void testDeletingErrorOldFiles() throws Exception {
        _testDeletingOldFiles("oozie-error.log");
    }

    public void testDeletingAuditOldFiles() throws Exception {
        _testDeletingOldFiles("oozie-audit.log", Calendar.DAY_OF_MONTH);
    }

    private void _testDeletingOldFiles(String oozieLogName) throws IOException {
        _testDeletingOldFiles(oozieLogName, Calendar.HOUR_OF_DAY);
    }

    private void _testDeletingOldFiles(String oozieLogName, int calendarUnit) throws IOException{
        // OozieRollingPolicy gets the log path and log name from XLogService by calling Services.get.get(XLogService.class) so we
        // use a mock version where we overwrite the XLogService.getOozieLogName() and XLogService.getOozieLogPath() to simply
        // return these values instead of involving Services.  We then overwrite OozieRollingPolicy.getXLogService() to return the
        // mock one instead.
        String oozieLogPath = getTestCaseDir();

        OozieRollingPolicy orp = new OozieRollingPolicy();

        if (calendarUnit == Calendar.DAY_OF_MONTH) {
            orp.setFileNamePattern(oozieLogPath + "/" + oozieLogName + "-%d{yyyy-MM-dd}");
        }
        else {
            orp.setFileNamePattern(oozieLogPath + "/" + oozieLogName + "-%d{yyyy-MM-dd-HH}");

        }
        orp.setMaxHistory(3);   // only keep 3 newest logs

        Calendar cal = new GregorianCalendar();
        final File f0 = new File(oozieLogPath, oozieLogName);
        f0.createNewFile();
        f0.setLastModified(cal.getTimeInMillis());
        cal.add(calendarUnit, 1);
        final File f1 = new File(oozieLogPath, oozieLogName + formatDateForFilename(cal, calendarUnit) + ".gz");
        f1.createNewFile();
        cal.add(calendarUnit, 1);
        final File f2 = new File(oozieLogPath, oozieLogName + formatDateForFilename(cal, calendarUnit) + ".gz");
        f2.createNewFile();
        cal.add(calendarUnit, 1);
        final File f3 = new File(oozieLogPath, oozieLogName + formatDateForFilename(cal, calendarUnit) + ".gz");
        f3.createNewFile();
        cal.add(calendarUnit, 1);
        final File f4 = new File(oozieLogPath, oozieLogName + formatDateForFilename(cal, calendarUnit) + ".gz");
        f4.createNewFile();

        // Test that it only deletes the oldest file (f1)
        orp.isTriggeringEvent(null, null, null, 0);
        waitFor(60 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return (f0.exists() && !f1.exists() && f2.exists() && f3.exists() && f4.exists());
            }
        });
        assertTrue(f0.exists() && !f1.exists() && f2.exists() && f3.exists() && f4.exists());

        cal.add(calendarUnit, 1);
        final File f5 = new File(oozieLogPath, oozieLogName + formatDateForFilename(cal, calendarUnit));
        f5.createNewFile();
        f5.setLastModified(cal.getTimeInMillis());

        cal.add(calendarUnit, -15);
        final File f6 = new File(oozieLogPath, oozieLogName + formatDateForFilename(cal, calendarUnit));
        f6.createNewFile();
        f6.setLastModified(cal.getTimeInMillis());

        // Test that it can delete more than one file when necessary and that it works with non .gz files
        orp.isTriggeringEvent(null, null, null, 0);
        waitFor(60 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return (f0.exists() && !f1.exists() && !f2.exists() && f3.exists() && f4.exists() && f5.exists() && !f6.exists());
            }
        });
        assertTrue(f0.exists() && !f1.exists() && !f2.exists() && f3.exists() && f4.exists() && f5.exists() && !f6.exists());

        final File f7 = new File(oozieLogPath, "blah.txt");
        f7.createNewFile();
        f7.setLastModified(cal.getTimeInMillis());

        cal.add(calendarUnit, 1);
        final File f8 = new File(oozieLogPath, oozieLogName + formatDateForFilename(cal, calendarUnit));
        cal.add(calendarUnit, 15);
        f8.createNewFile();
        f8.setLastModified(cal.getTimeInMillis());

        // Test that it ignores "other" files even if they are oldest and test that it uses the modified time for non .gz files
        // (instead of the time from the filename)
        orp.isTriggeringEvent(null, null, null, 0);
        waitFor(60 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return (f0.exists() && !f1.exists() && !f2.exists() && !f3.exists() && f4.exists() && f5.exists() && !f6.exists() &&
                        f7.exists() && f8.exists());
            }
        });
        assertTrue(f0.exists() && !f1.exists() && !f2.exists() && !f3.exists() && f4.exists() && f5.exists() && !f6.exists() &&
                   f7.exists() && f8.exists());
    }


    private String formatDateForFilename(Calendar cal, int calendarUnit) {
        int year = cal.get(Calendar.YEAR);
        int month = cal.get(Calendar.MONTH) + 1;
        int date = cal.get(Calendar.DATE);
        int hour = cal.get(Calendar.HOUR_OF_DAY);

        StringBuilder sb = new StringBuilder("-");
        if (year < 10) {
            sb.append("000");
        }
        else if (year < 100) {
            sb.append("00");
        }
        else if (year < 1000) {
            sb.append("0");
        }
        sb.append(year);
        sb.append("-");
        if (month < 10) {
            sb.append("0");
        }
        sb.append(month);
        sb.append("-");
        if (date < 10) {
            sb.append("0");
        }
        sb.append(date);
        if (calendarUnit == Calendar.HOUR_OF_DAY) {
            sb.append("-");
            if (hour < 10) {
                sb.append("0");
            }
            sb.append(hour);

        }
        return sb.toString();
    }

}
