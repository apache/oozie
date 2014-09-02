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
import java.util.Calendar;
import java.util.GregorianCalendar;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.LogManager;
import org.apache.oozie.service.XLogService;
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
        // OozieRollingPolicy gets the log path and log name from XLogService by calling Services.get.get(XLogService.class) so we
        // use a mock version where we overwrite the XLogService.getOozieLogName() and XLogService.getOozieLogPath() to simply 
        // return these values instead of involving Services.  We then overwrite OozieRollingPolicy.getXLogService() to return the 
        // mock one instead.
        String oozieLogPath = getTestCaseDir();
        String oozieLogName = "oozie.log";
        final XLogService xls = new MockXLogService(oozieLogPath, oozieLogName);
        OozieRollingPolicy orp = new OozieRollingPolicy() {

            @Override
            XLogService getXLogService() {
                return xls;
            }
            
        };
        orp.setMaxHistory(3);   // only keep 3 newest logs
        
        Calendar cal = new GregorianCalendar();
        final File f0 = new File(oozieLogPath, oozieLogName);
        f0.createNewFile();
        f0.setLastModified(cal.getTimeInMillis());
        cal.add(Calendar.HOUR_OF_DAY, 1);
        final File f1 = new File(oozieLogPath, oozieLogName + formatDateForFilename(cal) + ".gz");
        f1.createNewFile();
        cal.add(Calendar.HOUR_OF_DAY, 1);
        final File f2 = new File(oozieLogPath, oozieLogName + formatDateForFilename(cal) + ".gz");
        f2.createNewFile();
        cal.add(Calendar.HOUR_OF_DAY, 1);
        final File f3 = new File(oozieLogPath, oozieLogName + formatDateForFilename(cal) + ".gz");
        f3.createNewFile();
        cal.add(Calendar.HOUR_OF_DAY, 1);
        final File f4 = new File(oozieLogPath, oozieLogName + formatDateForFilename(cal) + ".gz");
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
        
        cal.add(Calendar.HOUR_OF_DAY, 1);
        final File f5 = new File(oozieLogPath, oozieLogName + formatDateForFilename(cal));
        f5.createNewFile();
        f5.setLastModified(cal.getTimeInMillis());
        
        cal.add(Calendar.HOUR_OF_DAY, -15);
        final File f6 = new File(oozieLogPath, oozieLogName + formatDateForFilename(cal));
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
        
        cal.add(Calendar.HOUR_OF_DAY, 1);
        final File f8 = new File(oozieLogPath, oozieLogName + formatDateForFilename(cal));
        cal.add(Calendar.HOUR_OF_DAY, 15);
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
    
    private String formatDateForFilename(Calendar cal) {
        int year = cal.get(Calendar.YEAR);
        int month = cal.get(Calendar.MONTH) + 1;
        int date = cal.get(Calendar.DATE);
        int hour = cal.get(Calendar.HOUR_OF_DAY);
        
        StringBuilder sb = new StringBuilder("-");
        if (year < 10) {
            sb.append("000");
        } else if (year < 100) {
            sb.append("00");
        } else if (year < 1000) {
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
        sb.append("-");
        if (hour < 10) {
            sb.append("0");
        }
        sb.append(hour);
        
        return sb.toString();
    }
    
    class MockXLogService extends XLogService {

        private String logPath;
        private String logName;
        
        public MockXLogService(String lp, String ln) {
            logPath = lp;
            logName = ln;
        }
        
        @Override
        public String getOozieLogName() {
            return logName;
        }

        @Override
        public String getOozieLogPath() {
            return logPath;
        }
    }
}
