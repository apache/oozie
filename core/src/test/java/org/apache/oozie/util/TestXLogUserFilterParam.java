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
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.XLogService;
import org.apache.oozie.service.XLogStreamingService;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.XLogFilter;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XLogUserFilterParam;

public class TestXLogUserFilterParam extends XTestCase {

    final static SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void setLogFile() throws Exception {
        XLogFilter.reset();
        File log4jFile = new File(getTestCaseConfDir(), "test-log4j.properties");
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream is = cl.getResourceAsStream("test-no-dash-log4j.properties");

        InputStream errorLogStream = cl.getResourceAsStream("userLogFilterTestlog.log");

        Properties log4jProps = new Properties();
        log4jProps.load(is);
        // prevent conflicts with other tests by changing the log file location
        log4jProps.setProperty("log4j.appender.oozie.File", getTestCaseDir() + "/oozie.log");
        log4jProps.store(new FileOutputStream(log4jFile), "");
        setSystemProperty(XLogService.LOG4J_FILE, log4jFile.getName());

        new Services().init();
        File logFile = new File(Services.get().get(XLogService.class).getOozieLogPath(), Services.get()
                .get(XLogService.class).getOozieLogName());
        IOUtils.copyStream(errorLogStream, new FileOutputStream(logFile));
    }

    public void testloglevel_Error() throws Exception {
        setLogFile();
        String logLevel = XLogUserFilterParam.LOG_LEVEL + "=ERROR";
        Map<String, String[]> paramMap = new HashMap<String, String[]>();
        paramMap.put(RestConstants.LOG_FILTER_OPTION, new String[] { logLevel });

        XLogFilter xf = new XLogFilter(new XLogUserFilterParam(paramMap));
        String out = doStreamLog(xf);
        int count = 0;
        for (String line : out.split(System.getProperty("line.separator"))) {
            assertFalse(line.contains("DEBUG"));
            assertFalse(line.contains("INFO"));
            count++;
        }
        assertEquals(count, 20);
    }

    // Test multiple log level
    public void testloglevel_DEBUF_INFO() throws Exception {
        setLogFile();

        String logLevel = XLogUserFilterParam.LOG_LEVEL + "=DEBUG|INFO";
        Map<String, String[]> paramMap = new HashMap<String, String[]>();
        paramMap.put(RestConstants.LOG_FILTER_OPTION, new String[] { logLevel });
        XLogFilter xf = new XLogFilter(new XLogUserFilterParam(paramMap));
        String out = doStreamLog(xf);

        String lines[] = out.split(System.getProperty("line.separator"));

        assertEquals(lines.length, 4);

        assertTrue(lines[1]
                .startsWith("2014-02-27 02:06:49,215 DEBUG CoordActionStartXCommand:545 [pool-2-thread-236] - USER[-] GROUP[-] "
                        + "TOKEN[-] APP[-]"));
        assertTrue(lines[2]
                .startsWith("2014-02-27 02:06:49,215 DEBUG CoordActionStartXCommand:545 [pool-2-thread-236] - USER[-] GROUP[-] "
                        + "TOKEN[-] APP[-] JOB[0601839-140127221758655-oozie-wrkf-C]"));
        assertTrue(lines[3]
                .startsWith("2014-02-27 02:06:49,215 INFO CoordActionStartXCommand:545 [pool-2-thread-236] - USER[-] GROUP[-] "
                        + "TOKEN[-] APP[-] JOB[0601839-140127221758655-oozie-wrkf-C]"));

    }

    // test log level with length
    public void testloglevel_ErrorWithLen() throws Exception {
        setLogFile();
        String logLevel = XLogUserFilterParam.LOG_LEVEL + "=ERROR;" + XLogUserFilterParam.LIMIT + "=1";
        Map<String, String[]> paramMap = new HashMap<String, String[]>();
        paramMap.put(RestConstants.LOG_FILTER_OPTION, new String[] { logLevel });

        XLogFilter xf = new XLogFilter(new XLogUserFilterParam(paramMap));
        String out = doStreamLog(xf);
        String lines[] = out.split(System.getProperty("line.separator"));

        assertEquals(lines.length, 1);
    }

    // test length
    public void testLength() throws Exception {
        setLogFile();

        String logLevel = XLogUserFilterParam.LIMIT + "=1";
        Map<String, String[]> paramMap = new HashMap<String, String[]>();
        paramMap.put(RestConstants.LOG_FILTER_OPTION, new String[] { logLevel });
        XLogFilter xf = new XLogFilter(new XLogUserFilterParam(paramMap));
        String out = doStreamLog(xf);
        String lines[] = out.split(System.getProperty("line.separator"));

        assertEquals(lines.length, 1);
    }

    // Test text search
    public void testTextSearch() throws Exception {
        setLogFile();
        XLogFilter filter = new XLogFilter();

        String param = XLogUserFilterParam.SEARCH_TEXT + "=substitution;" + XLogUserFilterParam.LIMIT + "=2";
        Map<String, String[]> paramMap = new HashMap<String, String[]>();
        paramMap.put(RestConstants.LOG_FILTER_OPTION, new String[] { param });

        XLogUserFilterParam logUtil = new XLogUserFilterParam(paramMap);
        filter.setUserLogFilter(logUtil);
        String out = doStreamLog(filter);
        String lines[] = out.split(System.getProperty("line.separator"));
        assertEquals(lines.length, 2);

        assertTrue(lines[0].contains("E0803: IO error, Variable substitution depth too large: 20 ${dniInputDir}"));
        assertTrue(lines[1].contains("E0803: IO error, Variable substitution depth too large: 20 ${dniInputDir}"));

    }

    // Test text search with log level
    public void testsearchText_logLevel() throws Exception {
        setLogFile();
        String param = XLogUserFilterParam.SEARCH_TEXT + "=substitution;" + XLogUserFilterParam.LOG_LEVEL + "=DEBUG";
        Map<String, String[]> paramMap = new HashMap<String, String[]>();
        paramMap.put(RestConstants.LOG_FILTER_OPTION, new String[] { param });

        XLogFilter filter = new XLogFilter(new XLogUserFilterParam(paramMap));
        String out = doStreamLog(filter);
        String lines[] = out.split(System.getProperty("line.separator"));
        assertEquals(lines.length, 1);

        assertTrue(lines[0]
                .contains("2014-02-27 02:06:47,499 DEBUG CoordActionStartXCommand:536 [pool-2-thread-236] - USER[-]"));
        assertTrue(lines[0].contains("E0803: IO error, Variable substitution depth too large: 20 ${dniInputDir}"));
    }

    // Test logduration, out of range
    public void testException() throws Exception {
        File log4jFile = new File(getTestCaseConfDir(), "test-log4j.properties");
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream is = cl.getResourceAsStream("test-no-dash-log4j.properties");

        Properties log4jProps = new Properties();
        log4jProps.load(is);
        // prevent conflicts with other tests by changing the log file location
        log4jProps.setProperty("log4j.appender.oozie.File", getTestCaseDir() + "/oozie.log");
        log4jProps.store(new FileOutputStream(log4jFile), "");
        setSystemProperty(XLogService.LOG4J_FILE, log4jFile.getName());

        new Services().init();
        Services.get().getConf().setInt(XLogFilter.MAX_SCAN_DURATION, 10);
        Map<String, String[]> paramMap = new HashMap<String, String[]>();
        paramMap.put(RestConstants.LOG_FILTER_OPTION, new String[] {});
        XLogFilter filter = new XLogFilter(new XLogUserFilterParam(paramMap));
        Date startDate = new Date();
        Date endDate = new Date(startDate.getTime() + 60 * 60 * 1000 * 11);

        try {
            doStreamLog(filter, startDate, endDate);
            fail("should not come here");
        }
        catch (Exception e) {
            e.printStackTrace();
            assertTrue(e.getMessage().contains(
                    "Request log streaming time range is higher than configured."));
        }
    }

    // Test log duration - in range
    public void testNoException_Withrecent() throws Exception {
        File log4jFile = new File(getTestCaseConfDir(), "test-log4j.properties");
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream is = cl.getResourceAsStream("test-no-dash-log4j.properties");

        Properties log4jProps = new Properties();
        log4jProps.load(is);
        // prevent conflicts with other tests by changing the log file location
        log4jProps.setProperty("log4j.appender.oozie.File", getTestCaseDir() + "/oozie.log");
        log4jProps.store(new FileOutputStream(log4jFile), "");
        setSystemProperty(XLogService.LOG4J_FILE, log4jFile.getName());

        new Services().init();
        Services.get().getConf().setInt(XLogFilter.MAX_SCAN_DURATION, 10);
        Map<String, String[]> paramMap = new HashMap<String, String[]>();
        String param = XLogUserFilterParam.RECENT_LOG_OFFSET + "=9";

        paramMap.put(RestConstants.LOG_FILTER_OPTION, new String[] { param });
        XLogFilter filter = new XLogFilter(new XLogUserFilterParam(paramMap));
        Date startDate = new Date();
        Date endDate = new Date(startDate.getTime() + 60 * 60 * 1000 * 15);

        try {
            doStreamLog(filter, startDate, endDate);
        }
        catch (Exception e) {
            fail("should not throw exception");
        }

    }

    // Test multiple combination
    public void testCombination() throws Exception {
        File log4jFile = new File(getTestCaseConfDir(), "test-log4j.properties");
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream is = cl.getResourceAsStream("test-no-dash-log4j.properties");

        Properties log4jProps = new Properties();
        log4jProps.load(is);
        log4jProps.setProperty("log4j.appender.oozie.File", getTestCaseDir() + "/oozie.log");
        log4jProps.store(new FileOutputStream(log4jFile), "");
        setSystemProperty(XLogService.LOG4J_FILE, log4jFile.getName());

        new Services().init();

        XLogFilter.reset();

        Map<String, String[]> paramMap = new HashMap<String, String[]>();
        String param = "start=14-02-20 02:06:25,499;end=14-02-27 02:06:47,550;debug;loglevel=ERROR|WARN;recent=3m";
        paramMap.put(RestConstants.LOG_FILTER_OPTION, new String[] { param });
        XLogFilter filter = new XLogFilter(new XLogUserFilterParam(paramMap));
        // Param date will be overwritten by user param
        String out = doStreamLog(filter, new Date(), new Date());
        assertEquals(out.split(System.getProperty("line.separator")).length, 1);
        assertTrue(out.split(System.getProperty("line.separator"))[0].contains("Log start time = Tue Feb 27 02:03:47"));
        assertTrue(out.split(System.getProperty("line.separator"))[0].contains("Log end time = Tue Feb 27 02:06:47"));

    }

    // Test start and end date, start as absolute
    public void testStartEnd_EndOffset() throws Exception {
        File log4jFile = new File(getTestCaseConfDir(), "test-log4j.properties");
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream is = cl.getResourceAsStream("test-no-dash-log4j.properties");

        Properties log4jProps = new Properties();
        log4jProps.load(is);
        // prevent conflicts with other tests by changing the log file location
        log4jProps.setProperty("log4j.appender.oozie.File", getTestCaseDir() + "/oozie.log");
        log4jProps.store(new FileOutputStream(log4jFile), "");
        setSystemProperty(XLogService.LOG4J_FILE, log4jFile.getName());

        new Services().init();
        XLogFilter.reset();
        Map<String, String[]> paramMap = new HashMap<String, String[]>();
        String param = "start=14-02-20 02:06:25,499;end=3m;debug";

        paramMap.put(RestConstants.LOG_FILTER_OPTION, new String[] { param });

        XLogFilter filter = new XLogFilter(new XLogUserFilterParam(paramMap));
        // Param date will be overwritten by user param
        String out = doStreamLog(filter, dt.parse("14-02-20 02:06:25,499"), new Date());
        assertEquals(out.split(System.getProperty("line.separator")).length, 1);
        assertTrue(out.split(System.getProperty("line.separator"))[0].contains("Log start time = Tue Feb 20 02:06:25"));
        assertTrue(out.split(System.getProperty("line.separator"))[0].contains("Log end time = Tue Feb 20 02:11:25"));

    }

    // Test start and end date, both offset
    public void testStartEnd_bothOffset() throws Exception {
        File log4jFile = new File(getTestCaseConfDir(), "test-log4j.properties");
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream is = cl.getResourceAsStream("test-no-dash-log4j.properties");

        Properties log4jProps = new Properties();
        log4jProps.load(is);
        // prevent conflicts with other tests by changing the log file location
        log4jProps.setProperty("log4j.appender.oozie.File", getTestCaseDir() + "/oozie.log");
        log4jProps.store(new FileOutputStream(log4jFile), "");
        setSystemProperty(XLogService.LOG4J_FILE, log4jFile.getName());
        new Services().init();
        Map<String, String[]> paramMap = new HashMap<String, String[]>();
        String param = "start=3m;end=13m;debug";

        paramMap.put(RestConstants.LOG_FILTER_OPTION, new String[] { param });

        XLogFilter filter = new XLogFilter(new XLogUserFilterParam(paramMap));
        // Param date will be overwritten by user param
        String out = doStreamLog(filter, dt.parse("14-02-20 02:06:25,499"), new Date());
        assertEquals(out.split(System.getProperty("line.separator")).length, 1);
        assertTrue(out.split(System.getProperty("line.separator"))[0].contains("Log start time = Tue Feb 20 02:07:25"));
        assertTrue(out.split(System.getProperty("line.separator"))[0].contains("Log end time = Tue Feb 20 02:21:25"));

    }

    // Test start and end date, both absolute
    public void testStartEnd_bothabsoulte() throws Exception {
        File log4jFile = new File(getTestCaseConfDir(), "test-log4j.properties");
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream is = cl.getResourceAsStream("test-no-dash-log4j.properties");

        Properties log4jProps = new Properties();
        log4jProps.load(is);
        // prevent conflicts with other tests by changing the log file location
        log4jProps.setProperty("log4j.appender.oozie.File", getTestCaseDir() + "/oozie.log");
        log4jProps.store(new FileOutputStream(log4jFile), "");
        setSystemProperty(XLogService.LOG4J_FILE, log4jFile.getName());
        new Services().init();

        Map<String, String[]> paramMap = new HashMap<String, String[]>();
        String param = "start=14-03-20 02:06:25,499;end=14-03-20 02:10:25,499;debug";
        paramMap.put(RestConstants.LOG_FILTER_OPTION, new String[] { param });
        XLogFilter filter = new XLogFilter(new XLogUserFilterParam(paramMap));
        // Param date will be overwritten by user param
        String out = doStreamLog(filter, dt.parse("14-01-20 02:06:25,499"), dt.parse("14-02-20 02:06:25,499"));
        assertEquals(out.split(System.getProperty("line.separator")).length, 1);
        assertTrue(out.split(System.getProperty("line.separator"))[0].contains("Log start time = Tue Mar 20 02:06:25"));
        assertTrue(out.split(System.getProperty("line.separator"))[0].contains("Log end time = Tue Mar 20 02:10:25"));

        paramMap = new HashMap<String, String[]>();
        param = "start=14-03-20 02:06:25;end=14-03-20 02:10:25;debug";
        paramMap.put(RestConstants.LOG_FILTER_OPTION, new String[] { param });
        filter = new XLogFilter(new XLogUserFilterParam(paramMap));
        // Param date will be overwritten by user param
        out = doStreamLog(filter, dt.parse("14-01-20 02:06:25,499"), dt.parse("14-02-20 02:06:25,499"));
        assertEquals(out.split(System.getProperty("line.separator")).length, 1);
        assertTrue(out.split(System.getProperty("line.separator"))[0].contains("Log start time = Tue Mar 20 02:06:25"));
        assertTrue(out.split(System.getProperty("line.separator"))[0].contains("Log end time = Tue Mar 20 02:10:25"));


    }

    // Test start and end date, both absolute
    public void testStartEnd_startAbsolute() throws Exception {
        File log4jFile = new File(getTestCaseConfDir(), "test-log4j.properties");
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream is = cl.getResourceAsStream("test-no-dash-log4j.properties");

        Properties log4jProps = new Properties();
        log4jProps.load(is);
        // prevent conflicts with other tests by changing the log file location
        log4jProps.setProperty("log4j.appender.oozie.File", getTestCaseDir() + "/oozie.log");
        log4jProps.store(new FileOutputStream(log4jFile), "");
        setSystemProperty(XLogService.LOG4J_FILE, log4jFile.getName());
        new Services().init();
        Map<String, String[]> paramMap = new HashMap<String, String[]>();
        String param = "start=14-03-20 02:06:25,499;end=4m;debug";
        paramMap.put(RestConstants.LOG_FILTER_OPTION, new String[] { param });
        XLogFilter filter = new XLogFilter(new XLogUserFilterParam(paramMap));
        // Param date will be overwritten by user param
        String out = doStreamLog(filter, dt.parse("14-01-20 02:06:25,499"), dt.parse("14-02-20 02:06:25,499"));
        assertEquals(out.split(System.getProperty("line.separator")).length, 1);
        assertTrue(out.split(System.getProperty("line.separator"))[0].contains("Log start time = Tue Mar 20 02:06:25"));
        assertTrue(out.split(System.getProperty("line.separator"))[0].contains("Log end time = Tue Mar 20 02:12:25"));

    }

    private String doStreamLog(XLogFilter xf) throws Exception {
        return doStreamLog(xf, null, null);
    }

    private String doStreamLog(XLogFilter xf, Date startDate, Date endDate) throws Exception {
        StringWriter w = new StringWriter();
        Services.get().get(XLogStreamingService.class)
                .streamLog(xf, startDate, endDate, w, new HashMap<String, String[]>());
        return w.toString();
    }

}
