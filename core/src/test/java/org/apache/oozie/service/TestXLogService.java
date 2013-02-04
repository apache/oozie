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
package org.apache.oozie.service;

import junit.framework.Assert;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.LogManager;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XLog;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Properties;
import org.apache.oozie.util.XLogStreamer;

public class TestXLogService extends XTestCase {

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

    public void testDefaultLog4jPropertiesFromClassLoader() throws Exception {
        XLogService ls = new XLogService();
        ls.init(null);
        Assert.assertTrue(ls.getFromClasspath());
        Assert.assertEquals(XLogService.DEFAULT_LOG4J_PROPERTIES, ls.getLog4jProperties());
        ls.destroy();
    }

    public void testCustomLog4jPropertiesFromClassLoader() throws Exception {
        setSystemProperty(XLogService.LOG4J_FILE, "test-custom-log4j.properties");
        XLogService ls = new XLogService();
        ls.init(null);
        Assert.assertTrue(ls.getFromClasspath());
        Assert.assertEquals("test-custom-log4j.properties", ls.getLog4jProperties());
        ls.destroy();
    }

    public void testDefaultLog4jFromConfigDir() throws Exception {
        File log4jFile = new File(getTestCaseConfDir(), XLogService.DEFAULT_LOG4J_PROPERTIES);
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream is = cl.getResourceAsStream("test-oozie-log4j.properties");
        IOUtils.copyStream(is, new FileOutputStream(log4jFile));
        XLogService ls = new XLogService();
        ls.init(null);
        Assert.assertFalse(ls.getFromClasspath());
        Assert.assertEquals(XLogService.DEFAULT_LOG4J_PROPERTIES, ls.getLog4jProperties());
        ls.destroy();
    }

    public void testCustomLog4jFromConfigDir() throws Exception {
        File log4jFile = new File(getTestCaseConfDir(), "test-log4j.properties");
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream is = cl.getResourceAsStream("test-oozie-log4j.properties");
        IOUtils.copyStream(is, new FileOutputStream(log4jFile));
        setSystemProperty(XLogService.LOG4J_FILE, "test-log4j.properties");
        XLogService ls = new XLogService();
        ls.init(null);
        Assert.assertFalse(ls.getFromClasspath());
        Assert.assertEquals("test-log4j.properties", ls.getLog4jProperties());
        ls.destroy();
    }

    public void testLog4jReload() throws Exception {
        File log4jFile = new File(getTestCaseConfDir(), XLogService.DEFAULT_LOG4J_PROPERTIES);
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream is = cl.getResourceAsStream("test-oozie-log4j.properties");
        IOUtils.copyStream(is, new FileOutputStream(log4jFile));
        setSystemProperty(XLogService.LOG4J_RELOAD, "1");
        XLogService ls = new XLogService();
        ls.init(null);
        assertTrue(LogFactory.getLog("a").isTraceEnabled());
        sleep(1 * 1000);
        is = cl.getResourceAsStream("test-custom-log4j.properties");
        IOUtils.copyStream(is, new FileOutputStream(log4jFile));
        float originalRatio = XTestCase.WAITFOR_RATIO;
        try {
            XTestCase.WAITFOR_RATIO = 1;

            waitFor(5 * 1000, new Predicate() {
                public boolean evaluate() throws Exception {
                    return !LogFactory.getLog("a").isTraceEnabled();
                }
            });
            assertFalse(LogFactory.getLog("a").isTraceEnabled());
        }
        finally {
            XTestCase.WAITFOR_RATIO = originalRatio;
        }
        ls.destroy();
    }

    public void testInfoParameters() throws Exception {
        XLogService ls = new XLogService();
        ls.init(null);
        XLog.Info.get().clear();
        assertEquals("USER[-] GROUP[-]", XLog.Info.get().createPrefix());
        ls.destroy();
    }

    public void testDefaultLogsDir() throws Exception {
        setSystemProperty(XLogService.OOZIE_LOG_DIR, null);
        String logs = Services.getOozieHome() + "/logs";
        XLogService ls = new XLogService();
        ls.init(null);
        assertEquals(logs, System.getProperty(XLogService.OOZIE_LOG_DIR));
        ls.destroy();
    }

    public void testCustomLogsDir() throws Exception {
        String logs = "/tmp/oozie/logs";
        setSystemProperty(XLogService.OOZIE_LOG_DIR, logs);
        XLogService ls = new XLogService();
        ls.init(null);
        assertEquals(logs, System.getProperty(XLogService.OOZIE_LOG_DIR));
        ls.destroy();
    }
    
    public void testDisableLogOverWS() throws Exception {
        Properties props = new Properties();
        // Test missing logfile
        props.setProperty("log4j.appender.oozie.File", "");
        File propsFile = new File(getTestCaseConfDir(), "test-disable-log-over-ws-log4j.properties");
        FileOutputStream fos = new FileOutputStream(propsFile);
        props.store(fos, "");
        setSystemProperty(XLogService.LOG4J_FILE, propsFile.getName());
        XLogService ls = new XLogService();
        ls.init(null);
        assertTrue(doStreamDisabledCheck(ls));
        ls.destroy();
        
        // Test non-absolute path for logfile
        props.setProperty("log4j.appender.oozie.File", "oozie.log");
        fos = new FileOutputStream(propsFile);
        props.store(fos, "");
        ls = new XLogService();
        ls.init(null);
        assertTrue(doStreamDisabledCheck(ls));
        ls.destroy();
        
        // Test missing appender class
        props.setProperty("log4j.appender.oozie.File", "${oozie.log.dir}/oozie.log");
        props.setProperty("log4j.appender.oozie", "");
        fos = new FileOutputStream(propsFile);
        props.store(fos, "");
        ls = new XLogService();
        ls.init(null);
        assertTrue(doStreamDisabledCheck(ls));
        ls.destroy();
        
        // Test appender class not DailyRollingFileAppender or RollingFileAppender
        props.setProperty("log4j.appender.oozie", "org.blah.blah");
        fos = new FileOutputStream(propsFile);
        props.store(fos, "");
        ls = new XLogService();
        ls.init(null);
        assertTrue(doStreamDisabledCheck(ls));
        ls.destroy();
        
        // Test DailyRollingFileAppender but missing DatePattern
        props.setProperty("log4j.appender.oozie", "org.apache.log4j.DailyRollingFileAppender");
        props.setProperty("log4j.appender.oozie.DatePattern", "");
        fos = new FileOutputStream(propsFile);
        props.store(fos, "");
        ls = new XLogService();
        ls.init(null);
        assertTrue(doStreamDisabledCheck(ls));
        ls.destroy();
        
        // Test DailyRollingFileAppender but DatePattern that doesn't end with 'HH' or 'dd'
        props.setProperty("log4j.appender.oozie.DatePattern", "'.'yyyy-MM");
        fos = new FileOutputStream(propsFile);
        props.store(fos, "");
        ls = new XLogService();
        ls.init(null);
        assertTrue(doStreamDisabledCheck(ls));
        ls.destroy();
        
        // Test DailyRollingFileAppender with everything correct (dd)
        props.setProperty("log4j.appender.oozie.DatePattern", "'.'yyyy-MM-dd");
        fos = new FileOutputStream(propsFile);
        props.store(fos, "");
        ls = new XLogService();
        ls.init(null);
        assertFalse(doStreamDisabledCheck(ls));
        ls.destroy();
        
        // Test DailyRollingFileAppender with everything correct (HH)
        props.setProperty("log4j.appender.oozie.DatePattern", "'.'yyyy-MM-dd-HH");
        fos = new FileOutputStream(propsFile);
        props.store(fos, "");
        ls = new XLogService();
        ls.init(null);
        assertFalse(doStreamDisabledCheck(ls));
        ls.destroy();
        
        // Test RollingFileAppender but missing FileNamePattern
        props.setProperty("log4j.appender.oozie", "org.apache.log4j.rolling.RollingFileAppender");
        props.setProperty("log4j.appender.oozie.RollingPolicy.FileNamePattern", "");
        fos = new FileOutputStream(propsFile);
        props.store(fos, "");
        ls = new XLogService();
        ls.init(null);
        assertTrue(doStreamDisabledCheck(ls));
        ls.destroy();
        
        // Test RollingFileAppender but FileNamePattern with incorrect ending
        props.setProperty("log4j.appender.oozie.RollingPolicy.FileNamePattern", "${oozie.log.dir}/oozie.log-blah");
        fos = new FileOutputStream(propsFile);
        props.store(fos, "");
        ls = new XLogService();
        ls.init(null);
        assertTrue(doStreamDisabledCheck(ls));
        ls.destroy();
        
        // Test RollingFileAppender but FileNamePattern with incorrect beginning
        props.setProperty("log4j.appender.oozie.RollingPolicy.FileNamePattern", "${oozie.log.dir}/blah.log-%d{yyyy-MM-dd-HH}");
        fos = new FileOutputStream(propsFile);
        props.store(fos, "");
        ls = new XLogService();
        ls.init(null);
        assertTrue(doStreamDisabledCheck(ls));
        ls.destroy();
        
        // Test RollingFileAppender with everything correct
        props.setProperty("log4j.appender.oozie.RollingPolicy.FileNamePattern", "${oozie.log.dir}/oozie.log-%d{yyyy-MM-dd-HH}");
        fos = new FileOutputStream(propsFile);
        props.store(fos, "");
        ls = new XLogService();
        ls.init(null);
        assertFalse(doStreamDisabledCheck(ls));
        ls.destroy();
        
        // Test RollingFileAppender with everything correct (gz)
        props.setProperty("log4j.appender.oozie.RollingPolicy.FileNamePattern", "${oozie.log.dir}/oozie.log-%d{yyyy-MM-dd-HH}.gz");
        fos = new FileOutputStream(propsFile);
        props.store(fos, "");
        ls = new XLogService();
        ls.init(null);
        assertFalse(doStreamDisabledCheck(ls));
        ls.destroy();
    }

    public void testNoDashInConversionPattern() throws Exception{
        XLogStreamer.Filter.reset();
        XLogStreamer.Filter.defineParameter("USER");
        XLogStreamer.Filter.defineParameter("GROUP");
        XLogStreamer.Filter.defineParameter("TOKEN");
        XLogStreamer.Filter.defineParameter("APP");
        XLogStreamer.Filter.defineParameter("JOB");
        XLogStreamer.Filter.defineParameter("ACTION");
        XLogStreamer.Filter xf = new XLogStreamer.Filter();
        xf.setParameter("USER", "oozie");
        xf.setLogLevel("DEBUG|INFO");
        // Previously, a dash ("-") was always required somewhere in a line in order for that line to pass the filter; this test
        // checks that this condition is no longer required for log streaming to work
        File log4jFile = new File(getTestCaseConfDir(), "test-log4j.properties");
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream is = cl.getResourceAsStream("test-no-dash-log4j.properties");
        IOUtils.copyStream(is, new FileOutputStream(log4jFile));
        setSystemProperty(XLogService.LOG4J_FILE, "test-log4j.properties");
        final XLogService ls = new XLogService();
        ls.init(null);
        assertFalse((doStreamDisabledCheck(ls)));
        LogFactory.getLog("a").info("2009-06-24 02:43:14,505 INFO _L1_:317 - USER[oozie] GROUP[oozie] TOKEN[-] APP[-] JOB[-] "
                + "ACTION[-] Released Lock");
        LogFactory.getLog("a").info("2009-06-24 02:43:14,505 INFO _L2_:317 - USER[blah] GROUP[oozie] TOKEN[-] APP[-] JOB[-] "
                + "ACTION[-] Released Lock");
        LogFactory.getLog("a").info("2009-06-24 02:43:14,505 INFO _L3_:317 USER[oozie] GROUP[oozie] TOKEN[-] APP[-] JOB[-] "
                + "ACTION[-] Released Lock");
        LogFactory.getLog("a").info("2009-06-24 02:43:14,505 INFO _L4_:317 USER[blah] GROUP[oozie] TOKEN[-] APP[-] JOB[-] "
                + "ACTION[-] Released Lock");
        String out = doStreamLog(ls, xf);
        String outArr[] = out.split("\n");
        // Lines 2 and 4 are filtered out because they have the wrong user
        assertEquals(2, outArr.length);
        assertTrue(outArr[0].contains("_L1_"));
        assertFalse(out.contains("_L2_"));
        assertTrue(outArr[1].contains("_L3_"));
        assertFalse(out.contains("_L4_"));
        ls.destroy();
    }

    private boolean doStreamDisabledCheck(XLogService ls) throws Exception {
        if (doStreamLog(ls).equals("Log streaming disabled!!")) {
            return true;
        }
        return false;
    }

    private String doStreamLog(XLogService ls) throws Exception {
        return doStreamLog(ls, null);
    }

    private String doStreamLog(XLogService ls, XLogStreamer.Filter xf) throws Exception {
        StringWriter w = new StringWriter();
        ls.streamLog(xf, null, null, w);
        return w.toString();
    }
}
