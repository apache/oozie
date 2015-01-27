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

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.LogFactory;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.test.EmbeddedServletContainer;
import org.apache.oozie.test.ZKXTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.XLogFilter;
import org.apache.oozie.util.ZKUtils;

public class TestZKXLogStreamingService extends ZKXTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testRegisterUnregister() throws Exception {
        assertEquals(0, ZKUtils.getUsers().size());
        ZKXLogStreamingService zkxlss = new ZKXLogStreamingService();
        try {
            zkxlss.init(Services.get());
            assertEquals(1, ZKUtils.getUsers().size());
            assertEquals(zkxlss, ZKUtils.getUsers().iterator().next());
            zkxlss.destroy();
            assertEquals(0, ZKUtils.getUsers().size());
        }
        finally {
           zkxlss.destroy();
        }
    }

    public void testDisableLogOverWS() throws Exception {
        Properties props = new Properties();
        // Test missing logfile
        props.setProperty("log4j.appender.oozie.File", "");
        File propsFile = new File(getTestCaseConfDir(), "test-disable-log-over-ws-log4j.properties");
        FileOutputStream fos = new FileOutputStream(propsFile);
        props.store(fos, "");
        setSystemProperty(XLogService.LOG4J_FILE, propsFile.getName());
        assertTrue(doStreamDisabledCheck());

        // Test non-absolute path for logfile
        props.setProperty("log4j.appender.oozie.File", "oozie.log");
        fos = new FileOutputStream(propsFile);
        props.store(fos, "");
        assertTrue(doStreamDisabledCheck());

        // Test missing appender class
        props.setProperty("log4j.appender.oozie.File", "${oozie.log.dir}/oozie.log");
        props.setProperty("log4j.appender.oozie", "");
        fos = new FileOutputStream(propsFile);
        props.store(fos, "");
        assertTrue(doStreamDisabledCheck());

        // Test appender class not DailyRollingFileAppender or RollingFileAppender
        props.setProperty("log4j.appender.oozie", "org.blah.blah");
        fos = new FileOutputStream(propsFile);
        props.store(fos, "");
        assertTrue(doStreamDisabledCheck());

        // Test DailyRollingFileAppender but missing DatePattern
        props.setProperty("log4j.appender.oozie", "org.apache.log4j.DailyRollingFileAppender");
        props.setProperty("log4j.appender.oozie.DatePattern", "");
        fos = new FileOutputStream(propsFile);
        props.store(fos, "");
        assertTrue(doStreamDisabledCheck());

        // Test DailyRollingFileAppender but DatePattern that doesn't end with 'HH' or 'dd'
        props.setProperty("log4j.appender.oozie.DatePattern", "'.'yyyy-MM");
        fos = new FileOutputStream(propsFile);
        props.store(fos, "");
        assertTrue(doStreamDisabledCheck());

        // Test DailyRollingFileAppender with everything correct (dd)
        props.setProperty("log4j.appender.oozie.DatePattern", "'.'yyyy-MM-dd");
        fos = new FileOutputStream(propsFile);
        props.store(fos, "");
        assertFalse(doStreamDisabledCheck());

        // Test DailyRollingFileAppender with everything correct (HH)
        props.setProperty("log4j.appender.oozie.DatePattern", "'.'yyyy-MM-dd-HH");
        fos = new FileOutputStream(propsFile);
        props.store(fos, "");
        assertFalse(doStreamDisabledCheck());

        // Test RollingFileAppender but missing FileNamePattern
        props.setProperty("log4j.appender.oozie", "org.apache.log4j.rolling.RollingFileAppender");
        props.setProperty("log4j.appender.oozie.RollingPolicy.FileNamePattern", "");
        fos = new FileOutputStream(propsFile);
        props.store(fos, "");
        assertTrue(doStreamDisabledCheck());

        // Test RollingFileAppender but FileNamePattern with incorrect ending
        props.setProperty("log4j.appender.oozie.RollingPolicy.FileNamePattern", "${oozie.log.dir}/oozie.log-blah");
        fos = new FileOutputStream(propsFile);
        props.store(fos, "");
        assertTrue(doStreamDisabledCheck());

        // Test RollingFileAppender but FileNamePattern with incorrect beginning
        props.setProperty("log4j.appender.oozie.RollingPolicy.FileNamePattern", "${oozie.log.dir}/blah.log-%d{yyyy-MM-dd-HH}");
        fos = new FileOutputStream(propsFile);
        props.store(fos, "");
        assertTrue(doStreamDisabledCheck());

        // Test RollingFileAppender with everything correct
        props.setProperty("log4j.appender.oozie.RollingPolicy.FileNamePattern", "${oozie.log.dir}/oozie.log-%d{yyyy-MM-dd-HH}");
        fos = new FileOutputStream(propsFile);
        props.store(fos, "");
        assertFalse(doStreamDisabledCheck());

        // Test RollingFileAppender with everything correct (gz)
        props.setProperty("log4j.appender.oozie.RollingPolicy.FileNamePattern", "${oozie.log.dir}/oozie.log-%d{yyyy-MM-dd-HH}.gz");
        fos = new FileOutputStream(propsFile);
        props.store(fos, "");
        assertFalse(doStreamDisabledCheck());
    }

    public void testNoDashInConversionPattern() throws Exception{
        XLogFilter.reset();
        XLogFilter.defineParameter("USER");
        XLogFilter.defineParameter("GROUP");
        XLogFilter.defineParameter("TOKEN");
        XLogFilter.defineParameter("APP");
        XLogFilter.defineParameter("JOB");
        XLogFilter.defineParameter("ACTION");
        XLogFilter xf = new XLogFilter();
        xf.setParameter("USER", "oozie");
        xf.setLogLevel("DEBUG|INFO");
        // Previously, a dash ("-") was always required somewhere in a line in order for that line to pass the filter; this test
        // checks that this condition is no longer required for log streaming to work
        File log4jFile = new File(getTestCaseConfDir(), "test-log4j.properties");
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream is = cl.getResourceAsStream("test-no-dash-log4j.properties");
        Properties log4jProps = new Properties();
        log4jProps.load(is);
        // prevent conflicts with other tests by changing the log file location
        log4jProps.setProperty("log4j.appender.oozie.File", getTestCaseDir() + "/oozie.log");
        log4jProps.store(new FileOutputStream(log4jFile), "");
        setSystemProperty(XLogService.LOG4J_FILE, log4jFile.getName());
        assertFalse(doStreamDisabledCheck());
        LogFactory.getLog("a").info("2009-06-24 02:43:14,505 INFO _L1_:317 - SERVER[foo] USER[oozie] GROUP[oozie] TOKEN[-] APP[-] "
                + "JOB[-] ACTION[-] Released Lock");
        LogFactory.getLog("a").info("2009-06-24 02:43:14,505 INFO _L2_:317 - SERVER[foo] USER[blah] GROUP[oozie] TOKEN[-] APP[-] "
                + "JOB[-] ACTION[-] Released Lock");
        LogFactory.getLog("a").info("2009-06-24 02:43:14,505 INFO _L3_:317 SERVER[foo] USER[oozie] GROUP[oozie] TOKEN[-] APP[-] "
                + "JOB[-] ACTION[-] Released Lock");
        LogFactory.getLog("a").info("2009-06-24 02:43:14,505 INFO _L4_:317 SERVER[foo] USER[blah] GROUP[oozie] TOKEN[-] APP[-] "
                + "JOB[-] ACTION[-] Released Lock");
        String out = doStreamLog(xf);
        String outArr[] = out.split("\n");
        // Lines 2 and 4 are filtered out because they have the wrong user
        assertEquals(2, outArr.length);
        assertTrue(outArr[0].contains("_L1_"));
        assertFalse(out.contains("_L2_"));
        assertTrue(outArr[1].contains("_L3_"));
        assertFalse(out.contains("_L4_"));
    }

    private boolean doStreamDisabledCheck() throws Exception {
        Services.get().get(XLogService.class).init(Services.get());
        return doStreamLog(new XLogFilter()).equals("Log streaming disabled!!");
    }

    protected String doStreamLog(XLogFilter xf) throws Exception {
        return doStreamLog(xf, new HashMap<String, String[]>());
    }

    protected String doStreamErrorLog(XLogFilter xf) throws Exception {
        return doStreamLog(xf, new HashMap<String, String[]>(), true);
    }

    protected String doStreamLog(XLogFilter xf, Map<String, String[]> param) throws Exception {
        return doStreamLog(xf, param, false);
    }

    protected String doStreamLog(XLogFilter xf, Map<String, String[]> param, boolean isErrorLog) throws Exception {
        StringWriter w = new StringWriter();
        ZKXLogStreamingService zkxlss = new ZKXLogStreamingService();
        try {
            Services services = Services.get();
            services.setService(ZKJobsConcurrencyService.class);
            zkxlss.init(services);
            sleep(1000); // Sleep to allow ZKUtils ServiceCache to update
            if (isErrorLog) {
                zkxlss.streamErrorLog(xf, null, null, w, param);
            }
            else {
                zkxlss.streamLog(xf, null, null, w, param);
            }
        }
        finally {
            zkxlss.destroy();
        }
        String wStr = w.toString();
        System.out.println("\ndoStreamLog:\n" + wStr + "\n");
        return wStr;
    }

    public void testStreamingWithMultipleOozieServers() throws Exception {
        XLogFilter.reset();
        XLogFilter.defineParameter("USER");
        XLogFilter.defineParameter("GROUP");
        XLogFilter.defineParameter("TOKEN");
        XLogFilter.defineParameter("APP");
        XLogFilter.defineParameter("JOB");
        XLogFilter.defineParameter("ACTION");
        XLogFilter xf = new XLogFilter();
        xf.setParameter("JOB", "0000003-130610102426873-oozie-rkan-W");
        xf.setLogLevel("WARN|INFO");
        File log4jFile = new File(getTestCaseConfDir(), "test-log4j.properties");
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream is = cl.getResourceAsStream("test-no-dash-log4j.properties");
        Properties log4jProps = new Properties();
        log4jProps.load(is);
        // prevent conflicts with other tests by changing the log file location
        log4jProps.setProperty("log4j.appender.oozie.File", getTestCaseDir() + "/oozie.log");
        log4jProps.store(new FileOutputStream(log4jFile), "");
        setSystemProperty(XLogService.LOG4J_FILE, log4jFile.getName());
        assertFalse(doStreamDisabledCheck());
        File logFile = new File(Services.get().get(XLogService.class).getOozieLogPath(),
                                Services.get().get(XLogService.class).getOozieLogName());
        logFile.getParentFile().mkdirs();
        FileWriter logWriter = new FileWriter(logFile);
        // local logs
        logWriter.append("2013-06-10 10:25:44,008 WARN HiveActionExecutor:542 SERVER[foo] USER[rkanter] GROUP[-] TOKEN[] "
                + "APP[hive-wf] JOB[0000003-130610102426873-oozie-rkan-W] ACTION[0000003-130610102426873-oozie-rkan-W@hive-node] "
                + "credentials is null for the action _L3_").append("\n")
                .append("2013-06-10 10:26:10,008 INFO HiveActionExecutor:539 SERVER[foo] USER[rkanter] GROUP[-] TOKEN[] "
                + "APP[hive-wf] JOB[0000003-130610102426873-oozie-rkan-W] ACTION[0000003-130610102426873-oozie-rkan-W@hive-node] "
                + "action completed, external ID [job_201306101021_0005] _L4_").append("\n")
                .append("2013-06-10 10:26:10,341 WARN ActionStartXCommand:542 USER[rkanter] GROUP[-] TOKEN[] "
                + "APP[hive-wf] JOB[0000003-130610102426873-oozie-rkan-W] ACTION[0000003-130610102426873-oozie-rkan-W@end] "
                + "[***0000003-130610102426873-oozie-rkan-W@end***]Action updated in DB! _L6_").append("\n");
        logWriter.close();
        // logs to be returned by another "Oozie server"
        DummyLogStreamingServlet.logs =
                "2013-06-10 10:25:43,575 WARN ActionStartXCommand:542 SERVER[foo] USER[rkanter] GROUP[-] TOKEN[] APP[hive-wf] "
                + "JOB[0000003-130610102426873-oozie-rkan-W] ACTION[0000003-130610102426873-oozie-rkan-W@:start:] "
                + "[***0000003-130610102426873-oozie-rkan-W@:start:***]Action status=DONE _L1_"
                + "\n"
                + "2013-06-10 10:25:43,575 WARN ActionStartXCommand:542 SERVER[foo] USER[rkanter] GROUP[-] TOKEN[] APP[hive-wf] "
                + "JOB[0000003-130610102426873-oozie-rkan-W] ACTION[0000003-130610102426873-oozie-rkan-W@:start:] "
                + "[***0000003-130610102426873-oozie-rkan-W@:start:***]Action updated in DB! _L2_"
                + "\n"
                + "2013-06-10 10:26:10,148 INFO HiveActionExecutor:539 SERVER[foo] USER[rkanter] GROUP[-] TOKEN[] APP[hive-wf] "
                + "JOB[0000003-130610102426873-oozie-rkan-W] ACTION[0000003-130610102426873-oozie-rkan-W@hive-node] action produced"
                + " output _L5_"
                + "\n"
                // a multiline message with a stack trace
                + "2013-06-10 10:26:30,202  WARN ActionStartXCommand:542 - SERVER[foo] USER[rkanter] GROUP[-] TOKEN[] APP[hive-wf] "
                + "JOB[0000003-130610102426873-oozie-rkan-W] ACTION[0000003-130610102426873-oozie-rkan-W@hive-node] Error starting "
                + "action [hive-node]. ErrorType [TRANSIENT], ErrorCode [JA009], Message [JA009: java.io.IOException: Unknown "
                + "protocol to name node: org.apache.hadoop.mapred.JobSubmissionProtocol _L7_\n"
                + "     at org.apache.hadoop.hdfs.server.namenode.NameNode.getProtocolVersion(NameNode.java:156) _L8_\n"
                + "     at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)_L9_\n"
                + "     at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1190) _L10_\n"
                + "     at org.apache.hadoop.ipc.Server$Handler.run(Server.java:1426) _L11_\n"
                + "] _L12_\n"
                + "org.apache.oozie.action.ActionExecutorException: JA009: java.io.IOException: Unknown protocol to name node: "
                + "org.apache.hadoop.mapred.JobSubmissionProtocol _L13_\n"
                + "     at org.apache.hadoop.hdfs.server.namenode.NameNode.getProtocolVersion(NameNode.java:156) _L14_\n"
                + "     at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method) _L15_\n"
                + "     at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:39) _L16_\n";

        String out = doStreamLog(xf);
        String[] outArr = out.split("\n");
        assertEquals(3, outArr.length);
        assertFalse(out.contains("_L1_"));
        assertFalse(out.contains("_L2_"));
        assertTrue(outArr[0].contains("_L3_"));
        assertTrue(outArr[1].contains("_L4_"));
        assertFalse(out.contains("_L5_"));
        assertTrue(outArr[2].contains("_L6_"));

        // We'll use a DummyZKOozie to create an entry in ZK and then set its url to an (unrelated) servlet that will simply return
        // some log messages
        DummyZKOozie dummyOozie = null;
        EmbeddedServletContainer container = new EmbeddedServletContainer("oozie");
        container.addServletEndpoint("/other-oozie-server/*", DummyLogStreamingServlet.class);
        try {
            container.start();
            dummyOozie = new DummyZKOozie("9876", container.getServletURL("/other-oozie-server/*"));

            DummyLogStreamingServlet.lastQueryString = null;
            out = doStreamLog(xf);
            outArr = out.split("\n");
            assertEquals(16, outArr.length);
            assertTrue(outArr[0].contains("_L1_"));
            assertTrue(outArr[1].contains("_L2_"));
            assertTrue(outArr[2].contains("_L3_"));
            assertTrue(outArr[3].contains("_L4_"));
            assertTrue(outArr[4].contains("_L5_"));
            assertTrue(outArr[5].contains("_L6_"));
            assertTrue(outArr[6].contains("_L7_"));
            assertTrue(outArr[7].contains("_L8_"));
            assertTrue(outArr[8].contains("_L9_"));
            assertTrue(outArr[9].contains("_L10_"));
            assertTrue(outArr[10].contains("_L11_"));
            assertTrue(outArr[11].contains("_L12_"));
            assertTrue(outArr[12].contains("_L13_"));
            assertTrue(outArr[13].contains("_L14_"));
            assertTrue(outArr[14].contains("_L15_"));
            assertTrue(outArr[15].contains("_L16_"));
            assertEquals("show=log&allservers=false", DummyLogStreamingServlet.lastQueryString);

            // If we stop the container but leave the DummyZKOozie running, it will simulate if that server is down but still has
            // info in ZK; we should be able to get the logs from other servers (in this case, this server) and a message about
            // which servers it couldn't reach
            container.stop();
            out = doStreamLog(xf);
            outArr = out.split("\n");
            assertEquals(6, outArr.length);
            assertTrue(outArr[0].startsWith("Unable"));
            assertEquals("9876", outArr[1].trim());
            assertEquals("", outArr[2]);
            assertFalse(out.contains("_L1_"));
            assertFalse(out.contains("_L2_"));
            assertTrue(outArr[3].contains("_L3_"));
            assertTrue(outArr[4].contains("_L4_"));
            assertFalse(out.contains("_L5_"));
            assertTrue(outArr[5].contains("_L6_"));
        }
        finally {
            if (dummyOozie != null) {
                dummyOozie.teardown();
            }
            container.stop();
        }
    }
    public void testStreamingWithMultipleOozieServers_coordActionList() throws Exception {
        XLogFilter.reset();

        File log4jFile = new File(getTestCaseConfDir(), "test-log4j.properties");
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream is = cl.getResourceAsStream("test-no-dash-log4j.properties");
        Properties log4jProps = new Properties();
        log4jProps.load(is);
        // prevent conflicts with other tests by changing the log file location
        log4jProps.setProperty("log4j.appender.oozie.File", getTestCaseDir() + "/oozie.log");
        log4jProps.store(new FileOutputStream(log4jFile), "");
        setSystemProperty(XLogService.LOG4J_FILE, log4jFile.getName());
        Services.get().get(XLogService.class).init(Services.get());

        File logFile = new File(Services.get().get(XLogService.class).getOozieLogPath(), Services.get()
                .get(XLogService.class).getOozieLogName());
        logFile.getParentFile().mkdirs();
        FileWriter logWriter = new FileWriter(logFile);
        // local logs
        StringBuffer bf = new StringBuffer();
        bf.append(
                "2014-02-06 00:26:56,126 DEBUG CoordActionInputCheckXCommand:545 [pool-2-thread-26] - USER[-] GROUP[-] "
                     + "TOKEN[-] APP[-] JOB[0000003-140205233038063-oozie-oozi-C] ACTION[0000003-140205233038063-oozie-oozi-C@1] "
                     + "checking for the file ~:8020/user/purushah/examples/input-data/rawLogs/2010/01/01/01/00/_SUCCESS\n")
                .append("2014-02-06 00:26:56,150  INFO CoordActionInputCheckXCommand:539 [pool-2-thread-26] - USER[-] GROUP[-] "
                     + "TOKEN[-] APP[-] JOB[0000003-140205233038063-oozie-oozi-C] ACTION[0000003-140205233038063-oozie-oozi-C@1] "
                     + "[0000003-140205233038063-oozie-oozi-C@1]::ActionInputCheck:: File::8020/user/purushah/examples/input-data/"
                     + "rawLogs/2010/01/01/01/00/_SUCCESS, Exists? :false" + "Action updated in DB! _L1_")
                .append("\n")
                .append("2014-02-06 00:27:56,126 DEBUG CoordActionInputCheckXCommand:545 [pool-2-thread-26] - USER[-] GROUP[-] "
                     + "TOKEN[-] APP[-] JOB[0000003-140205233038063-oozie-oozi-C] ACTION[0000003-140205233038063-oozie-oozi-C@2] "
                     + "checking for the file ~:8020/user/purushah/examples/input-data/rawLogs/2010/01/01/01/00/_SUCCESS\n")
                .append("2014-02-06 00:27:56,150  INFO CoordActionInputCheckXCommand:539 [pool-2-thread-26] - USER[-] GROUP[-] "
                     + "TOKEN[-] APP[-] JOB[0000003-140205233038063-oozie-oozi-C] ACTION[0000003-140205233038063-oozie-oozi-C@2] "
                     + "[0000003-140205233038063-oozie-oozi-C@2]::ActionInputCheck:: File::8020/user/purushah/examples/input-data/"
                     + "rawLogs/2010/01/01/01/00/_SUCCESS, Exists? :false" + "Action updated in DB! _L2_")
                .append("\n");
        logWriter.append(bf);

        logWriter.close();

        XLogFilter.reset();
        XLogFilter.defineParameter("USER");
        XLogFilter.defineParameter("GROUP");
        XLogFilter.defineParameter("TOKEN");
        XLogFilter.defineParameter("APP");
        XLogFilter.defineParameter("JOB");
        XLogFilter.defineParameter("ACTION");

        XLogFilter xf = new XLogFilter();

        xf.setLogLevel("DEBUG|INFO");
        xf.setParameter("USER", ".*");
        xf.setParameter("GROUP", ".*");
        xf.setParameter("TOKEN", ".*");
        xf.setParameter("APP", ".*");
        xf.setParameter("JOB", "0000003-140205233038063-oozie-oozi-C");
        xf.setParameter(DagXLogInfoService.ACTION, "0000003-140205233038063-oozie-oozi-C@1");

        String out = doStreamLog(xf);
        String[] outArr = out.split("\n");
        assertEquals(2, outArr.length);
        assertTrue(out.contains("_L1_"));
        assertFalse(out.contains("_L2_"));

        // We'll use a DummyZKOozie to create an entry in ZK and then set its
        // url to an (unrelated) servlet that will simply return
        // some log messages
        DummyZKOozie dummyOozie = null;
        EmbeddedServletContainer container = new EmbeddedServletContainer("oozie");
        container.addServletEndpoint("/other-oozie-server/*", DummyLogStreamingServlet.class);
        try {
            container.start();
            dummyOozie = new DummyZKOozie("9876", container.getServletURL("/other-oozie-server/*"));
            DummyLogStreamingServlet.logs = "";

            DummyLogStreamingServlet.lastQueryString = null;
            Map<String, String[]> param = new HashMap<String, String[]>();
            param.put(RestConstants.JOB_COORD_RANGE_TYPE_PARAM, new String[] { RestConstants.JOB_LOG_ACTION });
            param.put(RestConstants.JOB_COORD_SCOPE_PARAM, new String[] { "1" });
            out = doStreamLog(xf, param);
            assertTrue(DummyLogStreamingServlet.lastQueryString.contains("show=log&allservers=false" ));
            assertTrue(DummyLogStreamingServlet.lastQueryString.contains("type=" + RestConstants.JOB_LOG_ACTION ));
            assertTrue(DummyLogStreamingServlet.lastQueryString.contains(RestConstants.JOB_COORD_SCOPE_PARAM + "=1" ));

            param.clear();
            param.put(RestConstants.JOB_COORD_RANGE_TYPE_PARAM, new String[] { RestConstants.JOB_LOG_ACTION });
            param.put(RestConstants.JOB_COORD_SCOPE_PARAM, new String[] { "1-4,5" });
            out = doStreamLog(xf, param);
            assertTrue(DummyLogStreamingServlet.lastQueryString.contains("show=log&allservers=false" ));
            assertTrue(DummyLogStreamingServlet.lastQueryString.contains("type=" + RestConstants.JOB_LOG_ACTION ));
            assertTrue(DummyLogStreamingServlet.lastQueryString.contains(RestConstants.JOB_COORD_SCOPE_PARAM + "=1-4,5" ));

            param.clear();
            Date endDate = new Date();
            Date createdDate = new Date(endDate.getTime() / 2);
            String date = DateUtils.formatDateOozieTZ(createdDate) + "::" + DateUtils.formatDateOozieTZ(endDate);
            param.put(RestConstants.JOB_COORD_RANGE_TYPE_PARAM, new String[] { RestConstants.JOB_LOG_DATE });
            param.put(RestConstants.JOB_COORD_SCOPE_PARAM, new String[] { date });
            out = doStreamLog(xf, param);
            assertTrue(DummyLogStreamingServlet.lastQueryString.contains("show=log&allservers=false" ));
            assertTrue(DummyLogStreamingServlet.lastQueryString.contains("type=" + RestConstants.JOB_LOG_DATE ));
            assertTrue(DummyLogStreamingServlet.lastQueryString.contains(RestConstants.JOB_COORD_SCOPE_PARAM + "=" + date ));

            container.stop();
        }
        finally {
            if (dummyOozie != null) {
                dummyOozie.teardown();
            }
            container.stop();
        }
    }

    public void testStreamingWithMultipleOozieServers_errorLog() throws Exception {
        XLogFilter.reset();

        File log4jFile = new File(getTestCaseConfDir(), "test-log4j.properties");
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream is = cl.getResourceAsStream("test-no-dash-log4j.properties");
        Properties log4jProps = new Properties();
        log4jProps.load(is);
        // prevent conflicts with other tests by changing the log file location
        log4jProps.setProperty("log4j.appender.oozie.File", getTestCaseDir() + "/oozie.log");
        log4jProps.setProperty("log4j.appender.oozieError.File", getTestCaseDir() + "/oozie-error.log");

        log4jProps.store(new FileOutputStream(log4jFile), "");
        setSystemProperty(XLogService.LOG4J_FILE, log4jFile.getName());
        Services.get().get(XLogService.class).init(Services.get());

        File logFile = new File(Services.get().get(XLogService.class).getOozieErrorLogPath(), Services.get()
                .get(XLogService.class).getOozieErrorLogName());
        logFile.getParentFile().mkdirs();
        FileWriter logWriter = new FileWriter(logFile);
        // local logs
        StringBuffer bf = new StringBuffer();
        bf.append(
                "2014-02-06 00:26:56,126 WARN CoordActionInputCheckXCommand:545 [pool-2-thread-26] - USER[-] GROUP[-] "
                     + "TOKEN[-] APP[-] JOB[0000003-140205233038063-oozie-oozi-C] ACTION[0000003-140205233038063-oozie-oozi-C@1] "
                     + "checking for the file ~:8020/user/purushah/examples/input-data/rawLogs/2010/01/01/01/00/_SUCCESS\n")
                .append("2014-02-06 00:26:56,150  WARN CoordActionInputCheckXCommand:539 [pool-2-thread-26] - USER[-] GROUP[-] "
                     + "TOKEN[-] APP[-] JOB[0000003-140205233038063-oozie-oozi-C] ACTION[0000003-140205233038063-oozie-oozi-C@1] "
                     + "[0000003-140205233038063-oozie-oozi-C@1]::ActionInputCheck::File::8020/user/purushah/examples/input-data/"
                     + "rawLogs/2010/01/01/01/00/_SUCCESS, Exists? :false" + "Action updated in DB! _L1_")
                .append("\n")
                .append("2014-02-06 00:27:56,126 WARN CoordActionInputCheckXCommand:545 [pool-2-thread-26] - USER[-] GROUP[-] "
                     + "TOKEN[-] APP[-] JOB[0000003-140205233038063-oozie-oozi-C] ACTION[0000003-140205233038063-oozie-oozi-C@2] "
                     + "checking for the file ~:8020/user/purushah/examples/input-data/rawLogs/2010/01/01/01/00/_SUCCESS\n")
                .append("2014-02-06 00:27:56,150  WARN CoordActionInputCheckXCommand:539 [pool-2-thread-26] - USER[-] GROUP[-] "
                     + "TOKEN[-] APP[-] JOB[0000003-140205233038063-oozie-oozi-C] ACTION[0000003-140205233038063-oozie-oozi-C@2] "
                     + "[0000003-140205233038063-oozie-oozi-C@2]::ActionInputCheck::File::8020/user/purushah/examples/input-data/"
                     + "rawLogs/2010/01/01/01/00/_SUCCESS, Exists? :false" + "Action updated in DB! _L2_")
                .append("\n");
        logWriter.append(bf);

        logWriter.close();

        XLogFilter.reset();
        XLogFilter.defineParameter("USER");
        XLogFilter.defineParameter("GROUP");
        XLogFilter.defineParameter("TOKEN");
        XLogFilter.defineParameter("APP");
        XLogFilter.defineParameter("JOB");
        XLogFilter.defineParameter("ACTION");

        XLogFilter xf = new XLogFilter();

        xf.setParameter("USER", ".*");
        xf.setParameter("GROUP", ".*");
        xf.setParameter("TOKEN", ".*");
        xf.setParameter("APP", ".*");
        xf.setParameter("JOB", "0000003-140205233038063-oozie-oozi-C");
        xf.setParameter(DagXLogInfoService.ACTION, "0000003-140205233038063-oozie-oozi-C@1");


        String out = doStreamErrorLog(xf);
        String[] outArr = out.split("\n");
        assertEquals(2, outArr.length);
        assertTrue(out.contains("_L1_"));
        assertFalse(out.contains("_L2_"));


        // We'll use a DummyZKOozie to create an entry in ZK and then set its
        // url to an (unrelated) servlet that will simply return
        // some log messages
        DummyZKOozie dummyOozie = null;
        EmbeddedServletContainer container = new EmbeddedServletContainer("oozie");
        container.addServletEndpoint("/other-oozie-server/*", DummyLogStreamingServlet.class);
        try {
            container.start();
            dummyOozie = new DummyZKOozie("9876", container.getServletURL("/other-oozie-server/*"));
            StringBuffer newLog = new StringBuffer();

            newLog.append(
                    "2014-02-07 00:26:56,126 WARN CoordActionInputCheckXCommand:545 [pool-2-thread-26] - USER[-] GROUP[-] "
                     + "TOKEN[-] APP[-] JOB[0000003-140205233038063-oozie-oozi-C] ACTION[0000003-140205233038063-oozie-oozi-C@1] "
                     + "checking for the file ~:8020/user/purushah/examples/input-data/rawLogs/2010/01/01/01/00/_SUCCESS\n")
                 .append("2014-02-07 00:26:56,150  WARN CoordActionInputCheckXCommand:539 [pool-2-thread-26] - USER[-] GROUP[-] "
                     + "TOKEN[-] APP[-] JOB[0000003-140205233038063-oozie-oozi-C] ACTION[0000003-140205233038063-oozie-oozi-C@1] "
                     + "[0000003-140205233038063-oozie-oozi-C@1]::ActionInputCheck::File::8020/user/purushah/examples/input-data/"
                     + "rawLogs/2010/01/01/01/00/_SUCCESS, Exists? :false" + "Action updated in DB! _L3_")
                    .append("\n");

            DummyLogStreamingServlet.logs = newLog.toString();


            out = doStreamErrorLog(xf);
            outArr = out.split("\n");
            assertEquals(4, outArr.length);
            assertTrue(out.contains("_L1_"));
            assertTrue(out.contains("_L3_"));
            assertFalse(out.contains("_L2_"));

            container.stop();
        }
        finally {
            if (dummyOozie != null) {
                dummyOozie.teardown();
            }
            container.stop();
        }
    }

}
