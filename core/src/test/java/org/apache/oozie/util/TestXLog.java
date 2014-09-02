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

import org.apache.commons.logging.impl.SimpleLog;
import org.apache.oozie.test.XTestCase;

public class TestXLog extends XTestCase {

    protected void setUp() throws Exception {
        super.setUp();
        XLog.Info.reset();
        XLog.Info.remove();
    }

    protected void tearDown() throws Exception {
        XLog.Info.reset();
        XLog.Info.remove();
        super.tearDown();
    }

    public static class TestLog extends SimpleLog {
        private String message;

        public TestLog() {
            super("");
        }

        protected void write(StringBuffer stringBuffer) {
            message = stringBuffer.toString();
        }

        public void resetMessage() {
            message = null;
        }

        public String getMessage() {
            return message;
        }
    }

    public void testInfoReset() {
        XLog.Info logInfo = new XLog.Info();
        XLog.Info.defineParameter("A");
        assertEquals("A[-]", logInfo.createPrefix());
        XLog.Info.reset();
        assertEquals("", logInfo.createPrefix());
    }

    public void testInfoParameters() {
        XLog.Info logInfo = new XLog.Info();
        assertEquals("", logInfo.createPrefix());
        XLog.Info.defineParameter("A");
        assertEquals("A[-]", logInfo.createPrefix());
        XLog.Info.defineParameter("B");
        assertEquals("A[-] B[-]", logInfo.createPrefix());
        logInfo.setParameter("A", "a");
        assertEquals("A[a] B[-]", logInfo.createPrefix());
        logInfo.setParameter("B", "b");
        assertEquals("A[a] B[b]", logInfo.createPrefix());
    }

    public void testInfoConstructorPropagation() {
        XLog.Info.defineParameter("A");
        XLog.Info logInfo = new XLog.Info();
        logInfo.setParameter("A", "a");
        assertEquals("A[a]", logInfo.createPrefix());
        logInfo = new XLog.Info(logInfo);
        assertEquals("A[a]", logInfo.createPrefix());
    }

    public void testInfoThreadLocal() throws Exception {
        XLog.Info.defineParameter("A");
        assertEquals("A[-]", XLog.Info.get().createPrefix());
        XLog.Info.get().setParameter("A", "a");
        assertEquals("A[a]", XLog.Info.get().createPrefix());
        Thread t = new Thread() {
            public void run() {
                assertEquals("A[-]", XLog.Info.get().createPrefix());
                XLog.Info.get().setParameter("A", "aa");
            }
        };
        t.start();
        t.join();
        assertEquals("A[a]", XLog.Info.get().createPrefix());
    }

    public class LogPrinter {
        XLog LOG = XLog.getLog(LogPrinter.class);
        public void setMsgPrefix(String prefix) {
            LOG.setMsgPrefix(prefix);
        }
        public String getLogMsgPrefix() {
            return LOG.getMsgPrefix();
        }
        public String getThreadLocalPrefix() {
            return XLog.Info.get().getPrefix();
        }
        public String getLogPrefix() {
            return LOG.getMsgPrefix() != null ? LOG.getMsgPrefix() : XLog.Info.get().getPrefix();
        }
    }

    public void testInfoThreadLocalPrefix() throws Exception {
        XLog.Info.defineParameter("JOB");
        XLog.Info.defineParameter("ACTION");

        assertEquals("JOB[-] ACTION[-]", XLog.Info.get().createPrefix());

        String jobId = "XXX-W";
        LogUtils.setLogInfo(jobId+"@start");
        assertEquals("JOB[XXX-W] ACTION[XXX-W@start]", XLog.Info.get().createPrefix());

        final StringBuilder sb1 = new StringBuilder();
        final StringBuilder sb2 = new StringBuilder();

        final LogPrinter printer = new LogPrinter();

        Thread t = new Thread() {
            public void run() {
                LogUtils.setLogInfo("XXX-W@hive");
                sb1.append(printer.getLogMsgPrefix());
                sb2.append(printer.getThreadLocalPrefix());
            }
        };
        t.start();
        t.join();

        assertNull(printer.getLogMsgPrefix());
        assertEquals("JOB[XXX-W] ACTION[XXX-W@start]", printer.getThreadLocalPrefix());
        assertEquals("JOB[XXX-W] ACTION[XXX-W@start]", printer.getLogPrefix());

        assertEquals("null", sb1.toString());
        assertEquals("JOB[XXX-W] ACTION[XXX-W@hive]", sb2.toString());
    }

    public void testLogMsg() throws Exception {
        XLog.Info.defineParameter("JOB");
        XLog.Info.defineParameter("ACTION");

        final LogPrinter printer = new LogPrinter();
        assertNull(printer.getLogMsgPrefix());

        String jobId = "XXX-W";
        LogUtils.setLogInfo(jobId+"@start");
        assertEquals("JOB[XXX-W] ACTION[XXX-W@start]", printer.getThreadLocalPrefix());
        assertEquals("JOB[XXX-W] ACTION[XXX-W@start]", printer.getLogPrefix());

        printer.setMsgPrefix("prefix");
        assertEquals("prefix", printer.getLogPrefix());
    }

    public void testFactory() {
        XLog log = XLog.getLog(getClass());
        assertNotNull(log);
        assertEquals(XLog.class, log.getClass());
    }

    public void testFactoryLogInfoPrefix() {
        XLog.Info.defineParameter("A");
        XLog.Info.get().setParameter("A", "a");
        XLog log = XLog.getLog(getClass());
        log.setMsgPrefix(XLog.Info.get().createPrefix());
        assertEquals("A[a]", log.getMsgPrefix());
    }

    public void testXLogFunctionality() {
        TestLog log = new TestLog();
        TestLog ops = new TestLog();
        XLog xLog = new XLog(log);

        assertNull(xLog.getMsgPrefix());
        xLog.setMsgPrefix("prefix");
        assertEquals("prefix", xLog.getMsgPrefix());
        xLog.setMsgPrefix(null);

        xLog.loggers[1] = ops;

        log.setLevel(SimpleLog.LOG_LEVEL_OFF);
        ops.setLevel(SimpleLog.LOG_LEVEL_OFF);
        assertFalse(xLog.isDebugEnabled());
        assertFalse(xLog.isErrorEnabled());
        assertFalse(xLog.isFatalEnabled());
        assertFalse(xLog.isInfoEnabled());
        assertFalse(xLog.isWarnEnabled());
        assertFalse(xLog.isTraceEnabled());

        log.setLevel(SimpleLog.LOG_LEVEL_ALL);
        ops.setLevel(SimpleLog.LOG_LEVEL_OFF);
        assertTrue(xLog.isDebugEnabled());
        assertTrue(xLog.isErrorEnabled());
        assertTrue(xLog.isFatalEnabled());
        assertTrue(xLog.isInfoEnabled());
        assertTrue(xLog.isWarnEnabled());
        assertTrue(xLog.isTraceEnabled());

        log.setLevel(SimpleLog.LOG_LEVEL_OFF);
        ops.setLevel(SimpleLog.LOG_LEVEL_ALL);
        assertTrue(xLog.isDebugEnabled());
        assertTrue(xLog.isErrorEnabled());
        assertTrue(xLog.isFatalEnabled());
        assertTrue(xLog.isInfoEnabled());
        assertTrue(xLog.isWarnEnabled());
        assertTrue(xLog.isTraceEnabled());

        log.setLevel(SimpleLog.LOG_LEVEL_ALL);
        ops.setLevel(SimpleLog.LOG_LEVEL_ALL);
        assertTrue(xLog.isDebugEnabled());
        assertTrue(xLog.isErrorEnabled());
        assertTrue(xLog.isFatalEnabled());
        assertTrue(xLog.isInfoEnabled());
        assertTrue(xLog.isWarnEnabled());
        assertTrue(xLog.isTraceEnabled());

        log.setLevel(SimpleLog.LOG_LEVEL_OFF);
        ops.setLevel(SimpleLog.LOG_LEVEL_OFF);
        log.resetMessage();
        xLog.debug("");
        assertNull(log.getMessage());
        xLog.error("");
        assertNull(log.getMessage());
        xLog.fatal("");
        assertNull(log.getMessage());
        xLog.info("");
        assertNull(log.getMessage());
        xLog.warn("");
        assertNull(log.getMessage());
        xLog.trace("");
        assertNull(log.getMessage());

        log.setLevel(SimpleLog.LOG_LEVEL_ALL);
        ops.setLevel(SimpleLog.LOG_LEVEL_OFF);
        log.resetMessage();
        ops.resetMessage();
        xLog.debug("debug");
        assertTrue(log.getMessage().endsWith("debug"));
        assertNull(ops.getMessage());
        xLog.error("error");
        assertTrue(log.getMessage().endsWith("error"));
        assertNull(ops.getMessage());
        xLog.fatal("fatal");
        assertTrue(log.getMessage().endsWith("fatal"));
        assertNull(ops.getMessage());
        xLog.info("info");
        assertTrue(log.getMessage().endsWith("info"));
        assertNull(ops.getMessage());
        xLog.warn("warn");
        assertTrue(log.getMessage().endsWith("warn"));
        assertNull(ops.getMessage());
        xLog.trace("trace");
        assertTrue(log.getMessage().endsWith("trace"));
        assertNull(ops.getMessage());

        log.setLevel(SimpleLog.LOG_LEVEL_ALL);
        ops.setLevel(SimpleLog.LOG_LEVEL_ALL);
        log.resetMessage();
        ops.resetMessage();
        xLog.debug("debug");
        assertTrue(log.getMessage().endsWith("debug"));
        assertNull(ops.getMessage());

        log.setLevel(SimpleLog.LOG_LEVEL_ALL);
        ops.setLevel(SimpleLog.LOG_LEVEL_ALL);
        log.resetMessage();
        ops.resetMessage();
        xLog.debug("debug");
        assertTrue(log.getMessage().endsWith("debug"));
        assertNull(ops.getMessage());

        log.setLevel(SimpleLog.LOG_LEVEL_ALL);
        ops.setLevel(SimpleLog.LOG_LEVEL_ALL);
        log.resetMessage();
        ops.resetMessage();
        xLog.debug(XLog.OPS, "debug");
        assertTrue(log.getMessage().endsWith("debug"));
        assertTrue(ops.getMessage().endsWith("debug"));

        log.setLevel(SimpleLog.LOG_LEVEL_ALL);
        ops.setLevel(SimpleLog.LOG_LEVEL_ALL);
        log.resetMessage();
        ops.resetMessage();
        xLog.debug(XLog.OPS, "debug {0}", "debug");
        assertTrue(log.getMessage().endsWith("debug debug"));
        assertTrue(ops.getMessage().endsWith("debug debug"));

        assertNull(XLog.getCause("a", "b"));
        assertNotNull(XLog.getCause("a", "b", new Exception()));
    }

}
