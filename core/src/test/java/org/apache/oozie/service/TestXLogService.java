/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

public class TestXLogService extends XTestCase {

    protected void setUp() throws Exception {
        super.setUp();
        LogFactory.getFactory().release();
        LogManager.resetConfiguration();
    }

    protected void tearDown() throws Exception {
        LogFactory.getFactory().release();
        LogManager.resetConfiguration();
        super.tearDown();
    }

    public void testDefaultFileViaClassLoader() throws Exception {
        XLogService ls = new XLogService();
        XLogService.testingDefaultFile = true;
        ls.init(null);
        Assert.assertTrue(LogFactory.getLog("a").isTraceEnabled());
        ls.destroy();
    }

    public void testCustomFileViaClassLoader() throws Exception {
        setSystemProperty(XLogService.LOG4J_FILE, "test-custom-log4j.properties");
        XLogService ls = new XLogService();
        XLogService.testingDefaultFile = true;
        ls.init(null);
        Assert.assertFalse(LogFactory.getLog("a").isTraceEnabled());
        ls.destroy();
    }

    public void testDefaultFileViaConfigPath() throws Exception {
        File file = new File(getTestCaseDir(), "test-oozie-log4j.properties");
        InputStream is = Thread.currentThread().getContextClassLoader().
                getResourceAsStream("test-oozie-log4j.properties");
        IOUtils.copyStream(is, new FileOutputStream(file));
        setSystemProperty(ConfigurationService.CONFIG_PATH, file.getParent());
        XLogService ls = new XLogService();
        XLogService.testingDefaultFile = true;
        ls.init(null);
        Assert.assertTrue(LogFactory.getLog("a").isTraceEnabled());
        ls.destroy();
    }

    public void testCustomFileViaConfigPath() throws Exception {
        File file = new File(getTestCaseDir(), "test-custom-log4j.properties");
        InputStream is = Thread.currentThread().getContextClassLoader().
                getResourceAsStream("test-custom-log4j.properties");
        IOUtils.copyStream(is, new FileOutputStream(file));
        setSystemProperty(ConfigurationService.CONFIG_PATH, file.getParent());
        setSystemProperty(XLogService.LOG4J_FILE, "test-custom-log4j.properties");
        XLogService ls = new XLogService();
        XLogService.testingDefaultFile = true;
        ls.init(null);
        Assert.assertFalse(LogFactory.getLog("a").isTraceEnabled());
        ls.destroy();
    }

    public void _testReload() throws Exception {
        File file = new File(getTestCaseDir(), "reload-log4j.properties");

        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(
                "test-oozie-log4j.properties");
        IOUtils.copyStream(is, new FileOutputStream(file));

        setSystemProperty(ConfigurationService.CONFIG_PATH, getTestCaseDir());
        setSystemProperty(XLogService.RELOAD_INTERVAL, "1");
        setSystemProperty(XLogService.LOG4J_FILE, "reload-log4j.properties");
        XLogService ls = new XLogService();
        XLogService.testingDefaultFile = true;
        ls.init(null);
        assertTrue(LogFactory.getLog("a").isTraceEnabled());

        is = Thread.currentThread().getContextClassLoader().getResourceAsStream("test-custom-log4j.properties");
        IOUtils.copyStream(is, new FileOutputStream(file));

        waitFor(5 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return !LogFactory.getLog("a").isTraceEnabled();
            }
        });

        assertFalse(LogFactory.getLog("a").isTraceEnabled());

        ls.destroy();
    }

    public void testInfoParameters() throws Exception {
        XLogService ls = new XLogService();
        ls.init(null);
        assertEquals("USER[-] GROUP[-]", XLog.Info.get().createPrefix());
        ls.destroy();
    }

}