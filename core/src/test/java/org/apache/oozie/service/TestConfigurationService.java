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

import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.IOUtils;

import java.io.File;
import java.io.FileOutputStream;

public class TestConfigurationService extends XTestCase {

    public void testOriginalDefault() throws Exception {
        ConfigurationService cl = new ConfigurationService();
        cl.init(null);
        assertNotNull(cl.getConf().get("oozie.systemmode"));
        cl.destroy();
    }

    public void testDefault() throws Exception {
        ConfigurationService cl = new ConfigurationService();
        ConfigurationService.testingDefaultFile = true;
        cl.init(null);
        assertEquals("DEFAULT", cl.getConf().get("oozie.dummy"));
        cl.destroy();
    }

    public void testSiteFromClasspath() throws Exception {
        setSystemProperty(ConfigurationService.CONFIG_FILE, "oozie-site1.xml");
        ConfigurationService cl = new ConfigurationService();
        ConfigurationService.testingDefaultFile = true;
        cl.init(null);
        assertEquals("SITE1", cl.getConf().get("oozie.dummy"));
        cl.destroy();
    }

    public void testSiteFromDir() throws Exception {
        String dir = getTestCaseDir();
        IOUtils.copyStream(IOUtils.getResourceAsStream("test-oozie-log4j.properties", -1),
                           new FileOutputStream(new File(dir, "test-oozie-log4j.properties")));
        setSystemProperty(ConfigurationService.CONFIG_FILE, "oozie-site.xml");
        IOUtils.copyStream(IOUtils.getResourceAsStream("oozie-site2.xml", -1),
                           new FileOutputStream(new File(dir, "oozie-site.xml")));
        setSystemProperty(ConfigurationService.CONFIG_PATH, dir);
        ConfigurationService cl = new ConfigurationService();
        ConfigurationService.testingDefaultFile = true;
        cl.init(null);
        assertEquals("SITE2", cl.getConf().get("oozie.dummy"));
        cl.destroy();
    }

    public void testCustomSiteFromDir() throws Exception {
        String dir = getTestCaseDir();
        IOUtils.copyStream(IOUtils.getResourceAsStream("test-oozie-log4j.properties", -1),
                           new FileOutputStream(new File(dir, "test-oozie-log4j.properties")));
        IOUtils.copyStream(IOUtils.getResourceAsStream("oozie-site2.xml", -1),
                           new FileOutputStream(new File(dir, "oozie-site2.xml")));
        setSystemProperty(ConfigurationService.CONFIG_PATH, dir);
        setSystemProperty(ConfigurationService.CONFIG_FILE, "oozie-site2.xml");
        ConfigurationService cl = new ConfigurationService();
        ConfigurationService.testingDefaultFile = true;
        cl.init(null);
        assertEquals("SITE2", cl.getConf().get("oozie.dummy"));
        cl.destroy();
    }

    public void testSysPropOverride() throws Exception {
        setSystemProperty("oozie.dummy", "OVERRIDE");
        ConfigurationService cl = new ConfigurationService();
        ConfigurationService.testingDefaultFile = true;
        cl.init(null);
        assertEquals("OVERRIDE", cl.getConf().get("oozie.dummy"));
        cl.destroy();
    }

}