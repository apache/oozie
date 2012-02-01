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

import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.IOUtils;

import java.io.File;
import java.io.FileOutputStream;

public class TestConfigurationService extends XTestCase {

    private void prepareOozieConfDir(String oozieSite) throws Exception {
        prepareOozieConfDir(oozieSite, ConfigurationService.SITE_CONFIG_FILE);
    }

    private void prepareOozieConfDir(String oozieSite, String alternateSiteFile) throws Exception {
        if (!alternateSiteFile.equals(ConfigurationService.SITE_CONFIG_FILE)) {
            setSystemProperty(ConfigurationService.OOZIE_CONFIG_FILE, alternateSiteFile);
        }
        File siteFile = new File(getTestCaseConfDir(), alternateSiteFile);
        IOUtils.copyStream(IOUtils.getResourceAsStream(oozieSite, -1),
                           new FileOutputStream(siteFile));
    }

    public void testValueFromDefault() throws Exception {
        prepareOozieConfDir("oozie-site1.xml");
        ConfigurationService cl = new ConfigurationService();
        cl.init(null);
        assertEquals("oozie-" + System.getProperty("user.name"), cl.getConf().get("oozie.system.id"));
        assertNull(cl.getConf().get("oozie.dummy"));
        cl.destroy();
    }

    public void testMissingSite() throws Exception {
        prepareOozieConfDir("oozie-site2.xml");
        setSystemProperty(ConfigurationService.OOZIE_CONFIG_FILE, "oozie-site-missing.xml");
        ConfigurationService cl = new ConfigurationService();
        cl.init(null);
        assertEquals("oozie-" + System.getProperty("user.name"), cl.getConf().get("oozie.system.id"));
        assertNull(cl.getConf().get("oozie.dummy"));
        cl.destroy();
    }

    public void testValueFromSite() throws Exception {
        prepareOozieConfDir("oozie-site2.xml");
        ConfigurationService cl = new ConfigurationService();
        cl.init(null);
        assertEquals("SITE1", cl.getConf().get("oozie.system.id"));
        assertEquals("SITE2", cl.getConf().get("oozie.dummy"));
        cl.destroy();
    }

    public void testValueFromSiteAlternate() throws Exception {
        prepareOozieConfDir("oozie-sitealternate.xml", "oozie-alternate.xml");
        ConfigurationService cl = new ConfigurationService();
        cl.init(null);
        assertEquals("ALTERNATE1", cl.getConf().get("oozie.system.id"));
        assertEquals("ALTERNATE2", cl.getConf().get("oozie.dummy"));
        cl.destroy();
    }

    public void testSysPropOverride() throws Exception {
        prepareOozieConfDir("oozie-site2.xml");
        setSystemProperty("oozie.dummy", "OVERRIDE");
        ConfigurationService cl = new ConfigurationService();
        cl.init(null);
        assertEquals("OVERRIDE", cl.getConf().get("oozie.dummy"));
        cl.destroy();
    }

    public void testAlternateConfDir() throws Exception {
        String customConfDir = createTestCaseSubDir("xconf");
        setSystemProperty(ConfigurationService.OOZIE_CONFIG_DIR, customConfDir);

        IOUtils.copyStream(IOUtils.getResourceAsStream("oozie-site1.xml", -1),
                           new FileOutputStream(new File(customConfDir, "oozie-site.xml")));

        ConfigurationService cl = new ConfigurationService();
        cl.init(null);
        assertEquals("oozie-" + System.getProperty("user.name"), cl.getConf().get("oozie.system.id"));
        assertNull(cl.getConf().get("oozie.dummy"));
        cl.destroy();

    }
}
