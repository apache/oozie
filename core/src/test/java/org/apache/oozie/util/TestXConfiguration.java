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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapFile;
import org.apache.oozie.test.XTestCase;

public class TestXConfiguration extends XTestCase {

    public void testFromStream() throws Exception {
        String configPath = "test-oozie-default.xml";
        InputStream is = IOUtils.getResourceAsStream(configPath, -1);
        XConfiguration conf = new XConfiguration(is);
        assertEquals("DEFAULT", conf.get("oozie.dummy"));
    }

    public void testFromReader() throws Exception {
        String configPath = "test-oozie-default.xml";
        Reader reader = IOUtils.getResourceAsReader(configPath, -1);
        XConfiguration conf = new XConfiguration(reader);
        assertEquals("DEFAULT", conf.get("oozie.dummy"));
    }

    public void testAddXIncludeFromStream() throws IOException {
        String parentXml = "parentXml";
        prepareXmlWithInclude(parentXml);
        try {
            XConfiguration conf = new XConfiguration(new FileInputStream(new File(getTestCaseDir(), parentXml)));
            assertEquals("DEFAULT", conf.get("oozie.dummy"));
            // verify the properties from include file
            assertEquals("bar", conf.get("foo"));
            assertEquals("def", conf.get("abc"));
        } catch (IOException e) {
            e.printStackTrace();
            fail("XInclude failed");
        }

    }

    public void testAddXIncludeFromReader() throws IOException {
        String parentXml = "parentXml";
        prepareXmlWithInclude(parentXml);
        try {
            XConfiguration conf = new XConfiguration(new FileReader(new File(getTestCaseDir(), parentXml)));
            assertEquals("DEFAULT", conf.get("oozie.dummy"));
            // verify the properties from include file
            assertEquals("bar", conf.get("foo"));
            assertEquals("def", conf.get("abc"));
        }  catch (IOException e) {
            e.printStackTrace();
            fail("XInclude failed");
        }

    }

    // Copy the parent xml to testCaseDir and add the include element
    private void prepareXmlWithInclude(String parentXml) throws IOException {
        // Get the path for the file to be included
        String targetPath = Thread.currentThread().getContextClassLoader().getResource("test-hadoop-config.xml")
                .getFile();
        // Get the parent file which will contain the include element
        URL url = Thread.currentThread().getContextClassLoader().getResource("test-oozie-default.xml");
        BufferedReader br = new BufferedReader(new FileReader(url.getFile()));
        // Copy the parent file to testcase dir
        BufferedWriter bw = new BufferedWriter(new FileWriter(new File(getTestCaseDir(), parentXml)));
        // While copying, add the path for xml to be included
        // Make sure the path is absolute
        while (br.ready()) {
            String s = br.readLine();
            bw.write(s);
            if (s.contains("configuration xmlns")) {
                bw.write("<xi:include href=" + "\"" + targetPath + "\"" + "/>");
            }
        }
        br.close();
        bw.close();
    }


    public void testInvalidParsing() throws Exception {
        try {
            new XConfiguration(new StringReader("<configurationx></configurationx>"));
            fail();
        }
        catch (IOException ex) {
            //NOP
        }
        catch (Throwable ex) {
            fail();
        }
    }

    public void testCopy() throws Exception {
        Configuration srcConf = new Configuration(false);
        Configuration targetConf = new Configuration(false);

        srcConf.set("testParameter1", "valueFromSource");
        srcConf.set("testParameter2", "valueFromSource");

        targetConf.set("testParameter2", "valueFromTarget");
        targetConf.set("testParameter3", "valueFromTarget");

        XConfiguration.copy(srcConf, targetConf);

        assertEquals(targetConf.get("testParameter1"), "valueFromSource");
        assertEquals(targetConf.get("testParameter2"), "valueFromSource");
        assertEquals(targetConf.get("testParameter3"), "valueFromTarget");

    }

    public void testInjectDefaults() throws Exception {
        Configuration srcConf = new Configuration(false);
        Configuration targetConf = new Configuration(false);

        srcConf.set("testParameter1", "valueFromSource");
        srcConf.set("testParameter2", "valueFromSource");

        targetConf.set("testParameter2", "originalValueFromTarget");
        targetConf.set("testParameter3", "originalValueFromTarget");

        XConfiguration.injectDefaults(srcConf, targetConf);

        assertEquals(targetConf.get("testParameter1"), "valueFromSource");
        assertEquals(targetConf.get("testParameter2"), "originalValueFromTarget");
        assertEquals(targetConf.get("testParameter3"), "originalValueFromTarget");

        assertEquals(srcConf.get("testParameter1"), "valueFromSource");
        assertEquals(srcConf.get("testParameter2"), "valueFromSource");
        assertNull(srcConf.get("testParameter3"));
    }

    public void testTrim() {
        XConfiguration conf = new XConfiguration();
        conf.set("a", " A ");
        conf.set("b", "B");
        conf = conf.trim();
        assertEquals("A", conf.get("a"));
        assertEquals("B", conf.get("b"));
    }

    public void testResolve() {
        XConfiguration conf = new XConfiguration();
        conf.set("a", "A");
        conf.set("b", "${a}");
        assertEquals("A", conf.getRaw("a"));
        assertEquals("${a}", conf.getRaw("b"));
        conf = conf.resolve();
        assertEquals("A", conf.getRaw("a"));
        assertEquals("A", conf.getRaw("b"));
    }

    public void testVarResolutionAndSysProps() {
        setSystemProperty("aa", "foo");
        XConfiguration conf = new XConfiguration();
        conf.set("a", "A");
        conf.set("b", "${a}");
        conf.set("c", "${aa}");
        conf.set("d", "${aaa}");
        assertEquals("A", conf.getRaw("a"));
        assertEquals("${a}", conf.getRaw("b"));
        assertEquals("${aa}", conf.getRaw("c"));
        assertEquals("A", conf.get("a"));
        assertEquals("A", conf.get("b"));
        assertEquals("foo", conf.get("c"));
        assertEquals("${aaa}", conf.get("d"));

        conf.set("un","${user.name}");
        assertEquals(System.getProperty("user.name"), conf.get("un"));
        setSystemProperty("user.name", "foo");
        assertEquals("foo", conf.get("un"));

    }
}
