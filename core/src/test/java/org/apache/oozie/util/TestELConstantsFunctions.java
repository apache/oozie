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

import org.apache.oozie.test.XTestCase;
import org.jdom.Element;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

public class TestELConstantsFunctions extends XTestCase {

    public void testTrim() {
        assertEquals("", ELConstantsFunctions.trim(null));
        assertEquals("a", ELConstantsFunctions.trim(" a "));
    }

    public void testConcat() {
        assertEquals("a", ELConstantsFunctions.concat("a", null));
        assertEquals("b", ELConstantsFunctions.concat(null, "b"));
        assertEquals("ab", ELConstantsFunctions.concat("a", "b"));
        assertEquals("", ELConstantsFunctions.concat(null, null));
    }

    public void testTimestamp() throws Exception {
        String s = ELConstantsFunctions.timestamp();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        assertNotNull(sdf.parse(s));
    }

    public void testUrlEncode() {
        assertEquals("+", ELConstantsFunctions.urlEncode(" "));
        assertEquals("%25", ELConstantsFunctions.urlEncode("%"));
    }

    public void testToJsonStr() throws Exception {
        Map<String, String> map = new HashMap<String, String>();
        map.put("a", "A");
        map.put("b", "&");
        String str = ELConstantsFunctions.toJsonStr(map);
        Element e = XmlUtils.parseXml("<x>" + str + "</x>");
        JSONObject json = (JSONObject) new JSONParser().parse(e.getText());
        Map<String, String> map2 = new HashMap<String, String>(json);
        assertEquals(map, map2);
    }

    public void testToPropertiesStr() throws Exception {
        Map<String, String> map = new HashMap<String, String>();
        map.put("a", "A");
        map.put("b", "&");
        String str = ELConstantsFunctions.toPropertiesStr(map);
        Element e = XmlUtils.parseXml("<x>" + str + "</x>");
        Properties map2 = PropertiesUtils.stringToProperties(e.getText());
        assertEquals(map, map2);
    }

    public void testToConfigurationStr() throws Exception {
        Map<String, String> map = new HashMap<String, String>();
        map.put("a", "A");
        map.put("b", "&");
        String str = ELConstantsFunctions.toConfigurationStr(map);
        Element e = XmlUtils.parseXml("<x>" + str + "</x>");
        XConfiguration conf = new XConfiguration(new StringReader(e.getText()));
        Map<String, String> map2 = new HashMap<String, String>();
        for (Map.Entry entry : conf) {
            map2.put((String) entry.getKey(), (String) entry.getValue());
        }
        assertEquals(map, map2);
    }

}
