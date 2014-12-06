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

public class TestXmlUtils extends XTestCase {

    private static String EXTERNAL_ENTITY_XML = "<!DOCTYPE foo [<!ENTITY xxe SYSTEM \"file:///etc/passwd\">]>\n"
            + "<foo>&xxe;</foo>";

    public void testExternalEntity() throws Exception {
        Element e = XmlUtils.parseXml(EXTERNAL_ENTITY_XML);
        assertEquals(0, e.getText().length());
    }

    public void testRemoveComments() throws Exception {
        String xmlStr = "<test1> <!-- Comment1 -->1234 <test2> ABCD <!-- Comment2 --> </test2> "
                + "<!-- Comment3 --> <test3> <!-- Comment4 -->EFGH  </test3> <!-- Comment5 --></test1>";
        String result = XmlUtils.removeComments(xmlStr);
        System.out.println("Result After Comments removal :\n" + result);
    }
}
