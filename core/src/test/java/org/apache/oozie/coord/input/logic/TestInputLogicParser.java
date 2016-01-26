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

package org.apache.oozie.coord.input.logic;


import junit.framework.TestCase;

import org.apache.oozie.coord.input.logic.InputLogicParser;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.JDOMException;

public class TestInputLogicParser extends TestCase {

    public void testAndOr() throws JDOMException {
        //@formatter:off
        String xml =
                "<input-logic>" +
                        "<and>" +
                            "<or>" +
                                "<data-in dataset=\"A\"/> " +
                                "<data-in dataset=\"B\"/> " +
                            "</or>" +
                            "<or>" +
                                "<data-in dataset=\"C\"/>" +
                                "<data-in dataset=\"D\"/>" +
                            "</or>" +
                        "</and>" +
                 "</input-logic>";
        //@formatter:on
        Element root = XmlUtils.parseXml(xml);
        InputLogicParser inputLogicParser = new InputLogicParser();
        assertEquals("((dependencyBuilder.input(\"A\").build() || dependencyBuilder.input(\"B\").build()) && "
                + "(dependencyBuilder.input(\"C\").build() || dependencyBuilder.input(\"D\").build()))",
                inputLogicParser.parse(root));

    }

    public void testAnd() throws JDOMException {
        //@formatter:off
        String xml =
                "<input-logic>" +
                        "<and>" +
                            "<data-in dataset=\"A\"/> " +
                            "<data-in dataset=\"B\"/>" +
                        "</and>" +
                "</input-logic>";
      //@formatter:on
        Element root = XmlUtils.parseXml(xml);
        InputLogicParser inputLogicParser = new InputLogicParser();
        assertEquals("(dependencyBuilder.input(\"A\").build() && dependencyBuilder.input(\"B\").build())",
                inputLogicParser.parse(root));

    }

    public void testOr() throws JDOMException {
        //@formatter:off
        String xml =
                "<input-logic>" +
                        "<or>" +
                            "<data-in dataset=\"A\"/> " +
                            "<data-in dataset=\"B\"/>" +
                        "</or>" +
                 "</input-logic>";
        //@formatter:on
        Element root = XmlUtils.parseXml(xml);
        InputLogicParser inputLogicParser = new InputLogicParser();
        assertEquals("(dependencyBuilder.input(\"A\").build() || dependencyBuilder.input(\"B\").build())",
                inputLogicParser.parse(root));

    }

    public void testOrWithMin() throws JDOMException {
        //@formatter:off
        String xml = "<input-logic>" + "<or>" + "<data-in dataset=\"A\" min=\"3\"/> " + "<data-in dataset=\"B\"/>" + "</or>"
                + "</input-logic>";
        //@formatter:on
        Element root = XmlUtils.parseXml(xml);
        InputLogicParser inputLogicParser = new InputLogicParser();
        assertEquals("(dependencyBuilder.input(\"A\").min(3).build() || dependencyBuilder.input(\"B\").build())",
                inputLogicParser.parse(root));
    }

    public void testOrWithMinAtOr() throws JDOMException {
        //@formatter:off
        String xml =
                "<input-logic>" +
                        "<or min=\"10\">" +
                            "<data-in dataset=\"A\"/> " +
                            "<data-in dataset=\"B\"/>" +
                        "</or>" +
                 "</input-logic>";
        //@formatter:on
        Element root = XmlUtils.parseXml(xml);
        InputLogicParser inputLogicParser = new InputLogicParser();
        assertEquals(
                "(dependencyBuilder.input(\"A\").min(10).build() || dependencyBuilder.input(\"B\").min(10).build())",
                inputLogicParser.parse(root));
    }

    public void testWithName() throws JDOMException {
        //@formatter:off
        String xml =
                "<input-logic>" +
                        "<or name =\"test\" min=\"10\">" +
                            "<data-in dataset=\"A\"/> " +
                            "<data-in dataset=\"B\"/>" +
                        "</or>" +
                 "</input-logic>";
        //@formatter:on
        Element root = XmlUtils.parseXml(xml);
        InputLogicParser inputLogicParser = new InputLogicParser();
        assertEquals(
                "(dependencyBuilder.input(\"A\").min(10).build() || dependencyBuilder.input(\"B\").min(10).build())",
                inputLogicParser.parseWithName(root, "test"));
    }

    public void testCombine() throws JDOMException {
        //@formatter:off
        String xml =
                "<input-logic>" +
                      "<combine name =\"test\" min=\"10\">" +
                            "<data-in dataset=\"A\"/> " +
                            "<data-in dataset=\"B\"/>" +
                      "</combine>" +
                "</input-logic>";

        //@formatter:on
        Element root = XmlUtils.parseXml(xml);
        InputLogicParser inputLogicParser = new InputLogicParser();
        assertEquals("(dependencyBuilder.combine(\"A\",\"B\").min(10).build())",
                inputLogicParser.parseWithName(root, "test"));
    }

    public void testWithNameNested() throws JDOMException {
        //@formatter:off
        String xml =
                "<input-logic>" +
                        "<and>" +
                            "<or>" +
                                "<data-in dataset=\"A\"/> " +
                                "<data-in dataset=\"B\"/> " +
                            "</or>" +
                            "<or name=\"test\">" +
                                "<data-in dataset=\"C\"/>" +
                                "<data-in dataset=\"D\"/>" +
                            "</or>" +
                        "</and>" +
                 "</input-logic>";
      //@formatter:on

        Element root = XmlUtils.parseXml(xml);
        InputLogicParser inputLogicParser = new InputLogicParser();
        assertEquals("(dependencyBuilder.input(\"C\").build() || dependencyBuilder.input(\"D\").build())",
                inputLogicParser.parseWithName(root, "test"));

    }

    public void testDepth2() throws JDOMException {
        //@formatter:off
     String xml =
             "<input-logic>" +
                     "<and>" +
                         "<and>" +
                             "<or>" +
                                 "<data-in dataset=\"A\"/>" +
                                 "<data-in dataset=\"B\"/>" +
                             "</or>" +
                             "<or>" +
                                 "<data-in dataset=\"C\"/>" +
                                 "<data-in dataset=\"D\"/>" +
                             "</or>" +
                      "</and>" +
                      "<and>" +
                          "<data-in dataset=\"E\"/>" +
                          "<data-in dataset=\"F\"/>" +
                      "</and>" +
                   "</and>" +
         "</input-logic>";
     //@formatter:on

        Element root = XmlUtils.parseXml(xml);
        InputLogicParser inputLogicParser = new InputLogicParser();
        assertEquals("(((dependencyBuilder.input(\"A\").build() || dependencyBuilder.input(\"B\").build())"
                + " && (dependencyBuilder.input(\"C\").build() || dependencyBuilder.input(\"D\").build()))"
                + " && (dependencyBuilder.input(\"E\").build() && dependencyBuilder.input(\"F\").build()))",
                inputLogicParser.parse(root));

    }

    public void testDepth2WithCombine() throws JDOMException {
        //@formatter:off
        String xml =
                "<input-logic>" +
                        "<and>" +
                            "<and>" +
                                "<combine>" +
                                    "<data-in dataset=\"A\" />" +
                                    "<data-in dataset=\"B\" />" +
                                "</combine>" +
                                "<or>" +
                                    "<data-in dataset=\"C\" />" +
                                    "<data-in dataset=\"D\" />" +
                                "</or>" +
                           "</and>" +
                           "<combine>" +
                               "<data-in dataset=\"E\" />" +
                               "<data-in dataset=\"F\" />" +
                           "</combine>" +
                       "</and>" +
                 "</input-logic>";
        //@formatter:on
        Element root = XmlUtils.parseXml(xml);
        InputLogicParser inputLogicParser = new InputLogicParser();
        assertEquals("(((dependencyBuilder.combine(\"A\",\"B\").build()) && (dependencyBuilder.input(\"C\").build()"
                + " || dependencyBuilder.input(\"D\").build())) && (dependencyBuilder.combine(\"E\",\"F\").build()))",
                inputLogicParser.parse(root));
    }

    public void testAndCombine() throws JDOMException {
        //@formatter:off
        String xml =
                "<input-logic>" +
                        "<and>" +
                            "<combine>" +
                                "<data-in dataset=\"A\" />" +
                                "<data-in dataset=\"B\" />"+
                            "</combine>" +
                            "<combine>" +
                                "<data-in dataset=\"C\" />" +
                                "<data-in dataset=\"D\" />" +
                            "</combine>" +
                         "</and>" +
                 "</input-logic>";
        //@formatter:on
        Element root = XmlUtils.parseXml(xml);
        InputLogicParser inputLogicParser = new InputLogicParser();
        assertEquals(
                "((dependencyBuilder.combine(\"A\",\"B\").build()) && (dependencyBuilder.combine(\"C\",\"D\").build()))",
                inputLogicParser.parse(root));
    }

    public void testComplex1() throws JDOMException {
        //@formatter:off
        String xml=
            "<input-logic>"+
                "<and name=\"test\">"+
                    "<or>"+
                        "<and>" +
                            "<data-in dataset=\"A\" />"+
                            "<data-in dataset=\"B\" />"+
                        "</and>" +
                        "<and>"+
                            "<data-in dataset=\"C\" />"+
                            "<data-in dataset=\"D\" />"+
                        "</and>"+
                    "</or>"+
                  "<and>"+
                     "<data-in dataset=\"A\" />"+
                     "<data-in dataset=\"B\" />"+
                 "</and>"+
            "</and>"+
        "</input-logic>";
        //@formatter:on
        Element root = XmlUtils.parseXml(xml);
        InputLogicParser inputLogicParser = new InputLogicParser();
        assertEquals("(((dependencyBuilder.input(\"A\").build() && dependencyBuilder.input(\"B\").build())"
                + " || (dependencyBuilder.input(\"C\").build() && dependencyBuilder.input(\"D\").build()))"
                + " && (dependencyBuilder.input(\"A\").build() && dependencyBuilder.input(\"B\").build()))",
                inputLogicParser.parse(root));
    }

    public void testAllAnd() throws JDOMException {
        //@formatter:off
        String xml=
            "<input-logic>"+
                "<and name=\"test\">"+
                     "<data-in dataset=\"A\" />"+
                     "<data-in dataset=\"B\" />"+
                     "<data-in dataset=\"C\" />"+
                     "<data-in dataset=\"D\" />"+
                     "<data-in dataset=\"E\" />"+
                     "<data-in dataset=\"F\" />"+
            "</and>"+
        "</input-logic>";
        //@formatter:on
        Element root = XmlUtils.parseXml(xml);
        InputLogicParser inputLogicParser = new InputLogicParser();
        assertEquals("(dependencyBuilder.input(\"A\").build() && dependencyBuilder.input(\"B\").build() && "
                + "dependencyBuilder.input(\"C\").build() && dependencyBuilder.input(\"D\").build() && "
                + "dependencyBuilder.input(\"E\").build() && dependencyBuilder.input(\"F\").build())",
                inputLogicParser.parse(root));
    }

    public void testDataIn() throws JDOMException {
        //@formatter:off
        String xml=
            "<input-logic>"+
                  "<data-in dataset=\"A\" />"+
            "</input-logic>";
        //@formatter:on
        Element root = XmlUtils.parseXml(xml);
        InputLogicParser inputLogicParser = new InputLogicParser();
        assertEquals("dependencyBuilder.input(\"A\").build()", inputLogicParser.parse(root));
    }

    public void testMinWait() throws JDOMException {
        //@formatter:off
        String xml =
        "<input-logic>" +
             "<and name=\"test\" min=\"3\" wait=\"10\">" +
                  "<data-in dataset=\"A\"/> " +
                  "<data-in dataset=\"B\"/>" +
             "</and>" +
        "</input-logic>";
       //@formatter:on
        Element root = XmlUtils.parseXml(xml);
        InputLogicParser inputLogicParser = new InputLogicParser();
        assertEquals("(dependencyBuilder.input(\"A\").min(3).inputWait(10).build() "
                + "&& dependencyBuilder.input(\"B\").min(3).inputWait(10).build())",
                inputLogicParser.parseWithName(root, "test"));

        assertEquals("(dependencyBuilder.input(\"A\").min(3).inputWait(10).build() "
                + "&& dependencyBuilder.input(\"B\").min(3).inputWait(10).build())", inputLogicParser.parse(root));
    }

    public void testOrAndDataIn() throws JDOMException {
        //@formatter:off
        String xml =
                "<input-logic>" +
                       "<or>" +
                        "<and>" +
                            "<data-in dataset=\"A\"/> " +
                            "<data-in dataset=\"B\"/>" +
                        "</and>" +
                        "<data-in dataset=\"C\"/>" +
                        "</or>"+
                "</input-logic>";
      //@formatter:on
        Element root = XmlUtils.parseXml(xml);
        InputLogicParser inputLogicParser = new InputLogicParser();
        assertEquals(
                "((dependencyBuilder.input(\"A\").build() && dependencyBuilder.input(\"B\").build()) || "
                + "dependencyBuilder.input(\"C\").build())",
                inputLogicParser.parse(root));

    }



}
