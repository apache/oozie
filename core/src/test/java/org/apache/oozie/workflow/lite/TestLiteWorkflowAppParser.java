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

package org.apache.oozie.workflow.lite;


import java.io.StringReader;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.oozie.service.ActionService;
import org.apache.oozie.service.LiteWorkflowStoreService;
import org.apache.oozie.service.SchemaService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.TestLiteWorkflowAppService;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.lite.TestLiteWorkflowLib.TestActionNodeHandler;
import org.apache.oozie.workflow.lite.TestLiteWorkflowLib.TestDecisionNodeHandler;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.action.hadoop.DistcpActionExecutor;
import org.apache.oozie.action.hadoop.HiveActionExecutor;
import org.apache.hadoop.conf.Configuration;

public class TestLiteWorkflowAppParser extends XTestCase {
    public static String dummyConf = "<java></java>";

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        setSystemProperty("oozie.service.SchemaService.wf.ext.schemas", "hive-action-0.2.xsd");
        new Services().init();
    }

    @Override
    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    public void testParserGlobal() throws Exception {
        LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
            LiteWorkflowStoreService.LiteControlNodeHandler.class,
            LiteWorkflowStoreService.LiteDecisionHandler.class,
            LiteWorkflowStoreService.LiteActionHandler.class);

        LiteWorkflowApp app = parser.validateAndParse(IOUtils.getResourceAsReader("wf-schema-valid-global.xml", -1),
                new Configuration());

        String d = app.getNode("d").getConf();
        String expectedD =
             "<map-reduce xmlns=\"uri:oozie:workflow:0.4\">\r\n" +
             "  <prepare>\r\n" +
             "    <delete path=\"/tmp\" />\r\n" +
             "    <mkdir path=\"/tmp\" />\r\n" +
             "  </prepare>\r\n" +
             "  <streaming>\r\n" +
             "    <mapper>/mycat.sh</mapper>\r\n" +
             "    <reducer>/mywc.sh</reducer>\r\n" +
             "  </streaming>\r\n" +
             "  <file>/tmp</file>\r\n" +
             "  <archive>/tmp</archive>\r\n" +
             "  <job-tracker>${foo}</job-tracker>\r\n" +
             "  <name-node>bar</name-node>\r\n" +
             "  <configuration>\r\n" +
             "    <property>\r\n" +
             "      <name>b</name>\r\n" +
             "      <value>B</value>\r\n" +
             "    </property>\r\n" +
             "    <property>\r\n" +
             "      <name>a</name>\r\n" +
             "      <value>A</value>\r\n" +
             "    </property>\r\n" +
             "  </configuration>\r\n" +
             "</map-reduce>";
        d = d.replaceAll(" xmlns=?(\"|\')(\"|\')", "");
        d = d.replaceAll("\\s*<source>.*</source>", "");    // remove the <source> added by Hadoop 2
        d = d.replaceAll("\\s*<!--Loaded from Unknown-->", "");   // remove the <!--LoadedfromUnknown--> added by Hadoop 1.2.1
        System.out.println("\n" + d +"\n");
        assertEquals(expectedD.replaceAll(" ",""), d.replaceAll(" ", ""));

    }

    public void testParserGlobalJobXML() throws Exception {
        LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
            LiteWorkflowStoreService.LiteControlNodeHandler.class,
            LiteWorkflowStoreService.LiteDecisionHandler.class,
            LiteWorkflowStoreService.LiteActionHandler.class);

        LiteWorkflowApp app = parser.validateAndParse(IOUtils.getResourceAsReader("wf-schema-valid-global-jobXml.xml", -1),
                new Configuration());

        String d = app.getNode("d").getConf();
        String expectedD =
             "<map-reduce xmlns=\"uri:oozie:workflow:0.4\">\r\n" +
             "  <prepare>\r\n" +
             "    <delete path=\"/tmp\" />\r\n" +
             "    <mkdir path=\"/tmp\" />\r\n" +
             "  </prepare>\r\n" +
             "  <streaming>\r\n" +
             "    <mapper>/mycat.sh</mapper>\r\n" +
             "    <reducer>/mywc.sh</reducer>\r\n" +
             "  </streaming>\r\n" +
             "  <job-xml>/tmp</job-xml>\r\n" +
             "  <file>/tmp</file>\r\n" +
             "  <archive>/tmp</archive>\r\n" +
             "  <job-tracker>foo</job-tracker>\r\n" +
             "  <name-node>bar</name-node>\r\n" +
             "  <job-xml>/spam1</job-xml>\r\n" +
             "  <job-xml>/spam2</job-xml>\r\n" +
             "  <configuration>\r\n" +
             "    <property>\r\n" +
             "      <name>b</name>\r\n" +
             "      <value>B</value>\r\n" +
             "    </property>\r\n" +
             "    <property>\r\n" +
             "      <name>a</name>\r\n" +
             "      <value>A</value>\r\n" +
             "    </property>\r\n" +
             "  </configuration>\r\n" +
             "</map-reduce>";
        d = d.replaceAll(" xmlns=?(\"|\')(\"|\')", "");
        d = d.replaceAll("\\s*<source>.*</source>", "");    // remove the <source> added by Hadoop 2
        d = d.replaceAll("\\s*<!--Loaded from Unknown-->", "");   // remove the <!--LoadedfromUnknown--> added by Hadoop 1.2.1
        assertEquals(expectedD.replaceAll(" ",""), d.replaceAll(" ", ""));

    }

    public void testParserGlobalLocalAlreadyExists() throws Exception{
        LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
                LiteWorkflowStoreService.LiteControlNodeHandler.class,
                LiteWorkflowStoreService.LiteDecisionHandler.class,
                LiteWorkflowStoreService.LiteActionHandler.class);

        LiteWorkflowApp app = parser.validateAndParse(IOUtils.getResourceAsReader("wf-schema-valid-global.xml", -1),
                new Configuration());

        String e = app.getNode("e").getConf();
        String expectedE =
                "<pig xmlns=\"uri:oozie:workflow:0.4\">\r\n" +
                "  <prepare>\r\n" +
                "    <delete path=\"/tmp\" />\r\n" +
                "    <mkdir path=\"/tmp\" />\r\n" +
                "  </prepare>\r\n" +
                "  <configuration>\r\n" +
                "    <property>\r\n" +
                "      <name>b</name>\r\n" +
                "      <value>B</value>\r\n" +
                "    </property>\r\n" +
                "    <property>\r\n" +
                "      <name>a</name>\r\n" +
                "      <value>A2</value>\r\n" +
                "    </property>\r\n" +
                "  </configuration>\r\n" +
                "  <script>/tmp</script>\r\n" +
                "  <param>x</param>\r\n" +
                "  <file>/tmp</file>\r\n" +
                "  <file>/tmp</file>\r\n" +
                "  <job-tracker>${foo}</job-tracker>\r\n" +
                "  <name-node>bar</name-node>\r\n" +
                "</pig>";
        e = e.replaceAll(" xmlns=?(\"|\')(\"|\')", "");
        e = e.replaceAll("\\s*<source>.*</source>", "");    // remove the <source> added by Hadoop 2
        e = e.replaceAll("\\s*<!--Loaded from Unknown-->", "");   // remove the <!--LoadedfromUnknown--> added by Hadoop 1.2.1
        assertEquals(expectedE.replaceAll(" ", ""), e.replaceAll(" ", ""));

    }

    public void testParserGlobalExtensionActions() throws Exception {
        LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
            LiteWorkflowStoreService.LiteControlNodeHandler.class,
            LiteWorkflowStoreService.LiteDecisionHandler.class,
            LiteWorkflowStoreService.LiteActionHandler.class);

        LiteWorkflowApp app = parser.validateAndParse(IOUtils.getResourceAsReader("wf-schema-valid-global-ext.xml", -1),
                new Configuration());

        String a = app.getNode("a").getConf();
        String expectedA =
             "<hive xmlns=\"uri:oozie:hive-action:0.2\">\r\n" +
             "  <prepare>\r\n" +
             "    <delete path=\"/tmp\" />\r\n" +
             "    <mkdir path=\"/tmp\" />\r\n" +
             "  </prepare>\r\n" +
             "  <configuration>\r\n" +
             "    <property>\r\n" +
             "      <name>b</name>\r\n" +
             "      <value>B</value>\r\n" +
             "    </property>\r\n" +
             "    <property>\r\n" +
             "      <name>a</name>\r\n" +
             "      <value>A</value>\r\n" +
             "    </property>\r\n" +
             "    <property>\r\n" +
             "      <name>c</name>\r\n" +
             "      <value>C</value>\r\n" +
             "    </property>\r\n" +
             "  </configuration>\r\n" +
             "  <script>script.q</script>\r\n" +
             "  <param>INPUT=/tmp/table</param>\r\n" +
             "  <param>OUTPUT=/tmp/hive</param>\r\n" +
             "  <job-tracker>foo</job-tracker>\r\n" +
             "  <name-node>bar</name-node>\r\n" +
             "</hive>";
        a = a.replaceAll(" xmlns=?(\"|\')(\"|\')", "");
        a = a.replaceAll("\\s*<source>.*</source>", "");    // remove the <source> added by Hadoop 2
        a = a.replaceAll("\\s*<!--Loaded from Unknown-->", "");   // remove the <!--LoadedfromUnknown--> added by Hadoop 1.2.1
        assertEquals(expectedA.replaceAll(" ",""), a.replaceAll(" ", ""));
    }

    public void testParserGlobalExtensionActionsLocalAlreadyExists() throws Exception {
        LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
            LiteWorkflowStoreService.LiteControlNodeHandler.class,
            LiteWorkflowStoreService.LiteDecisionHandler.class,
            LiteWorkflowStoreService.LiteActionHandler.class);

        LiteWorkflowApp app = parser.validateAndParse(IOUtils.getResourceAsReader("wf-schema-valid-global-ext.xml", -1),
                new Configuration());

        String b = app.getNode("b").getConf();
        String expectedB =
             "<distcp xmlns=\"uri:oozie:distcp-action:0.1\">\r\n" +
             "  <job-tracker>blah</job-tracker>\r\n" +
             "  <name-node>meh</name-node>\r\n" +
             "  <prepare>\r\n" +
             "    <delete path=\"/tmp2\" />\r\n" +
             "    <mkdir path=\"/tmp2\" />\r\n" +
             "  </prepare>\r\n" +
             "  <configuration>\r\n" +
             "    <property>\r\n" +
             "      <name>b</name>\r\n" +
             "      <value>B</value>\r\n" +
             "    </property>\r\n" +
             "    <property>\r\n" +
             "      <name>a</name>\r\n" +
             "      <value>A2</value>\r\n" +
             "    </property>\r\n" +
             "  </configuration>\r\n" +
             "  <arg>/tmp/data.txt</arg>\r\n" +
             "  <arg>/tmp2/data.txt</arg>\r\n" +
             "</distcp>";
        b = b.replaceAll(" xmlns=?(\"|\')(\"|\')", "");
        b = b.replaceAll("\\s*<source>.*</source>", "");    // remove the <source> added by Hadoop 2
        b = b.replaceAll("\\s*<!--Loaded from Unknown-->", "");   // remove the <!--LoadedfromUnknown--> added by Hadoop 1.2.1
        assertEquals(expectedB.replaceAll(" ",""), b.replaceAll(" ", ""));
    }

    public void testParserGlobalExtensionActionsNoGlobal() throws Exception {
        LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
            LiteWorkflowStoreService.LiteControlNodeHandler.class,
            LiteWorkflowStoreService.LiteDecisionHandler.class,
            LiteWorkflowStoreService.LiteActionHandler.class);

        // If no global section is defined, some extension actions (e.g. hive) must still have name-node and job-tracker elements
        // or the handleGlobal() method will throw an exception

        parser.validateAndParse(IOUtils.getResourceAsReader("wf-schema-valid-global-ext-no-global.xml", -1), new Configuration());

        try {
            parser.validateAndParse(IOUtils.getResourceAsReader("wf-schema-invalid-global-ext-no-global.xml", -1),
                    new Configuration());
            fail();
        }
        catch (WorkflowException ex) {
            assertEquals(ErrorCode.E0701, ex.getErrorCode());
        }
        catch (Exception ex) {
            fail();
        }
    }

    public void testParser() throws Exception {
        LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
                                                                 LiteWorkflowStoreService.LiteControlNodeHandler.class,
                                                                 LiteWorkflowStoreService.LiteDecisionHandler.class,
                                                                 LiteWorkflowStoreService.LiteActionHandler.class);

        parser.validateAndParse(IOUtils.getResourceAsReader("wf-schema-valid.xml", -1), new Configuration());

        try {
            // Check TestLiteWorkflowAppService.TestActionExecutor is registered.
            parser.validateAndParse(IOUtils.getResourceAsReader("wf-unsupported-action.xml", -1), new Configuration());
            fail();
        }
        catch (WorkflowException ex) {
            assertEquals(ErrorCode.E0723, ex.getErrorCode());
        }
        catch (Exception ex) {
            fail();
        }

        try {
            parser.validateAndParse(IOUtils.getResourceAsReader("wf-loop2-invalid.xml", -1), new Configuration());
            fail();
        }
        catch (WorkflowException ex) {
            assertEquals(ErrorCode.E0706, ex.getErrorCode());
        }
        catch (Exception ex) {
            fail();
        }

        try {
            parser.validateAndParse(IOUtils.getResourceAsReader("wf-transition-invalid.xml", -1), new Configuration());
            fail();
        }
        catch (WorkflowException ex) {
            assertEquals(ErrorCode.E0708, ex.getErrorCode());
        }
        catch (Exception ex) {
            fail();
        }
    }

    /*
     * 1->ok->2
     * 2->ok->end
     */
   public void testWfNoForkJoin() throws WorkflowException  {
        LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
                LiteWorkflowStoreService.LiteControlNodeHandler.class,
                LiteWorkflowStoreService.LiteDecisionHandler.class,
                LiteWorkflowStoreService.LiteActionHandler.class);

        LiteWorkflowApp def = new LiteWorkflowApp("name", "def",
            new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class, "one"))
            .addNode(new ActionNodeDef("one", dummyConf, TestActionNodeHandler.class, "two", "three"))
            .addNode(new ActionNodeDef("two", dummyConf, TestActionNodeHandler.class, "end", "end"))
            .addNode(new ActionNodeDef("three", dummyConf, TestActionNodeHandler.class, "end", "end"))
            .addNode(new EndNodeDef("end", LiteWorkflowStoreService.LiteControlNodeHandler.class));

        try {
            invokeForkJoin(parser, def);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Unexpected Exception");
        }
    }

    /*
    f->(2,3)
    (2,3)->j
    */
    public void testSimpleForkJoin() throws WorkflowException {
        LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
                LiteWorkflowStoreService.LiteControlNodeHandler.class,
                LiteWorkflowStoreService.LiteDecisionHandler.class,
                LiteWorkflowStoreService.LiteActionHandler.class);

        LiteWorkflowApp def = new LiteWorkflowApp("wf", "<worklfow-app/>",
        new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class, "one"))
        .addNode(new ActionNodeDef("one", dummyConf, TestActionNodeHandler.class, "f", "end"))
        .addNode(new ForkNodeDef("f", LiteWorkflowStoreService.LiteControlNodeHandler.class,
                                 Arrays.asList(new String[]{"two", "three"})))
        .addNode(new ActionNodeDef("two", dummyConf, TestActionNodeHandler.class, "j", "k"))
        .addNode(new ActionNodeDef("three", dummyConf, TestActionNodeHandler.class, "j", "k"))
        .addNode(new JoinNodeDef("j", LiteWorkflowStoreService.LiteControlNodeHandler.class, "four"))
        .addNode(new ActionNodeDef("four", dummyConf, TestActionNodeHandler.class, "end", "end"))
        .addNode(new KillNodeDef("k", "kill", LiteWorkflowStoreService.LiteControlNodeHandler.class))
        .addNode(new EndNodeDef("end", LiteWorkflowStoreService.LiteControlNodeHandler.class));

        try {
            invokeForkJoin(parser, def);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Unexpected Exception");
        }
    }

    /*
     f->(2,3)
     2->f2
     3->j
     f2->(4,5,6)
     (4,5,6)->j2
     j2->7
     7->j
    */
    public void testNestedForkJoin() throws WorkflowException{
        LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
                LiteWorkflowStoreService.LiteControlNodeHandler.class,
                LiteWorkflowStoreService.LiteDecisionHandler.class,
                LiteWorkflowStoreService.LiteActionHandler.class);

        LiteWorkflowApp def = new LiteWorkflowApp("testWf", "<worklfow-app/>",
        new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class, "one"))
        .addNode(new ActionNodeDef("one", dummyConf, TestActionNodeHandler.class, "f","end"))
        .addNode(new ForkNodeDef("f", LiteWorkflowStoreService.LiteControlNodeHandler.class,
                                 Arrays.asList(new String[]{"two", "three"})))
        .addNode(new ActionNodeDef("two", dummyConf, TestActionNodeHandler.class, "f2", "k"))
        .addNode(new ActionNodeDef("three", dummyConf, TestActionNodeHandler.class, "j", "k"))
        .addNode(new ForkNodeDef("f2", LiteWorkflowStoreService.LiteControlNodeHandler.class,
                                 Arrays.asList(new String[]{"four", "five", "six"})))
        .addNode(new ActionNodeDef("four", dummyConf, TestActionNodeHandler.class, "j2", "k"))
        .addNode(new ActionNodeDef("five", dummyConf, TestActionNodeHandler.class, "j2", "k"))
        .addNode(new ActionNodeDef("six", dummyConf, TestActionNodeHandler.class, "j2", "k"))
        .addNode(new JoinNodeDef("j2", LiteWorkflowStoreService.LiteControlNodeHandler.class, "seven"))
        .addNode(new ActionNodeDef("seven", dummyConf, TestActionNodeHandler.class, "j", "k"))
        .addNode(new JoinNodeDef("j", LiteWorkflowStoreService.LiteControlNodeHandler.class, "end"))
        .addNode(new KillNodeDef("k", "kill", LiteWorkflowStoreService.LiteControlNodeHandler.class))
        .addNode(new EndNodeDef("end", LiteWorkflowStoreService.LiteControlNodeHandler.class));

        try {
            invokeForkJoin(parser, def);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Unexpected Exception");
        }
    }

    /*
      f->(2,3)
      2->j
      3->end
    */
    public void testForkJoinFailure() throws WorkflowException{
        LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
                LiteWorkflowStoreService.LiteControlNodeHandler.class,
                LiteWorkflowStoreService.LiteDecisionHandler.class,
                LiteWorkflowStoreService.LiteActionHandler.class);

        LiteWorkflowApp def = new LiteWorkflowApp("testWf", "<worklfow-app/>",
        new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class, "one"))
        .addNode(new ActionNodeDef("one", dummyConf, TestActionNodeHandler.class, "f","end"))
        .addNode(new ForkNodeDef("f", LiteWorkflowStoreService.LiteControlNodeHandler.class,
                                 Arrays.asList(new String[]{"two", "three"})))
        .addNode(new ActionNodeDef("two", dummyConf, TestActionNodeHandler.class, "j","k"))
        .addNode(new ActionNodeDef("three", dummyConf, TestActionNodeHandler.class, "end","k"))
        .addNode(new JoinNodeDef("j", LiteWorkflowStoreService.LiteControlNodeHandler.class, "k"))
        .addNode(new KillNodeDef("k", "kill", LiteWorkflowStoreService.LiteControlNodeHandler.class))
        .addNode(new EndNodeDef("end", LiteWorkflowStoreService.LiteControlNodeHandler.class));

        try {
            invokeForkJoin(parser, def);
            fail("Expected to catch an exception but did not encounter any");
        } catch (Exception ex) {
            WorkflowException we = (WorkflowException) ex.getCause();
            assertEquals(ErrorCode.E0737, we.getErrorCode());
            // Make sure the message contains the nodes involved in the invalid transition to end
            assertTrue(we.getMessage().contains("node [three]"));
            assertTrue(we.getMessage().contains("node [end]"));
        }
    }

    /*
     f->(2,3,4)
     2->j
     3->j
     4->f2
     f2->(5,6)
     5-j2
     6-j2
     j-j2
     j2-end
    */
    public void testNestedForkJoinFailure() throws WorkflowException {
        LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
                LiteWorkflowStoreService.LiteControlNodeHandler.class,
                LiteWorkflowStoreService.LiteDecisionHandler.class,
                LiteWorkflowStoreService.LiteActionHandler.class);

        LiteWorkflowApp def = new LiteWorkflowApp("testWf", "<worklfow-app/>",
            new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class, "one"))
            .addNode(new ActionNodeDef("one", dummyConf, TestActionNodeHandler.class, "f","end"))
            .addNode(new ForkNodeDef("f", LiteWorkflowStoreService.LiteControlNodeHandler.class,
                                     Arrays.asList(new String[]{"four", "three", "two"})))
            .addNode(new ActionNodeDef("two", dummyConf, TestActionNodeHandler.class, "j","k"))
            .addNode(new ActionNodeDef("three", dummyConf, TestActionNodeHandler.class, "j","k"))
            .addNode(new ActionNodeDef("four", dummyConf, TestActionNodeHandler.class, "f2","k"))
            .addNode(new ForkNodeDef("f2", LiteWorkflowStoreService.LiteControlNodeHandler.class,
                                     Arrays.asList(new String[]{"five", "six"})))
            .addNode(new ActionNodeDef("five", dummyConf, TestActionNodeHandler.class, "j2", "k"))
            .addNode(new ActionNodeDef("six", dummyConf, TestActionNodeHandler.class, "j2", "k"))
            .addNode(new JoinNodeDef("j", LiteWorkflowStoreService.LiteControlNodeHandler.class, "j2"))
            .addNode(new JoinNodeDef("j2", LiteWorkflowStoreService.LiteControlNodeHandler.class, "k"))
            .addNode(new KillNodeDef("k", "kill", LiteWorkflowStoreService.LiteControlNodeHandler.class))
            .addNode(new EndNodeDef("end", LiteWorkflowStoreService.LiteControlNodeHandler.class));

        try {
            invokeForkJoin(parser, def);
            fail("Expected to catch an exception but did not encounter any");
        } catch (Exception ex) {
            WorkflowException we = (WorkflowException) ex.getCause();
            assertEquals(ErrorCode.E0742, we.getErrorCode());
            assertTrue(we.getMessage().contains("[j2]"));
        }
    }

    /*
     f->(2,3)
     2->ok->3
     2->fail->j
     3->ok->j
     3->fail->k
     j->k
    */
    public void testTransitionFailure1() throws WorkflowException{
        LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
                LiteWorkflowStoreService.LiteControlNodeHandler.class,
                LiteWorkflowStoreService.LiteDecisionHandler.class,
                LiteWorkflowStoreService.LiteActionHandler.class);

        LiteWorkflowApp def = new LiteWorkflowApp("name", "def",
        new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class, "one"))
        .addNode(new ActionNodeDef("one", dummyConf, TestActionNodeHandler.class, "f", "end"))
        .addNode(new ForkNodeDef("f", LiteWorkflowStoreService.LiteControlNodeHandler.class,
                                 Arrays.asList(new String[]{"two", "three"})))
        .addNode(new ActionNodeDef("two", dummyConf, TestActionNodeHandler.class, "three", "j"))
        .addNode(new ActionNodeDef("three", dummyConf, TestActionNodeHandler.class, "j", "k"))
        .addNode(new JoinNodeDef("j", LiteWorkflowStoreService.LiteControlNodeHandler.class, "k"))
        .addNode(new KillNodeDef("k", "kill", LiteWorkflowStoreService.LiteControlNodeHandler.class))
        .addNode(new EndNodeDef("end", LiteWorkflowStoreService.LiteControlNodeHandler.class));

        try {
            invokeForkJoin(parser, def);
            fail("Expected to catch an exception but did not encounter any");
        } catch (Exception ex) {
            WorkflowException we = (WorkflowException) ex.getCause();
            assertEquals(ErrorCode.E0743, we.getErrorCode());
            // Make sure the message contains the node involved in the invalid transition
            assertTrue(we.getMessage().contains("three"));
        }

    }

    /*
    f->(2,3)
    2->fail->3
    2->ok->j
    3->ok->j
    3->fail->k
    j->end
   */
   public void testTransition2() throws WorkflowException{
       LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
               LiteWorkflowStoreService.LiteControlNodeHandler.class,
               LiteWorkflowStoreService.LiteDecisionHandler.class,
               LiteWorkflowStoreService.LiteActionHandler.class);

       LiteWorkflowApp def = new LiteWorkflowApp("name", "def",
       new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class, "one"))
       .addNode(new ActionNodeDef("one", dummyConf, TestActionNodeHandler.class, "f", "end"))
       .addNode(new ForkNodeDef("f", LiteWorkflowStoreService.LiteControlNodeHandler.class,
                                Arrays.asList(new String[]{"two","three"})))
       .addNode(new ActionNodeDef("two", dummyConf, TestActionNodeHandler.class, "j", "three"))
       .addNode(new ActionNodeDef("three", dummyConf, TestActionNodeHandler.class, "j", "k"))
       .addNode(new JoinNodeDef("j", LiteWorkflowStoreService.LiteControlNodeHandler.class, "end"))
       .addNode(new KillNodeDef("k", "kill", LiteWorkflowStoreService.LiteControlNodeHandler.class))
       .addNode(new EndNodeDef("end", LiteWorkflowStoreService.LiteControlNodeHandler.class));

       try {
           invokeForkJoin(parser, def);
       } catch (Exception ex) {
           ex.printStackTrace();
           fail("Unexpected Exception");
       }

   }

   /*
   f->(2,3)
   2->ok->j
   2->fail->4
   3->ok->4
   3->fail->k
   4->ok->j
   4->fail->k
   j->end
  */
   public void testTransition3() throws WorkflowException{
       LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
               LiteWorkflowStoreService.LiteControlNodeHandler.class,
               LiteWorkflowStoreService.LiteDecisionHandler.class,
               LiteWorkflowStoreService.LiteActionHandler.class);

       LiteWorkflowApp def = new LiteWorkflowApp("name", "def",
       new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class, "one"))
       .addNode(new ActionNodeDef("one", dummyConf, TestActionNodeHandler.class, "f", "end"))
       .addNode(new ForkNodeDef("f", LiteWorkflowStoreService.LiteControlNodeHandler.class,
                                Arrays.asList(new String[]{"two", "three"})))
       .addNode(new ActionNodeDef("two", dummyConf, TestActionNodeHandler.class, "j", "four"))
       .addNode(new ActionNodeDef("three", dummyConf, TestActionNodeHandler.class, "four", "k"))
       .addNode(new ActionNodeDef("four", dummyConf, TestActionNodeHandler.class, "j", "k"))
       .addNode(new JoinNodeDef("j", LiteWorkflowStoreService.LiteControlNodeHandler.class, "end"))
       .addNode(new KillNodeDef("k", "kill", LiteWorkflowStoreService.LiteControlNodeHandler.class))
       .addNode(new EndNodeDef("end", LiteWorkflowStoreService.LiteControlNodeHandler.class));

       try {
           invokeForkJoin(parser, def);
       } catch (Exception ex) {
           ex.printStackTrace();
           fail("Unexpected Exception");
       }
   }

   /*
    * f->(2,3)
    * 2->ok->j
    * 3->ok->j
    * j->6
    * 2->error->f1
    * 3->error->f1
    * f1->(4,5)
    * (4,5)->j1
    * j1->6
    * 6->k
    */
   public void testErrorTransitionForkJoin() throws WorkflowException {
       LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
               LiteWorkflowStoreService.LiteControlNodeHandler.class,
               LiteWorkflowStoreService.LiteDecisionHandler.class,
               LiteWorkflowStoreService.LiteActionHandler.class);

       LiteWorkflowApp def = new LiteWorkflowApp("wf", "<worklfow-app/>",
       new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class, "one"))
       .addNode(new ActionNodeDef("one", dummyConf, TestActionNodeHandler.class, "f", "end"))
       .addNode(new ForkNodeDef("f", LiteWorkflowStoreService.LiteControlNodeHandler.class,
                                Arrays.asList(new String[]{"two", "three"})))
       .addNode(new ActionNodeDef("two", dummyConf,  TestActionNodeHandler.class, "j", "f1"))
       .addNode(new ActionNodeDef("three", dummyConf, TestActionNodeHandler.class, "j", "f1"))
       .addNode(new ForkNodeDef("f1", LiteWorkflowStoreService.LiteControlNodeHandler.class,
                                Arrays.asList(new String[]{"four", "five"})))
       .addNode(new ActionNodeDef("four", dummyConf,  TestActionNodeHandler.class, "j1", "k"))
       .addNode(new ActionNodeDef("five", dummyConf, TestActionNodeHandler.class, "j1", "k"))
       .addNode(new JoinNodeDef("j", LiteWorkflowStoreService.LiteControlNodeHandler.class, "six"))
       .addNode(new JoinNodeDef("j1", LiteWorkflowStoreService.LiteControlNodeHandler.class, "six"))
       .addNode(new ActionNodeDef("six", dummyConf, TestActionNodeHandler.class, "k", "k"))
       .addNode(new KillNodeDef("k", "kill", LiteWorkflowStoreService.LiteControlNodeHandler.class))
       .addNode(new EndNodeDef("end", LiteWorkflowStoreService.LiteControlNodeHandler.class));

       try {
           invokeForkJoin(parser, def);
       } catch (Exception e) {
           e.printStackTrace();
           fail("Unexpected Exception");
       }
   }

   /*
    f->(2,3)
    2->decision node->{4,5,4}
    4->j
    5->j
    3->j
    */
    public void testDecisionForkJoin() throws WorkflowException{
        LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
                LiteWorkflowStoreService.LiteControlNodeHandler.class,
                LiteWorkflowStoreService.LiteDecisionHandler.class,
                LiteWorkflowStoreService.LiteActionHandler.class);
        LiteWorkflowApp def = new LiteWorkflowApp("name", "def",
        new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class, "one"))
        .addNode(new ActionNodeDef("one", dummyConf, TestActionNodeHandler.class, "f","end"))
        .addNode(new ForkNodeDef("f", LiteWorkflowStoreService.LiteControlNodeHandler.class,
                                 Arrays.asList(new String[]{"two", "three"})))
        .addNode(new DecisionNodeDef("two", dummyConf, TestDecisionNodeHandler.class,
                                     Arrays.asList(new String[]{"four","five","four"})))
        .addNode(new ActionNodeDef("four", dummyConf, TestActionNodeHandler.class, "j", "k"))
        .addNode(new ActionNodeDef("five", dummyConf, TestActionNodeHandler.class, "j", "k"))
        .addNode(new ActionNodeDef("three", dummyConf, TestActionNodeHandler.class, "j", "k"))
        .addNode(new JoinNodeDef("j", LiteWorkflowStoreService.LiteControlNodeHandler.class, "end"))
        .addNode(new KillNodeDef("k", "kill", LiteWorkflowStoreService.LiteControlNodeHandler.class))
        .addNode(new EndNodeDef("end", LiteWorkflowStoreService.LiteControlNodeHandler.class));

        try {
            invokeForkJoin(parser, def);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Unexpected Exception");
        }
    }

    /*
    f->(2,3)
    2->decision node->{4,j,4}
    3->decision node->{j,5,j}
    4->j
    5->j
    */
    public void testDecisionsToJoinForkJoin() throws WorkflowException{
        LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
                LiteWorkflowStoreService.LiteControlNodeHandler.class,
                LiteWorkflowStoreService.LiteDecisionHandler.class,
                LiteWorkflowStoreService.LiteActionHandler.class);
        LiteWorkflowApp def = new LiteWorkflowApp("name", "def",
        new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class, "one"))
        .addNode(new ActionNodeDef("one", dummyConf, TestActionNodeHandler.class, "f","end"))
        .addNode(new ForkNodeDef("f", LiteWorkflowStoreService.LiteControlNodeHandler.class,
                                 Arrays.asList(new String[]{"two","three"})))
        .addNode(new DecisionNodeDef("two", dummyConf, TestDecisionNodeHandler.class,
                                     Arrays.asList(new String[]{"four","j","four"})))
        .addNode(new DecisionNodeDef("three", dummyConf, TestDecisionNodeHandler.class,
                                     Arrays.asList(new String[]{"j","five","j"})))
        .addNode(new ActionNodeDef("four", dummyConf, TestActionNodeHandler.class, "j", "k"))
        .addNode(new ActionNodeDef("five", dummyConf, TestActionNodeHandler.class, "j", "k"))
        .addNode(new JoinNodeDef("j", LiteWorkflowStoreService.LiteControlNodeHandler.class, "end"))
        .addNode(new KillNodeDef("k", "kill", LiteWorkflowStoreService.LiteControlNodeHandler.class))
        .addNode(new EndNodeDef("end", LiteWorkflowStoreService.LiteControlNodeHandler.class));

        try {
            invokeForkJoin(parser, def);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Unexpected Exception");
        }
    }

    /*
    f->(2,3)
    2->decision node->{4,k,4}
    3->decision node->{k,5,k}
    4->j
    5->j
    */
    public void testDecisionsToKillForkJoin() throws WorkflowException{
        LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
                LiteWorkflowStoreService.LiteControlNodeHandler.class,
                LiteWorkflowStoreService.LiteDecisionHandler.class,
                LiteWorkflowStoreService.LiteActionHandler.class);
        LiteWorkflowApp def = new LiteWorkflowApp("name", "def",
        new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class, "one"))
        .addNode(new ActionNodeDef("one", dummyConf, TestActionNodeHandler.class, "f","end"))
        .addNode(new ForkNodeDef("f", LiteWorkflowStoreService.LiteControlNodeHandler.class,
                                 Arrays.asList(new String[]{"two","three"})))
        .addNode(new DecisionNodeDef("two", dummyConf, TestDecisionNodeHandler.class,
                                     Arrays.asList(new String[]{"four","k","four"})))
        .addNode(new DecisionNodeDef("three", dummyConf, TestDecisionNodeHandler.class,
                                     Arrays.asList(new String[]{"k","five","k"})))
        .addNode(new ActionNodeDef("four", dummyConf, TestActionNodeHandler.class, "j", "k"))
        .addNode(new ActionNodeDef("five", dummyConf, TestActionNodeHandler.class, "j", "k"))
        .addNode(new JoinNodeDef("j", LiteWorkflowStoreService.LiteControlNodeHandler.class, "end"))
        .addNode(new KillNodeDef("k", "kill", LiteWorkflowStoreService.LiteControlNodeHandler.class))
        .addNode(new EndNodeDef("end", LiteWorkflowStoreService.LiteControlNodeHandler.class));

        try {
            invokeForkJoin(parser, def);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Unexpected Exception");
        }
    }

    /*
     *f->(2,3)
     *2->decision node->{3,4}
     *3->ok->j
     *3->fail->k
     *4->ok->j
     *4->fail->k
     *j->end
     */
    public void testDecisionForkJoinFailure() throws WorkflowException{
        LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
                LiteWorkflowStoreService.LiteControlNodeHandler.class,
                LiteWorkflowStoreService.LiteDecisionHandler.class,
                LiteWorkflowStoreService.LiteActionHandler.class);
        LiteWorkflowApp def = new LiteWorkflowApp("name", "def",
        new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class, "one"))
        .addNode(new ActionNodeDef("one", dummyConf, TestActionNodeHandler.class, "f","end"))
        .addNode(new ForkNodeDef("f", LiteWorkflowStoreService.LiteControlNodeHandler.class,
                                 Arrays.asList(new String[]{"two","three"})))
        .addNode(new DecisionNodeDef("two", dummyConf, TestDecisionNodeHandler.class,
                                     Arrays.asList(new String[]{"four","three"})))
        .addNode(new ActionNodeDef("three", dummyConf, TestActionNodeHandler.class, "j", "k"))
        .addNode(new ActionNodeDef("four", dummyConf, TestActionNodeHandler.class, "j", "k"))
        .addNode(new JoinNodeDef("j", LiteWorkflowStoreService.LiteControlNodeHandler.class, "end"))
        .addNode(new KillNodeDef("k", "kill", LiteWorkflowStoreService.LiteControlNodeHandler.class))
        .addNode(new EndNodeDef("end", LiteWorkflowStoreService.LiteControlNodeHandler.class));

        try {
            invokeForkJoin(parser, def);
            fail("Expected to catch an exception but did not encounter any");
        } catch (Exception ex) {
            WorkflowException we = (WorkflowException) ex.getCause();
            assertEquals(ErrorCode.E0743, we.getErrorCode());
            // Make sure the message contains the node involved in the invalid transition
            assertTrue(we.getMessage().contains("three"));
        }
    }

    /*
     *f->(2,3)
     *2->decision node->{4,end}
     *3->ok->j
     *3->fail->k
     *4->ok->j
     *4->fail->k
     *j->end
     */
    public void testDecisionToEndForkJoinFailure() throws WorkflowException{
        LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
                LiteWorkflowStoreService.LiteControlNodeHandler.class,
                LiteWorkflowStoreService.LiteDecisionHandler.class,
                LiteWorkflowStoreService.LiteActionHandler.class);
        LiteWorkflowApp def = new LiteWorkflowApp("name", "def",
            new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class, "one"))
        .addNode(new ActionNodeDef("one", dummyConf, TestActionNodeHandler.class, "f","end"))
        .addNode(new ForkNodeDef("f", LiteWorkflowStoreService.LiteControlNodeHandler.class,
                                 Arrays.asList(new String[]{"two", "three"})))
        .addNode(new DecisionNodeDef("two", dummyConf, TestDecisionNodeHandler.class,
                                     Arrays.asList(new String[]{"four","end"})))
        .addNode(new ActionNodeDef("three", dummyConf, TestActionNodeHandler.class, "j", "k"))
        .addNode(new ActionNodeDef("four", dummyConf, TestActionNodeHandler.class, "j", "k"))
        .addNode(new JoinNodeDef("j", LiteWorkflowStoreService.LiteControlNodeHandler.class, "end"))
        .addNode(new KillNodeDef("k", "kill", LiteWorkflowStoreService.LiteControlNodeHandler.class))
        .addNode(new EndNodeDef("end", LiteWorkflowStoreService.LiteControlNodeHandler.class));

        try {
            invokeForkJoin(parser, def);
            fail("Expected to catch an exception but did not encounter any");
        } catch (Exception ex) {
            WorkflowException we = (WorkflowException) ex.getCause();
            assertEquals(ErrorCode.E0737, we.getErrorCode());
            // Make sure the message contains the nodes involved in the invalid transition to end
            assertTrue(we.getMessage().contains("node [two]"));
            assertTrue(we.getMessage().contains("node [end]"));
        }
    }

    /*
     *f->(2,j)
     *2->decision node->{3,4}
     *3->ok->4
     *3->fail->k
     *4->ok->j
     *4->fail->k
     *j->end
     */
    public void testDecisionTwoPathsForkJoin() throws WorkflowException{
        LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
                LiteWorkflowStoreService.LiteControlNodeHandler.class,
                LiteWorkflowStoreService.LiteDecisionHandler.class,
                LiteWorkflowStoreService.LiteActionHandler.class);
        LiteWorkflowApp def = new LiteWorkflowApp("name", "def",
            new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class, "one"))
        .addNode(new ActionNodeDef("one", dummyConf, TestActionNodeHandler.class, "f","end"))
        .addNode(new ForkNodeDef("f", LiteWorkflowStoreService.LiteControlNodeHandler.class,
                                 Arrays.asList(new String[]{"two", "j"})))
        .addNode(new DecisionNodeDef("two", dummyConf, TestDecisionNodeHandler.class,
                                     Arrays.asList(new String[]{"three","four"})))
        .addNode(new ActionNodeDef("three", dummyConf, TestActionNodeHandler.class, "four", "k"))
        .addNode(new ActionNodeDef("four", dummyConf, TestActionNodeHandler.class, "j", "k"))
        .addNode(new JoinNodeDef("j", LiteWorkflowStoreService.LiteControlNodeHandler.class, "end"))
        .addNode(new KillNodeDef("k", "kill", LiteWorkflowStoreService.LiteControlNodeHandler.class))
        .addNode(new EndNodeDef("end", LiteWorkflowStoreService.LiteControlNodeHandler.class));

        try {
            invokeForkJoin(parser, def);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Unexpected Exception");
        }
    }

    /*
     *f->(2,j)
     *2->decision node->{3,4}
     *3->decision node->{4,5}
     *4->ok->j
     *4->fail->k
     *5->ok->4
     *5->fail->k
     *j->end
     */
    public void testMultipleDecisionThreePathsForkJoin() throws WorkflowException{
        LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
                LiteWorkflowStoreService.LiteControlNodeHandler.class,
                LiteWorkflowStoreService.LiteDecisionHandler.class,
                LiteWorkflowStoreService.LiteActionHandler.class);
        LiteWorkflowApp def = new LiteWorkflowApp("name", "def",
            new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class, "one"))
        .addNode(new ActionNodeDef("one", dummyConf, TestActionNodeHandler.class, "f","end"))
        .addNode(new ForkNodeDef("f", LiteWorkflowStoreService.LiteControlNodeHandler.class,
                                 Arrays.asList(new String[]{"two", "j"})))
        .addNode(new DecisionNodeDef("two", dummyConf, TestDecisionNodeHandler.class,
                                     Arrays.asList(new String[]{"three","four"})))
        .addNode(new DecisionNodeDef("three", dummyConf, TestDecisionNodeHandler.class,
                             Arrays.asList(new String[]{"four","five"})))
        .addNode(new ActionNodeDef("four", dummyConf, TestActionNodeHandler.class, "j", "k"))
        .addNode(new ActionNodeDef("five", dummyConf, TestActionNodeHandler.class, "four", "k"))
        .addNode(new JoinNodeDef("j", LiteWorkflowStoreService.LiteControlNodeHandler.class, "end"))
        .addNode(new KillNodeDef("k", "kill", LiteWorkflowStoreService.LiteControlNodeHandler.class))
        .addNode(new EndNodeDef("end", LiteWorkflowStoreService.LiteControlNodeHandler.class));

        try {
            invokeForkJoin(parser, def);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Unexpected Exception");
        }
    }

    /*
     *f->(2,4)
     *2->decision node->{3,4}
     *3->decision node->{4,5}
     *4->ok->j
     *4->fail->k
     *5->ok->4
     *5->fail->k
     *j->end
     */
    public void testMultipleDecisionThreePathsForkJoinFailure() throws WorkflowException{
        LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
                LiteWorkflowStoreService.LiteControlNodeHandler.class,
                LiteWorkflowStoreService.LiteDecisionHandler.class,
                LiteWorkflowStoreService.LiteActionHandler.class);
        LiteWorkflowApp def = new LiteWorkflowApp("name", "def",
            new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class, "one"))
        .addNode(new ActionNodeDef("one", dummyConf, TestActionNodeHandler.class, "f","end"))
        .addNode(new ForkNodeDef("f", LiteWorkflowStoreService.LiteControlNodeHandler.class,
                                 Arrays.asList(new String[]{"two", "four"})))
        .addNode(new DecisionNodeDef("two", dummyConf, TestDecisionNodeHandler.class,
                                     Arrays.asList(new String[]{"three","four"})))
        .addNode(new DecisionNodeDef("three", dummyConf, TestDecisionNodeHandler.class,
                             Arrays.asList(new String[]{"four","five"})))
        .addNode(new ActionNodeDef("four", dummyConf, TestActionNodeHandler.class, "j", "k"))
        .addNode(new ActionNodeDef("five", dummyConf, TestActionNodeHandler.class, "four", "k"))
        .addNode(new JoinNodeDef("j", LiteWorkflowStoreService.LiteControlNodeHandler.class, "end"))
        .addNode(new KillNodeDef("k", "kill", LiteWorkflowStoreService.LiteControlNodeHandler.class))
        .addNode(new EndNodeDef("end", LiteWorkflowStoreService.LiteControlNodeHandler.class));

        try {
            invokeForkJoin(parser, def);
            fail("Expected to catch an exception but did not encounter any");
        } catch (Exception ex) {
            WorkflowException we = (WorkflowException) ex.getCause();
            assertEquals(ErrorCode.E0743, we.getErrorCode());
            // Make sure the message contains the node involved in the invalid transition
            assertTrue(we.getMessage().contains("four"));
        }
    }

    /*
     *f->(2,6)
     *2->decision node->{3,4}
     *3->decision node->{4,5}
     *6->decision node->{4,j}
     *4->ok->j
     *4->fail->k
     *5->ok->4
     *5->fail->k
     *j->end
     */
    public void testMultipleDecisionThreePathsForkJoinFailure2() throws WorkflowException{
        LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
                LiteWorkflowStoreService.LiteControlNodeHandler.class,
                LiteWorkflowStoreService.LiteDecisionHandler.class,
                LiteWorkflowStoreService.LiteActionHandler.class);
        LiteWorkflowApp def = new LiteWorkflowApp("name", "def",
            new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class, "one"))
        .addNode(new ActionNodeDef("one", dummyConf, TestActionNodeHandler.class, "f","end"))
        .addNode(new ForkNodeDef("f", LiteWorkflowStoreService.LiteControlNodeHandler.class,
                                 Arrays.asList(new String[]{"two", "four"})))
        .addNode(new DecisionNodeDef("two", dummyConf, TestDecisionNodeHandler.class,
                                     Arrays.asList(new String[]{"three","four"})))
        .addNode(new DecisionNodeDef("three", dummyConf, TestDecisionNodeHandler.class,
                             Arrays.asList(new String[]{"four","five"})))
        .addNode(new DecisionNodeDef("six", dummyConf, TestDecisionNodeHandler.class,
                     Arrays.asList(new String[]{"four","j"})))
        .addNode(new ActionNodeDef("four", dummyConf, TestActionNodeHandler.class, "j", "k"))
        .addNode(new ActionNodeDef("five", dummyConf, TestActionNodeHandler.class, "four", "k"))
        .addNode(new JoinNodeDef("j", LiteWorkflowStoreService.LiteControlNodeHandler.class, "end"))
        .addNode(new KillNodeDef("k", "kill", LiteWorkflowStoreService.LiteControlNodeHandler.class))
        .addNode(new EndNodeDef("end", LiteWorkflowStoreService.LiteControlNodeHandler.class));

        try {
            invokeForkJoin(parser, def);
            fail("Expected to catch an exception but did not encounter any");
        } catch (Exception ex) {
            WorkflowException we = (WorkflowException) ex.getCause();
            assertEquals(ErrorCode.E0743, we.getErrorCode());
            // Make sure the message contains the node involved in the invalid transition
            assertTrue(we.getMessage().contains("four"));
        }
    }

    /*
     * 1->decision node->{f1, f2}
     * f1->(2,3)
     * f2->(4,5)
     * (2,3)->j1
     * (4,5)->j2
     * j1->end
     * j2->end
     */
    public void testDecisionMultipleForks() throws WorkflowException{
        LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
                LiteWorkflowStoreService.LiteControlNodeHandler.class,
                LiteWorkflowStoreService.LiteDecisionHandler.class,
                LiteWorkflowStoreService.LiteActionHandler.class);
        LiteWorkflowApp def = new LiteWorkflowApp("name", "def",
        new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class, "one"))
        .addNode(new DecisionNodeDef("one", dummyConf, TestDecisionNodeHandler.class,
                                     Arrays.asList(new String[]{"f1","f2"})))
        .addNode(new ForkNodeDef("f1", LiteWorkflowStoreService.LiteControlNodeHandler.class,
                                 Arrays.asList(new String[]{"two", "three"})))
        .addNode(new ForkNodeDef("f2", LiteWorkflowStoreService.LiteControlNodeHandler.class,
                                 Arrays.asList(new String[]{"four","five"})))
        .addNode(new ActionNodeDef("two", dummyConf, TestActionNodeHandler.class, "j1", "k"))
        .addNode(new ActionNodeDef("three", dummyConf, TestActionNodeHandler.class, "j1", "k"))
        .addNode(new ActionNodeDef("four", dummyConf, TestActionNodeHandler.class, "j2", "k"))
        .addNode(new ActionNodeDef("five", dummyConf, TestActionNodeHandler.class, "j2", "k"))
        .addNode(new JoinNodeDef("j1", LiteWorkflowStoreService.LiteControlNodeHandler.class, "end"))
        .addNode(new JoinNodeDef("j2", LiteWorkflowStoreService.LiteControlNodeHandler.class, "end"))
        .addNode(new KillNodeDef("k", "kill", LiteWorkflowStoreService.LiteControlNodeHandler.class))
        .addNode(new EndNodeDef("end", LiteWorkflowStoreService.LiteControlNodeHandler.class));

        try {
            invokeForkJoin(parser, def);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Unexpected Exception");
        }
    }

    /*
     * f->(1,2)
     * 1->ok->j1
     * 1->fail->k
     * 2->ok->j2
     * 2->fail->k
     * j1->end
     * j2->f2
     * f2->k,k
     */
    public void testForkJoinMismatch() throws WorkflowException {
        LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
                LiteWorkflowStoreService.LiteControlNodeHandler.class,
                LiteWorkflowStoreService.LiteDecisionHandler.class,
                LiteWorkflowStoreService.LiteActionHandler.class);
        LiteWorkflowApp def = new LiteWorkflowApp("name", "def",
            new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class, "f"))
        .addNode(new ForkNodeDef("f", LiteWorkflowStoreService.LiteControlNodeHandler.class,
                        Arrays.asList(new String[]{"one", "two"})))
        .addNode(new ActionNodeDef("one", dummyConf, TestActionNodeHandler.class, "j1", "k"))
        .addNode(new JoinNodeDef("j1", LiteWorkflowStoreService.LiteControlNodeHandler.class, "end"))
        .addNode(new ActionNodeDef("two", dummyConf, TestActionNodeHandler.class, "j2", "k"))
        .addNode(new JoinNodeDef("j2", LiteWorkflowStoreService.LiteControlNodeHandler.class, "f2"))
        .addNode(new ForkNodeDef("f2", LiteWorkflowStoreService.LiteControlNodeHandler.class,
                        Arrays.asList(new String[]{"k", "k"})))
        .addNode(new KillNodeDef("k", "kill", LiteWorkflowStoreService.LiteControlNodeHandler.class))
        .addNode(new EndNodeDef("end", LiteWorkflowStoreService.LiteControlNodeHandler.class));
        try {
            invokeForkJoin(parser, def);
            fail("Expected to catch an exception but did not encounter any");
        } catch (Exception ex) {
            WorkflowException we = (WorkflowException) ex.getCause();
            assertEquals(ErrorCode.E0732, we.getErrorCode());
            assertTrue(we.getMessage().contains("Fork [f]"));
            assertTrue(we.getMessage().contains("Join [j1]") && we.getMessage().contains("been [j2]")
                    || we.getMessage().contains("Join [j2]") && we.getMessage().contains("been [j1]"));
        }
    }

    /*
     * f->(1,2,2)
     * 1->ok->j
     * 1->fail->k
     * 2->ok->j
     * 2->fail->k
     * j->end
     */
    public void testForkJoinDuplicateTransitionsFromFork() throws WorkflowException {
        LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
                LiteWorkflowStoreService.LiteControlNodeHandler.class,
                LiteWorkflowStoreService.LiteDecisionHandler.class,
                LiteWorkflowStoreService.LiteActionHandler.class);
        LiteWorkflowApp def = new LiteWorkflowApp("name", "def",
            new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class, "f"))
        .addNode(new ForkNodeDef("f", LiteWorkflowStoreService.LiteControlNodeHandler.class,
                        Arrays.asList(new String[]{"one", "two", "two"})))
        .addNode(new ActionNodeDef("one", dummyConf, TestActionNodeHandler.class, "j", "k"))
        .addNode(new JoinNodeDef("j", LiteWorkflowStoreService.LiteControlNodeHandler.class, "end"))
        .addNode(new ActionNodeDef("two", dummyConf, TestActionNodeHandler.class, "j", "k"))
        .addNode(new KillNodeDef("k", "kill", LiteWorkflowStoreService.LiteControlNodeHandler.class))
        .addNode(new EndNodeDef("end", LiteWorkflowStoreService.LiteControlNodeHandler.class));
        try {
            invokeForkJoin(parser, def);
            fail("Expected to catch an exception but did not encounter any");
        } catch (Exception ex) {
            WorkflowException we = (WorkflowException) ex.getCause();
            assertEquals(ErrorCode.E0744, we.getErrorCode());
            assertTrue(we.getMessage().contains("fork, [f],"));
            assertTrue(we.getMessage().contains("node, [two]"));
        }
    }

    // Invoke private validateForkJoin method using Reflection API
    private void invokeForkJoin(LiteWorkflowAppParser parser, LiteWorkflowApp def) throws Exception {
        Class<? extends LiteWorkflowAppParser> c = parser.getClass();
        Class<?> d = Class.forName("org.apache.oozie.workflow.lite.LiteWorkflowAppParser$VisitStatus");
        Field f = d.getField("VISITING");
        Map traversed = new HashMap();
        traversed.put(def.getNode(StartNodeDef.START).getName(), f);
        Method validate = c.getDeclaredMethod("validate", LiteWorkflowApp.class, NodeDef.class, Map.class);
        validate.setAccessible(true);
        // invoke validate method to populate the fork and join list
        validate.invoke(parser, def, def.getNode(StartNodeDef.START), traversed);
        Method validateForkJoin = c.getDeclaredMethod("validateForkJoin", LiteWorkflowApp.class);
        validateForkJoin.setAccessible(true);
        // invoke validateForkJoin
        validateForkJoin.invoke(parser, def);
    }

    // If Xerces 2.10.0 is not explicitly listed as a dependency in the poms, then Java will revert to an older version that has
    // a race conditon in the validator.  This test is to make sure we don't accidently remove the dependency.
    public void testRaceConditionWithOldXerces() throws Exception {
        javax.xml.validation.Schema schema = Services.get().get(SchemaService.class).getSchema(SchemaService.SchemaName.WORKFLOW);
        final int numThreads = 20;
        final RCThread[] threads = new RCThread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            LiteWorkflowAppParser parser = new LiteWorkflowAppParser(schema,
                                                                     LiteWorkflowStoreService.LiteControlNodeHandler.class,
                                                                     LiteWorkflowStoreService.LiteDecisionHandler.class,
                                                                     LiteWorkflowStoreService.LiteActionHandler.class);
            threads[i] = new RCThread(parser);
        }
        for (int i = 0; i < numThreads; i++) {
            threads[i].start();
        }
        waitFor(120 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                boolean allDone = true;
                for (int i = 0; i < numThreads; i++) {
                    allDone = allDone & threads[i].done;
                }
                return allDone;
            }
        });
        boolean error = false;
        for (int i = 0; i < numThreads; i++) {
            error = error || threads[i].error;
        }
        assertFalse(error);
    }

    public class RCThread extends Thread {

        private LiteWorkflowAppParser parser;
        boolean done = false;
        boolean error = false;

        public RCThread(LiteWorkflowAppParser parser) {
            this.parser = parser;
        }

        @Override
        public void run() {
            try {
                parser.validateAndParse(IOUtils.getResourceAsReader("wf-race-condition.xml", -1), new Configuration());
            }
            catch (Exception e) {
                error = true;
                e.printStackTrace();
            }
            done = true;
        }
    }

    public void testDisableWFValidateForkJoin() throws Exception {
        LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
            LiteWorkflowStoreService.LiteControlNodeHandler.class,
            LiteWorkflowStoreService.LiteDecisionHandler.class,
            LiteWorkflowStoreService.LiteActionHandler.class);

        // oozie level default, wf level default
        try {
            parser.validateAndParse(IOUtils.getResourceAsReader("wf-invalid-fork.xml", -1), new Configuration());
        }
        catch (WorkflowException wfe) {
            assertEquals(ErrorCode.E0730, wfe.getErrorCode());
            assertEquals("E0730: Fork/Join not in pair", wfe.getMessage());
        }

        // oozie level default, wf level disabled
        Configuration conf = new Configuration();
        conf.set("oozie.wf.validate.ForkJoin", "false");
        parser.validateAndParse(IOUtils.getResourceAsReader("wf-invalid-fork.xml", -1), conf);

        // oozie level default, wf level enabled
        conf.set("oozie.wf.validate.ForkJoin", "true");
        try {
            parser.validateAndParse(IOUtils.getResourceAsReader("wf-invalid-fork.xml", -1), conf);
        }
        catch (WorkflowException wfe) {
            assertEquals(ErrorCode.E0730, wfe.getErrorCode());
            assertEquals("E0730: Fork/Join not in pair", wfe.getMessage());
        }

        // oozie level disabled, wf level default
        Services.get().destroy();
        setSystemProperty("oozie.validate.ForkJoin", "false");
        new Services().init();
        parser.validateAndParse(IOUtils.getResourceAsReader("wf-invalid-fork.xml", -1), new Configuration());

        // oozie level disabled, wf level disabled
        conf.set("oozie.wf.validate.ForkJoin", "false");
        parser.validateAndParse(IOUtils.getResourceAsReader("wf-invalid-fork.xml", -1), conf);

        // oozie level disabled, wf level enabled
        conf.set("oozie.wf.validate.ForkJoin", "true");
        parser.validateAndParse(IOUtils.getResourceAsReader("wf-invalid-fork.xml", -1), conf);

        // oozie level enabled, wf level default
        Services.get().destroy();
        setSystemProperty("oozie.validate.ForkJoin", "true");
        new Services().init();
        try {
            parser.validateAndParse(IOUtils.getResourceAsReader("wf-invalid-fork.xml", -1), new Configuration());
        }
        catch (WorkflowException wfe) {
            assertEquals(ErrorCode.E0730, wfe.getErrorCode());
            assertEquals("E0730: Fork/Join not in pair", wfe.getMessage());
        }

        // oozie level enabled, wf level disabled
        conf.set("oozie.wf.validate.ForkJoin", "false");
        parser.validateAndParse(IOUtils.getResourceAsReader("wf-invalid-fork.xml", -1), conf);

        // oozie level enabled, wf level enabled
        conf.set("oozie.wf.validate.ForkJoin", "true");
        try {
            parser.validateAndParse(IOUtils.getResourceAsReader("wf-invalid-fork.xml", -1), new Configuration());
        }
        catch (WorkflowException wfe) {
            assertEquals(ErrorCode.E0730, wfe.getErrorCode());
            assertEquals("E0730: Fork/Join not in pair", wfe.getMessage());
        }
    }

    // Test parameterization of retry-max and retry-interval
    public void testParameterizationRetry() throws Exception {
        LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
                LiteWorkflowStoreService.LiteControlNodeHandler.class,
                LiteWorkflowStoreService.LiteDecisionHandler.class, LiteWorkflowStoreService.LiteActionHandler.class);

        String wf = "<workflow-app xmlns=\"uri:oozie:workflow:0.5\" name=\"test\" > "
                + "<global> <job-tracker>localhost</job-tracker><name-node>localhost</name-node></global>"
                + "<start to=\"retry\"/><action name=\"retry\" retry-max=\"${retryMax}\" retry-interval=\"${retryInterval}\">"
                + "<java> <main-class>com.retry</main-class>" + "</java>" + "<ok to=\"end\"/>" + "<error to=\"end\"/>"
                + "</action> <end name=\"end\"/></workflow-app>";
        Configuration conf = new Configuration();
        conf.set("retryMax", "3");
        conf.set("retryInterval", "10");
        LiteWorkflowApp app = parser.validateAndParse(new StringReader(wf), conf);
        assertEquals(app.getNode("retry").getUserRetryMax(), "3");
        assertEquals(app.getNode("retry").getUserRetryInterval(), "10");
    }

}
