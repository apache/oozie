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
import java.util.concurrent.TimeoutException;



import org.apache.oozie.service.LiteWorkflowStoreService;
import org.apache.oozie.service.SchemaService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.service.SchemaService.SchemaName;
import org.apache.oozie.service.TestSchemaService;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.lite.TestLiteWorkflowLib.TestActionNodeHandler;
import org.apache.oozie.workflow.lite.TestLiteWorkflowLib.TestDecisionNodeHandler;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.ErrorCode;

public class TestLiteWorkflowAppParser extends XTestCase {
    public static String dummyConf = "<java></java>";

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        new Services().init();
    }

    @Override
    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    public void testParser() throws Exception {
        LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
                                                                 LiteWorkflowStoreService.LiteDecisionHandler.class,
                                                                 LiteWorkflowStoreService.LiteActionHandler.class);

        parser.validateAndParse(IOUtils.getResourceAsReader("wf-schema-valid.xml", -1));

        try {
            parser.validateAndParse(IOUtils.getResourceAsReader("wf-loop1-invalid.xml", -1));
            fail();
        }
        catch (WorkflowException ex) {
            assertEquals(ErrorCode.E0707, ex.getErrorCode());
        }
        catch (Exception ex) {
            fail();
        }

        try {
            parser.validateAndParse(IOUtils.getResourceAsReader("wf-unsupported-action.xml", -1));
            fail();
        }
        catch (WorkflowException ex) {
            assertEquals(ErrorCode.E0723, ex.getErrorCode());
        }
        catch (Exception ex) {
            fail();
        }

        try {
            parser.validateAndParse(IOUtils.getResourceAsReader("wf-loop2-invalid.xml", -1));
            fail();
        }
        catch (WorkflowException ex) {
            assertEquals(ErrorCode.E0706, ex.getErrorCode());
        }
        catch (Exception ex) {
            fail();
        }

        try {
            parser.validateAndParse(IOUtils.getResourceAsReader("wf-transition-invalid.xml", -1));
            fail();
        }
        catch (WorkflowException ex) {
            assertEquals(ErrorCode.E0708, ex.getErrorCode());
        }
        catch (Exception ex) {
            fail();
        }
    }

    // Test for validation of workflow definition against pattern defined in schema to complete within 3 seconds
    public void testWfValidationFailure() throws Exception {
        SchemaService wss = Services.get().get(SchemaService.class);
        final LiteWorkflowAppParser parser = new LiteWorkflowAppParser(wss.getSchema(SchemaName.WORKFLOW),
                LiteWorkflowStoreService.LiteDecisionHandler.class, LiteWorkflowStoreService.LiteActionHandler.class);

        Thread testThread = new Thread() {
            public void run() {
                try {
                    // Validate against wf def
                    parser.validateAndParse(new StringReader(TestSchemaService.APP_NEG_TEST));
                    fail("Expected to catch WorkflowException but didn't encounter any");
                } catch (WorkflowException we) {
                    assertEquals(ErrorCode.E0701, we.getErrorCode());
                    assertTrue(we.getCause().toString().contains("SAXParseException"));
                } catch (Exception e) {
                    fail("Expected to catch WorkflowException but an unexpected error happened");
                }

            }
        };
        testThread.start();
        Thread.sleep(3000);
        // Timeout if validation takes more than 3 seconds
        testThread.interrupt();

        if (testThread.isInterrupted()) {
            throw new TimeoutException("the pattern validation took too long to complete");
        }
    }

    /*
     * 1->ok->2
     * 2->ok->end
     */
   public void testWfNoForkJoin() throws WorkflowException  {
        LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
                LiteWorkflowStoreService.LiteDecisionHandler.class,
                LiteWorkflowStoreService.LiteActionHandler.class);

        LiteWorkflowApp def = new LiteWorkflowApp("name", "def", new StartNodeDef("one"))
            .addNode(new ActionNodeDef("one", dummyConf, TestActionNodeHandler.class, "two", "three"))
            .addNode(new ActionNodeDef("two", dummyConf, TestActionNodeHandler.class, "end", "end"))
            .addNode(new ActionNodeDef("three", dummyConf, TestActionNodeHandler.class, "end", "end"))
            .addNode(new EndNodeDef( "end"));

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
                LiteWorkflowStoreService.LiteDecisionHandler.class,
                LiteWorkflowStoreService.LiteActionHandler.class);

        LiteWorkflowApp def = new LiteWorkflowApp("wf", "<worklfow-app/>", new StartNodeDef("one"))
        .addNode(new ActionNodeDef("one", dummyConf, TestActionNodeHandler.class, "f", "end"))
        .addNode(new ForkNodeDef("f", Arrays.asList(new String[]{"two", "three"})))
        .addNode(new ActionNodeDef("two", dummyConf,  TestActionNodeHandler.class, "j", "end"))
        .addNode(new ActionNodeDef("three", dummyConf, TestActionNodeHandler.class, "j", "end"))
        .addNode(new JoinNodeDef("j", "four"))
        .addNode(new ActionNodeDef("four", dummyConf, TestActionNodeHandler.class, "end", "end"))
        .addNode(new EndNodeDef("end"));

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
                LiteWorkflowStoreService.LiteDecisionHandler.class,
                LiteWorkflowStoreService.LiteActionHandler.class);

        LiteWorkflowApp def = new LiteWorkflowApp("testWf", "<worklfow-app/>", new StartNodeDef("one"))
        .addNode(new ActionNodeDef("one", dummyConf, TestActionNodeHandler.class, "f","end"))
        .addNode(new ForkNodeDef("f", Arrays.asList(new String[]{"two", "three"})))
        .addNode(new ActionNodeDef("two", dummyConf, TestActionNodeHandler.class, "f2","end"))
        .addNode(new ActionNodeDef("three", dummyConf, TestActionNodeHandler.class, "j","end"))
        .addNode(new ForkNodeDef("f2", Arrays.asList(new String[]{"four", "five", "six"})))
        .addNode(new ActionNodeDef("four", dummyConf, TestActionNodeHandler.class, "j2","end"))
        .addNode(new ActionNodeDef("five", dummyConf, TestActionNodeHandler.class, "j2","end"))
        .addNode(new ActionNodeDef("six", dummyConf, TestActionNodeHandler.class,"j2", "end"))
        .addNode(new JoinNodeDef("j2", "seven"))
        .addNode(new ActionNodeDef("seven", dummyConf, TestActionNodeHandler.class, "j","end"))
        .addNode(new JoinNodeDef("j", "end"))
        .addNode(new EndNodeDef("end"));

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
                LiteWorkflowStoreService.LiteDecisionHandler.class,
                LiteWorkflowStoreService.LiteActionHandler.class);

        LiteWorkflowApp def = new LiteWorkflowApp("testWf", "<worklfow-app/>", new StartNodeDef("one"))
        .addNode(new ActionNodeDef("one", dummyConf, TestActionNodeHandler.class, "f","end"))
        .addNode(new ForkNodeDef("f", Arrays.asList(new String[]{"two", "three"})))
        .addNode(new ActionNodeDef("two", dummyConf, TestActionNodeHandler.class, "j","end"))
        .addNode(new ActionNodeDef("three", dummyConf, TestActionNodeHandler.class, "end","end"))
        .addNode(new JoinNodeDef("j", "end"))
        .addNode(new EndNodeDef("end"));

        try {
            invokeForkJoin(parser, def);
            fail("Expected to catch an exception but did not encounter any");
        } catch (Exception ex) {
            WorkflowException we = (WorkflowException) ex.getCause();
            assertEquals(ErrorCode.E0730, we.getErrorCode());
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
                LiteWorkflowStoreService.LiteDecisionHandler.class,
                LiteWorkflowStoreService.LiteActionHandler.class);

        LiteWorkflowApp def = new LiteWorkflowApp("testWf", "<worklfow-app/>", new StartNodeDef("one"))
            .addNode(new ActionNodeDef("one", dummyConf, TestActionNodeHandler.class, "f","end"))
            .addNode(new ForkNodeDef("f", Arrays.asList(new String[]{"four", "three", "two"})))
            .addNode(new ActionNodeDef("two", dummyConf, TestActionNodeHandler.class, "j","end"))
            .addNode(new ActionNodeDef("three", dummyConf, TestActionNodeHandler.class, "j","end"))
            .addNode(new ActionNodeDef("four", dummyConf, TestActionNodeHandler.class, "f2","end"))
            .addNode(new ForkNodeDef("f2", Arrays.asList(new String[]{"five", "six"})))
            .addNode(new ActionNodeDef("five", dummyConf, TestActionNodeHandler.class, "j2","end"))
            .addNode(new ActionNodeDef("six", dummyConf, TestActionNodeHandler.class, "j2","end"))
            .addNode(new JoinNodeDef("j", "j2"))
            .addNode(new JoinNodeDef("j2", "end"))
            .addNode(new EndNodeDef("end"));

        try {
            invokeForkJoin(parser, def);
            fail("Expected to catch an exception but did not encounter any");
        } catch (Exception ex) {
            WorkflowException we = (WorkflowException) ex.getCause();
            assertEquals(ErrorCode.E0730, we.getErrorCode());
        }
    }

    /*
     f->(2,3)
     2->ok->3
     2->fail->j
     3->ok->j
     3->fail->end
    */
    public void testTransitionFailure1() throws WorkflowException{
        LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
                LiteWorkflowStoreService.LiteDecisionHandler.class,
                LiteWorkflowStoreService.LiteActionHandler.class);

        LiteWorkflowApp def = new LiteWorkflowApp("name", "def", new StartNodeDef("one"))
        .addNode(new ActionNodeDef("one", dummyConf, TestActionNodeHandler.class, "f","end"))
        .addNode(new ForkNodeDef("f", Arrays.asList(new String[]{"two","three"})))
        .addNode(new ActionNodeDef("two", dummyConf, TestActionNodeHandler.class, "three", "j"))
        .addNode(new ActionNodeDef("three", dummyConf, TestActionNodeHandler.class, "j", "end"))
        .addNode(new JoinNodeDef("j", "end"))
        .addNode(new EndNodeDef("end"));

        try {
            invokeForkJoin(parser, def);
            fail("Expected to catch an exception but did not encounter any");
        } catch (Exception ex) {
            WorkflowException we = (WorkflowException) ex.getCause();
            assertEquals(ErrorCode.E0734, we.getErrorCode());
            // Make sure the message contains the nodes involved in the invalid transition
            assertTrue(we.getMessage().contains("two"));
            assertTrue(we.getMessage().contains("three"));
        }

    }

    /*
    f->(2,3)
    2->fail->3
    2->ok->j
    3->ok->j
    3->fail->end
   */
   public void testTransitionFailure2() throws WorkflowException{
       LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
               LiteWorkflowStoreService.LiteDecisionHandler.class,
               LiteWorkflowStoreService.LiteActionHandler.class);

       LiteWorkflowApp def = new LiteWorkflowApp("name", "def", new StartNodeDef("one"))
       .addNode(new ActionNodeDef("one", dummyConf, TestActionNodeHandler.class, "f","end"))
       .addNode(new ForkNodeDef("f", Arrays.asList(new String[]{"two","three"})))
       .addNode(new ActionNodeDef("two", dummyConf, TestActionNodeHandler.class, "j", "three"))
       .addNode(new ActionNodeDef("three", dummyConf, TestActionNodeHandler.class, "j", "end"))
       .addNode(new JoinNodeDef("j", "end"))
       .addNode(new EndNodeDef("end"));

       try {
           invokeForkJoin(parser, def);
           fail("Expected to catch an exception but did not encounter any");
       } catch (Exception ex) {
           WorkflowException we = (WorkflowException) ex.getCause();
           assertEquals(ErrorCode.E0734, we.getErrorCode());
           // Make sure the message contains the nodes involved in the invalid transition
           assertTrue(we.getMessage().contains("two"));
           assertTrue(we.getMessage().contains("three"));
       }

   }

   /*
   f->(2,3)
   2->ok->j
   3->ok->4
   2->fail->4
   4->ok->j
  */
   public void testTransitionFailure3() throws WorkflowException{
       LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
               LiteWorkflowStoreService.LiteDecisionHandler.class,
               LiteWorkflowStoreService.LiteActionHandler.class);

       LiteWorkflowApp def = new LiteWorkflowApp("name", "def", new StartNodeDef("one"))
       .addNode(new ActionNodeDef("one", dummyConf, TestActionNodeHandler.class, "f","end"))
       .addNode(new ForkNodeDef("f", Arrays.asList(new String[]{"two","three"})))
       .addNode(new ActionNodeDef("two", dummyConf, TestActionNodeHandler.class, "j", "four"))
       .addNode(new ActionNodeDef("three", dummyConf, TestActionNodeHandler.class, "four", "end"))
       .addNode(new ActionNodeDef("four", dummyConf, TestActionNodeHandler.class, "j", "end"))
       .addNode(new JoinNodeDef("j", "end"))
       .addNode(new EndNodeDef("end"));

       try {
           invokeForkJoin(parser, def);
           fail("Expected to catch an exception but did not encounter any");
       } catch (Exception ex) {
           WorkflowException we = (WorkflowException) ex.getCause();
           assertEquals(ErrorCode.E0735, we.getErrorCode());
           // Make sure the message contains the node involved in the invalid transition
           assertTrue(we.getMessage().contains("four"));
       }
   }

   /*
    * f->(2,3)
    * 2->ok->j
    * 3->ok->j
    * j->end
    * 2->error->f1
    * 3->error->f1
    * f1->(4,5)
    * (4,5)->j1
    * j1->end
    */
   public void testErrorTransitionForkJoin() throws WorkflowException {
       LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
               LiteWorkflowStoreService.LiteDecisionHandler.class,
               LiteWorkflowStoreService.LiteActionHandler.class);

       LiteWorkflowApp def = new LiteWorkflowApp("wf", "<worklfow-app/>", new StartNodeDef("one"))
       .addNode(new ActionNodeDef("one", dummyConf, TestActionNodeHandler.class, "f", "end"))
       .addNode(new ForkNodeDef("f", Arrays.asList(new String[]{"two", "three"})))
       .addNode(new ActionNodeDef("two", dummyConf,  TestActionNodeHandler.class, "j", "f1"))
       .addNode(new ActionNodeDef("three", dummyConf, TestActionNodeHandler.class, "j", "f1"))
       .addNode(new ForkNodeDef("f1", Arrays.asList(new String[]{"four", "five"})))
       .addNode(new ActionNodeDef("four", dummyConf,  TestActionNodeHandler.class, "j1", "end"))
       .addNode(new ActionNodeDef("five", dummyConf, TestActionNodeHandler.class, "j1", "end"))
       .addNode(new JoinNodeDef("j", "six"))
       .addNode(new JoinNodeDef("j1", "six"))
       .addNode(new ActionNodeDef("six", dummyConf, TestActionNodeHandler.class, "end", "end"))
       .addNode(new EndNodeDef("end"));

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
                LiteWorkflowStoreService.LiteDecisionHandler.class,
                LiteWorkflowStoreService.LiteActionHandler.class);
        LiteWorkflowApp def = new LiteWorkflowApp("name", "def", new StartNodeDef("one"))
        .addNode(new ActionNodeDef("one", dummyConf, TestActionNodeHandler.class, "f","end"))
        .addNode(new ForkNodeDef("f", Arrays.asList(new String[]{"two","three"})))
        .addNode(new DecisionNodeDef("two", dummyConf, TestDecisionNodeHandler.class, Arrays.asList(new String[]{"four","five","four"})))
        .addNode(new ActionNodeDef("four", dummyConf, TestActionNodeHandler.class, "j", "end"))
        .addNode(new ActionNodeDef("five", dummyConf, TestActionNodeHandler.class, "j", "end"))
        .addNode(new ActionNodeDef("three", dummyConf, TestActionNodeHandler.class, "j", "end"))
        .addNode(new JoinNodeDef("j", "end"))
        .addNode(new EndNodeDef("end"));

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
     *3->end
     *4->end
     */
    public void testDecisionForkJoinFailure() throws WorkflowException{
        LiteWorkflowAppParser parser = new LiteWorkflowAppParser(null,
                LiteWorkflowStoreService.LiteDecisionHandler.class,
                LiteWorkflowStoreService.LiteActionHandler.class);
        LiteWorkflowApp def = new LiteWorkflowApp("name", "def", new StartNodeDef("one"))
        .addNode(new ActionNodeDef("one", dummyConf, TestActionNodeHandler.class, "f","end"))
        .addNode(new ForkNodeDef("f", Arrays.asList(new String[]{"two","three"})))
        .addNode(new DecisionNodeDef("two", dummyConf, TestDecisionNodeHandler.class, Arrays.asList(new String[]{"four","three"})))
        .addNode(new ActionNodeDef("three", dummyConf, TestActionNodeHandler.class, "j", "end"))
        .addNode(new ActionNodeDef("four", dummyConf, TestActionNodeHandler.class, "j", "end"))
        .addNode(new JoinNodeDef("j", "end"))
        .addNode(new EndNodeDef("end"));

        try {
            invokeForkJoin(parser, def);
            fail("Expected to catch an exception but did not encounter any");
        } catch (Exception ex) {
            WorkflowException we = (WorkflowException) ex.getCause();
            assertEquals(ErrorCode.E0734, we.getErrorCode());
            // Make sure the message contains the nodes involved in the invalid transition
            assertTrue(we.getMessage().contains("two"));
            assertTrue(we.getMessage().contains("three"));
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
                LiteWorkflowStoreService.LiteDecisionHandler.class,
                LiteWorkflowStoreService.LiteActionHandler.class);
        LiteWorkflowApp def = new LiteWorkflowApp("name", "def", new StartNodeDef("one"))
        .addNode(new DecisionNodeDef("one", dummyConf, TestDecisionNodeHandler.class, Arrays.asList(new String[]{"f1","f2"})))
        .addNode(new ForkNodeDef("f1", Arrays.asList(new String[]{"two","three"})))
        .addNode(new ForkNodeDef("f2", Arrays.asList(new String[]{"four","five"})))
        .addNode(new ActionNodeDef("two", dummyConf, TestActionNodeHandler.class, "j1", "end"))
        .addNode(new ActionNodeDef("three", dummyConf, TestActionNodeHandler.class, "j1", "end"))
        .addNode(new ActionNodeDef("four", dummyConf, TestActionNodeHandler.class, "j2", "end"))
        .addNode(new ActionNodeDef("five", dummyConf, TestActionNodeHandler.class, "j2", "end"))
        .addNode(new JoinNodeDef("j1", "end"))
        .addNode(new JoinNodeDef("j2", "end"))
        .addNode(new EndNodeDef("end"));

        try {
            invokeForkJoin(parser, def);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Unexpected Exception");
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

}
