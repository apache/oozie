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


import org.apache.oozie.service.Services;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.WritableUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.ErrorCode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestLiteWorkflowLib extends XTestCase {

    static Map<String, Integer> enters = new HashMap<String, Integer>();
    static Map<String, Integer> exits = new HashMap<String, Integer>();
    static Map<String, Integer> kills = new HashMap<String, Integer>();
    static Map<String, Integer> fails = new HashMap<String, Integer>();

    static int enterCounter = 0;
    static int exitCounter = 0;
    static int killCounter = 0;
    static int failCounter = 0;

    private Services services;

    public static abstract class BaseNodeHandler extends NodeHandler {
        private boolean synch;

        protected BaseNodeHandler(Boolean synch) {
            this.synch = synch;
        }

        @Override
        public boolean enter(Context context) {
            enters.put(context.getNodeDef().getName(), enterCounter++);
            return synch;
        }

        @Override
        public String exit(Context context) {
            exits.put(context.getNodeDef().getName(), exitCounter++);
            return context.getNodeDef().getTransitions().get(0);
        }

        @Override
        public void kill(Context context) {
            kills.put(context.getNodeDef().getName(), killCounter++);
        }

        @Override
        public void fail(Context context) {
            fails.put(context.getNodeDef().getName(), failCounter++);
        }
    }

    public static class AsynchNodeHandler extends BaseNodeHandler {

        public AsynchNodeHandler() {
            super(false);
        }
    }

    public static class SynchNodeHandler extends BaseNodeHandler {

        public SynchNodeHandler() {
            super(true);
        }
    }

    public static class TestActionNodeHandler extends ActionNodeHandler {

        @Override
        public void start(Context context) {
        }

        @Override
        public void end(Context context) {
        }
    }

    public static class TestDecisionNodeHandler extends DecisionNodeHandler {
        @Override
        public void start(Context context) {
            enters.put(context.getNodeDef().getName(), enterCounter++);
        }

        @Override
        public void end(Context context) {
            exits.put(context.getNodeDef().getName(), exitCounter++);
        }
    }

    public static class TestControlNodeHandler extends ControlNodeHandler {

        @Override
        public boolean enter(Context context) throws WorkflowException {
            boolean done = true;
            super.enter(context);
            Class<? extends NodeDef> nodeClass = context.getNodeDef().getClass();
            if (nodeClass.equals(JoinNodeDef.class)) {
                String parentExecutionPath = context.getExecutionPath();
                String forkCount = context.getVar(FORK_COUNT_PREFIX + parentExecutionPath);
                done = forkCount == null;
            }
            return done;
        }

        @Override
        public void touch(Context context) throws WorkflowException {
        }
    }

    public static class TestRootContextHandler extends SynchNodeHandler {

        @Override
        public boolean enter(Context context) {
            assertNotNull(context.getNodeDef());
            assertNotNull(context.getSignalValue());
            assertNotNull(context.getProcessInstance());
            assertEquals("/", context.getExecutionPath());
            assertEquals(null, context.getParentExecutionPath("/"));
            assertEquals("A", context.getVar("a"));
            assertEquals("AA", context.getTransientVar("ta"));
            context.setVar("b", "B");
            context.setTransientVar("tb", "BB");
            return super.enter(context);
        }

        @Override
        public String exit(Context context) {
            assertEquals("A", context.getVar("a"));
            assertEquals("AA", context.getTransientVar("ta"));
            context.setVar("b", "B");
            context.setTransientVar("tb", "BB");
            return super.exit(context);
        }
    }

    public static class TestForkedContextHandler extends SynchNodeHandler {

        @Override
        public boolean enter(Context context) {
            assertNotNull(context.getNodeDef());
            assertNotNull(context.getSignalValue());
            assertNotNull(context.getProcessInstance());
            assertEquals("/a/", context.getExecutionPath());
            assertEquals("/", context.getParentExecutionPath("/a/"));
            return super.enter(context);
        }

    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        enters.clear();
        exits.clear();
        kills.clear();
        fails.clear();
        enterCounter = 0;
        exitCounter = 0;
        killCounter = 0;
        failCounter = 0;
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testEmptyWorkflow() throws WorkflowException {
        LiteWorkflowApp def = new LiteWorkflowApp("wf", "<worklfow-app/>",
                                                  new StartNodeDef(TestControlNodeHandler.class, "end"))
                .addNode(new EndNodeDef("end", TestControlNodeHandler.class));

        final LiteWorkflowInstance job = new LiteWorkflowInstance(def, new XConfiguration(), "1");
        assertEquals(WorkflowInstance.Status.PREP, job.getStatus());
        job.start();
        waitFor(5 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return job.getStatus() == WorkflowInstance.Status.SUCCEEDED;
            }
        });
        assertEquals(WorkflowInstance.Status.SUCCEEDED, job.getStatus());
    }

    public void testKillWorkflow() throws WorkflowException {
        LiteWorkflowApp def = new LiteWorkflowApp("wf", "<worklfow-app/>",
            new StartNodeDef(TestControlNodeHandler.class, "kill"))
                .addNode(new KillNodeDef("kill", "killed", TestControlNodeHandler.class))
                .addNode(new EndNodeDef("end", TestControlNodeHandler.class));

        LiteWorkflowInstance job = new LiteWorkflowInstance(def, new XConfiguration(), "1");
        assertEquals(WorkflowInstance.Status.PREP, job.getStatus());
        job.start();
        assertEquals(WorkflowInstance.Status.KILLED, job.getStatus());
    }

    public void testWorkflowStates() throws WorkflowException {
        LiteWorkflowApp def = new LiteWorkflowApp("wf", "<worklfow-app/>",
                                                  new StartNodeDef(TestControlNodeHandler.class, "one"))
                .addNode(new NodeDef("one", null, AsynchNodeHandler.class, Arrays.asList(new String[]{"end"})))
                .addNode(new EndNodeDef("end", TestControlNodeHandler.class));

        LiteWorkflowInstance job = new LiteWorkflowInstance(def, new XConfiguration(), "1");
        assertEquals(WorkflowInstance.Status.PREP, job.getStatus());

        job.kill();
        assertEquals(WorkflowInstance.Status.KILLED, job.getStatus());

        job = new LiteWorkflowInstance(def, new XConfiguration(), "1");
        job.fail("one");
        assertEquals(WorkflowInstance.Status.FAILED, job.getStatus());

        job = new LiteWorkflowInstance(def, new XConfiguration(), "1");

        try {
            job.suspend();
            fail();
        }
        catch (WorkflowException ex) {
            //nop
        }

        try {
            job.resume();
            fail();
        }
        catch (WorkflowException ex) {
            //nop
        }

        job.start();
        assertEquals(WorkflowInstance.Status.RUNNING, job.getStatus());

        try {
            job.resume();
            fail();
        }
        catch (WorkflowException ex) {
            //nop
        }

        try {
            job.start();
            fail();
        }
        catch (WorkflowException ex) {
            //nop
        }

        job.suspend();
        assertEquals(WorkflowInstance.Status.SUSPENDED, job.getStatus());

        try {
            job.suspend();
            fail();
        }
        catch (WorkflowException ex) {
            //nop
        }

        try {
            job.start();
            fail();
        }
        catch (WorkflowException ex) {
            //nop
        }

        job.resume();
        assertEquals(WorkflowInstance.Status.RUNNING, job.getStatus());

        try {
            job.resume();
            fail();
        }
        catch (WorkflowException ex) {
            //nop
        }

        try {
            job.start();
            fail();
        }
        catch (WorkflowException ex) {
            //nop
        }

        job.kill();
        assertEquals(WorkflowInstance.Status.KILLED, job.getStatus());

        try {
            job.kill();
            fail();
        }
        catch (WorkflowException ex) {
            //nop
        }

        try {
            job.suspend();
            fail();
        }
        catch (WorkflowException ex) {
            //nop
        }

        try {
            job.resume();
            fail();
        }
        catch (WorkflowException ex) {
            //nop
        }

        try {
            job.start();
            fail();
        }
        catch (WorkflowException ex) {
            //nop
        }
    }

    public void testSynchSimple() throws WorkflowException {
        LiteWorkflowApp def = new LiteWorkflowApp("wf", "<worklfow-app/>",
                                                  new StartNodeDef(TestControlNodeHandler.class, "one"))
                .addNode(new NodeDef("one", null, SynchNodeHandler.class, Arrays.asList(new String[]{"end"})))
                .addNode(new EndNodeDef("end", TestControlNodeHandler.class));

        LiteWorkflowInstance job = new LiteWorkflowInstance(def, new XConfiguration(), "1");
        job.start();

        assertEquals(WorkflowInstance.Status.SUCCEEDED, job.getStatus());
        assertEquals(1, enters.size());
        assertEquals(1, exits.size());
        assertEquals(0, kills.size());
        assertEquals(0, fails.size());
    }

    public void testNodeContext() throws WorkflowException {
        LiteWorkflowApp def = new LiteWorkflowApp("wf", "<worklfow-app/>",
                                                  new StartNodeDef(TestControlNodeHandler.class, "one"))
                .addNode(new NodeDef("one", null, TestRootContextHandler.class, Arrays.asList(new String[]{"end"})))
                .addNode(new EndNodeDef("end", TestControlNodeHandler.class));

        LiteWorkflowInstance job = new LiteWorkflowInstance(def, new XConfiguration(), "1");
        job.setVar("a", "A");
        job.setTransientVar("ta", "AA");
        job.start();

        assertEquals(WorkflowInstance.Status.SUCCEEDED, job.getStatus());
        assertEquals("B", job.getVar("b"));
        assertEquals("BB", job.getTransientVar("tb"));
        assertEquals(1, enters.size());
        assertEquals(1, exits.size());
        assertEquals(0, kills.size());
        assertEquals(0, fails.size());
    }

    public void testSynchDouble() throws WorkflowException {
        LiteWorkflowApp def = new LiteWorkflowApp("wf", "<worklfow-app/>",
                                                  new StartNodeDef(TestControlNodeHandler.class, "one"))
                .addNode(new NodeDef("one", null, SynchNodeHandler.class, Arrays.asList(new String[]{"two"})))
                .addNode(new NodeDef("two", null, SynchNodeHandler.class, Arrays.asList(new String[]{"end"})))
                .addNode(new EndNodeDef("end", TestControlNodeHandler.class));

        LiteWorkflowInstance job = new LiteWorkflowInstance(def, new XConfiguration(), "1");
        job.start();

        assertEquals(WorkflowInstance.Status.SUCCEEDED, job.getStatus());
        assertEquals(2, enters.size());
        assertEquals(2, exits.size());
        assertEquals(0, kills.size());
        assertEquals(0, fails.size());
    }

    public void testAsynchSimple() throws WorkflowException {
        LiteWorkflowApp def = new LiteWorkflowApp("wf", "<worklfow-app/>",
                                                  new StartNodeDef(TestControlNodeHandler.class, "one"))
                .addNode(new NodeDef("one", null, AsynchNodeHandler.class, Arrays.asList(new String[]{"end"})))
                .addNode(new EndNodeDef("end", TestControlNodeHandler.class));

        LiteWorkflowInstance job = new LiteWorkflowInstance(def, new XConfiguration(), "1");
        job.start();

        assertEquals(WorkflowInstance.Status.RUNNING, job.getStatus());

        job.signal("/", "");

        assertEquals(WorkflowInstance.Status.SUCCEEDED, job.getStatus());
        assertEquals(1, enters.size());
        assertEquals(1, exits.size());
        assertEquals(0, kills.size());
        assertEquals(0, fails.size());
    }

    public void testInvalidExecutionPath() throws WorkflowException {
        LiteWorkflowApp def = new LiteWorkflowApp("wf", "<worklfow-app/>",
                                                  new StartNodeDef(TestControlNodeHandler.class, "one"))
                .addNode(new NodeDef("one", null, AsynchNodeHandler.class, Arrays.asList(new String[]{"end"})))
                .addNode(new EndNodeDef("end", TestControlNodeHandler.class));

        LiteWorkflowInstance job = new LiteWorkflowInstance(def, new XConfiguration(), "1");
        job.start();

        assertEquals(WorkflowInstance.Status.RUNNING, job.getStatus());

        job.signal("/a/", "");
        assertEquals(WorkflowInstance.Status.FAILED, job.getStatus());
    }

    public void testSimpleFork() throws WorkflowException {

        LiteWorkflowApp def = new LiteWorkflowApp("wf", "<worklfow-app/>",
                                                  new StartNodeDef(TestControlNodeHandler.class, "one"))
                .addNode(new NodeDef("one", null, SynchNodeHandler.class, Arrays.asList(new String[]{"f"})))
                .addNode(new ForkNodeDef("f", TestControlNodeHandler.class,
                                         Arrays.asList(new String[]{"two", "three"})))
                .addNode(new NodeDef("two", null, SynchNodeHandler.class, Arrays.asList(new String[]{"j"})))
                .addNode(new NodeDef("three", null, SynchNodeHandler.class, Arrays.asList(new String[]{"j"})))
                .addNode(new JoinNodeDef("j", TestControlNodeHandler.class, "four"))
                .addNode(new NodeDef("four", null, SynchNodeHandler.class, Arrays.asList(new String[]{"end"})))
                .addNode(new EndNodeDef("end", TestControlNodeHandler.class));

        LiteWorkflowInstance job = new LiteWorkflowInstance(def, new XConfiguration(), "1");
        job.start();

        assertEquals(WorkflowInstance.Status.SUCCEEDED, job.getStatus());
        assertEquals(4, enters.size());
        assertEquals(4, exits.size());
        assertEquals(0, kills.size());
        assertEquals(0, fails.size());

        assertTrue(enters.get("one") < enters.get("two"));
        assertTrue(enters.get("one") < enters.get("three"));
        assertTrue(enters.get("three") < enters.get("four"));
        assertTrue(enters.get("two") < enters.get("four"));
    }

    public void testForkedContext() throws WorkflowException {

        LiteWorkflowApp def = new LiteWorkflowApp("wf", "<worklfow-app/>",
            new StartNodeDef(TestControlNodeHandler.class, "f"))
                .addNode(new ForkNodeDef("f", TestControlNodeHandler.class,  Arrays.asList(new String[]{"a", "b"})))
                .addNode(new NodeDef("a", null, TestForkedContextHandler.class, Arrays.asList(new String[]{"j"})))
                .addNode(new NodeDef("b", null, SynchNodeHandler.class, Arrays.asList(new String[]{"j"})))
                .addNode(new JoinNodeDef("j", TestControlNodeHandler.class, "end"))
                .addNode(new EndNodeDef("end", TestControlNodeHandler.class));

        LiteWorkflowInstance job = new LiteWorkflowInstance(def, new XConfiguration(), "1");
        job.start();

        assertEquals(WorkflowInstance.Status.SUCCEEDED, job.getStatus());
    }


    public void testNestedFork() throws WorkflowException {

        LiteWorkflowApp def = new LiteWorkflowApp("testWf", "<worklfow-app/>",
            new StartNodeDef(TestControlNodeHandler.class, "one"))
                .addNode(new NodeDef("one", null, SynchNodeHandler.class, Arrays.asList(new String[]{"f"})))
                .addNode(new ForkNodeDef("f", TestControlNodeHandler.class,
                                         Arrays.asList(new String[]{"two", "three"})))
                .addNode(new NodeDef("two", null, SynchNodeHandler.class, Arrays.asList(new String[]{"f2"})))
                .addNode(new NodeDef("three", null, SynchNodeHandler.class, Arrays.asList(new String[]{"j"})))
                .addNode(new ForkNodeDef("f2", TestControlNodeHandler.class,
                                         Arrays.asList(new String[]{"four", "five", "six"})))
                .addNode(new NodeDef("four", null, SynchNodeHandler.class, Arrays.asList(new String[]{"j2"})))
                .addNode(new NodeDef("five", null, SynchNodeHandler.class, Arrays.asList(new String[]{"j2"})))
                .addNode(new NodeDef("six", null, SynchNodeHandler.class, Arrays.asList(new String[]{"j2"})))
                .addNode(new JoinNodeDef("j2", TestControlNodeHandler.class, "seven"))
                .addNode(new NodeDef("seven", null, SynchNodeHandler.class, Arrays.asList(new String[]{"j"})))
                .addNode(new JoinNodeDef("j", TestControlNodeHandler.class, "end"))
                .addNode(new EndNodeDef("end", TestControlNodeHandler.class));

        LiteWorkflowInstance job = new LiteWorkflowInstance(def, new XConfiguration(), "abcde");
        job.start();

        assertEquals(WorkflowInstance.Status.SUCCEEDED, job.getStatus());
        assertEquals(7, enters.size());
        assertEquals(7, exits.size());
        assertEquals(0, kills.size());
        assertEquals(0, fails.size());

        assertTrue(enters.get("one") < enters.get("two"));
        assertTrue(enters.get("one") < enters.get("three"));
        assertTrue(enters.get("two") < enters.get("four"));

        assertTrue(enters.get("four") < enters.get("seven"));
        assertTrue(enters.get("five") < enters.get("seven"));
        assertTrue(enters.get("six") < enters.get("seven"));
    }


    public void testKillWithRunningNodes() throws WorkflowException {

        LiteWorkflowApp def = new LiteWorkflowApp("wf", "<worklfow-app/>",
                                                  new StartNodeDef(TestControlNodeHandler.class, "f"))
                .addNode(new ForkNodeDef("f", TestControlNodeHandler.class, Arrays.asList(new String[]{"a", "b"})))
                .addNode(new NodeDef("a", null, SynchNodeHandler.class, Arrays.asList(new String[]{"j"})))
                .addNode(new NodeDef("b", null, AsynchNodeHandler.class, Arrays.asList(new String[]{"j"})))
                .addNode(new JoinNodeDef("j", TestControlNodeHandler.class, "end"))
                .addNode(new EndNodeDef("end", TestControlNodeHandler.class));

        LiteWorkflowInstance job = new LiteWorkflowInstance(def, new XConfiguration(), "1");
        job.start();
        assertEquals(WorkflowInstance.Status.RUNNING, job.getStatus());
        job.kill();
        assertEquals(2, enters.size());
        assertEquals(1, kills.size());
        assertEquals(1, exits.size());
        assertEquals(0, fails.size());
    }

    public void testFailWithRunningNodes() throws WorkflowException {

        LiteWorkflowApp def = new LiteWorkflowApp("wf", "<worklfow-app/>",
            new StartNodeDef(TestControlNodeHandler.class, "f"))
                .addNode(new ForkNodeDef("f", TestControlNodeHandler.class, Arrays.asList(new String[]{"a", "b"})))
                .addNode(new NodeDef("a", null, SynchNodeHandler.class, Arrays.asList(new String[]{"j"})))
                .addNode(new NodeDef("b", null, AsynchNodeHandler.class, Arrays.asList(new String[]{"j"})))
                .addNode(new JoinNodeDef("j", TestControlNodeHandler.class, "end"))
            .addNode(new EndNodeDef("end", TestControlNodeHandler.class));


        LiteWorkflowInstance job = new LiteWorkflowInstance(def, new XConfiguration(), "1");
        job.start();
        assertEquals(WorkflowInstance.Status.RUNNING, job.getStatus());
        job.fail("b");
        assertEquals(2, enters.size());
        assertEquals(0, kills.size());
        assertEquals(1, exits.size());
        assertEquals(1, fails.size());
    }


    public void testDoneWithRunningNodes() throws WorkflowException {

        LiteWorkflowApp def = new LiteWorkflowApp("wf", "<worklfow-app/>",
            new StartNodeDef(TestControlNodeHandler.class, "f"))
                .addNode(new ForkNodeDef("f", TestControlNodeHandler.class, Arrays.asList(new String[]{"a", "b"})))
                .addNode(new NodeDef("a", null, AsynchNodeHandler.class, Arrays.asList(new String[]{"j"})))
                .addNode(new NodeDef("b", null, AsynchNodeHandler.class, Arrays.asList(new String[]{"end"})))
                .addNode(new JoinNodeDef("j", TestControlNodeHandler.class, "end"))
                .addNode(new EndNodeDef("end", TestControlNodeHandler.class));

        LiteWorkflowInstance job = new LiteWorkflowInstance(def, new XConfiguration(), "1");
        job.start();
        assertEquals(WorkflowInstance.Status.RUNNING, job.getStatus());
        job.signal("/b/", "");
        assertEquals(2, enters.size());
        assertEquals(1, kills.size());
        assertEquals(1, exits.size());
        assertEquals(0, fails.size());
    }

    public void testWFKillWithRunningNodes() throws WorkflowException {

        LiteWorkflowApp def = new LiteWorkflowApp("wf", "<worklfow-app/>",
            new StartNodeDef(TestControlNodeHandler.class, "f"))
                .addNode(new ForkNodeDef("f", TestControlNodeHandler.class, Arrays.asList(new String[]{"a", "b"})))
                .addNode(new NodeDef("a", null, AsynchNodeHandler.class, Arrays.asList(new String[]{"j"})))
                .addNode(new NodeDef("b", null, AsynchNodeHandler.class, Arrays.asList(new String[]{"kill"})))
                .addNode(new JoinNodeDef("j", TestControlNodeHandler.class, "end"))
                .addNode(new KillNodeDef("kill", "killed", TestControlNodeHandler.class))
                .addNode(new EndNodeDef("end", TestControlNodeHandler.class));

        LiteWorkflowInstance job = new LiteWorkflowInstance(def, new XConfiguration(), "1");
        job.start();
        assertEquals(WorkflowInstance.Status.RUNNING, job.getStatus());
        job.signal("/b/", "");
        assertEquals(2, enters.size());
        assertEquals(1, kills.size());
        assertEquals(1, exits.size());
        assertEquals(0, fails.size());
    }

    public void testWfFailWithRunningNodes() throws WorkflowException {

        LiteWorkflowApp def = new LiteWorkflowApp("wf", "<worklfow-app/>",
            new StartNodeDef(TestControlNodeHandler.class, "f"))
                .addNode(new ForkNodeDef("f", TestControlNodeHandler.class, Arrays.asList(new String[]{"a", "b"})))
                .addNode(new NodeDef("a", null, AsynchNodeHandler.class, Arrays.asList(new String[]{"j"})))
                .addNode(new NodeDef("b", null, AsynchNodeHandler.class, Arrays.asList(new String[]{"x"})))
                .addNode(new JoinNodeDef("j", TestControlNodeHandler.class, "end"))
                .addNode(new EndNodeDef("end", TestControlNodeHandler.class));

        LiteWorkflowInstance job = new LiteWorkflowInstance(def, new XConfiguration(), "1");
        try {
            job.start();
            job.signal("/b/", "");
        }
        catch (WorkflowException ex) {
        }
        assertEquals(WorkflowInstance.Status.FAILED, job.getStatus());
        assertEquals(2, enters.size());
        assertEquals(1, fails.size());
        //assertEquals(1, kills.size());
        assertEquals(1, exits.size());
        assertEquals(1, fails.size());
    }

    public void testDecision() throws WorkflowException {
        List<String> decTrans = new ArrayList<String>();
        decTrans.add("one");
        decTrans.add("two");
        decTrans.add("three");
        LiteWorkflowApp def = new LiteWorkflowApp("testWf", "<worklfow-app/>",
            new StartNodeDef(TestControlNodeHandler.class, "d"))
                .addNode(new DecisionNodeDef("d", "", TestDecisionNodeHandler.class, decTrans))
                .addNode(new NodeDef("one", null, SynchNodeHandler.class, Arrays.asList(new String[]{"end"})))
                .addNode(new NodeDef("two", null, SynchNodeHandler.class, Arrays.asList(new String[]{"end"})))
                .addNode(new NodeDef("three", null, SynchNodeHandler.class, Arrays.asList(new String[]{"end"})))
                .addNode(new EndNodeDef("end", TestControlNodeHandler.class));

        LiteWorkflowInstance job = new LiteWorkflowInstance(def, new XConfiguration(), "abcde");
        job.start();

        assertEquals(WorkflowInstance.Status.RUNNING, job.getStatus());
        job.signal("/", "one");

        assertEquals(WorkflowInstance.Status.SUCCEEDED, job.getStatus());
        assertEquals(2, enters.size());
        assertEquals(2, exits.size());
        assertTrue(enters.containsKey("one"));
        assertTrue(!enters.containsKey("two"));
        assertTrue(!enters.containsKey("three"));

        enters.clear();
        job = new LiteWorkflowInstance(def, new XConfiguration(), "abcde");
        job.start();

        assertEquals(WorkflowInstance.Status.RUNNING, job.getStatus());
        job.signal("/", "two");

        assertEquals(WorkflowInstance.Status.SUCCEEDED, job.getStatus());
        assertTrue(!enters.containsKey("one"));
        assertTrue(enters.containsKey("two"));
        assertTrue(!enters.containsKey("three"));

        enters.clear();
        job = new LiteWorkflowInstance(def, new XConfiguration(), "abcde");
        job.start();

        assertEquals(WorkflowInstance.Status.RUNNING, job.getStatus());
        job.signal("/", "three");

        assertEquals(WorkflowInstance.Status.SUCCEEDED, job.getStatus());
        assertTrue(!enters.containsKey("one"));
        assertTrue(!enters.containsKey("two"));
        assertTrue(enters.containsKey("three"));

        enters.clear();
        job = new LiteWorkflowInstance(def, new XConfiguration(), "abcde");
        job.start();

        assertEquals(WorkflowInstance.Status.RUNNING, job.getStatus());
        try {
            job.signal("/", "bla");
            fail();
        }
        catch (Exception e) {
        }
        assertEquals(WorkflowInstance.Status.FAILED, job.getStatus());

    }


    public void testActionOKError() throws WorkflowException {
        LiteWorkflowApp def = new LiteWorkflowApp("wf", "<worklfow-app/>",
            new StartNodeDef(TestControlNodeHandler.class, "a"))
                .addNode(new ActionNodeDef("a", "", TestActionNodeHandler.class, "b", "c"))
                .addNode(new NodeDef("b", null, SynchNodeHandler.class, Arrays.asList(new String[]{"end"})))
                .addNode(new NodeDef("c", null, SynchNodeHandler.class, Arrays.asList(new String[]{"end"})))
                .addNode(new EndNodeDef("end", TestControlNodeHandler.class));

        LiteWorkflowInstance job = new LiteWorkflowInstance(def, new XConfiguration(), "1");

        job.start();
        assertEquals(WorkflowInstance.Status.RUNNING, job.getStatus());

        job.signal("/", "OK");
        assertEquals(WorkflowInstance.Status.SUCCEEDED, job.getStatus());
        assertTrue(enters.containsKey("b"));
        assertTrue(!enters.containsKey("c"));

        enters.clear();
        job = new LiteWorkflowInstance(def, new XConfiguration(), "1");

        job.start();
        assertEquals(WorkflowInstance.Status.RUNNING, job.getStatus());

        job.signal("/", "ERROR");
        assertEquals(WorkflowInstance.Status.SUCCEEDED, job.getStatus());
        assertTrue(!enters.containsKey("b"));
        assertTrue(enters.containsKey("c"));

    }

    public void testJobPersistance() throws WorkflowException {
        LiteWorkflowApp def = new LiteWorkflowApp("wf", "<worklfow-app/>",
            new StartNodeDef(TestControlNodeHandler.class, "one"))
                .addNode(new NodeDef("one", null, AsynchNodeHandler.class, Arrays.asList(new String[]{"end"})))
                .addNode(new EndNodeDef("end", TestControlNodeHandler.class));

        LiteWorkflowInstance job = new LiteWorkflowInstance(def, new XConfiguration(), "1");
        job.setVar("a", "A");
        job.setTransientVar("b", "B");
        assertEquals(WorkflowInstance.Status.PREP, job.getStatus());
        assertEquals("A", job.getVar("a"));
        assertEquals("B", job.getTransientVar("b"));
        assertEquals("1", job.getId());

        byte[] array = WritableUtils.toByteArray(job);
        job = WritableUtils.fromByteArray(array, LiteWorkflowInstance.class);
        assertEquals(WorkflowInstance.Status.PREP, job.getStatus());
        assertEquals("A", job.getVar("a"));
        assertEquals(null, job.getTransientVar("b"));
        assertEquals("1", job.getId());

        job.start();
        assertEquals(WorkflowInstance.Status.RUNNING, job.getStatus());
        array = WritableUtils.toByteArray(job);
        job = WritableUtils.fromByteArray(array, LiteWorkflowInstance.class);
        assertEquals(WorkflowInstance.Status.RUNNING, job.getStatus());
        job.signal("/", "");
        assertEquals(WorkflowInstance.Status.SUCCEEDED, job.getStatus());
    }


    public void testImmediateError() throws WorkflowException {
        LiteWorkflowApp workflowDef = new LiteWorkflowApp("testWf", "<worklfow-app/>",
            new StartNodeDef(TestControlNodeHandler.class, "one"))
                .addNode(new NodeDef("one", null, SynchNodeHandler.class, Arrays.asList(new String[]{"two"})))
                .addNode(new NodeDef("two", null, SynchNodeHandler.class, Arrays.asList(new String[]{"four"})))
                .addNode(new NodeDef("three", null, SynchNodeHandler.class, Arrays.asList(new String[]{"end"})))
                .addNode(new EndNodeDef("end", TestControlNodeHandler.class));

        LiteWorkflowInstance workflowJob = new LiteWorkflowInstance(workflowDef, new XConfiguration(), "abcde");
        try {
            workflowJob.start();
        }
        catch (WorkflowException e) {
        }

        assertEquals(WorkflowInstance.Status.FAILED, workflowJob.getStatus());
        assertEquals(2, enters.size());
        assertEquals(2, exits.size());
        assertEquals(0, kills.size());
        assertEquals(0, fails.size());

    }

    public void testSelfTransition() throws WorkflowException {
        try {
            new LiteWorkflowApp("wf", "<worklfow-app/>", new StartNodeDef(TestControlNodeHandler.class, "one"))
                    .addNode(new NodeDef("one", null, SynchNodeHandler.class, Arrays.asList(new String[]{"one"})))
                    .addNode(new EndNodeDef("end", TestControlNodeHandler.class));
            fail();
        }
        catch (WorkflowException ex) {
            assertEquals(ErrorCode.E0706, ex.getErrorCode());
        }
    }

    public void testLoopSimple() throws WorkflowException {
        LiteWorkflowApp def = new LiteWorkflowApp("wf", "<worklfow-app/>",
            new StartNodeDef(TestControlNodeHandler.class, "one"))
                .addNode(new NodeDef("one", null, SynchNodeHandler.class, Arrays.asList(new String[]{"two"})))
                .addNode(new NodeDef("two", null, SynchNodeHandler.class, Arrays.asList(new String[]{"one"})))
                .addNode(new EndNodeDef("end", TestControlNodeHandler.class));

        LiteWorkflowInstance job = new LiteWorkflowInstance(def, new XConfiguration(), "1");
        try {
            job.start();
            fail();
        }
        catch (WorkflowException ex) {
            assertEquals(ErrorCode.E0709, ex.getErrorCode());
        }
        assertEquals(WorkflowInstance.Status.FAILED, job.getStatus());
    }

    public void testLoopFork() throws WorkflowException {

        LiteWorkflowApp def = new LiteWorkflowApp("wf", "<worklfow-app/>",
            new StartNodeDef(TestControlNodeHandler.class, "one"))
                .addNode(new NodeDef("one", null, SynchNodeHandler.class, Arrays.asList(new String[]{"f"})))
                .addNode(new ForkNodeDef("f", TestControlNodeHandler.class,
                                         Arrays.asList(new String[]{"two", "three"})))
                .addNode(new NodeDef("two", null, SynchNodeHandler.class, Arrays.asList(new String[]{"j"})))
                .addNode(new NodeDef("three", null, SynchNodeHandler.class, Arrays.asList(new String[]{"j"})))
                .addNode(new JoinNodeDef("j", TestControlNodeHandler.class, "four"))
                .addNode(new NodeDef("four", null, SynchNodeHandler.class, Arrays.asList(new String[]{"f"})))
                .addNode(new EndNodeDef("end", TestControlNodeHandler.class));

        LiteWorkflowInstance job = new LiteWorkflowInstance(def, new XConfiguration(), "1");
        try {
            job.start();
            fail();
        }
        catch (WorkflowException ex) {
            assertEquals(ErrorCode.E0709, ex.getErrorCode());
        }
        assertEquals(WorkflowInstance.Status.FAILED, job.getStatus());
    }

}
