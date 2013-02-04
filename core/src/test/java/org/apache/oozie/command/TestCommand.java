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
package org.apache.oozie.command;

import org.apache.oozie.store.StoreException;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.store.Store;
import org.apache.oozie.service.DagXLogInfoService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.XCallable;
import org.apache.oozie.util.XLog;
import org.apache.oozie.ErrorCode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class TestCommand extends XTestCase {
    private static List<String> EXECUTED = Collections.synchronizedList(new ArrayList<String>());

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

    private static class DummyXCallable implements XCallable<Void> {
        private String name;
        private String key = null;

        public DummyXCallable(String name) {
            this.name = name;
            this.key = name + "_" + UUID.randomUUID();
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getType() {
            return "type";
        }

        @Override
        public int getPriority() {
            return 0;
        }

        @Override
        public long getCreatedTime() {
            return 1;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Type:").append(getType());
            sb.append(",Priority:").append(getPriority());
            return sb.toString();
        }

        @Override
        public void setInterruptMode(boolean mode) {
        }

        @Override
        public boolean inInterruptMode() {
            return false;
        }

        public Void call() throws Exception {
            EXECUTED.add(name);
            return null;
        }

        @Override
        public String getKey() {
            return this.key;
        }

        @Override
        public String getEntityKey() {
            return null;
        }

    }

    private static class MyCommand extends Command<Object, WorkflowStore> {
        private boolean exception;

        public MyCommand(boolean exception) {
            super("test", "test", 1, XLog.OPS);
            this.exception = exception;
        }

        @Override
        protected Object call(WorkflowStore store) throws StoreException, CommandException {
            assertTrue(logInfo.createPrefix().contains("JOB[job]"));
            assertTrue(XLog.Info.get().createPrefix().contains("JOB[job]"));
            assertTrue(logInfo.createPrefix().contains("ACTION[action]"));
            assertTrue(XLog.Info.get().createPrefix().contains("ACTION[action]"));
            assertNotNull(store);
            assertEquals("test", getName());
            assertEquals(1, getPriority());
            queueCallable(new DummyXCallable("a"));
            queueCallable(Arrays.asList(new DummyXCallable("b"), new DummyXCallable("c")));
            queueCallable(new DummyXCallable("d"), 300);
            queueCallable(new DummyXCallable("e"), 200);
            queueCallable(new DummyXCallable("f"), 100);
            queueCallableForException(new DummyXCallable("ex"));
            if (exception) {
                throw new CommandException(ErrorCode.E0800);
            }
            return null;
        }

        /**
         * Return the public interface of the Workflow Store.
         *
         * @return {@link WorkflowStore}
         */
        @Override
        public Class<? extends Store> getStoreClass() {
            return WorkflowStore.class;
        }
    }

    public void testDagCommand() throws Exception {
        XLog.Info.get().clear();
        XLog.Info.get().setParameter(DagXLogInfoService.JOB, "job");
        XLog.Info.get().setParameter(DagXLogInfoService.ACTION, "action");

        Command command = new MyCommand(false);

        XLog.Info.get().clear();
        command.call();

        assertTrue(XLog.Info.get().createPrefix().contains("JOB[job]"));
        assertTrue(XLog.Info.get().createPrefix().contains("ACTION[action]"));
        command.resetLogInfoWorkflow();
        assertTrue(XLog.Info.get().createPrefix().contains("JOB[-]"));
        assertTrue(XLog.Info.get().createPrefix().contains("ACTION[action]"));
        command.resetLogInfoAction();
        assertTrue(XLog.Info.get().createPrefix().contains("ACTION[-]"));

        waitFor(2000, new Predicate() {
            public boolean evaluate() throws Exception {
                return EXECUTED.size() == 6;
            }
        });

        assertEquals(6, EXECUTED.size());
        assertEquals(Arrays.asList("a", "b", "c", "d", "e", "f"), EXECUTED);

        EXECUTED.clear();

        XLog.Info.get().setParameter(DagXLogInfoService.JOB, "job");
        XLog.Info.get().setParameter(DagXLogInfoService.ACTION, "action");
        command = new MyCommand(true);

        try {
            command.call();
            fail();
        }
        catch (CommandException ex) {
            //nop
        }

        waitFor(200, new Predicate() {
            public boolean evaluate() throws Exception {
                return EXECUTED.size() == 2;
            }
        });

        assertEquals(1, EXECUTED.size());
        assertEquals(Arrays.asList("ex"), EXECUTED);
    }

    /**
     * Return the public interface of the Workflow Store.
     *
     * @return {@link WorkflowStore}
     */
    public Class<? extends Store> getStoreClass() {
        return WorkflowStore.class;
    }

}
