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

import org.apache.oozie.ErrorCode;
import org.apache.oozie.service.MemoryLocksService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.MemoryLocks;

public class TestXCommand extends XTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        Services services = new Services();
        services.init();
    }

    @Override
    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    public static class AXCommand extends XCommand {
        private boolean lockRequired;
        public boolean eagerLoadState;
        public boolean eagerVerifyPrecondition;
        public boolean loadState;
        public boolean verifyPrecondition;
        public boolean execute;
        public boolean mark_fail = false;

        public AXCommand(boolean lockRequired) {
            super("name", "type", 1);
            this.lockRequired = lockRequired;
        }

        public AXCommand(boolean lockRequired, boolean mark_fail) {
            super("name", "type", 1);
            this.lockRequired = lockRequired;
            this.mark_fail = mark_fail;
        }

        @Override
        protected long getLockTimeOut() {
            assertEquals(5 * 1000, super.getLockTimeOut());
            return 100;
        }

        @Override
        protected boolean isLockRequired() {
            return lockRequired;
        }

        @Override
        protected boolean isReQueueRequired() {
            return false;
        }

        @Override
        public String getEntityKey() {
            return "key";
        }

        @Override
        protected void eagerLoadState() {
            assertFalse(eagerLoadState);
            assertFalse(eagerVerifyPrecondition);
            assertFalse(loadState);
            assertFalse(verifyPrecondition);
            assertFalse(execute);
            eagerLoadState = true;
        }

        @Override
        protected void eagerVerifyPrecondition() throws CommandException {
            assertTrue(eagerLoadState);
            if (this.mark_fail) {
                throw new CommandException(ErrorCode.E0000);
            }
            assertFalse(eagerVerifyPrecondition);
            assertFalse(loadState);
            assertFalse(verifyPrecondition);
            assertFalse(execute);
            eagerVerifyPrecondition = true;
        }

        @Override
        protected void loadState() {
            assertTrue(eagerLoadState);
            assertTrue(eagerVerifyPrecondition);
            assertFalse(loadState);
            assertFalse(verifyPrecondition);
            assertFalse(execute);
            loadState = true;
        }

        @Override
        protected void verifyPrecondition() throws CommandException {
            assertTrue(eagerLoadState);
            assertTrue(eagerVerifyPrecondition);
            assertTrue(loadState);
            if (this.mark_fail) {
                throw new CommandException(ErrorCode.E0000);
            }
            assertFalse(verifyPrecondition);
            assertFalse(execute);
            verifyPrecondition = true;
        }

        @Override
        protected Object execute() throws CommandException {
            assertTrue(eagerLoadState);
            assertTrue(eagerVerifyPrecondition);
            assertTrue(loadState);
            assertTrue(verifyPrecondition);
            assertFalse(execute);
            execute = true;
            return null;
        }
    }

    public void testXCommandGetters() throws Exception {
        XCommand command = new AXCommand(false);
        assertEquals("name", command.getName());
        assertEquals("type", command.getType());
        assertEquals(1, command.getPriority());
        assertEquals(false, command.isLockRequired());
        assertEquals("key", command.getEntityKey());
    }

    public void testXCommandLifecycleNotLocking() throws Exception {
        Thread t = new LockGetter();
        t.start();
        AXCommand command = new AXCommand(false);
        command.call();
        assertTrue(command.execute);
        t.interrupt();
    }

    public void testXCommandLifecycleLocking() throws Exception {
        AXCommand command = new AXCommand(true);
        command.call();
        assertTrue(command.execute);
    }

    public void testXCommandLifecycleLockingFailingToLock() throws Exception {
        Thread t = new LockGetter();
        t.start();
        Thread.sleep(150);
        AXCommand command = new AXCommand(true);
        try {
            command.call();
            fail();
        }
        catch (CommandException ex) {
        }
        catch (Exception ex) {
        }
        t.interrupt();
    }

    public void testXCommandeagerVerifyPreconditionFailing() throws Exception {
        AXCommand command = new AXCommand(false);
        command.mark_fail = true;
        try {
            command.call();
            fail();
        }
        catch (CommandException ex) {
        }
        catch (Exception ex) {
        }
    }

    public void testXCommandVerifyPreconditionFailing() throws Exception {
        AXCommand command = new AXCommand(true);
        command.mark_fail = true;
        try {
            command.call();
            fail();
        }
        catch (CommandException ex) {
        }
        catch (Exception ex) {
        }
    }

    private static class LockGetter extends Thread {

        @Override
        public void run() {
            try {
                MemoryLocks.LockToken lock = Services.get().get(MemoryLocksService.class).getWriteLock("key", 1);
                if (lock == null) {
                    fail();
                }
                Thread.sleep(150);
            }
            catch (InterruptedException ex) {
                // NOP
            }
            catch (Exception ex) {
                fail();
            }
        }
    }
}
