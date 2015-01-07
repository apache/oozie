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

import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.test.XTestCase;

public class TestActionService extends XTestCase {

    static final String TEST_ACTION_TYPE = "TestActionType";

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

    public void testService() throws Exception {
        assertNotNull(Services.get().get(ActionService.class));
    }

    public void testActions() throws Exception {
        ActionService as = Services.get().get(ActionService.class);
        assertNotNull(as.getExecutor("switch"));
    }

    @SuppressWarnings("deprecation")
    public void testDuplicateActionExecutors() throws Exception {
        ActionService as = new ActionService();
        Services.get().getConf().set("oozie.service.ActionService.executor.classes",
                DummyExecutor1.class.getName() + "," + DummyExecutor2.class.getName());
        Services.get().getConf().set("oozie.service.ActionService.executor.ext.classes", "");
        try {
            as.init(Services.get());
            // There are 5 hard-coded control action types + 1 TEST_ACTION_TYPE
            assertEquals(6, as.getActionTypes().size());
            ActionExecutor executor = as.getExecutor(TEST_ACTION_TYPE);
            assertTrue(executor instanceof DummyExecutor2);
            assertFalse(executor instanceof DummyExecutor1);
        } finally {
            as.destroy();
        }

        as = new ActionService();
        Services.get().getConf().set("oozie.service.ActionService.executor.classes", DummyExecutor1.class.getName());
        Services.get().getConf().set("oozie.service.ActionService.executor.ext.classes", DummyExecutor2.class.getName());
        try {
            as.init(Services.get());
            // There are 5 hard-coded control action types + 1 TEST_ACTION_TYPE
            assertEquals(6, as.getActionTypes().size());
            ActionExecutor executor = as.getExecutor(TEST_ACTION_TYPE);
            assertTrue(executor instanceof DummyExecutor2);
            assertFalse(executor instanceof DummyExecutor1);
        } finally {
            as.destroy();
        }

        as = new ActionService();
        Services.get().getConf().set("oozie.service.ActionService.executor.classes", "");
        Services.get().getConf().set("oozie.service.ActionService.executor.ext.classes",
                DummyExecutor1.class.getName() + "," + DummyExecutor2.class.getName());
        try {
            as.init(Services.get());
            // There are 5 hard-coded control action types + 1 TEST_ACTION_TYPE
            assertEquals(6, as.getActionTypes().size());
            ActionExecutor executor = as.getExecutor(TEST_ACTION_TYPE);
            assertTrue(executor instanceof DummyExecutor2);
            assertFalse(executor instanceof DummyExecutor1);
        } finally {
            as.destroy();
        }
    }
}
