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

import org.apache.oozie.service.Services;
import org.apache.oozie.service.CallbackService;
import org.apache.oozie.test.XTestCase;

public class TestCallbackService extends XTestCase {

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
        CallbackService cs = Services.get().get(CallbackService.class);
        assertNotNull(cs);
    }

    public void testCallbacks() throws Exception {
        CallbackService cs = Services.get().get(CallbackService.class);
        assertNotNull(cs);
        String callback = cs.createCallBackUrl("a", "@STATUS");
        assertTrue(callback.contains("http://"));
        assertTrue(callback.contains("id=a"));
        assertTrue(callback.contains("status=@STATUS"));
        callback = callback.replace("@STATUS", "OK");
        assertEquals("a", cs.getActionId(callback));
        assertEquals("OK", cs.getExternalStatus(callback));
    }

}
