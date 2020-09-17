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

import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.ForTestingActionExecutor;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.WorkflowStoreService;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.lite.NodeDef;
import org.apache.oozie.workflow.lite.NodeHandler;
import org.mockito.Mockito;

public class TestLiteWorkflowStoreService extends XTestCase {

    protected void setUp() throws Exception {
        super.setUp();
        new Services().init();
    }

    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    public void testService() throws Exception {
        assertNotNull(Services.get().get(WorkflowStoreService.class));
    }

    public void testCreateStore() throws Exception {
        WorkflowStoreService wls = Services.get().get(WorkflowStoreService.class);
        assertNotNull(wls);
        assertNotNull(wls.create());
    }

    public void testRetry() throws Exception {
        //  Introducing whitespaces in the error codes string
        String errorCodeWithWhitespaces = "\n\t\t" + ForTestingActionExecutor.TEST_ERROR + "\n  ";
        Configuration testConf = Services.get().get(ConfigurationService.class).getConf();
        // Setting configuration parameter for error codes
        testConf.set(LiteWorkflowStoreService.CONF_USER_RETRY_ERROR_CODE, errorCodeWithWhitespaces);
        testConf.set(LiteWorkflowStoreService.CONF_USER_RETRY_ERROR_CODE_EXT, " ");
        // Retrieval to enlist the codes properly, otherwise whitespaces cause the key-value lookup to return false
        Set<String> allowedRetryCodes = LiteWorkflowStoreService.getUserRetryErrorCode();
        assertTrue(allowedRetryCodes.contains(ForTestingActionExecutor.TEST_ERROR));
    }

    public void testRetryAllErrorCode() throws Exception {
        String errorCodeWithWhitespaces = "\n\t\t" + ForTestingActionExecutor.TEST_ERROR + "," +
                LiteWorkflowStoreService.USER_ERROR_CODE_ALL + "\n  ";
        Configuration testConf = Services.get().get(ConfigurationService.class).getConf();
        // Setting configuration parameter for retry.error.code
        testConf.set(LiteWorkflowStoreService.CONF_USER_RETRY_ERROR_CODE, errorCodeWithWhitespaces);
        Set<String> allowedRetryCodes = LiteWorkflowStoreService.getUserRetryErrorCode();
        assertTrue(allowedRetryCodes.contains(ForTestingActionExecutor.TEST_ERROR));
        assertTrue(allowedRetryCodes.contains(LiteWorkflowStoreService.USER_ERROR_CODE_ALL));

        // Setting configuration parameter for retry.error.code and retry.error.code.ext
        testConf.set(LiteWorkflowStoreService.CONF_USER_RETRY_ERROR_CODE_EXT, "ALL");
        allowedRetryCodes = LiteWorkflowStoreService.getUserRetryErrorCode();
        assertTrue(allowedRetryCodes.contains(ForTestingActionExecutor.TEST_ERROR));
        assertTrue(allowedRetryCodes.contains(LiteWorkflowStoreService.USER_ERROR_CODE_ALL));

        testConf.set(LiteWorkflowStoreService.CONF_USER_RETRY_ERROR_CODE, " ");
        testConf.set(LiteWorkflowStoreService.CONF_USER_RETRY_ERROR_CODE_EXT, "ALL");
        allowedRetryCodes = LiteWorkflowStoreService.getUserRetryErrorCode();
        assertTrue(allowedRetryCodes.contains(LiteWorkflowStoreService.USER_ERROR_CODE_ALL));
    }

    public void testGetUserRetryMax() throws WorkflowException {
        NodeHandler.Context context = Mockito.mock(NodeHandler.Context.class);
        NodeDef nodeDef = Mockito.mock(NodeDef.class);
        Mockito.when(context.getNodeDef()).thenReturn(nodeDef);

        assertEquals("System defined user retry max should be 3",
                3, ConfigurationService.getInt("oozie.service.LiteWorkflowStoreService.user.retry.max"));
        assertEquals("System defined user retry default should be 0",
                0, ConfigurationService.getInt("oozie.service.LiteWorkflowStoreService.user.retry.default"));

        // User defined retry-max not greater than system max
        Mockito.when(nodeDef.getUserRetryMax()).thenReturn("2");
        assertEquals("Should return user defined if user defined is not over system max",
                2, LiteWorkflowStoreService.getUserRetryMax(context));

        // User defined retry-max greater than system max
        ConfigurationService.set("oozie.service.LiteWorkflowStoreService.user.retry.max", "1");
        ConfigurationService.set("oozie.service.LiteWorkflowStoreService.user.retry.default", "0");
        Mockito.when(nodeDef.getUserRetryMax()).thenReturn("2");
        assertEquals("Should return system max if user defined is over system max",
                1, LiteWorkflowStoreService.getUserRetryMax(context));

        // User defined retry-max is an invalid number
        Mockito.when(nodeDef.getUserRetryMax()).thenReturn("abc");
        try {
            LiteWorkflowStoreService.getUserRetryMax(context);
            fail("Should have thrown exception because user defined retry-max is an invalid number");
        } catch (WorkflowException ex) {
            // Pass
        }

        // User defined retry-max is null, return system default
        ConfigurationService.set("oozie.service.LiteWorkflowStoreService.user.retry.max", "3");
        ConfigurationService.set("oozie.service.LiteWorkflowStoreService.user.retry.default", "2");
        Mockito.when(nodeDef.getUserRetryMax()).thenReturn("null");
        assertEquals("Should return system default if user defined is null",
                2, LiteWorkflowStoreService.getUserRetryMax(context));

        // User defined retry-max is null, and system default greater than system max, return system max
        ConfigurationService.set("oozie.service.LiteWorkflowStoreService.user.retry.max", "1");
        ConfigurationService.set("oozie.service.LiteWorkflowStoreService.user.retry.default", "2");
        Mockito.when(nodeDef.getUserRetryMax()).thenReturn("null");
        assertEquals("Should return system max if user defined is null and system default is over system max",
                1, LiteWorkflowStoreService.getUserRetryMax(context));
    }

}
