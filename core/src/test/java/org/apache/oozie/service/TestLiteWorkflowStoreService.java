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


}
