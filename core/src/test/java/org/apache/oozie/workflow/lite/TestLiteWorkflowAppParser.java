/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.workflow.lite;

import org.apache.oozie.service.LiteWorkflowStoreService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.ErrorCode;

public class TestLiteWorkflowAppParser extends XTestCase {

    protected void setUp() throws Exception {
        super.setUp();
        new Services().init();
    }

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
    
}
