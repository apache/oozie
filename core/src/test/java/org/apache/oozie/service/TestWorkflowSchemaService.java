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
package org.apache.oozie.service;

import org.apache.oozie.service.Services;
import org.apache.oozie.service.WorkflowSchemaService;
import org.apache.oozie.test.XTestCase;

import javax.xml.validation.Validator;
import javax.xml.transform.stream.StreamSource;
import java.io.StringReader;

public class TestWorkflowSchemaService extends XTestCase {


    private static final String APP1 = "<workflow-app xmlns='uri:oozie:workflow:0.1' name='app'>" +
                                       "<start to='end'/>" +
                                       "<end name='end'/>" +
                                       "</workflow-app>";

    private static final String APP2 = "<workflow-app xmlns='uri:oozie:workflow:0.1' name='app'>" +
                                       "<start to='a'/>" +
                                       "<action name='a'>" +
                                       "<test xmlns='uri:test'>" +
                                       "<signal-value>a</signal-value>" +
                                       "<external-status>b</external-status>" +
                                       "<error>c</error>" +
                                       "<avoid-set-execution-data>d</avoid-set-execution-data>" +
                                       "<avoid-set-end-data>d</avoid-set-end-data>" +
                                       "<running-mode>e</running-mode>" +
                                       "</test>" +
                                       "<ok to='end'/>" +
                                       "<error to='end'/>" +
                                       "</action>" +
                                       "<end name='end'/>" +
                                       "</workflow-app>";

    protected void setUp() throws Exception {
        super.setUp();
        new Services().init();
    }

    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    public void testService() throws Exception {
        assertNotNull(Services.get().get(WorkflowSchemaService.class));
    }

    public void testOozieSchema() throws Exception {
        WorkflowSchemaService wss = Services.get().get(WorkflowSchemaService.class);
        Validator validator = wss.getSchema().newValidator();
        validator.validate(new StreamSource(new StringReader(APP1)));
    }

    public void testExtSchema() throws Exception {
        Services.get().destroy();
        setSystemProperty(WorkflowSchemaService.CONF_EXT_SCHEMAS, "wf-ext-schema.xsd");
        new Services().init();
        WorkflowSchemaService wss = Services.get().get(WorkflowSchemaService.class);
        Validator validator = wss.getSchema().newValidator();
        validator.validate(new StreamSource(new StringReader(APP2)));
    }

}