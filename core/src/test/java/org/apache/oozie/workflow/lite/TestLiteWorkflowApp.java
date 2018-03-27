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

import com.google.common.base.Strings;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.oozie.service.LiteWorkflowStoreService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.IOUtils;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;


public class TestLiteWorkflowApp extends XTestCase {

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

    @Test
    public void testReadWrite() throws Exception{
        String definition = "test"+ RandomStringUtils.random(100 * 1024);
        LiteWorkflowApp app = new LiteWorkflowApp(
                "name", definition, new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class, "foo"));
        app.addNode(new EndNodeDef("foo", LiteWorkflowStoreService.LiteControlNodeHandler.class));
        ByteArrayOutputStream baos= new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        app.write(out);
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream in = new DataInputStream(bais);
        LiteWorkflowApp app2 = new LiteWorkflowApp();
        app2.readFields(in);
        assertTrue(app.equals(app2));
    }

    /**
     * Before OOZIE-2777 the "defintion" field of LiteWorkflowApp was split into 20k long strings and serialized out
     * one after the other.
     * @throws Exception
     */
    @Test
    public void testOldFormatRead() throws Exception{
        String definition = Strings.repeat("abcdefghijk", 6234);
        LiteWorkflowApp app = new LiteWorkflowApp(
                "name", definition, new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class, "foo"));
        app.addNode(new EndNodeDef("foo", LiteWorkflowStoreService.LiteControlNodeHandler.class));
        DataInputStream in = new DataInputStream(IOUtils.getResourceAsStream("oldWorkFlowApp.serialized", -1));
        LiteWorkflowApp app2 = new LiteWorkflowApp();
        app2.readFields(in);
        assertTrue(app.equals(app2));
    }
}
