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
package org.apache.oozie.action.hadoop;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XFsTestCase;
import org.apache.hadoop.fs.FileSystem;

public class TestPrepareActionsDriver extends XFsTestCase {

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

    // Test to check if prepare action is performed as expected when the prepare XML block is a valid one
    public void testDoOperationsWithValidXML() throws LauncherException, IOException {
        Path actionDir = getFsTestCaseDir();
        FileSystem fs = getFileSystem();
        Path newDir = new Path(actionDir, "newDir");
        String prepareXML = "<prepare>" + "<mkdir path='" + newDir + "'/>" + "</prepare>";

        // Delete the file if it is already there
        if (fs.exists(newDir)) {
            fs.delete(newDir, true);
        }

        PrepareActionsDriver.doOperations(Arrays.asList("hdfs"), prepareXML);
        assertTrue(fs.exists(actionDir));
    }

    // Test to check if LauncherException is thrown when the prepare XML block is invalid
    public void testDoOperationsWithInvalidXML() throws LauncherException, IOException {
        Path actionDir = getFsTestCaseDir();
        FileSystem fs = getFileSystem();
        Path newDir = new Path(actionDir, "newDir");
        String prepareXML = "";

        // Delete the file if it is already there
        if (fs.exists(newDir)) {
            fs.delete(newDir, true);
        }

        try {
            prepareXML = "prepare>" + "<mkdir path='" + newDir + "'/>" + "</prepare>";
            PrepareActionsDriver.doOperations(Arrays.asList("hdfs"), prepareXML);
            fail("Expected to catch an exception but did not encounter any");
        } catch (LauncherException le) {
            assertEquals(le.getCause().getClass(), org.xml.sax.SAXParseException.class);
            assertEquals(le.getMessage(), "Content is not allowed in prolog.");
        } catch(Exception ex){
            fail("Expected a LauncherException but received an Exception");
        }
    }
}
