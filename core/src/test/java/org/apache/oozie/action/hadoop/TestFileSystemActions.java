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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.service.Services;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

public class TestFileSystemActions extends XFsTestCase {

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

    // Test for delete as prepare action
    public void testDelete() throws Exception {
        Path actionDir = getFsTestCaseDir();
        FileSystem fs = getFileSystem();
        Path newDir = new Path(actionDir, "newDir");
        // Delete the file if it is already there
        if (fs.exists(newDir)) {
            fs.delete(newDir, true);
        }
        fs.mkdirs(newDir);
        // Prepare block that contains delete action
        String prepareXML = "<prepare>" + "<delete path='" + newDir + "'/>" + "</prepare>";

        // Parse the XML to get the node
        Document doc = PrepareActionsDriver.getDocumentFromXML(prepareXML);
        Node n = doc.getDocumentElement().getChildNodes().item(0);

        new FileSystemActions().execute(n);
        assertFalse(fs.exists(newDir));
    }

    // Test for mkdir as prepare action
    public void testMkdir() throws Exception {
        Path actionDir = getFsTestCaseDir();
        FileSystem fs = getFileSystem();
        Path newDir = new Path(actionDir, "newDir");
        // Delete the file if it is already there
        if (fs.exists(newDir)) {
            fs.delete(newDir, true);
        }

        // Prepare block that contains mkdir action
        String prepareXML = "<prepare>" + "<mkdir path='" + newDir + "'/>" + "</prepare>";

        // Parse the XML to get the node
        Document doc = PrepareActionsDriver.getDocumentFromXML(prepareXML);
        Node n = doc.getDocumentElement().getChildNodes().item(0);

        new FileSystemActions().execute(n);
        assertTrue(fs.exists(newDir));
    }

    // Test for invalid scheme value in the path for action
    public void testForInvalidScheme() throws Exception {
        Path actionDir = getFsTestCaseDir();
        // Construct a path with invalid scheme
        Path newDir = new Path("http" + actionDir.toString().substring(4) + "/delete");
        // Construct prepare XML block with the path
        String prepareXML = "<prepare>" + "<delete path='" + newDir + "'/>" + "</prepare>";
        // Parse the XML to get the node
        Document doc = PrepareActionsDriver.getDocumentFromXML(prepareXML);
        Node n = doc.getDocumentElement().getChildNodes().item(0);

        try {
            new FileSystemActions().execute(n);
            fail("Expected to catch an exception but did not encounter any");
        } catch (LauncherException le) {
            Path path = new Path(n.getAttributes().getNamedItem("path").getNodeValue().trim());
            assertEquals(le.getMessage(), "Scheme of the provided path " + path + " is of type not supported.");
        } catch(Exception ex){
            fail("Expected a LauncherException but received an Exception");
        }
    }

    // Test for null scheme value in the path for action
    public void testForNullScheme() throws Exception {
        // Construct a path without scheme
        Path newDir = new Path("test/oozietests/testDelete/delete");
        FileSystem fs = getFileSystem();
        // Delete the file if it is already there
        if (fs.exists(newDir)) {
            fs.delete(newDir, true);
        }
        // Construct prepare XML block with the path
        String prepareXML = "<prepare>" + "<delete path='" + newDir + "'/>" + "</prepare>";

        // Parse the XML to get the node
        Document doc = PrepareActionsDriver.getDocumentFromXML(prepareXML);
        Node n = doc.getDocumentElement().getChildNodes().item(0);

        try {
            new FileSystemActions().execute(n);
            fail("Expected to catch an exception but did not encounter any");
        } catch (LauncherException le) {
            Path path = new Path(n.getAttributes().getNamedItem("path").getNodeValue().trim());
            assertEquals(le.getMessage(), "Scheme of the path " + path + " is null");
        } catch(Exception ex) {
            fail("Expected a LauncherException but received an Exception");
        }
    }

}
