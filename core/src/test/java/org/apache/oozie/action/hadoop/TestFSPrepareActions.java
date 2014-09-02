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
import org.apache.hadoop.mapred.JobConf;
import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

public class TestFSPrepareActions extends XFsTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        Services services = new Services();
        services.getConf().set(HadoopAccessorService.SUPPORTED_FILESYSTEMS, "hdfs");
        services.init();
    }

    @Override
    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    // Test for delete as prepare action
    @Test
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

        JobConf conf = createJobConf();
        LauncherMapperHelper.setupLauncherURIHandlerConf(conf);
        PrepareActionsDriver.doOperations(prepareXML, conf);
        assertFalse(fs.exists(newDir));
    }

    // Test for delete as prepare action with glob
    @Test
    public void testDeleteWithGlob() throws Exception {
        Path actionDir = getFsTestCaseDir();
        FileSystem fs = getFileSystem();
        Path newDir = new Path(actionDir, "newDir");
        // Delete the file if it is already there
        if (fs.exists(newDir)) {
            fs.delete(newDir, true);
        }
        fs.mkdirs(newDir);
        fs.mkdirs(new Path(newDir, "2010"));
        fs.mkdirs(new Path(newDir + "/2010/10"));
        fs.mkdirs(new Path(newDir, "2011"));
        fs.mkdirs(new Path(newDir + "/2011/10"));
        fs.mkdirs(new Path(newDir, "2012"));
        fs.mkdirs(new Path(newDir + "/2012/10"));
        // Prepare block that contains delete action
        String prepareXML = "<prepare>" + "<delete path='" + newDir + "/201[0-1]/*" + "'/>" + "</prepare>";

        JobConf conf = createJobConf();
        LauncherMapperHelper.setupLauncherURIHandlerConf(conf);
        PrepareActionsDriver.doOperations(prepareXML, conf);
        assertFalse(fs.exists(new Path(newDir + "/2010/10")));
        assertFalse(fs.exists(new Path(newDir + "/2011/10")));
        assertTrue(fs.exists(new Path(newDir + "/2012/10")));
        fs.delete(newDir, true);
    }

    // Test for mkdir as prepare action
    @Test
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

        JobConf conf = createJobConf();
        LauncherMapperHelper.setupLauncherURIHandlerConf(conf);
        PrepareActionsDriver.doOperations(prepareXML, conf);
        assertTrue(fs.exists(newDir));
    }

    // Test for invalid scheme value in the path for action
    @Test
    public void testForInvalidScheme() throws Exception {
        Path actionDir = getFsTestCaseDir();
        // Construct a path with invalid scheme
        Path newDir = new Path("hftp:/" + actionDir.toString().substring(5) + "/delete");
        // Construct prepare XML block with the path
        String prepareXML = "<prepare>" + "<delete path='" + newDir + "'/>" + "</prepare>";
        // Parse the XML to get the node
        Document doc = PrepareActionsDriver.getDocumentFromXML(prepareXML);
        Node n = doc.getDocumentElement().getChildNodes().item(0);

        try {
            JobConf conf = createJobConf();
            LauncherMapperHelper.setupLauncherURIHandlerConf(conf);
            PrepareActionsDriver.doOperations(prepareXML, conf);
            fail("Expected to catch an exception but did not encounter any");
        } catch (LauncherException le) {
            Path path = new Path(n.getAttributes().getNamedItem("path").getNodeValue().trim());
            assertEquals("Scheme hftp not supported in uri " + path, le.getMessage());
        } catch(Exception ex){
            fail("Expected a LauncherException but received an Exception");
        }
    }

    // Test for null scheme value in the path for action
    @Test
    public void testForNullScheme() throws Exception {
        Path actionDir = getFsTestCaseDir();
        Path newDir = new Path(actionDir, "newDir");
        // Construct a path without scheme
        String noSchemePath = newDir.toUri().getPath();
        FileSystem fs = getFileSystem();
        // Delete the file if it is already there
        if (fs.exists(newDir)) {
            fs.delete(newDir, true);
        }
        // Construct prepare XML block with the path
        String prepareXML = "<prepare>" + "<mkdir path='" + noSchemePath + "'/>" + "</prepare>";

        JobConf conf = createJobConf();
        LauncherMapperHelper.setupLauncherURIHandlerConf(conf);
        PrepareActionsDriver.doOperations(prepareXML, conf);

        assertTrue(fs.exists(new Path(noSchemePath)));
    }

}
