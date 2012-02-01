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
package org.apache.oozie.test;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class TestXFsTestCase extends XFsTestCase {

    public void testFsDir() throws Exception {
        assertNotNull(getFsTestCaseDir());
        assertNotNull(getFileSystem());

        String testDir = getTestCaseDir();
        String nameNode = getNameNodeUri();
        String user = getTestUser();
        Path fsTestDir = getFsTestCaseDir();

        assertTrue(fsTestDir.toString().startsWith(nameNode));
        assertTrue(fsTestDir.toString().contains(user + testDir));

        FileSystem fs = getFileSystem();
        assertTrue(fs.getUri().toString().startsWith(getNameNodeUri()));

        assertTrue(fs.exists(fsTestDir));
        assertTrue(fs.listStatus(fsTestDir).length == 0);
    }

}
