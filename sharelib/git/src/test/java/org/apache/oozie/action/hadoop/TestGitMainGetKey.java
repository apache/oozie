/** * Licensed to the Apache Software Foundation (ASF) under one
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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;
import java.io.IOException;
import java.lang.Class;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URISyntaxException;

import java.io.File;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.oozie.action.hadoop.ActionExecutorTestCase.Context;
import org.apache.oozie.action.hadoop.SharelibUtils;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.command.wf.ActionXCommand.ActionExecutorContext;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.test.XTestCase.Predicate;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.WorkflowJobBean;
import org.jdom.Element;

import org.apache.oozie.test.XFsTestCase;

public class TestGitMainGetKey extends XFsTestCase {

    GitMain gitmain = null;
    Method getKeyMethod = null;

    private static List<String> getAllFilePath(Path filePath, FileSystem fs) throws FileNotFoundException, IOException {
        List<String> fileList = new ArrayList<String>();
        FileStatus[] fileStatus = fs.listStatus(filePath);
        for (FileStatus fileStat : fileStatus) {
            if (fileStat.isDirectory()) {
                fileList.addAll(getAllFilePath(fileStat.getPath(), fs));
            } else {
                fileList.add(fileStat.getPath().toString());
            }
        }
        return fileList;
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        gitmain = new GitMain();
        gitmain.nameNode = getFileSystem().getUri().toString();

        // allow us to call getKeyFromFS()
        Class<?>[] args = new Class<?>[] { Path.class } ;
        getKeyMethod = gitmain.getClass().getDeclaredMethod("getKeyFromFS", args);
        getKeyMethod.setAccessible(true);
    }

    /*
     * Test handling the key properly and that the file is secured
     */
    public void testKeyFile() throws Exception {
        final Path credentialFilePath = Path.mergePaths(getFsTestCaseDir(), new Path("/key_dir/my_key.dsa"));
        final String credentialFileData = "Key file data";
        final Path destDir = Path.mergePaths(getFsTestCaseDir(), new Path("/destDir"));
        final String repoUrl = "https://github.com/apache/oozie";

        // setup credential file data
        FSDataOutputStream credentialFile = getFileSystem().create(credentialFilePath);
        credentialFile.write(credentialFileData.getBytes());
        credentialFile.flush();
        credentialFile.close();

        WorkflowActionBean ga = new WorkflowActionBean();
		ga.setType("git-action");

        // actually copy the key to the local file system
        File localFile = (File) getKeyMethod.invoke(gitmain, credentialFilePath);

        // verify the key is the same as uploaded to HDFS
        try(FileReader reader = new FileReader(localFile)) {
            char[] testOutput = new char[credentialFileData.length()];
            assertEquals(13, credentialFileData.length());
            reader.read(testOutput, 0, credentialFileData.length());

            assertEquals(credentialFileData, String.valueOf(testOutput));
        }
    }

}
