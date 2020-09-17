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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.oozie.WorkflowJobBean;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

/**
 * Test Credentials
 */

public class TestCredentials extends ActionExecutorTestCase {

    private Map<String, CredentialsProperties> credPropertiesMap;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        credPropertiesMap = new HashMap<>();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testHbaseCredentials() {
        CredentialsProperties prop = new CredentialsProperties("dummyName", "dummyType");
        prop.getProperties().put("hbase.zookeeper.quorum", "dummyHost");
        HbaseCredentials hb = new HbaseCredentials();
        JobConf jc = new JobConf(false);
        hb.copyHbaseConfToJobConf(jc, prop);
        assertEquals("dummyHost", jc.get("hbase.zookeeper.quorum"));
    }

    @Test
    public void testThrowErrorWhenMissingFileSystemPathProperty() throws Exception {
        CredentialsProperties props = new CredentialsProperties("filesystem", "filesystem");
        String exMsg = FileSystemCredentials.FILESYSTEM_PATH + " property is required to get filesystem type credential";
        FileSystemCredentials fs = new FileSystemCredentials();
        try {
            fs.updateCredentials(null, null, props, null);
            Assert.fail("No exception was thrown!");
        } catch (CredentialException ex) {
            Assert.assertTrue(ex.getMessage().contains(exMsg));
        }
    }

    /**
     * Test adding credential for default NameNode defined in mapreduce.job.hdfs-servers.
     */
    @Test
    public void testAddNameNodeCredentials() {
        Configuration actionConf = new Configuration();
        String jobNameNodes = "hdfs://namenode1";
        actionConf.set(MRJobConfig.JOB_NAMENODES, jobNameNodes);
        new JavaActionExecutor().addNameNodeCredentials(actionConf, credPropertiesMap);
        verifyCredentialsMapping(jobNameNodes);
    }

    /**
     * Test adding credentials for multiple NameNodes/resources in case of cross cluster distcp action.
     * Should override the value of mapreduce.job.hdfs-servers with oozie.launcher.mapreduce.job.hdfs-servers, in case its defined.
     */
    @Test
    public void testAddDistCpNameNodeCredentialsOverrideConf() {
        Configuration actionConf = new Configuration();
        String jobNameNodes = "hdfs://namenode1,abfs://resource2,s3a://resource3";
        String defaultNameNode = "hdfs://namenode1";
        actionConf.set(DistcpActionExecutor.OOZIE_LAUNCHER_MAPREDUCE_JOB_HDFS_SERVERS, jobNameNodes);
        actionConf.set(MRJobConfig.JOB_NAMENODES, defaultNameNode);
        new DistcpActionExecutor().addNameNodeCredentials(actionConf, credPropertiesMap);
        verifyCredentialsMapping(jobNameNodes);
        Assert.assertEquals(actionConf.get(MRJobConfig.JOB_NAMENODES), jobNameNodes);
        Assert.assertNotEquals(actionConf.get(MRJobConfig.JOB_NAMENODES), defaultNameNode);
    }

    /**
     * Test adding credentials for multiple NameNodes in case of cross cluster distcp action.
     * Should NOT override the value of mapreduce.job.hdfs-servers, as oozie.launcher.mapreduce.job.hdfs-servers is not defined.
     */
    @Test
    public void testAddDistCpNameNodeCredentialsDefaultConf() {
        Configuration actionConf = new Configuration();
        String jobNameNodes = "hdfs://namenode1,hdfs://namenode2,hdfs://namenode3";
        actionConf.set(MRJobConfig.JOB_NAMENODES, jobNameNodes);
        new DistcpActionExecutor().addNameNodeCredentials(actionConf, credPropertiesMap);
        verifyCredentialsMapping(jobNameNodes);
        Assert.assertEquals(actionConf.get(MRJobConfig.JOB_NAMENODES), jobNameNodes);
    }

    /**
     * Test adding credentials for workflow application path.
     */
    @Test
    public void testAddWorkflowAppFileSystemCredentials() {
        String wfAppPath = "hdfs://namenode1/user/test_user/app/";
        Context context = Mockito.mock(Context.class);
        WorkflowJobBean wf = Mockito.mock(WorkflowJobBean.class);
        Mockito.when(context.getWorkflow()).thenReturn(wf);
        Mockito.when(wf.getAppPath()).thenReturn(wfAppPath);
        new JavaActionExecutor().addWorkflowAppFileSystemCredentials(context, credPropertiesMap);
        CredentialsProperties props = credPropertiesMap.get(CredentialsProviderFactory.WORKFLOW_APP_FS);
        Assert.assertEquals(props.getProperties().get(FileSystemCredentials.FILESYSTEM_PATH), wfAppPath);
    }

    private void verifyCredentialsMapping(String jobNameNodes) {
        CredentialsProperties props = credPropertiesMap.get(CredentialsProviderFactory.NAMENODE_FS);
        Assert.assertNotNull(props);
        Assert.assertEquals(CredentialsProviderFactory.FS, props.getType());
        Assert.assertEquals(CredentialsProviderFactory.NAMENODE_FS, props.getName());
        Assert.assertEquals(jobNameNodes, props.getProperties().get(FileSystemCredentials.FILESYSTEM_PATH));
    }
}
