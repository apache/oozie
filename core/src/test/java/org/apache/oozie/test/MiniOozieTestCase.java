/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.oozie.test;

import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.local.LocalOozie;

/**
 * Base JUnit <code>TestCase</code> subclass used to run independent test toward Oozie.
 * <p/>
 * This class provides the following functionality:
 * <p/>
 * <ul>
 *   <li>Creates a unique test working directory per test method on HDFS using MiniDFSCluster.</li>
 *   <li>Provides a test framework to execute tests for Oozie Workflow, Coordinator and Bundle.</li>
 *   <li>WaitFor that supports a predicate,to wait for a condition. It has timeout.</li>
 * </ul>
 * <p/>
 * As part of its setup, this testcase class creates a unique test working directory per test method in the FS.
 * <p/>
 * The URI of the namenode can be specified via the <code>oozie.test.name.node</code> system property. The default value is
 * 'hdfs://localhost:9000'.
 * <p/>
 * The test working directory is created in the specified FS URI, under the current user name home directory, under the
 * subdirectory name specified wit the system property {@link XTestCase#OOZIE_TEST_DIR}. The default value is '/tmp'.
 * <p/> The path of the test working directory is: '$FS_URI/user/$USER/$OOZIE_TEST_DIR/oozietest/$TEST_CASE_CLASS/$TEST_CASE_METHOD/'
 * <p/> For example: 'hdfs://localhost:9000/user/tucu/tmp/oozietest/org.apache.oozie.service.TestELService/testEL/'
 * <p/>
 * To run Oozie test, subclass create OozieClient via <code>OozieClient wc = getClient()</code> and submit Oozie job
 * with job properties and job xml.
 * <p/>
 * To check job's progress, subclass retrieve job object via <code>getJobInfo()</code> and then check job's status via <code>getStatus()</code>.
 * <p/>
 * For example,
 * <code>
 * WorkflowJob wf = wc.getJobInfo(jobId);<p/> assertNotNull(wf);<p/> assertEquals(WorkflowJob.Status.PREP, wf.getStatus());
 * </code>
 */
public abstract class MiniOozieTestCase extends XFsTestCase {

    @Override
    protected void setUp() throws Exception {
        System.setProperty("hadoop20", "true");
        super.setUp();
        LocalOozie.start();
    }

    @Override
    protected void tearDown() throws Exception {
        LocalOozie.stop();
        super.tearDown();
    }

    public static OozieClient getClient() {
        return LocalOozie.getClient();
    }

}
