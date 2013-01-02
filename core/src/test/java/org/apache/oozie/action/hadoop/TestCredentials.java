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

import org.apache.hadoop.mapred.JobConf;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.hadoop.Credentials;
import org.apache.oozie.action.hadoop.CredentialsProperties;

/**
 * Test Credentials
 *
 */
public class TestCredentials extends ActionExecutorTestCase {

    public void testHbaseCredentials() {
        CredentialsProperties prop = new CredentialsProperties("dummyName", "dummyType");
        prop.getProperties().put("hbase.zookeeper.quorum", "dummyHost");
        Credentials hb = new HbaseCredentials();
        WorkflowJobBean wfBean = new WorkflowJobBean();
        wfBean.setUser("dummyUser");
        JobConf jc = new JobConf(false);
        try {
            hb.addtoJobConf(jc, prop, new Context(wfBean, new WorkflowActionBean()));
        }
        catch (Exception e) {
            // Change this when security related classes are available from
            // hbase maven repo
            if (!(e.getCause() instanceof ClassNotFoundException)) {
                fail("unexpected exception " + e.getMessage());
            }
        }
        assertEquals("dummyHost", jc.get("hbase.zookeeper.quorum"));
    }

}
