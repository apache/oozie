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

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.oozie.test.XTestCase;
import org.junit.Assert;

public class TestSqoopMain extends XTestCase {

    public void testJobIDPattern() {
        List<String> lines = new ArrayList<String>();
        lines.add("Job complete: job_001");
        lines.add("Job job_002 has completed successfully");
        lines.add("Submitted application application_003");
        // Non-matching ones
        lines.add("Job complete: application_004");
        lines.add("Job application_005 has completed successfully");
        lines.add("Submitted application job_006");
        Set<String> jobIds = new LinkedHashSet<String>();
        for (String line : lines) {
            LauncherMain.extractJobIDs(line, SqoopMain.SQOOP_JOB_IDS_PATTERNS,
                    jobIds);
        }
        Set<String> expected = new LinkedHashSet<String>();
        expected.add("job_001");
        expected.add("job_002");
        expected.add("job_003");
        assertEquals(expected, jobIds);
    }

    public void testIfDelegationTokenForTezAdded() throws Exception {
        final File actionXml = new File(LauncherAM.ACTION_CONF_XML);
        LauncherMain.sysenv = new MockedSystemEnvironment();
        setSystemProperty(LauncherAM.OOZIE_ACTION_CONF_XML, LauncherAM.ACTION_CONF_XML);

        try {
            actionXml.createNewFile();
            final Configuration conf = SqoopMain.setUpSqoopSite();
            Assert.assertNotNull(
                    String.format("Property [%s] shall be part of configuration", SqoopMain.TEZ_CREDENTIALS_PATH),
                    conf.get(SqoopMain.TEZ_CREDENTIALS_PATH));
        } finally {
            actionXml.delete();
            // sqoop-site.xml stays there after test run so that shall be cleaned up explicitly
            new File(SqoopMain.SQOOP_SITE_CONF).delete();
        }
    }

    class MockedSystemEnvironment extends SystemEnvironment {
        @Override
        public String getenv(final String name) {
            if(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION.equals(name)) {
                return UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;
            }
            return super.getenv(name);
        }
    }
}