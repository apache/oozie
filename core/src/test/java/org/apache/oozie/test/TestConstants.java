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

import org.apache.hadoop.util.Shell;

public class TestConstants {
    /**
     * System property that specifies the default test user name used by
     * the tests. The defalt value of this property is <tt>test</tt>.
     */
    public static final String TEST_USER1_PROP = "oozie.test.user.test";
    /**
     * System property that specifies the test groiup used by the tests.
     * The default value of this property is <tt>testg</tt>.
     */
    public static final String TEST_GROUP_PROP2 = "oozie.test.group2";
    /**
     * Name of the shell command
     */
    public static final String SHELL_COMMAND_NAME = (Shell.WINDOWS) ? "cmd" : "bash";
    /**
     * Extension for shell script files
     */
    protected static final String SHELL_COMMAND_SCRIPTFILE_EXTENSION = (Shell.WINDOWS) ? "cmd" : "sh";
    /**
     * Option for shell command to pass script files
     */
    public static final String SHELL_COMMAND_SCRIPTFILE_OPTION = (Shell.WINDOWS) ? "/c" : "-c";
    /**
     * System property to specify the parent directory for the 'oozietests' directory to be used as base for all test
     * working directories. </p> If this property is not set, the assumed value is '/tmp'.
     */
    static final String OOZIE_TEST_DIR = "oozie.test.dir";
    /**
     * System property to specify the Hadoop Job Tracker to use for testing. </p> If this property is not set, the
     * assumed value is 'locahost:9001'.
     */
    static final String OOZIE_TEST_JOB_TRACKER = "oozie.test.job.tracker";
    /**
     * System property to specify the Hadoop Name Node to use for testing. </p> If this property is not set, the assumed
     * value is 'locahost:9000'.
     */
    static final String OOZIE_TEST_NAME_NODE = "oozie.test.name.node";
    /**
     * System property to specify the second Hadoop Name Node to use for testing. </p> If this property is not set, the assumed
     * value is 'locahost:9100'.
     */
    static final String OOZIE_TEST_NAME_NODE2 = "oozie.test.name.node2";
    /**
     * System property to specify the Hadoop Version to use for testing. </p> If this property is not set, the assumed
     * value is "0.20.0"
     */
    static final String HADOOP_VERSION = "hadoop.version";
    /**
     * System property that specifies the user that test oozie instance runs as.
     * The value of this property defaults to the "${user.name} system property.
     */
    static final String TEST_OOZIE_USER_PROP = "oozie.test.user.oozie";
    /**
     * System property that specifies an auxilliary test user name used by the
     * tests. The default value of this property is <tt>test2</tt>.
     */
    static final String TEST_USER2_PROP = "oozie.test.user.test2";
    /**
     * System property that specifies another auxilliary test user name used by
     * the tests. The default value of this property is <tt>test3</tt>.
     */
    static final String TEST_USER3_PROP = "oozie.test.user.test3";
    /**
     * System property that specifies the test groiup used by the tests.
     * The default value of this property is <tt>testg</tt>.
     */
    static final String TEST_GROUP_PROP = "oozie.test.group";
    /**
     * System property that specifies the wait time, in seconds, between testcases before
     * triggering a shutdown. The default value is 10 sec.
     */
    static final String TEST_MINICLUSTER_MONITOR_SHUTDOWN_WAIT = "oozie.test.minicluster.monitor.shutdown.wait";
}