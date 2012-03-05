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

import java.util.concurrent.Callable;

import org.apache.oozie.test.XFsTestCase;

public abstract class ShellTestCase extends XFsTestCase implements Callable<Void> {
    protected static String scriptContent = "";
    protected static String scriptName = "";
    protected boolean expectedSuccess = true;

    private static final String SUCCESS_SHELL_SCRIPT_CONTENT = "ls -ltr\necho $1 $2\necho $PATH\npwd\ntype sh";
    private static final String FAIL_SHELLSCRIPT_CONTENT = "ls -ltr\necho $1 $2\nexit 1";

    /**
     * Test a shell script that returns success
     *
     * @throws Exception
     */
    public void testShellScriptSuccess() throws Exception {
        scriptContent = SUCCESS_SHELL_SCRIPT_CONTENT;
        scriptName = "script.sh";
        expectedSuccess = true;
        MainTestCase.execute(getTestUser(), this);
    }

    /**
     * Test a shell script that returns failure
     *
     * @throws Exception
     */
    public void testShellScriptFailure() throws Exception {
        scriptContent = FAIL_SHELLSCRIPT_CONTENT;
        scriptName = "script.sh";
        expectedSuccess = false;
        MainTestCase.execute(getTestUser(), this);
    }
}
