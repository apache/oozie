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

import junit.framework.TestCase;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;

import java.io.File;
import java.io.IOException;

class TestCaseDirectories {

    /**
     * Create the test working directory.
     *
     * @param testCase testcase instance to obtain the working directory.
     * @param cleanup indicates if the directory should be cleaned up if it exists.
     * @return return the path of the test working directory, it is always an absolute path.
     * @throws Exception if the test working directory could not be created or cleaned up.
     */
    String createTestCaseDir(final TestCase testCase, final boolean cleanup) throws Exception {
        final String testCaseDir = getTestCaseDirInternal(testCase);
        System.out.println();
        System.out.println(XLog.format("Setting testcase work dir[{0}]", testCaseDir));
        if (cleanup) {
            delete(new File(testCaseDir));
        }
        final File dir = new File(testCaseDir);
        if (!dir.mkdirs()) {
            throw new RuntimeException(XLog.format("Could not create testcase dir[{0}]", testCaseDir));
        }
        return testCaseDir;
    }

    /**
     * Return the test working directory.
     * <p/>
     * It returns <code>${oozie.test.dir}/oozietests/TESTCLASSNAME/TESTMETHODNAME</code>.
     *
     * @param testCase testcase instance to obtain the working directory.
     * @return the test working directory.
     */
    private String getTestCaseDirInternal(final TestCase testCase) {
        ParamChecker.notNull(testCase, "testCase");

        File dir = new File(System.getProperty(TestConstants.OOZIE_TEST_DIR, "target/test-data"));

        dir = new File(dir, "oozietests").getAbsoluteFile();
        dir = new File(dir, testCase.getClass().getName());
        dir = new File(dir, testCase.getName());

        return dir.getAbsolutePath();
    }

    protected void delete(final File file) throws IOException {
        ParamChecker.notNull(file, "file");
        if (file.getAbsolutePath().length() < 5) {
            throw new RuntimeException(XLog.format("path [{0}] is too short, not deleting", file.getAbsolutePath()));
        }
        if (file.exists()) {
            if (file.isDirectory()) {
                final File[] children = file.listFiles();
                if (children != null) {
                    for (final File child : children) {
                        delete(child);
                    }
                }
            }
            if (!file.delete()) {
                throw new RuntimeException(XLog.format("could not delete path [{0}]", file.getAbsolutePath()));
            }
        }
        else {
            // With a dangling symlink, exists() doesn't return true so try to delete it anyway; we fail silently in case the file
            // truely doesn't exist
            file.delete();
        }
    }

    String createTestCaseSubdir(String testCaseDir, String[] subDirNames) {
        ParamChecker.notNull(subDirNames, "subDirName");
        if (subDirNames.length == 0) {
            throw new RuntimeException(XLog.format("Could not create testcase subdir ''; it already exists"));
        }

        File dir = new File(testCaseDir);
        for (int i = 0; i < subDirNames.length; i++) {
            ParamChecker.notNull(subDirNames[i], "subDirName[" + i + "]");
            dir = new File(dir, subDirNames[i]);
        }

        if (!dir.mkdirs()) {
            throw new RuntimeException(XLog.format("Could not create testcase subdir[{0}]", dir));
        }
        return dir.getAbsolutePath();
    }

    void createTestDirOrError() {
        final String baseDir = System.getProperty(TestConstants.OOZIE_TEST_DIR, new File("target/test-data").getAbsolutePath());
        String msg = null;
        final File testDir = new File(baseDir);
        if (!testDir.isAbsolute()) {
            msg = XLog.format("System property [{0}]=[{1}] must be set to an absolute path", TestConstants.OOZIE_TEST_DIR, baseDir);
        }
        else {
            if (baseDir.length() < 4) {
                msg = XLog.format("System property [{0}]=[{1}] path must be at least 4 chars", TestConstants.OOZIE_TEST_DIR, baseDir);
            }
        }
        if (msg != null) {
            System.err.println();
            System.err.println(msg);
            System.exit(-1);
        }
        testDir.mkdirs();
        if (!testDir.exists() || !testDir.isDirectory()) {
            System.err.println();
            System.err.println(XLog.format("Could not create test dir [{0}]", baseDir));
            System.exit(-1);
        }
    }
}
