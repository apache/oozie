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

public class TestXTestCase extends TestCase {
    static boolean TESTING = false;
    static String SYS_PROP = "oozie.test.testProp";
    static String testBaseDir;

    protected void setUp() throws Exception {
        super.setUp();
        testBaseDir = null;
        TESTING = true;
    }

    protected void tearDown() throws Exception {
        TESTING = false;
        super.tearDown();
    }

    public void testBaseDir() throws Exception {
        testBaseDir = System.getProperty(XTestCase.OOZIE_TEST_DIR);
        try {
            MyXTestCase testcase = new MyXTestCase();
            testcase.setName(getName());
            testcase.setUp();
            testcase.testBaseDir();
            testcase.tearDown();
        }
        finally {
            if (testBaseDir != null) {
                System.getProperties().setProperty(XTestCase.OOZIE_TEST_DIR, testBaseDir);
            }
        }
    }

    public void testSysPropSetting() throws Exception {
        try {
            System.getProperties().remove(SYS_PROP);
            MyXTestCase testcase = new MyXTestCase();
            testcase.setName(getName());
            testcase.setUp();
            testcase.testUnsetSysProperty();
            assertEquals("A", System.getProperty(SYS_PROP));
            testcase.tearDown();
            assertNull(System.getProperty(SYS_PROP));

            testcase = new MyXTestCase();
            testcase.setName(getName() + "A");
            testcase.setUp();
            System.getProperties().setProperty(SYS_PROP, "B");
            testcase.testSetSysProperty();
            assertEquals("C", System.getProperty(SYS_PROP));
            testcase.tearDown();
            assertEquals("B", System.getProperty(SYS_PROP));

        }
        finally {
            System.getProperties().remove(SYS_PROP);
        }

    }

    public void testWaitFor() throws Exception {
        MyXTestCase testcase = new MyXTestCase();
        testcase.setName(getName());
        testcase.setUp();
        testcase.testWaitFor();
        testcase.tearDown();

        testcase.setName(getName() + "A");
        testcase.setUp();
        testcase.testWaitForTimeOut();
        testcase.tearDown();
    }


    public static class MyXTestCase extends XTestCase {
        protected void setUp() throws Exception {
            if(TESTING){
                super.setUp();
            }
        }

        protected void tearDown() throws Exception {
            if(TESTING){
                super.tearDown();
            }
        }


        public void testDummy() {
        }

        public void testBaseDir() {
            if (TESTING) {
                assertTrue(TestXTestCase.testBaseDir == null ||
                        getTestCaseDir().startsWith(TestXTestCase.testBaseDir));
            }
        }

        public void testUnsetSysProperty() {
            if (TESTING) {
                assertNull(System.getProperty(TestXTestCase.SYS_PROP));
                setSystemProperty(TestXTestCase.SYS_PROP, "A");
                assertEquals("A", System.getProperty(TestXTestCase.SYS_PROP));
            }
        }

        public void testSetSysProperty() {
            if (TESTING) {
                assertEquals("B", System.getProperty(TestXTestCase.SYS_PROP));
                setSystemProperty(TestXTestCase.SYS_PROP, "C");
                assertEquals("C", System.getProperty(TestXTestCase.SYS_PROP));
            }
        }

        public void testWaitFor() {
            if (TESTING) {
                long start = System.currentTimeMillis();
                long waited = waitFor(60 * 1000, new Predicate() {
                    public boolean evaluate() throws Exception {
                        return true;
                    }
                });
                long end = System.currentTimeMillis();
                assertEquals(0, waited, 100);
                assertEquals(0, end - start, 300);
            }
        }

        public void testWaitForTimeOut() {
            float originalRatio = XTestCase.WAITFOR_RATIO;
            try {
                XTestCase.WAITFOR_RATIO = 1;
                if (TESTING) {
                    long start = System.currentTimeMillis();
                    long waited = waitFor(1000, new Predicate() {
                        public boolean evaluate() throws Exception {
                            return false;
                        }
                    });
                    long end = System.currentTimeMillis();
                    assertEquals(1000 * XTestCase.WAITFOR_RATIO, waited, 100);
                    assertEquals(1000 * XTestCase.WAITFOR_RATIO, end - start, 300);
                }
            }
            finally {
                XTestCase.WAITFOR_RATIO = originalRatio;
            }
        }

        public void testWaitForTimeOutWithRatio() {
            float originalRatio = XTestCase.WAITFOR_RATIO;
            try {
                XTestCase.WAITFOR_RATIO = 2;
                if (TESTING) {
                    long start = System.currentTimeMillis();
                    long waited = waitFor(1000, new Predicate() {
                        public boolean evaluate() throws Exception {
                            return false;
                        }
                    });
                    long end = System.currentTimeMillis();
                    assertEquals(1000 * XTestCase.WAITFOR_RATIO, end - start, 300);
                }
            }
            finally {
                XTestCase.WAITFOR_RATIO = originalRatio;
            }
        }

        public void testHadoopSysProps() {
            if (TESTING) {
                setSystemProperty(XTestCase.OOZIE_TEST_NAME_NODE, "hdfs://xyz:9000");
                setSystemProperty(XTestCase.OOZIE_TEST_JOB_TRACKER, "xyz:9001");
                assertEquals("hdfs://xyz:9000", getNameNodeUri());
                assertEquals("xyz:9001", getJobTrackerUri());
            }
        }

    }

    public void testHadopSysProps() throws Exception {
        MyXTestCase testcase = new MyXTestCase();
        testcase.setName(getName());
        testcase.setUp();
        testcase.testHadoopSysProps();
        testcase.tearDown();
    }

}
