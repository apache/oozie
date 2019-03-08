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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.URI;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.util.XConfiguration;

import static org.apache.oozie.action.hadoop.JavaActionExecutor.ACTION_SHARELIB_FOR;
import static org.apache.oozie.action.hadoop.JavaActionExecutor.SHARELIB_EXCLUDE_SUFFIX;
import static org.apache.oozie.action.hadoop.ShareLibExcluder.VALUE_NULL_MSG;

public class TestShareLibExcluder {

    private static final String[] EMPTY_ARRAY = {};
    private ShareLibExcluder shareLibExcluder;
    private Configuration actionConf;
    private Configuration servicesConf;
    private XConfiguration jobConf;
    private static final String executorType = "spark";
    private static final String excludeProperty = ACTION_SHARELIB_FOR + executorType + SHARELIB_EXCLUDE_SUFFIX;
    private static URI sharelibRootURI;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static final String SHARELIB_ROOT = "/user/oozie/share/lib/lib20180612/";
    private static final String[] OOZIE_SHARELIB_JARS = {
            SHARELIB_ROOT + "oozie/jackson-core-2.3.jar",
            SHARELIB_ROOT + "oozie/jackson-databind-2.3.jar",
            SHARELIB_ROOT + "oozie/other-lib.jar",
            SHARELIB_ROOT + "oozie/oozie-library.jar"
    };
    private static final String[] SPARK_SHARELIB_JARS = {
            SHARELIB_ROOT + "spark/jackson-core-2.6.5.jar",
            SHARELIB_ROOT + "spark/jackson-databind-2.6.5.jar",
            SHARELIB_ROOT + "spark/spark-lib.jar",
            SHARELIB_ROOT + "spark/lib/another-lib.jar"
    };
    private static final String[] PIG_SHARELIB_JARS = {
            SHARELIB_ROOT + "pig/lib/jackson-pig-0.3.3.jar",
            SHARELIB_ROOT + "pig/lib/jackson-datapig-0.3.5.jar",
            SHARELIB_ROOT + "pig/temp/pig_data.txt"
    };

    private List<String> libs;

    private static final String PIG_LIB_PATTERN = "pig/lib.*";
    private static final String ALL_JARS_PATTERN = ".*";
    private static final String ALL_JACKSON_PATTERN = ".*jackson.*";
    private static final String OOZIE_JACKSON_PATTERN = "oozie/jackson.*";
    private static final String SPARK_JACKSON_PATTERN = "spark/jackson.*";
    private static final String PIG_LIB_JACKSON_SPARK_JACKSON_PATTERN = "pig/lib/jackson.*|spark/jackson.*";
    private static final String ALL_EXCEPT_OOZIE_PATTERN = "^(?!.*oozie).*$";

    @Before
    public void setUp() {
        libs = new LinkedList<>();
        libs.addAll(Arrays.asList(OOZIE_SHARELIB_JARS));
        libs.addAll(Arrays.asList(SPARK_SHARELIB_JARS));
        libs.addAll(Arrays.asList(PIG_SHARELIB_JARS));

        sharelibRootURI = URI.create(SHARELIB_ROOT);
        actionConf = new Configuration();
        servicesConf = new Configuration();
        jobConf = new XConfiguration();
    }

    @After
    public void tearDown() throws Exception {
        shareLibExcluder = null;
    }

    private void doShareLibExclude(final String excludePattern, final String... libsExpectedToExclude) {
        doShareLibExclude(excludePattern, sharelibRootURI, libsExpectedToExclude);
    }

    private void doShareLibExclude(final String excludePattern, URI rootURI, final String... libsExpectedToExclude) {
        jobConf.set(excludeProperty, excludePattern);
        shareLibExcluder = new ShareLibExcluder(actionConf, servicesConf, jobConf, executorType, rootURI);
        checkShareLibExclude(libsExpectedToExclude);
    }

    private void checkShareLibExclude(final String... libsExpectedToExclude) {
        libs.removeAll(Arrays.asList(libsExpectedToExclude));

        for (String excludeLibPath : libsExpectedToExclude) {
            Assert.assertTrue("Lib path is not excluded, but should be",
                    shareLibExcluder.shouldExclude(URI.create(excludeLibPath)));
        }

        for (String dontExcludeLibPath : libs) {
            Assert.assertFalse("Lib path is excluded, but should not be",
                    shareLibExcluder.shouldExclude(URI.create(dontExcludeLibPath)));
        }
    }

    @Test
    public void testExcludeLibsFromEverywhere() {
        doShareLibExclude(ALL_JACKSON_PATTERN,
                OOZIE_SHARELIB_JARS[0], OOZIE_SHARELIB_JARS[1],
                SPARK_SHARELIB_JARS[0], SPARK_SHARELIB_JARS[1],
                PIG_SHARELIB_JARS[0], PIG_SHARELIB_JARS[1]);
    }

    @Test
    public void testExcludeLibsFromOozieDirOnly() {
        doShareLibExclude(OOZIE_JACKSON_PATTERN, OOZIE_SHARELIB_JARS[0], OOZIE_SHARELIB_JARS[1]);
    }

    @Test
    public void testExcludeLibsFromSparkDirOnly() {
        doShareLibExclude(SPARK_JACKSON_PATTERN, SPARK_SHARELIB_JARS[0], SPARK_SHARELIB_JARS[1]);
    }

    @Test
    public void testExcludeLibsFromPigAndSparkDirOnly() {
        doShareLibExclude(PIG_LIB_JACKSON_SPARK_JACKSON_PATTERN,
                PIG_SHARELIB_JARS[0], PIG_SHARELIB_JARS[1],
                SPARK_SHARELIB_JARS[0], SPARK_SHARELIB_JARS[1]);
    }

    @Test
    public void testDontExcludeSharelibRoot() {
        doShareLibExclude(SHARELIB_ROOT + PIG_LIB_PATTERN, EMPTY_ARRAY);
    }

    @Test
    public void testDontExcludeWithNotSetExcludeConfiguration() {
        shareLibExcluder = new ShareLibExcluder(actionConf, servicesConf, jobConf, executorType, sharelibRootURI);
        checkShareLibExclude(EMPTY_ARRAY);
    }

    @Test
    public void testDontExcludeWithNullRootURIValue() {
        doShareLibExclude(PIG_LIB_PATTERN, null, EMPTY_ARRAY);
    }

    @Test
    public void testDontExcludeWithNullExecutorType() {
        jobConf.set(excludeProperty, PIG_LIB_PATTERN);
        shareLibExcluder = new ShareLibExcluder(actionConf, servicesConf, jobConf, null, sharelibRootURI);
        checkShareLibExclude(EMPTY_ARRAY);
    }

    @Test
    public void testForExceptionsWhenIllegalActionConfParameterPassed() {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage(String.format(VALUE_NULL_MSG, "actionConf"));
        shareLibExcluder = new ShareLibExcluder(null, servicesConf, jobConf, executorType, sharelibRootURI);
    }

    @Test
    public void testForExceptionsWhenIllegalServicesConfParameterPassed() {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage(String.format(VALUE_NULL_MSG, "servicesConf"));
        shareLibExcluder = new ShareLibExcluder(actionConf, null, jobConf, executorType, sharelibRootURI);
    }

    @Test
    public void testForExceptionsWhenIllegalJobConfParameterPassed() {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage(String.format(VALUE_NULL_MSG, "jobConf"));
        shareLibExcluder = new ShareLibExcluder(actionConf, servicesConf, null, executorType, sharelibRootURI);
    }

    @Test
    public void testGetExcludePropertyFromConfigurations() {
        actionConf.set(excludeProperty, ALL_JARS_PATTERN);
        jobConf.set(excludeProperty, ALL_JARS_PATTERN);

        shareLibExcluder = new ShareLibExcluder(actionConf, servicesConf, jobConf, executorType, sharelibRootURI);
        jobConf.unset(excludeProperty);
        Assert.assertNotNull(actionConf.get(excludeProperty, null));

        actionConf.unset(excludeProperty);
        servicesConf.set(excludeProperty, ALL_JARS_PATTERN);
        shareLibExcluder = new ShareLibExcluder(actionConf, servicesConf, jobConf, executorType, sharelibRootURI);
        Assert.assertNotNull(actionConf.get(excludeProperty, null));
    }

    @Test
    public void testExcludeAllExceptOozie() {
        doShareLibExclude(ALL_EXCEPT_OOZIE_PATTERN,
                SPARK_SHARELIB_JARS[0], SPARK_SHARELIB_JARS[1], SPARK_SHARELIB_JARS[2], SPARK_SHARELIB_JARS[3],
                PIG_SHARELIB_JARS[0], PIG_SHARELIB_JARS[1],  PIG_SHARELIB_JARS[2]);
    }

    @Test
    public void testExcludeAll() {
        String[] libArray = new String[libs.size()];
        doShareLibExclude(ALL_JARS_PATTERN, libs.toArray(libArray));
    }

    @Test
    public void testWithNullLibPathArgShouldThrowException() {
        servicesConf.set(excludeProperty, ALL_JACKSON_PATTERN);
        shareLibExcluder = new ShareLibExcluder(actionConf, servicesConf, jobConf, executorType, sharelibRootURI);

        // expect exception when illegal parameters passed
        thrown.expect(NullPointerException.class);
        thrown.expectMessage(String.format(VALUE_NULL_MSG, "actionLibURI"));
        shareLibExcluder.shouldExclude(null);
    }
}
