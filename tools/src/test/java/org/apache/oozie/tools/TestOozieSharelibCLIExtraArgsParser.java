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

package org.apache.oozie.tools;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static org.apache.oozie.tools.OozieSharelibCLI.getExtraLibs;

public class TestOozieSharelibCLIExtraArgsParser {

    private final static String TEST_SHAERELIBNAME1 = "sharelibName";
    private final static String TEST_SHAERELIBNAME2 = "sharelibName2";
    private final static String TEST_EXTRALIBS_PATHS1 = "/path/to/source/,/path/to/some/file";
    private final static String TEST_EXTRALIBS_PATHS2 = "hdfs://my/jar.jar#myjar.jar,path/to/source/";

    @Test
    public void testParsingExtraSharelibs() {
        String extraLibOption1 = TEST_SHAERELIBNAME1 + "=" + TEST_EXTRALIBS_PATHS1;
        String extraLibOption2 = TEST_SHAERELIBNAME2 + "=" + TEST_EXTRALIBS_PATHS2;
        Map<String, String> addLibs = getExtraLibs(new String[] {extraLibOption1, extraLibOption2});

        Assert.assertEquals("Extra libs count mismatch", 2, addLibs.size());
        Assert.assertEquals("Missing extra lib", TEST_EXTRALIBS_PATHS1, addLibs.get(TEST_SHAERELIBNAME1));
        Assert.assertEquals("Missing extra lib", TEST_EXTRALIBS_PATHS2, addLibs.get(TEST_SHAERELIBNAME2));
    }

    @Test (expected = IllegalArgumentException.class)
    public void testParsingExtraSharelibsMissingValue() {
        String extraLibOption1 = TEST_SHAERELIBNAME1 + "=";
        String extraLibOption2 = TEST_SHAERELIBNAME2 + "=" + TEST_EXTRALIBS_PATHS2;
        getExtraLibs(new String[] {extraLibOption1, extraLibOption2});
    }

    @Test (expected = IllegalArgumentException.class)
    public void testParsingExtraSharelibsMissingKey() {
        String extraLibOption1 = TEST_EXTRALIBS_PATHS1;
        String extraLibOption2 = TEST_SHAERELIBNAME2 + "=" + TEST_EXTRALIBS_PATHS2;
        getExtraLibs(new String[] {extraLibOption1, extraLibOption2});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParsingSharelibNamePresentMultipleTimes() {
        String extraLibOption1 = TEST_SHAERELIBNAME1 + "=" + TEST_EXTRALIBS_PATHS1;
        String extraLibOption2 = TEST_SHAERELIBNAME1 + "=" + TEST_EXTRALIBS_PATHS2;
        getExtraLibs(new String[] {extraLibOption1, extraLibOption2});
    }
}