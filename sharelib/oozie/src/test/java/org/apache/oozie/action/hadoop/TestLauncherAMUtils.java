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

import static org.apache.oozie.action.hadoop.LauncherAMUtils.CONF_OOZIE_ACTION_MAIN_ARG_COUNT;
import static org.apache.oozie.action.hadoop.LauncherAMUtils.CONF_OOZIE_ACTION_MAIN_ARG_PREFIX;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.anyBoolean;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestLauncherAMUtils {
    @Mock
    private Configuration conf;  // we have to use mock, because conf.set(null) throws exception

    @Test
    public void testArgsHandlingWithoutNullsAndNullsNotAllowed() {
       setupConf(Arrays.asList("a", "b", "c"));
       setEnableNullArgsAllowed(false);

       String args[] = LauncherAMUtils.getMainArguments(conf);

       assertTrue(Arrays.equals(new String[] { "a", "b", "c"}, args));
    }

    @Test
    public void testHandlingWhenArgsContainNullsAndNullsNotAllowed() {
        setupConf(Arrays.asList("a", null, "b", null, "c"));
        setEnableNullArgsAllowed(false);

        String args[] = LauncherAMUtils.getMainArguments(conf);

        assertTrue(Arrays.equals(new String[] { "a", "b", "c"}, args));
    }

    @Test
    public void testArgsHandlingWhenArgsContainsNullsOnlyAndNullsNotAllowed() {
        setupConf(Arrays.asList(null, null, null));
        setEnableNullArgsAllowed(false);

        String args[] = LauncherAMUtils.getMainArguments(conf);

        assertTrue(Arrays.equals(new String[] {}, args));
    }

    @Test
    public void testArgsHandlingWhenArgsContainsOneNullAndNullsNotAllowed() {
        setupConf(Arrays.asList((String) null));
        setEnableNullArgsAllowed(false);

        String args[] = LauncherAMUtils.getMainArguments(conf);

        assertTrue(Arrays.equals(new String[] {}, args));
    }

    @Test
    public void testHandlingWhenArgsContainNullsAndNullAllowed() {
        setupConf(Arrays.asList("a", null, "b", null, "c"));
        setEnableNullArgsAllowed(true);

        String args[] = LauncherAMUtils.getMainArguments(conf);

        assertTrue(Arrays.equals(new String[] { "a", null, "b", null, "c"}, args));
    }

    @Test
    public void testArgsHandlingWhenArgsContainsOneNullAndNullsAllowed() {
        setupConf(Arrays.asList((String) null));
        setEnableNullArgsAllowed(true);

        String args[] = LauncherAMUtils.getMainArguments(conf);

        assertTrue(Arrays.equals(new String[] { null }, args));
    }

    private void setupConf(List<String> argList) {
        int argCount = argList.size();

        given(conf.getInt(eq(CONF_OOZIE_ACTION_MAIN_ARG_COUNT), eq(0))).willReturn(argCount);

        for (int i = 0; i < argCount; i++) {
            given(conf.get(eq(CONF_OOZIE_ACTION_MAIN_ARG_PREFIX + i))).willReturn(argList.get(i));
        }
    }

    private void setEnableNullArgsAllowed(boolean nullArgsAllowed) {
        given(conf.getBoolean(eq(LauncherAMUtils.CONF_OOZIE_NULL_ARGS_ALLOWED), anyBoolean())).willReturn(nullArgsAllowed);
    }
}
