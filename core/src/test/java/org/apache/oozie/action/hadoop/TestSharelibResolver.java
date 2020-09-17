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

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.util.XConfiguration;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class TestSharelibResolver {
    public final String defaultValue = "default";
    private final String serverConfigSharelib = "serverConfigSharelib";
    private final String globalConfigSharelib = "globalConfigSharelib";
    private final String globalLauncherSharelib = "globalLauncherSharelib";
    private final String localConfigSharelib = "localConfigSharelib";
    private final String localLauncherSharelib = "localLauncherSharelib";
    private final String sharelibProperty = "sharelibProperty";

    /**
     * Add sharelib values in reverse priority order and check that the last added value is the one that gets resolved.
     */
    @Test
    public void testResolvingOrder() {
        Configuration oozieServerConfiguration = new Configuration(false);
        XConfiguration workflowConf = new XConfiguration();
        Configuration actionConf = new Configuration(false);

        SharelibResolver resolver =
                new SharelibResolver(sharelibProperty, actionConf, workflowConf, oozieServerConfiguration, defaultValue);

        assertThat("Without setting anything we should've got the default value.",
                resolver.resolve(), is(new String[]{defaultValue}));

        oozieServerConfiguration.set(sharelibProperty, serverConfigSharelib);
        assertThat("Server-level sharelib configuration is not processed",
                resolver.resolve(), is(new String[]{serverConfigSharelib}));

        workflowConf.set(sharelibProperty, globalConfigSharelib);
        assertThat("Global workflow-level sharelib configuration is not processed",
                resolver.resolve(), is(new String[]{globalConfigSharelib}));

        workflowConf.set(LauncherAM.OOZIE_LAUNCHER_SHARELIB_PROPERTY, globalLauncherSharelib);
        assertThat("Global workflow-level sharelib configuration is not processed",
                resolver.resolve(), is(new String[]{globalLauncherSharelib}));

        actionConf.set(sharelibProperty, localConfigSharelib);
        assertThat("Local action-level sharelib configuration is not processed",
                resolver.resolve(), is(new String[]{localConfigSharelib}));

        actionConf.set(LauncherAM.OOZIE_LAUNCHER_SHARELIB_PROPERTY, localLauncherSharelib);
        assertThat("Local action-level sharelib configuration is not processed",
                resolver.resolve(), is(new String[]{localLauncherSharelib}));
    }

}
