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

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.util.XConfiguration;

import java.io.IOException;
import java.util.List;

class SharelibResolver {
    private final String[] defaultValue;
    private final List<SharelibNameProvider> configProviders;


    /**
     * Return the sharelib names for the action based on the given configurations.
     * <p>
     * If <code>NULL</code> or empty, it means that the action does not use the action
     * sharelib.
     * <p>
     * If a non-empty string, i.e. <code>foo</code>, it means the action uses the
     * action sharelib sub-directory <code>foo</code> and all JARs in the sharelib
     * <code>foo</code> directory will be in the action classpath. Multiple sharelib
     * sub-directories can be specified as a comma separated list.
     * <p>
     * The resolution is done using the following precedence order:
     * <ul>
     *     <li><b><sharelib></b> tag in the action's launcher configuration</li>
     *     <li><b>oozie.action.sharelib.for.#ACTIONTYPE#</b> in the action configuration</li>
     *     <li><b><sharelib></b> tag in the workflow's launcher configuration</li>
     *     <li><b>oozie.action.sharelib.for.#ACTIONTYPE#</b> in the workflow configuration</li>
     *     <li><b>oozie.action.sharelib.for.#ACTIONTYPE#</b> in the oozie configuration</li>
     *     <li>Action Executor <code>getDefaultShareLibName()</code> method</li>
     * </ul>
     *
     * @param sharelibPropertyName the property for the current sharelib. E.g. oozie.action.sharelib.for.java
     * @param actionConf the configuration of the current action
     * @param workflowConf the global configuration for the workflow
     * @param oozieServerConfiguration the Oozie server's configuration
     * @param defaultValue the default value to use if there is no sharelib definition in the configs
     */
    SharelibResolver(final String sharelibPropertyName, final Configuration actionConf,
                     final XConfiguration workflowConf, final Configuration oozieServerConfiguration,
                     final String defaultValue) {
        if (defaultValue == null) {
            this.defaultValue = new String[0];
        } else {
            this.defaultValue = new String[] { defaultValue };
        }
        configProviders = Lists.newArrayList(
                () -> actionConf.getStrings(LauncherAM.OOZIE_LAUNCHER_SHARELIB_PROPERTY),
                () -> actionConf.getStrings(sharelibPropertyName),
                () -> workflowConf.getStrings(LauncherAM.OOZIE_LAUNCHER_SHARELIB_PROPERTY),
                () -> workflowConf.getStrings(sharelibPropertyName),
                () -> oozieServerConfiguration.getStrings(sharelibPropertyName)
        );
    }

    /**
     * Return the sharelib names for the action.
     * @return the sharelib names
     */
    public String[] resolve() {
        for (SharelibNameProvider cp : configProviders) {
            if (isValidSharelibProperty(cp.getSharelibNames())) {
                return cp.getSharelibNames();
            }
        }
        return defaultValue;
    }

    private boolean isValidSharelibProperty(String[] value){
        return value != null && value.length > 0;
    }
}

interface SharelibNameProvider {
    String[] getSharelibNames();
}