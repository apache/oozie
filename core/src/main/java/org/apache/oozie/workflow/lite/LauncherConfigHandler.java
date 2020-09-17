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
package org.apache.oozie.workflow.lite;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.action.hadoop.JavaActionExecutor;
import org.apache.oozie.action.hadoop.LauncherAM;
import org.jdom.Element;
import org.jdom.Namespace;

class LauncherConfigHandler {
    private static final String LAUNCHER_MEMORY_MB = "memory.mb";
    private static final String LAUNCHER_VCORES = "vcores";
    private static final String LAUNCHER_PRIORITY = "priority";
    private static final String LAUNCHER_JAVAOPTS = "java-opts";
    private static final String LAUNCHER_ENV = "env";
    private static final String LAUNCHER_SHARELIB = "sharelib";
    private static final String LAUNCHER_QUEUE = "queue";
    private static final String LAUNCHER_VIEW_ACL = "view-acl";
    private static final String LAUNCHER_MODIFY_ACL = "modify-acl";

    private final Configuration entries;
    private final Element xmlLauncherElement;
    private final Namespace ns;

    public LauncherConfigHandler(Configuration entries, Element globalLauncherElement, Namespace ns) {
        this.entries = entries;
        this.xmlLauncherElement = globalLauncherElement;
        this.ns = ns;
    }

    private void setStringCfgSetting(String xmlTag, String configKey) {
        Element launcherSetting = xmlLauncherElement.getChild(xmlTag, ns);
        if (launcherSetting != null) {
            entries.set(configKey, launcherSetting.getText());
        }
    }

    private void setIntCfgSetting(String xmlTag, String configKey) {
        Element launcherSetting = xmlLauncherElement.getChild(xmlTag, ns);
        if (launcherSetting != null) {
            entries.setInt(configKey, Integer.parseInt(launcherSetting.getText()));
        }
    }

    public void processSettings() {
        setIntCfgSetting(LAUNCHER_MEMORY_MB, LauncherAM.OOZIE_LAUNCHER_MEMORY_MB_PROPERTY);
        setIntCfgSetting(LAUNCHER_VCORES, LauncherAM.OOZIE_LAUNCHER_VCORES_PROPERTY);
        setIntCfgSetting(LAUNCHER_PRIORITY, LauncherAM.OOZIE_LAUNCHER_PRIORITY_PROPERTY);
        setStringCfgSetting(LAUNCHER_JAVAOPTS, LauncherAM.OOZIE_LAUNCHER_JAVAOPTS_PROPERTY);
        setStringCfgSetting(LAUNCHER_ENV, LauncherAM.OOZIE_LAUNCHER_ENV_PROPERTY);
        setStringCfgSetting(LAUNCHER_QUEUE, LauncherAM.OOZIE_LAUNCHER_QUEUE_PROPERTY);
        setStringCfgSetting(LAUNCHER_SHARELIB, LauncherAM.OOZIE_LAUNCHER_SHARELIB_PROPERTY);
        setStringCfgSetting(LAUNCHER_VIEW_ACL, JavaActionExecutor.LAUNCER_VIEW_ACL);
        setStringCfgSetting(LAUNCHER_MODIFY_ACL, JavaActionExecutor.LAUNCER_MODIFY_ACL);
    }
}
