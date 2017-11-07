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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.oozie.util.XLog;

class YarnACLHandler {
    private XLog LOG = XLog.getLog(getClass());
    private final Configuration launcherConf;

    public YarnACLHandler(Configuration launcherConf) {
        this.launcherConf = launcherConf;
    }

    public void setACLs(ContainerLaunchContext containerLaunchContext) {
        boolean mrAclsEnabled = launcherConf.getBoolean(MRConfig.MR_ACLS_ENABLED, true);

        // This is checked for backward compatibility
        if (mrAclsEnabled) {
            Map<ApplicationAccessType, String> aclDefinition = new HashMap<>();

            String viewAcl = launcherConf.get(JavaActionExecutor.LAUNCER_VIEW_ACL);
            if (viewAcl != null) {
                aclDefinition.put(ApplicationAccessType.VIEW_APP, viewAcl);
            }

            String modifyAcl = launcherConf.get(JavaActionExecutor.LAUNCER_MODIFY_ACL);
            if (modifyAcl != null) {
                aclDefinition.put(ApplicationAccessType.MODIFY_APP, modifyAcl);
            }

            if (!aclDefinition.isEmpty()) {
                containerLaunchContext.setApplicationACLs(aclDefinition);
            }
        } else {
            LOG.info("Not setting ACLs because {0} is set to false", MRConfig.MR_ACLS_ENABLED);
        }
    }
}
