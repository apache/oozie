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
package org.apache.oozie.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.workflow.WorkflowApp;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.WorkflowLib;
import org.apache.oozie.util.ParamChecker;

/**
 * Service that provides workflow application definition reading, parsing and creating proto configuration.
 */
public class LiteWorkflowAppService extends WorkflowAppService {
    /**
     * Parse workflow definition.
     *
     * @param jobConf workflow job configuration.
     * @param authToken authorization token.
     * @return workflow application.
     */
    public WorkflowApp parseDef(Configuration jobConf, String authToken) throws WorkflowException {
        String appPath = ParamChecker.notEmpty(jobConf.get(OozieClient.APP_PATH), OozieClient.APP_PATH);
        String user = ParamChecker.notEmpty(jobConf.get(OozieClient.USER_NAME), OozieClient.USER_NAME);
        String workflowXml = readDefinition(appPath, user, authToken, jobConf);
        return parseDef(workflowXml);
    }

    public WorkflowApp parseDef(String workflowXml) throws WorkflowException {
        WorkflowLib workflowLib = Services.get().get(WorkflowStoreService.class).getWorkflowLibWithNoDB();
        return workflowLib.parseDef(workflowXml);
    }
}
