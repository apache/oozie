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


import org.apache.oozie.workflow.WorkflowApp;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.WorkflowLib;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.service.UUIDService.ApplicationType;
import org.apache.oozie.util.ParamChecker;
import org.apache.hadoop.conf.Configuration;

import javax.xml.validation.Schema;
import java.io.StringReader;

//TODO javadoc
public abstract class LiteWorkflowLib implements WorkflowLib {
    private Schema schema;
    private Class<? extends ControlNodeHandler> controlHandlerClass;
    private Class<? extends DecisionNodeHandler> decisionHandlerClass;
    private Class<? extends ActionNodeHandler> actionHandlerClass;

    public LiteWorkflowLib(Schema schema,
                           Class<? extends ControlNodeHandler> controlNodeHandler,
                           Class<? extends DecisionNodeHandler> decisionHandlerClass,
                           Class<? extends ActionNodeHandler> actionHandlerClass) {
        this.schema = schema;
        this.controlHandlerClass = controlNodeHandler;
        this.decisionHandlerClass = decisionHandlerClass;
        this.actionHandlerClass = actionHandlerClass;
    }

    @Override
    public WorkflowApp parseDef(String appXml, Configuration jobConf) throws WorkflowException {
        ParamChecker.notEmpty(appXml, "appXml");
        return new LiteWorkflowAppParser(schema, controlHandlerClass, decisionHandlerClass, actionHandlerClass)
                .validateAndParse(new StringReader(appXml), jobConf);
    }

    @Override
    public WorkflowInstance createInstance(WorkflowApp app, Configuration conf) throws WorkflowException {
        ParamChecker.notNull(app, "app");
        String jobId = Services.get().get(UUIDService.class).generateId(ApplicationType.WORKFLOW);
        return new LiteWorkflowInstance((LiteWorkflowApp) app, conf, jobId);
    }

    @Override
    public WorkflowInstance createInstance(WorkflowApp app, Configuration conf, String wfId) throws WorkflowException {
        ParamChecker.notNull(app, "app");
        ParamChecker.notNull(wfId, "wfId");
        return new LiteWorkflowInstance((LiteWorkflowApp) app, conf, wfId);
    }
}
