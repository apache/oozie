/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.service;

import org.apache.oozie.command.wf.ReRunCommand;

import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.lite.ActionNodeHandler;
import org.apache.oozie.workflow.lite.DecisionNodeHandler;
import org.apache.oozie.workflow.lite.NodeHandler;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.JDOMException;

import java.util.ArrayList;
import java.util.List;

public abstract class LiteWorkflowStoreService extends WorkflowStoreService {

    /**
     * Delegation method used by the Action and Decision {@link NodeHandler} on start. <p/> This method provides the
     * necessary information to create ActionExecutors.
     *
     * @param context NodeHandler context.
     * @throws WorkflowException thrown if there was an error parsing the action configuration.
     */
    @SuppressWarnings("unchecked")
    protected static void liteExecute(NodeHandler.Context context) throws WorkflowException {
        XLog log = XLog.getLog(LiteWorkflowStoreService.class);
        String jobId = context.getProcessInstance().getId();
        String nodeName = context.getNodeDef().getName();
        String skipVar = context.getProcessInstance().getVar(context.getNodeDef().getName()
                + WorkflowInstance.NODE_VAR_SEPARATOR + ReRunCommand.TO_SKIP);
        boolean skipAction = false;
        if (skipVar != null) {
            skipAction = skipVar.equals("true");
        }
        WorkflowActionBean action = new WorkflowActionBean();
        String actionId = Services.get().get(UUIDService.class).generateChildId(jobId, nodeName);
        if (!skipAction) {
            String nodeConf = context.getNodeDef().getConf();
            String executionPath = context.getExecutionPath();

            String actionType;
            try {
                Element element = XmlUtils.parseXml(nodeConf);
                actionType = element.getName();
                nodeConf = XmlUtils.prettyPrint(element).toString();
            }
            catch (JDOMException ex) {
                throw new WorkflowException(ErrorCode.E0700, ex.getMessage(), ex);
            }

            log.debug(" Creating action for node [{0}]", nodeName);
            action.setType(actionType);
            action.setExecutionPath(executionPath);
            action.setConf(nodeConf);
            action.setLogToken(((WorkflowJobBean) context.getTransientVar(WORKFLOW_BEAN)).getLogToken());
            action.setStatus(WorkflowAction.Status.PREP);
            action.setJobId(jobId);
        }
        action.setName(nodeName);
        action.setId(actionId);
        context.setVar(nodeName + WorkflowInstance.NODE_VAR_SEPARATOR + ACTION_ID, actionId);
        List list = (List) context.getTransientVar(ACTIONS_TO_START);
        if (list == null) {
            list = new ArrayList();
            context.setTransientVar(ACTIONS_TO_START, list);
        }
        list.add(action);
    }

    /**
     * Delegation method used when failing actions. <p/>
     *
     * @param context NodeHandler context.
     */
    @SuppressWarnings("unchecked")
    protected static void liteFail(NodeHandler.Context context) {
        liteTerminate(context, ACTIONS_TO_FAIL);
    }

    /**
     * Delegation method used when killing actions. <p/>
     *
     * @param context NodeHandler context.
     */
    @SuppressWarnings("unchecked")
    protected static void liteKill(NodeHandler.Context context) {
        liteTerminate(context, ACTIONS_TO_KILL);
    }

    /**
     * Used to terminate jobs - FAIL or KILL. <p/>
     *
     * @param context NodeHandler context.
     * @param transientVar The transient variable name.
     */
    @SuppressWarnings("unchecked")
    private static void liteTerminate(NodeHandler.Context context, String transientVar) {
        List<String> list = (List<String>) context.getTransientVar(transientVar);
        if (list == null) {
            list = new ArrayList<String>();
            context.setTransientVar(transientVar, list);
        }
        list.add(context.getVar(context.getNodeDef().getName() + WorkflowInstance.NODE_VAR_SEPARATOR + ACTION_ID));
    }

    // wires workflow lib action execution with Oozie Dag
    public static class LiteActionHandler extends ActionNodeHandler {

        @Override
        public void start(Context context) throws WorkflowException {
            liteExecute(context);
        }

        public void end(Context context) {
        }

        public void kill(Context context) {
            liteKill(context);
        }

        public void fail(Context context) {
            liteFail(context);
        }
    }

    // wires workflow lib decision execution with Oozie Dag
    public static class LiteDecisionHandler extends DecisionNodeHandler {

        @Override
        public void start(Context context) throws WorkflowException {
            liteExecute(context);
        }

        public void end(Context context) {
        }

        public void kill(Context context) {
            liteKill(context);
        }

        public void fail(Context context) {
            liteFail(context);
        }
    }
}
