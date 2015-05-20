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
import org.apache.hadoop.util.StringUtils;
import org.apache.oozie.action.control.EndActionExecutor;
import org.apache.oozie.action.control.ForkActionExecutor;
import org.apache.oozie.action.control.JoinActionExecutor;
import org.apache.oozie.action.control.KillActionExecutor;
import org.apache.oozie.action.control.StartActionExecutor;
import org.apache.oozie.command.wf.ReRunXCommand;

import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.lite.ActionNodeHandler;
import org.apache.oozie.workflow.lite.ControlNodeHandler;
import org.apache.oozie.workflow.lite.DecisionNodeHandler;
import org.apache.oozie.workflow.lite.EndNodeDef;
import org.apache.oozie.workflow.lite.ForkNodeDef;
import org.apache.oozie.workflow.lite.JoinNodeDef;
import org.apache.oozie.workflow.lite.KillNodeDef;
import org.apache.oozie.workflow.lite.NodeDef;
import org.apache.oozie.workflow.lite.NodeHandler;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.workflow.lite.StartNodeDef;
import org.jdom.Element;
import org.jdom.JDOMException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class LiteWorkflowStoreService extends WorkflowStoreService {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "LiteWorkflowStoreService.";
    public static final String CONF_PREFIX_USER_RETRY = CONF_PREFIX + "user.retry.";
    public static final String CONF_USER_RETRY_MAX = CONF_PREFIX_USER_RETRY + "max";
    public static final String CONF_USER_RETRY_INTEVAL = CONF_PREFIX_USER_RETRY + "inteval";
    public static final String CONF_USER_RETRY_ERROR_CODE = CONF_PREFIX_USER_RETRY + "error.code";
    public static final String CONF_USER_RETRY_ERROR_CODE_EXT = CONF_PREFIX_USER_RETRY + "error.code.ext";

    public static final String NODE_DEF_VERSION_0 = "_oozie_inst_v_0";
    public static final String NODE_DEF_VERSION_1 = "_oozie_inst_v_1";
    public static final String CONF_NODE_DEF_VERSION = CONF_PREFIX + "node.def.version";

    public static final String USER_ERROR_CODE_ALL = "ALL";

    /**
     * Delegation method used by the Action and Decision {@link NodeHandler} on start. <p/> This method provides the
     * necessary information to create ActionExecutors.
     *
     * @param context NodeHandler context.
     * @param actionType the action type.
     * @throws WorkflowException thrown if there was an error parsing the action configuration.
     */
    @SuppressWarnings("unchecked")
    protected static void liteExecute(NodeHandler.Context context, String actionType) throws WorkflowException {
        XLog log = XLog.getLog(LiteWorkflowStoreService.class);
        String jobId = context.getProcessInstance().getId();
        String nodeName = context.getNodeDef().getName();
        String skipVar = context.getProcessInstance().getVar(context.getNodeDef().getName()
                + WorkflowInstance.NODE_VAR_SEPARATOR + ReRunXCommand.TO_SKIP);
        boolean skipAction = false;
        if (skipVar != null) {
            skipAction = skipVar.equals("true");
        }
        WorkflowActionBean action = new WorkflowActionBean();
        String actionId = Services.get().get(UUIDService.class).generateChildId(jobId, nodeName);

        if (!skipAction) {
            String nodeConf = context.getNodeDef().getConf();

            if (actionType == null) {
                try {
                    Element element = XmlUtils.parseXml(nodeConf);
                    actionType = element.getName();
                    nodeConf = XmlUtils.prettyPrint(element).toString();
                }
                catch (JDOMException ex) {
                    throw new WorkflowException(ErrorCode.E0700, ex.getMessage(), ex);
                }
            }
            log.debug(" Creating action for node [{0}]", nodeName);
            action.setType(actionType);
            action.setConf(nodeConf);
            action.setLogToken(((WorkflowJobBean) context.getTransientVar(WORKFLOW_BEAN)).getLogToken());
            action.setStatus(WorkflowAction.Status.PREP);
            action.setJobId(jobId);
        }

        String executionPath = context.getExecutionPath();
        action.setExecutionPath(executionPath);
        action.setCred(context.getNodeDef().getCred());
        log.debug("Setting action for cred: '"+context.getNodeDef().getCred() +
        		"', name: '"+ context.getNodeDef().getName() + "'");

        action.setUserRetryCount(0);
        int userRetryMax = getUserRetryMax(context);
        int userRetryInterval = getUserRetryInterval(context);
        action.setUserRetryMax(userRetryMax);
        action.setUserRetryInterval(userRetryInterval);
        log.debug("Setting action for userRetryMax: '"+ userRetryMax +
        		"', userRetryInterval: '" + userRetryInterval +
        		"', name: '"+ context.getNodeDef().getName() + "'");

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

    private static int getUserRetryInterval(NodeHandler.Context context) throws WorkflowException {
        int ret = ConfigurationService.getInt(CONF_USER_RETRY_INTEVAL);
        String userRetryInterval = context.getNodeDef().getUserRetryInterval();

        if (!userRetryInterval.equals("null")) {
            try {
                ret = Integer.parseInt(userRetryInterval);
            }
            catch (NumberFormatException nfe) {
                throw new WorkflowException(ErrorCode.E0700, nfe.getMessage(), nfe);
            }
        }
        return ret;
    }

    private static int getUserRetryMax(NodeHandler.Context context) throws WorkflowException {
        XLog log = XLog.getLog(LiteWorkflowStoreService.class);
        int ret = ConfigurationService.getInt(CONF_USER_RETRY_MAX);
        int max = ret;
        String userRetryMax = context.getNodeDef().getUserRetryMax();

        if (!userRetryMax.equals("null")) {
            try {
                ret = Integer.parseInt(userRetryMax);
                if (ret > max) {
                    ret = max;
                    log.warn(ErrorCode.E0820.getTemplate(), ret, max);
                }
            }
            catch (NumberFormatException nfe) {
                throw new WorkflowException(ErrorCode.E0700, nfe.getMessage(), nfe);
            }
        }
        else {
            ret = 0;
        }
        return ret;
    }

    /**
     * Get system defined and instance defined error codes for which USER_RETRY is allowed
     *
     * @return set of error code user-retry is allowed for
     */
    public static Set<String> getUserRetryErrorCode() {
        // eliminating whitespaces in the error codes value specification
        String errorCodeString = ConfigurationService.get(CONF_USER_RETRY_ERROR_CODE).replaceAll("\\s+", "");
        Collection<String> strings = StringUtils.getStringCollection(errorCodeString);
        String errorCodeExtString = ConfigurationService.get(CONF_USER_RETRY_ERROR_CODE_EXT).replaceAll("\\s+", "");
        Collection<String> extra = StringUtils.getStringCollection(errorCodeExtString);
        Set<String> set = new HashSet<String>();
        set.addAll(strings);
        set.addAll(extra);
        return set;
    }

    /**
     * Get NodeDef default version, _oozie_inst_v_0 or _oozie_inst_v_1
     *
     * @return nodedef default version
     * @throws WorkflowException thrown if there was an error parsing the action configuration.
    */
    public static String getNodeDefDefaultVersion() throws WorkflowException {
        String ret = ConfigurationService.get(CONF_NODE_DEF_VERSION);
        if (ret == null) {
            ret = NODE_DEF_VERSION_1;
        }
        return ret;
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
            liteExecute(context, null);
        }

        @Override
        public void end(Context context) {
        }

        @Override
        public void kill(Context context) {
            liteKill(context);
        }

        @Override
        public void fail(Context context) {
            liteFail(context);
        }
    }

    // wires workflow lib decision execution with Oozie Dag
    public static class LiteDecisionHandler extends DecisionNodeHandler {

        @Override
        public void start(Context context) throws WorkflowException {
            liteExecute(context, null);
        }

        @Override
        public void end(Context context) {
        }

        @Override
        public void kill(Context context) {
            liteKill(context);
        }

        @Override
        public void fail(Context context) {
            liteFail(context);
        }
    }

    // wires workflow lib control nodes with Oozie Dag
    public static class LiteControlNodeHandler extends ControlNodeHandler {

      @Override
      public void touch(Context context) throws WorkflowException {
          Class<? extends NodeDef> nodeClass = context.getNodeDef().getClass();
          String nodeType;
          if (nodeClass.equals(StartNodeDef.class)) {
            nodeType = StartActionExecutor.TYPE;
          }
          else if (nodeClass.equals(EndNodeDef.class)) {
              nodeType = EndActionExecutor.TYPE;
          }
          else if (nodeClass.equals(KillNodeDef.class)) {
              nodeType = KillActionExecutor.TYPE;
          }
          else if (nodeClass.equals(ForkNodeDef.class)) {
              nodeType = ForkActionExecutor.TYPE;
          }
          else if (nodeClass.equals(JoinNodeDef.class)) {
              nodeType = JoinActionExecutor.TYPE;
          } else {
            throw new IllegalStateException("Invalid node type: " + nodeClass);
          }

          liteExecute(context, nodeType);
      }

    }
}
