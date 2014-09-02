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

package org.apache.oozie;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.action.hadoop.MapReduceActionExecutor;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.CallbackService;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.PropertiesUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XmlUtils;
import org.jdom.JDOMException;
import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;
import java.util.Map;

/**
 * DAG EL functions.
 */
public class DagELFunctions {

    public static final String HADOOP_JOBS_PREFIX = "hadoopJobs:";
    private static final String WORKFLOW = "oozie.el.workflow.bean";
    private static final String ACTION = "oozie.el.action.bean";
    private static final String ACTION_PROTO_CONF = "oozie.el.action.proto.conf";

    private static final String LAST_ACTION_IN_ERROR = "oozie.el.last.action.in.error";

    private static final String ACTION_DATA = "action.data";
    private static final String ACTION_ERROR_CODE = "action.error.code";
    private static final String ACTION_ERROR_MESSAGE = "action.error.message";
    private static final String ACTION_EXTERNAL_ID = "action.external.id";
    private static final String ACTION_TRACKER_URI = "action.tracker.uri";
    private static final String ACTION_EXTERNAL_STATUS = "action.external.status";

    public static void configureEvaluator(ELEvaluator evaluator, WorkflowJobBean workflow, WorkflowActionBean action) {
        evaluator.setVariable(WORKFLOW, workflow);
        evaluator.setVariable(ACTION, action);

        for (Map.Entry<String, String> entry : workflow.getWorkflowInstance().getConf()) {
            if (ParamChecker.isValidIdentifier(entry.getKey())) {
                String value = entry.getValue().trim();
                try {
                    String valueElem = "<value>"+value+"</value>";
                    XmlUtils.parseXml(valueElem);
                }
                catch (JDOMException ex) {
                    // If happens, try escaping the characters for XML. The escaping may or
                    // may not solve the problem since the JDOMException could be for a range of issues.
                    value = XmlUtils.escapeCharsForXML(value);
                }
                evaluator.setVariable(entry.getKey().trim(), value);
            }
        }
        try {
            evaluator.setVariable(ACTION_PROTO_CONF,
                                  new XConfiguration(new StringReader(workflow.getProtoActionConf())));
        }
        catch (IOException ex) {
            throw new RuntimeException("It should not happen", ex);
        }
    }

    public static WorkflowActionBean getAction() {
        ELEvaluator eval = ELEvaluator.getCurrent();
        return (WorkflowActionBean) eval.getVariable(ACTION);
    }

    public static WorkflowJobBean getWorkflow() {
        ELEvaluator eval = ELEvaluator.getCurrent();
        return (WorkflowJobBean) eval.getVariable(WORKFLOW);
    }

    public static Configuration getProtoActionConf() {
        ELEvaluator eval = ELEvaluator.getCurrent();
        return (Configuration) eval.getVariable(ACTION_PROTO_CONF);
    }

    public static void setActionInfo(WorkflowInstance workflowInstance, WorkflowAction action) {
        if (action.getExternalId() != null) {
            workflowInstance.setVar(action.getName() + WorkflowInstance.NODE_VAR_SEPARATOR + ACTION_EXTERNAL_ID,
                                    action.getExternalId());
        }
        if (action.getTrackerUri() != null) {
            workflowInstance.setVar(action.getName() + WorkflowInstance.NODE_VAR_SEPARATOR + ACTION_TRACKER_URI,
                                    action.getTrackerUri());
        }
        if (action.getExternalStatus() != null) {
            workflowInstance.setVar(action.getName() + WorkflowInstance.NODE_VAR_SEPARATOR + ACTION_EXTERNAL_STATUS,
                                    action.getExternalStatus());
        }
        if (action.getData() != null) {
            workflowInstance
                    .setVar(action.getName() + WorkflowInstance.NODE_VAR_SEPARATOR + ACTION_DATA, action.getData());
        }
        if (action.getExternalChildIDs() != null) {
            workflowInstance.setVar(action.getName() + WorkflowInstance.NODE_VAR_SEPARATOR + ACTION_DATA,
                    HADOOP_JOBS_PREFIX + action.getExternalChildIDs());
        }
        if (action.getStats() != null) {
            workflowInstance.setVar(action.getName() + WorkflowInstance.NODE_VAR_SEPARATOR + MapReduceActionExecutor.HADOOP_COUNTERS,
                    action.getStats());
        }
        if (action.getErrorCode() != null) {
            workflowInstance.setVar(action.getName() + WorkflowInstance.NODE_VAR_SEPARATOR + ACTION_ERROR_CODE,
                                    action.getErrorCode());
        }
        if (action.getErrorMessage() != null) {
            workflowInstance.setVar(action.getName() + WorkflowInstance.NODE_VAR_SEPARATOR + ACTION_ERROR_MESSAGE,
                                    action.getErrorMessage());
        }
        if (action.getStatus() == WorkflowAction.Status.ERROR) {
            workflowInstance.setVar(LAST_ACTION_IN_ERROR, action.getName());
        }
    }

    /**
     * Return the job Id.
     *
     * @return the job Id.
     */
    public static String wf_id() {
        return getWorkflow().getId();
    }

    /**
     * Return the application name.
     *
     * @return the application name.
     */
    public static String wf_name() {
        return getWorkflow().getAppName();
    }

    /**
     * Return the application path.
     *
     * @return the application path.
     */
    public static String wf_appPath() {
        return getWorkflow().getAppPath();
    }

    /**
     * Return a job configuration property.
     *
     * @param property property name.
     * @return the value of the property, <code>null</code> if the property is undefined.
     */
    public static String wf_conf(String property) {
        return getWorkflow().getWorkflowInstance().getConf().get(property);
    }

    /**
     * Return the job owner user name.
     *
     * @return the job owner user name.
     */
    public static String wf_user() {
        return getWorkflow().getUser();
    }

    /**
     * Return the job owner group name.
     *
     * @return the job owner group name.
     */
    public static String wf_group() {
        return getWorkflow().getGroup();
    }

    /**
     * Create a callback URL for the current action.
     *
     * @param externalStatusVar variable for the caller to inject the external status.
     * @return the callback URL for the current action.
     */
    public static String wf_callback(String externalStatusVar) {
        return Services.get().get(CallbackService.class).createCallBackUrl(getAction().getId(), externalStatusVar);
    }

    /**
     * Return the transition taken by a workflow job action/decision action.
     *
     * @param actionName action/decision action name.
     * @return the transition taken, <code>null</code> if the action has not completed yet.
     */
    public static String wf_transition(String actionName) {
        return getWorkflow().getWorkflowInstance().getTransition(actionName);
    }

    /**
     * Return the name of the last action that ended in error.
     *
     * @return the name of the last action that ended in error, <code>null</code> if no action in the workflow job has
     *         ended in error.
     */
    public static String wf_lastErrorNode() {
        return getWorkflow().getWorkflowInstance().getVar(LAST_ACTION_IN_ERROR);
    }

    /**
     * Return the error code for an action.
     *
     * @param actionName action name.
     * @return the error code for the action, <code>null</code> if the action has not ended in error.
     */
    public static String wf_errorCode(String actionName) {
        return getWorkflow().getWorkflowInstance()
                .getVar(actionName + WorkflowInstance.NODE_VAR_SEPARATOR + ACTION_ERROR_CODE);
    }

    /**
     * Return the error message for an action.
     *
     * @param actionName action name.
     * @return the error message for the action, <code>null</code> if the action has not ended in error.
     */
    public static String wf_errorMessage(String actionName) {
        return getWorkflow().getWorkflowInstance()
                .getVar(actionName + WorkflowInstance.NODE_VAR_SEPARATOR + ACTION_ERROR_MESSAGE);
    }

    /**
     * Return the workflow run number, unless a rerun it is always 1.
     *
     * @return the workflow run number, unless a rerun it is always 1.
     */
    public static int wf_run() {
        return getWorkflow().getRun();
    }

    /**
     * Return the action data for an action.
     *
     * @param actionName action name.
     * @return value of the property.
     */
    @SuppressWarnings("unchecked")
    public static Map<String, String> wf_actionData(String actionName) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        Properties props = (Properties) eval.getVariable(actionName + ACTION_ERROR_MESSAGE);

        if (props == null) {
            String data = getWorkflow().getWorkflowInstance()
                    .getVar(actionName + WorkflowInstance.NODE_VAR_SEPARATOR + ACTION_DATA);
            if (data != null) {
                props = PropertiesUtils.stringToProperties(data);
            }
            else {
                props = new Properties();
            }
            eval.setVariable(actionName + ACTION_ERROR_MESSAGE, props);
        }
        return (Map<String, String>) (Map) props;
    }

    /**
     * Return the external ID of an action.
     *
     * @param actionName action name.
     * @return the external ID of an action.
     */
    public static String wf_actionExternalId(String actionName) {
        return getWorkflow().getWorkflowInstance()
                .getVar(actionName + WorkflowInstance.NODE_VAR_SEPARATOR + ACTION_EXTERNAL_ID);
    }

    /**
     * Return the tracker URI of an action.
     *
     * @param actionName action name.
     * @return the tracker URI of an action.
     */
    public static String wf_actionTrackerUri(String actionName) {
        return getWorkflow().getWorkflowInstance()
                .getVar(actionName + WorkflowInstance.NODE_VAR_SEPARATOR + ACTION_TRACKER_URI);
    }

    /**
     * Return the action external status.
     *
     * @param actionName action/decision action name.
     * @return the action external status.
     */
    public static String wf_actionExternalStatus(String actionName) {
        return getWorkflow().getWorkflowInstance()
                .getVar(actionName + WorkflowInstance.NODE_VAR_SEPARATOR + ACTION_EXTERNAL_STATUS);
    }

    public static String getActionVar(String actionName, String varName) {
        return getWorkflow().getWorkflowInstance().getVar(actionName + WorkflowInstance.NODE_VAR_SEPARATOR + varName);
    }

}
