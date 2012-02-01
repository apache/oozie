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

import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;

public class ForTestingActionExecutor extends ActionExecutor {
    public final static String TEST_ERROR = "TEST_ERROR";

    protected ForTestingActionExecutor() {
        super("test");
    }

    public void initActionType() {
    }

    private Element getConfiguration(String strConf) throws ActionExecutorException {
        try {
            return XmlUtils.parseXml(strConf);
        }
        catch (JDOMException ex) {
            throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, TEST_ERROR, ex.getMessage(), ex);
        }
    }

    public void start(Context context, WorkflowAction action) throws ActionExecutorException {
        Element eConf = getConfiguration(action.getConf());
        Namespace ns = eConf.getNamespace();
        String error = eConf.getChild("error", ns).getText().trim();

        if ("start.transient".equals(error)) {
            throw new ActionExecutorException(ActionExecutorException.ErrorType.TRANSIENT, TEST_ERROR, "start");
        }
        if ("start.non-transient".equals(error)) {
            throw new ActionExecutorException(ActionExecutorException.ErrorType.NON_TRANSIENT, TEST_ERROR, "start");
        }
        if ("start.error".equals(error)) {
            throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, TEST_ERROR, "start");
        }
        String externalStatus = eConf.getChild("external-status", ns).getText().trim();

        String runningMode = "sync";
        Element runningModeElement = eConf.getChild("running-mode", ns);
        if (null != runningModeElement) {
            if (runningModeElement.getText().trim().equals("async")) {
                runningMode = "async";
            }
        }
        if (runningMode.equals("async")) {
            context.setStartData("blah", "blah", "blah");
            return;
        }

        boolean callSetExecutionData = true;
        Element setStartData = eConf.getChild("avoid-set-execution-data", ns);
        if (null != setStartData) {
            if (setStartData.getText().trim().equals("true")) {
                callSetExecutionData = false;
            }
        }
        if (callSetExecutionData) {
            context.setExecutionData(externalStatus, null);
        }
    }

    public void end(Context context, WorkflowAction action) throws ActionExecutorException {
        Element eConf = getConfiguration(action.getConf());
        Namespace ns = eConf.getNamespace();
        String error = eConf.getChild("error", ns).getText().trim();
        if ("end.transient".equals(error)) {
            throw new ActionExecutorException(ActionExecutorException.ErrorType.TRANSIENT, TEST_ERROR, "end");
        }
        if ("end.non-transient".equals(error)) {
            throw new ActionExecutorException(ActionExecutorException.ErrorType.NON_TRANSIENT, TEST_ERROR, "end");
        }
        if ("end.error".equals(error)) {
            throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, TEST_ERROR, "end");
        }
        String signalValue = eConf.getChild("signal-value", ns).getText().trim();
        String externalStatus = action.getExternalStatus();
        WorkflowAction.Status status = null;
        if (externalStatus.equals("ok")) {
            status = WorkflowAction.Status.OK;
        }
        else {
            status = WorkflowAction.Status.ERROR;
        }
        if (signalValue.equals("based_on_action_status")) {
            signalValue = status.toString();
        }

        boolean callSetEndData = true;
        Element setEndData = eConf.getChild("avoid-set-end-data", ns);
        if (null != setEndData) {
            if (setEndData.getText().trim().equals("true")) {
                callSetEndData = false;
            }
        }
        if (callSetEndData) {
            context.setEndData(status, signalValue);
        }
    }

    public void check(Context context, WorkflowAction action) throws ActionExecutorException {
        context.setExecutionData("ok", null);
    }

    public void kill(Context context, WorkflowAction action) throws ActionExecutorException {
    }

    public boolean isCompleted(String externalStatus) {
        return false;
    }

}
