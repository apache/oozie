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
package org.apache.oozie.action.control;

import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.action.hadoop.FsActionExecutor;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.workflow.lite.ControlNodeHandler;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;

import java.util.List;

/**
 * Base action executor for control nodes: START/END/KILL/FORK/JOIN
 * <p/>
 * This action executor, similar to {@link FsActionExecutor}, is completed during the
 * {@link #start(Context, WorkflowAction)}.
 * <p/>
 * By hooking control nodes to an action executor, control nodes get WF action entries in the DB.
 */
public abstract class ControlNodeActionExecutor extends ActionExecutor {

    public ControlNodeActionExecutor(String type) {
        super(type);
    }

    @SuppressWarnings("unchecked")
    public void start(Context context, WorkflowAction action) throws ActionExecutorException {
        context.setStartData("-", "-", "-");
        context.setExecutionData("OK", null);
    }

    public void end(Context context, WorkflowAction action) throws ActionExecutorException {
        context.setEndData(WorkflowAction.Status.OK, getActionSignal(WorkflowAction.Status.OK));
    }

    public void check(Context context, WorkflowAction action) throws ActionExecutorException {
        throw new UnsupportedOperationException();
    }

    public void kill(Context context, WorkflowAction action) throws ActionExecutorException {
        throw new UnsupportedOperationException();
    }

    public boolean isCompleted(String externalStatus) {
        return true;
    }

}
