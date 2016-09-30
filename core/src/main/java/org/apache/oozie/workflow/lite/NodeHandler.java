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

import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.ErrorCode;

import java.util.ArrayList;
import java.util.List;

//TODO javadoc
public abstract class NodeHandler {

    public interface Context {

        NodeDef getNodeDef();

        String getExecutionPath();

        String getParentExecutionPath(String executionPath);

        String getSignalValue();

        void setVar(String name, String value);

        String getVar(String name);

        void setTransientVar(String name, Object value);

        Object getTransientVar(String name);

        String createExecutionPath(String name);

        //can be called only from exit(), creation of execPaths is automatic
        //when a handler returns more than one transition.
        void deleteExecutionPath();

        //must be used by multiExit
        String createFullTransition(String executionPath, String transition);

        void killJob();

        void completeJob();

        LiteWorkflowInstance getProcessInstance();
    }

    private static final String VISITED = "visited";

    public static String getLoopFlag(String nodeName) {
        return nodeName + WorkflowInstance.NODE_VAR_SEPARATOR + VISITED;
    }

    public void loopDetection(Context context) throws WorkflowException {
        String flag = getLoopFlag(context.getNodeDef().getName());
        if (context.getVar(flag) != null) {
            throw new WorkflowException(ErrorCode.E0709, context.getNodeDef().getName());
        }
        context.setVar(flag, "true");
    }

    // TRUE means immediate exit, false means has to be signal
    public abstract boolean enter(Context context) throws WorkflowException;

    // the return list contains executionPath#transition, important for fork
    public List<String> multiExit(Context context) throws WorkflowException {
        List<String> transitions = new ArrayList<String>(1);
        String transition = exit(context);
        if (transition != null) {
            transitions.add(context.createFullTransition(context.getExecutionPath(), transition));
        }
        return transitions;
    }


    public abstract String exit(Context context) throws WorkflowException;

    public void kill(Context context) {
    }

    public void fail(Context context) {
    }
}
