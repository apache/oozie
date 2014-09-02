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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.workflow.WorkflowException;

/**
 * Node definition for JOIN control node.
 */
public class JoinNodeDef extends ControlNodeDef {

    JoinNodeDef() {
    }

    public JoinNodeDef(String name, Class<? extends ControlNodeHandler> klass, String transition) {
        super(name, "", klass, Arrays.asList(transition));
    }

    public static class JoinNodeHandler extends NodeHandler {

        public void loopDetection(Context context) throws WorkflowException {
            String flag = getLoopFlag(context.getNodeDef().getName());
            if (context.getVar(flag) != null) {
                throw new WorkflowException(ErrorCode.E0709, context.getNodeDef().getName());
            }
            String parentExecutionPath = context.getParentExecutionPath(context.getExecutionPath());
            String forkCount = context.getVar(ControlNodeHandler.FORK_COUNT_PREFIX + parentExecutionPath);
            if (forkCount == null) {
                throw new WorkflowException(ErrorCode.E0720, context.getNodeDef().getName());
            }
            int count = Integer.parseInt(forkCount) - 1;
            if (count == 0) {
                context.setVar(flag, "true");
            }
        }

        public boolean enter(Context context) throws WorkflowException {
            String parentExecutionPath = context.getParentExecutionPath(context.getExecutionPath());
            String forkCount = context.getVar(ControlNodeHandler.FORK_COUNT_PREFIX + parentExecutionPath);
            if (forkCount == null) {
                throw new WorkflowException(ErrorCode.E0720, context.getNodeDef().getName());
            }
            int count = Integer.parseInt(forkCount) - 1;
            if (count > 0) {
                context.setVar(ControlNodeHandler.FORK_COUNT_PREFIX + parentExecutionPath, "" + count);
                context.deleteExecutionPath();
            }
            else {
                context.setVar(ControlNodeHandler.FORK_COUNT_PREFIX + parentExecutionPath, null);
            }
            return (count == 0);
        }

        public List<String> multiExit(Context context) {
            String parentExecutionPath = context.getParentExecutionPath(context.getExecutionPath());
            // NOW we delete..
            context.deleteExecutionPath();

            String transition = context.getNodeDef().getTransitions().get(0);
            String fullTransition = context.createFullTransition(parentExecutionPath, transition);
            List<String> transitions = new ArrayList<String>(1);
            transitions.add(fullTransition);
            return transitions;
        }

        public String exit(Context context) {
            throw new UnsupportedOperationException();
        }

        public void kill(Context context) {
        }

        public void fail(Context context) {
        }
    }
}
