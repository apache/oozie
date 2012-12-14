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
import java.util.List;

/**
 * Node definition for FORK control node.
 */
public class ForkNodeDef extends ControlNodeDef {

    ForkNodeDef() {
    }

    public ForkNodeDef(String name, Class<? extends ControlNodeHandler> klass,
                       List<String> transitions) {
        super(name, "", klass, transitions);
    }

    public static class ForkNodeHandler extends NodeHandler {

        public boolean enter(Context context) {
            return true;
        }

        // the return list contains (parentExecutionPath/transition#transition)+
        public List<String> multiExit(Context context) {
            List<String> transitions = context.getNodeDef().getTransitions();
            context.setVar(ControlNodeHandler.FORK_COUNT_PREFIX + context.getExecutionPath(), "" + transitions.size());

            List<String> fullTransitions = new ArrayList<String>(transitions.size());

            for (String transition : transitions) {
                String childExecutionPath = context.createExecutionPath(transition);
                String fullTransition = context.createFullTransition(childExecutionPath, transition);
                fullTransitions.add(fullTransition);
            }
            return fullTransitions;
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
