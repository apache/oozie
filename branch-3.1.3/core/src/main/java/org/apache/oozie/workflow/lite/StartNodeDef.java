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
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.ErrorCode;

import java.util.ArrayList;
import java.util.List;

/**
 * Workflow lite start node definition.
 */
public class StartNodeDef extends NodeDef {

    /**
     * Reserved name fo the start node. <p/> It is an invalid token, it will never match an application node name.
     */
    public static final String START = "::start::";

    /**
     * Default constructor.
     */
    public StartNodeDef() {
    }

    /**
     * Create a start node definition.
     *
     * @param transitionTo transition on workflow start.
     */
    public StartNodeDef(String transitionTo) {
        super(START, null, StartNodeHandler.class, createList(ParamChecker.notEmpty(transitionTo, "transitionTo")));
    }

    private static List<String> createList(String transition) {
        List<String> list = new ArrayList<String>();
        list.add(transition);
        return list;
    }

    /**
     * Start node handler. <p/> It does an immediate transition to the transitionTo node.
     */
    public static class StartNodeHandler extends NodeHandler {

        public boolean enter(Context context) throws WorkflowException {
            if (!context.getSignalValue().equals(StartNodeDef.START)) {
                throw new WorkflowException(ErrorCode.E0715, context.getSignalValue());
            }
            return true;
        }

        public String exit(Context context) {
            return context.getNodeDef().getTransitions().get(0);
        }

        public void kill(Context context) {
        }

        public void fail(Context context) {
        }
    }

}
