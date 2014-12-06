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
import org.apache.oozie.ErrorCode;

import java.util.List;

//TODO javadoc
public abstract class ActionNodeHandler extends NodeHandler {
    public static final String OK = "OK";
    public static final String ERROR = "ERROR";

    @Override
    public final boolean enter(Context context) throws WorkflowException {
        start(context);
        return false;
    }

    @Override
    public final String exit(Context context) throws WorkflowException {
        end(context);
        List<String> transitions = context.getNodeDef().getTransitions();
        String signalValue = context.getSignalValue();
        if (OK.equals(signalValue)) {
            return transitions.get(0);
        }
        else {
            if (ERROR.equals(signalValue)) {
                return transitions.get(1);
            }
        }
        throw new WorkflowException(ErrorCode.E0722, context.getNodeDef().getName());
    }

    public abstract void start(Context context) throws WorkflowException;

    public abstract void end(Context context) throws WorkflowException;

}
