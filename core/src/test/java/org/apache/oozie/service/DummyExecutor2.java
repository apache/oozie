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

import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.WorkflowAction;

public class DummyExecutor2 extends ActionExecutor {
    public DummyExecutor2() {
        super(TestActionService.TEST_ACTION_TYPE);
    }

    @Override
    public void start(Context context, WorkflowAction action) throws ActionExecutorException {
    }

    @Override
    public void end(Context context, WorkflowAction action) throws ActionExecutorException {
    }

    @Override
    public void check(Context context, WorkflowAction action) throws ActionExecutorException {
    }

    @Override
    public void kill(Context context, WorkflowAction action) throws ActionExecutorException {
    }

    @Override
    public boolean isCompleted(String externalStatus) {
        return true;
    }
}
