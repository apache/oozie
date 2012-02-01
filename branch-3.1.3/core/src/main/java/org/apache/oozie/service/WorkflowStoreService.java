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

import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.WorkflowLib;
import org.apache.oozie.service.Service;
import org.apache.oozie.store.Store;

import java.util.Collections;
import java.util.List;

/**
 * Base service for persistency of jobs and actions.
 */
public abstract class WorkflowStoreService implements Service {

    public final static String TRANSIENT_VAR_PREFIX = "oozie.workflow.";
    public static final String WORKFLOW_BEAN = TRANSIENT_VAR_PREFIX + "workflow.bean";
    final static String ACTION_ID = "action.id";
    final static String ACTIONS_TO_KILL = TRANSIENT_VAR_PREFIX + "actions.to.kill";
    final static String ACTIONS_TO_FAIL = TRANSIENT_VAR_PREFIX + "actions.to.fail";
    final static String ACTIONS_TO_START = TRANSIENT_VAR_PREFIX + "actions.to.start";

    /**
     * Return the public interface of the service.
     *
     * @return {@link WorkflowStoreService}.
     */
    public Class<? extends Service> getInterface() {
        return WorkflowStoreService.class;
    }

    /**
     * Return a workkflow lib, giving access to the parser functionality.
     *
     * @return a workflow lib to use the parser.
     */
    public abstract WorkflowLib getWorkflowLibWithNoDB();

    /**
     * Return a workflow store instance with a fresh transaction. <p/> The workflow store has to be committed and then
     * closed to commit changes, if only close it rolls back.
     *
     * @return a workflow store.
     * @throws StoreException thrown if the workflow store could not be created.
     */
    public abstract WorkflowStore create() throws StoreException;

    /**
     * Return a workflow store instance with an existing transaction. <p/> The workflow store has to be committed and
     * then closed to commit changes, if only close it rolls back.
     *
     * @return a workflow store.
     * @throws StoreException thrown if the workflow store could not be created.
     */
    //to do this method can be abstract or should be overridden
    public <S extends Store> WorkflowStore create(S store) throws StoreException {
        return null;
    }

    /**
     * Return the list of actions started by a signal in an instance.
     *
     * @param instance workflow instance that has been signaled.
     * @return the list of actions started by the signaling.
     */
    @SuppressWarnings("unchecked")
    public static List<WorkflowActionBean> getStartedActions(WorkflowInstance instance) {
        List<WorkflowActionBean> list = (List<WorkflowActionBean>) instance.getTransientVar(ACTIONS_TO_START);
        instance.setTransientVar(ACTIONS_TO_START, null);
        return (list != null) ? list : Collections.EMPTY_LIST;
    }

    /**
     * Return the list of action IDs to kill.
     *
     * @param instance workflow instance
     * @return the list of action IDs to kill.
     */
    @SuppressWarnings("unchecked")
    public static List<String> getActionsToKill(WorkflowInstance instance) {
        List<String> list = (List<String>) instance.getTransientVar(ACTIONS_TO_KILL);
        instance.setTransientVar(ACTIONS_TO_KILL, null);
        return (list != null) ? list : Collections.EMPTY_LIST;
    }

    /**
     * Return the list of action IDs to fail.
     *
     * @param instance workflow instance
     * @return the list of action IDs to fail.
     */
    @SuppressWarnings("unchecked")
    public static List<String> getActionsToFail(WorkflowInstance instance) {
        List<String> list = (List<String>) instance.getTransientVar(ACTIONS_TO_FAIL);
        instance.setTransientVar(ACTIONS_TO_FAIL, null);
        return (list != null) ? list : Collections.EMPTY_LIST;
    }
}
