/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.store;

import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.WorkflowsInfo;

import java.util.List;
import java.util.Map;

/**
 * <code>WorkflowStore</code> Interface to persist the Jobs and Action
 */
public interface WorkflowStore {

    /**
     * Inserts the given workflow into the store
     * 
     * @param workflow workflow bean.
     * @throws StoreException
     */
    public void insertWorkflow(WorkflowJobBean workflow) throws StoreException;

    /**
     * Load the Workflow with given id to the WorkflowBean. Load the Process
     * Instance also to the bean. Lock the Workflow if locking option is given.
     * 
     * @param id Workflow ID.
     * @param locking flag to lock the job.
     * @return WorkflowBean Workflow bean.
     * @throws StoreException If the job cannot be laoded.
     */
    public WorkflowJobBean getWorkflow(String id, boolean locking) throws StoreException;

    /**
     * Load the Workflow Info. Load the List of actions of the workflow also.
     * process instance is not loaded.
     * 
     * @param id Workflow ID.
     * @return Workflow bean.
     * @throws StoreException
     */
    public WorkflowJobBean getWorkflowInfo(String id) throws StoreException;

    /**
     * Get the Workflow ID that belongs to the given external ID.
     *
     * @param extId External ID.
     * @return Workflow ID.
     * @throws StoreException If there are no jobs with given external id.
     */
    public String getWorkflowIdForExternalId(String extId) throws StoreException;

    /**
     * Update the data from the given WorkflowBean to Store.
     * 
     * @param workflow Workflow bean to be updated.
     * @throws StoreException if the bean cannot be updated.
     */
    public void updateWorkflow(WorkflowJobBean workflow) throws StoreException;

    /**
     * Get the number of Workflows with the given status.
     * 
     * @param status Workflow Status.
     * @return number of Workflows with given status.
     * @throws StoreException
     */
    public int getWorkflowCountWithStatus(String status) throws StoreException;

    /**
     * Get the number of Workflows with the given status which was modified in given time limit.
     * 
     * @param status Workflow Status.
     * @param secs No. of seconds within which the workflow got modified.
     * @return number of Workflows modified within given time with given status.
     * @throws StoreException
     */
    public int getWorkflowCountWithStatusInLastNSeconds(String status, int secs) throws StoreException;

    /**
     * Create a New Action record from the given ActionBean.
     * 
     * @param action Action bean.
     * @throws StoreException if the action cannot be inserted.
     */
    public void insertAction(WorkflowActionBean action) throws StoreException;

    /**
     * Load the action record with the given id to bean. Lock the action if
     * locking option is given.
     * 
     * @param id Action ID.
     * @param locking flag to lock the action.
     * @return Action bean.
     * @throws StoreException if the action with given id is not present.
     */
    public WorkflowActionBean getAction(String id, boolean locking) throws StoreException;

    /**
     * Update the data from the given ActionBean to the Store.
     * @param action Action bean.
     * @throws StoreException if the action cannot be updated.
     */
    public void updateAction(WorkflowActionBean action) throws StoreException;

    /**
     * Delete the action with given action id.
     * @param id Action id.
     * @throws StoreException if the action is not present.
     */
    public void deleteAction(String id) throws StoreException;

    /**
     * Load all the actions for the given Workflow id.
     * @param id Workflow ID.
     * @param locking flag to lock the actions.
     * @return List of action beans.
     * @throws StoreException if there is an error while loading the actions.
     */
    public List<WorkflowActionBean> getActionsForWorkflow(String id, boolean locking) throws StoreException;

    /**
     * Load the actions that are pending for more than given time.
     * @param minimumPendingAgeSecs minimum pending age in seconds.
     * @return list of action beans.
     * @throws StoreException if there is an error while loading the actions.
     */
    public List<WorkflowActionBean> getPendingActions(long minimumPendingAgeSecs) throws StoreException;

    /**
     * Load running actions which were last checked before the specified time.
     *
     * @param checkAgeSecs the check age in seconds.
     * @return A list of running actions which were last checked after now - checkAgeSecs.
     * @throws StoreException
     */
    public List<WorkflowActionBean> getRunningActions(long checkAgeSecs) throws StoreException;

    /**
     * Load the Workflows according to the given filter information.
     * @param filter Can be name, status, user, group and combination of these.
     * @param start returned result set offset.
     * @param len returned result set rows.
     * @return List of Workflows Satisfying the given filter.
     * @throws StoreException thrown if the workflow result could not be queried.
     */
    public WorkflowsInfo getWorkflowsInfo(Map<String, List<String>> filter, int start, int len)
            throws StoreException;

    /**
     * Purge the Jobs older than given days.
     * @param olderThanDays
     * @throws StoreException
     */
    public void purge(long olderThanDays) throws StoreException;

    /**
     * Commit the transaction.
     * @throws StoreException
     */
    public void commit() throws StoreException;

    /**
     * Close the connection.
     * @throws StoreException
     */
    public void close() throws StoreException;
}