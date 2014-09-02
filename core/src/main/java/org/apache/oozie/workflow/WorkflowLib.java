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

package org.apache.oozie.workflow;

import java.util.Date;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;


/**
 * The workflow library provides application parsing and storage capabilities for workflow instances. <p/> The
 * implementation is responsible for doing the store operations in a transactional way, either in autocommit or within
 * the scope of a transaction.
 */
public interface WorkflowLib {

    /**
     * Parse a workflow application definition.
     *
     * @param wfXml string containing the workflow definition.
     * @param jobConf job configuration
     * @param configDefault configuration from config-default.xml
     * @return the parse workflow application.
     * @throws WorkflowException thrown if the definition could not be parsed.
     */
    public WorkflowApp parseDef(String wfXml, Configuration jobConf, Configuration configDefault)
            throws WorkflowException;

    /**
     * Create a workflow instance.
     *
     * @param app application to create a workflow instance of.
     * @param conf job configuration.
     * @return the newly created workflow instance.
     * @throws WorkflowException thrown if the instance could not be created.
     */
    public WorkflowInstance createInstance(WorkflowApp app, Configuration conf) throws WorkflowException;

    /**
     * Create a workflow instance with the given wfId and actions endtime map. This will be used for re-running workflows.
     *
     * @param app application to create a workflow instance of.
     * @param conf job configuration.
     * @param wfId Workflow ID.
     * @param actionEndTimes A map of the actions to their endtimes; actions with no endtime should be omitted
     * @return the newly created workflow instance.
     * @throws WorkflowException thrown if the instance could not be created.
     */
    public WorkflowInstance createInstance(WorkflowApp app, Configuration conf, String wfId, Map<String, Date> actionEndTimes)
            throws WorkflowException;

    /**
     * Insert a workflow instance in storage.
     *
     * @param instance of the workflow instance to insert.
     * @throws WorkflowException thrown if the instance could not be inserted.
     */
    public void insert(WorkflowInstance instance) throws WorkflowException;

    /**
     * Load a workflow instance from storage.
     *
     * @param id ID of the workflow instance to load.
     * @return the loaded workflow instance.
     * @throws WorkflowException thrown if the instance could not be loaded.
     */
    public WorkflowInstance get(String id) throws WorkflowException;

    /**
     * Update a workflow instance in storage.
     *
     * @param instance workflow instance to update.
     * @throws WorkflowException thrown if the instance could not be loaded.
     */
    public void update(WorkflowInstance instance) throws WorkflowException;

    /**
     * Delete a workflow instance from storage.
     *
     * @param id ID of the workflow instance to delete.
     * @throws WorkflowException thrown if the instance could not be deleted.
     */
    public void delete(String id) throws WorkflowException;


    /**
     * Commit changes to store.
     *
     * @throws WorkflowException thrown if the commit could not be done.
     */
    public void commit() throws WorkflowException;

    /**
     * Close store. It rollbacks if there was no commit.
     *
     * @throws WorkflowException thrown if the close could not be done.
     */
    public void close() throws WorkflowException;

}
