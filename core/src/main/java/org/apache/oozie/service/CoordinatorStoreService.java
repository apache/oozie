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

import org.apache.oozie.store.StoreException;
import org.apache.oozie.service.Service;
import org.apache.oozie.store.Store;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.ErrorCode;

/**
 * Base service for persistency of jobs and actions.
 */
public class CoordinatorStoreService implements Service {

    public final static String TRANSIENT_VAR_PREFIX = "oozie.coordinator.";
    public static final String WORKFLOW_BEAN = TRANSIENT_VAR_PREFIX
            + "coordinator.bean";
    final static String ACTION_ID = "action.id";
    final static String ACTIONS_TO_KILL = TRANSIENT_VAR_PREFIX
            + "actions.to.kill";
    final static String ACTIONS_TO_FAIL = TRANSIENT_VAR_PREFIX
            + "actions.to.fail";
    final static String ACTIONS_TO_START = TRANSIENT_VAR_PREFIX
            + "actions.to.start";

    /**
     * Return the public interface of the service.
     *
     * @return {@link WorkflowStoreService}.
     */
    public Class<? extends Service> getInterface() {
        return CoordinatorStoreService.class;
    }

    /**
     * Return a workflow store instance with a fresh transaction. <p/> The coordinator store has to be committed and then
     * closed to commit changes, if only close it rolls back.
     *
     * @return a coordinator store.
     * @throws StoreException thrown if the workflow store could not be created.
     */
    public CoordinatorStore create() throws StoreException {
        try {
            return new CoordinatorStore(false);
        }
        catch (Exception ex) {
            throw new StoreException(ErrorCode.E0600, ex.getMessage(), ex);
        }
    }

    /**
     * Return a workflow store instance with an existing transaction. <p/> The workflow store has to be committed and then
     * closed to commit changes, if only close it rolls back.
     *
     * @return a workflow store.
     * @throws StoreException thrown if the workflow store could not be created.
     */
    // to do this method can be abstract or should be overridden
    public <S extends Store> CoordinatorStore create(S store)
            throws StoreException {
        try {
            return new CoordinatorStore(store, false);
        }
        catch (Exception ex) {
            throw new StoreException(ErrorCode.E0600, ex.getMessage(), ex);
        }
    }

    /**
     * Initializes the {@link StoreService}.
     *
     * @param services services instance.
     */
    public void init(Services services) throws ServiceException {
    }

    /**
     * Destroy the StoreService
	 */
	public void destroy() {
	}
}
