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

import org.apache.oozie.service.DBLiteWorkflowStoreService;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowsInfo;

import java.util.List;
import java.util.Map;

public class ForTestWorkflowStoreService extends DBLiteWorkflowStoreService {

    public WorkflowStore create() throws StoreException {
        final WorkflowStore wfs = super.create();
        return new WorkflowStore() {
            public void insertWorkflow(WorkflowJobBean workflow) throws StoreException {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            public WorkflowJobBean getWorkflow(String id, boolean locking) throws StoreException {
                WorkflowJobBean wf = new WorkflowJobBean();
                wf.setId(id);
                wf.setUser("u");
                wf.setGroup("g");
                return wf;
            }

            public WorkflowJobBean getWorkflowInfo(String id) throws StoreException {
                return null;//To change body of implemented methods use File | Settings | File Templates.
            }

            public String getWorkflowIdForExternalId(String extId) throws StoreException {
                return null;//To change body of implemented methods use File | Settings | File Templates.
            }

            public void updateWorkflow(WorkflowJobBean workflow) throws StoreException {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            public int getWorkflowCountWithStatus(String status) throws StoreException {
                return 0;//To change body of implemented methods use File | Settings | File Templates.
            }

            public int getWorkflowCountWithStatusInLastNSeconds(String status, int secs) throws StoreException {
                return 0;//To change body of implemented methods use File | Settings | File Templates.
            }

            public void insertAction(WorkflowActionBean action) throws StoreException {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            public WorkflowActionBean getAction(String id, boolean locking) throws StoreException {
                return null;//To change body of implemented methods use File | Settings | File Templates.
            }

            public void updateAction(WorkflowActionBean action) throws StoreException {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            public void deleteAction(String id) throws StoreException {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            public List<WorkflowActionBean> getActionsForWorkflow(String id, boolean locking) throws StoreException {
                return null;//To change body of implemented methods use File | Settings | File Templates.
            }

            public List<WorkflowActionBean> getPendingActions(long minimumPendingAgeSecs) throws StoreException {
                return null;//To change body of implemented methods use File | Settings | File Templates.
            }

            public List<WorkflowActionBean> getRunningActions(long checkAgeSecs) throws StoreException {
                return null;//To change body of implemented methods use File | Settings | File Templates.
            }

            public WorkflowsInfo getWorkflowsInfo(Map<String, List<String>> filter, int start, int len)
                    throws StoreException {
                return null;//To change body of implemented methods use File | Settings | File Templates.
            }

            public void purge(long olderThanDays) throws StoreException {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            public void commit() throws StoreException {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            public void close() throws StoreException {
                //To change body of implemented methods use File | Settings | File Templates.
            }
        };
    }

}
