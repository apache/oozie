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

package org.apache.oozie;

import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;

import java.util.List;

/**
 * Client API to submit and manage Oozie workflow jobs against an Oozie instance. <p> This class is thread safe. <p>
 * Syntax for filter for the {@link #getJobsInfo(String)}  {@link #getJobsInfo(String, int, int)}  methods:
 * <code>[NAME=VALUE][;NAME=VALUE]*</code>. <p> Valid filter names are: <ul> <li>name: the workflow application
 * name from the workflow definition.</li> <li>user: the user that submitted the job.</li> <li>group: the group for the
 * job.</li> <li>status: the status of the job.</li> </ul> <p> The query will do an AND among all the filter names. The
 * query will do an OR among all the filter values for the same name. Multiple values must be specified as different
 * name value pairs.
 */
public class LocalOozieClient extends BaseLocalOozieClient {

    private DagEngine dagEngine;

    /**
     * Create a workflow client for Oozie local use.
     *
     * @param dagEngine the dag engine instance to use.
     */
    public LocalOozieClient(DagEngine dagEngine) {
        super(dagEngine);
        this.dagEngine = dagEngine;
    }

    /**
     * Get the info of a workflow job.
     *
     * @param jobId job Id.
     * @return the job info.
     * @throws org.apache.oozie.client.OozieClientException thrown if the job info could not be retrieved.
     */
    @Override
    public WorkflowJob getJobInfo(String jobId) throws OozieClientException {
        try {
            return dagEngine.getJob(jobId);
        }
        catch (DagEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        }
    }

    /**
     * Return the info of the workflow jobs that match the filter.
     *
     * @param filter job filter. Refer to the {@link LocalOozieClient} for the filter syntax.
     * @param start jobs offset, base 1.
     * @param len number of jobs to return.
     * @return a list with the workflow jobs info, without node details.
     * @throws org.apache.oozie.client.OozieClientException thrown if the jobs info could not be retrieved.
     */
    @Override
    public List<WorkflowJob> getJobsInfo(String filter, int start, int len) throws OozieClientException {
        try {
            return (List) dagEngine.getJobs(filter, start, len).getWorkflows();
        }
        catch (DagEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        }
    }

    /**
     * Return the info of the workflow jobs that match the filter. <p> It returns the first 100 jobs that match the
     * filter.
     *
     * @param filter job filter. Refer to the {@link LocalOozieClient} for the filter syntax.
     * @return a list with the workflow jobs info, without node details.
     * @throws org.apache.oozie.client.OozieClientException thrown if the jobs info could not be retrieved.
     */
    @Override
    public List<WorkflowJob> getJobsInfo(String filter) throws OozieClientException {
        return getJobsInfo(filter, 1, 100);
    }

    @Override
    public WorkflowJob getJobInfo(String jobId, int start, int len) throws OozieClientException {
        try {
            return dagEngine.getJob(jobId, start, len);
        } catch (DagEngineException e) {
            throw new OozieClientException(e.getErrorCode().toString(), e);
        }
    }

    @Override
    public WorkflowAction getWorkflowActionInfo(String actionId) throws OozieClientException {
        try {
            return dagEngine.getWorkflowAction(actionId);
        } catch (BaseEngineException e) {
            throw new OozieClientException(e.getErrorCode().toString(), e);
        }
    }

    /**
     * Returns if Oozie is in safe mode or not.
     *
     * @return true if safe mode is ON<br> false if safe mode is OFF
     * @throws org.apache.oozie.client.OozieClientException throw if it could not obtain the safe mode status.
     */
    /*public SYSTEM_MODE isInSafeMode() throws OozieClientException {
        //return Services.get().isSafeMode();
        return Services.get().getSystemMode() ;
    }*/

}
