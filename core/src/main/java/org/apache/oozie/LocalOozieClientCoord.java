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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.coord.CoordUtils;
import org.apache.oozie.util.XConfiguration;

/**
 * Client API to submit and manage Oozie coordinator jobs against an Oozie
 * intance.
 * <p/>
 * This class is thread safe.
 * <p/>
 * Syntax for filter for the {@link #getJobsInfo(String)}
 * {@link #getJobsInfo(String, int, int)} methods:
 * <code>[NAME=VALUE][;NAME=VALUE]*</code>.
 * <p/>
 * Valid filter names are:
 * <p/>
 * <ul/>
 * <li>name: the coordinator application name from the coordinator definition.</li>
 * <li>user: the user that submitted the job.</li>
 * <li>group: the group for the job.</li>
 * <li>status: the status of the job.</li>
 * </ul>
 * <p/>
 * The query will do an AND among all the filter names. The query will do an OR
 * among all the filter values for the same name. Multiple values must be
 * specified as different name value pairs.
 */
public class LocalOozieClientCoord extends OozieClient {

    private final CoordinatorEngine coordEngine;

    /**
     * Create a coordinator client for Oozie local use.
     * <p/>
     *
     * @param coordEngine the engine instance to use.
     */
    public LocalOozieClientCoord(CoordinatorEngine coordEngine) {
        this.coordEngine = coordEngine;
    }

    /**
     * Return the Oozie URL of the coordinator client instance.
     * <p/>
     * This URL is the base URL fo the Oozie system, with not protocol
     * versioning.
     *
     * @return the Oozie URL of the coordinator client instance.
     */
    @Override
    public String getOozieUrl() {
        return "localoozie";
    }

    /**
     * Return the Oozie URL used by the client and server for WS communications.
     * <p/>
     * This URL is the original URL plus the versioning element path.
     *
     * @return the Oozie URL used by the client and server for communication.
     * @throws org.apache.oozie.client.OozieClientException thrown in the client
     *         and the server are not protocol compatible.
     */
    @Override
    public String getProtocolUrl() throws OozieClientException {
        return "localoozie";
    }

    /**
     * Validate that the Oozie client and server instances are protocol
     * compatible.
     *
     * @throws org.apache.oozie.client.OozieClientException thrown in the client
     *         and the server are not protocol compatible.
     */
    @Override
    public synchronized void validateWSVersion() throws OozieClientException {
    }

    /**
     * Create an empty configuration with just the {@link #USER_NAME} set to the
     * JVM user name and the {@link #GROUP_NAME} set to 'other'.
     *
     * @return an empty configuration.
     */
    @Override
    public Properties createConfiguration() {
        Properties conf = new Properties();
        if (coordEngine != null) {
            conf.setProperty(USER_NAME, coordEngine.getUser());
        }
        conf.setProperty(GROUP_NAME, "users");
        return conf;
    }

    /**
     * Set a HTTP header to be used in the WS requests by the coordinator
     * instance.
     *
     * @param name header name.
     * @param value header value.
     */
    @Override
    public void setHeader(String name, String value) {
    }

    /**
     * Get the value of a set HTTP header from the coordinator instance.
     *
     * @param name header name.
     * @return header value, <code>null</code> if not set.
     */
    @Override
    public String getHeader(String name) {
        return null;
    }

    /**
     * Remove a HTTP header from the coordinator client instance.
     *
     * @param name header name.
     */
    @Override
    public void removeHeader(String name) {
    }

    /**
     * Return an iterator with all the header names set in the coordinator
     * instance.
     *
     * @return header names.
     */
    @Override
    @SuppressWarnings("unchecked")
    public Iterator<String> getHeaderNames() {
        return Collections.EMPTY_SET.iterator();
    }

    /**
     * Submit a coordinator job.
     *
     * @param conf job configuration.
     * @return the job Id.
     * @throws org.apache.oozie.client.OozieClientException thrown if the job
     *         could not be submitted.
     */
    @Override
    public String submit(Properties conf) throws OozieClientException {
        try {
            return coordEngine.submitJob(new XConfiguration(conf), false);
        }
        catch (CoordinatorEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        }
    }

    /**
     * Start a coordinator job.
     *
     * @param jobId job Id.
     * @throws org.apache.oozie.client.OozieClientException thrown if the job
     *         could not be started.
     */
    @Override
    @Deprecated
    public void start(String jobId) throws OozieClientException {
        try {
            coordEngine.start(jobId);
        }
        catch (CoordinatorEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        }
        catch (BaseEngineException bex) {
            throw new OozieClientException(bex.getErrorCode().toString(), bex);
        }
    }

    /**
     * Submit and start a coordinator job.
     *
     * @param conf job configuration.
     * @return the job Id.
     * @throws org.apache.oozie.client.OozieClientException thrown if the job
     *         could not be submitted.
     */
    @Override
    public String run(Properties conf) throws OozieClientException {
        try {
            return coordEngine.submitJob(new XConfiguration(conf), true);
        }
        catch (CoordinatorEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        }
    }

    /**
     * Rerun a workflow job.
     *
     * @param jobId job Id to rerun.
     * @param conf configuration information for the rerun.
     * @throws org.apache.oozie.client.OozieClientException thrown if the job
     *         could not be started.
     */
    @Override
    @Deprecated
    public void reRun(String jobId, Properties conf) throws OozieClientException {
        throw new OozieClientException(ErrorCode.E0301.toString(), "no-op");
    }

    /**
     * Rerun coordinator actions.
     *
     * @param jobId coordinator jobId
     * @param rerunType rerun type 'date' if -date is used, 'action-id' if
     *        -action is used
     * @param scope rerun scope for date or actionIds
     * @param refresh true if -refresh is given in command option
     * @param noCleanup true if -nocleanup is given in command option
     * @throws OozieClientException
     */
    @Override
    public List<CoordinatorAction> reRunCoord(String jobId, String rerunType, String scope, boolean refresh,
                                              boolean noCleanup) throws OozieClientException {
        return getCoordinatorActions(jobId, rerunType, scope, refresh, noCleanup, false, null);
    }

    /**
     * Rerun coordinator actions with failed option.
     *
     * @param jobId coordinator jobId
     * @param rerunType rerun type 'date' if -date is used, 'action-id' if
     *        -action is used
     * @param scope rerun scope for date or actionIds
     * @param refresh true if -refresh is given in command option
     * @param noCleanup true if -nocleanup is given in command option
     * @param failed true if -failed is given in command option
     * @param conf configuration information for the rerun
     * @throws OozieClientException
     */
    @Override
    public List<CoordinatorAction> reRunCoord(String jobId, String rerunType, String scope, boolean refresh,
            boolean noCleanup, boolean failed, Properties conf ) throws OozieClientException {
        return getCoordinatorActions(jobId, rerunType, scope, refresh, noCleanup, failed, conf);
    }

    private List<CoordinatorAction> getCoordinatorActions(String jobId, String rerunType, String scope, boolean refresh,
            boolean noCleanup, boolean failed, Properties prop) throws OozieClientException {
        try {
            XConfiguration conf = null;
            if (prop != null) {
                conf = new XConfiguration(prop);
            }
            if (!(rerunType.equals(RestConstants.JOB_COORD_SCOPE_DATE) || rerunType
                    .equals(RestConstants.JOB_COORD_SCOPE_ACTION))) {
                throw new CommandException(ErrorCode.E1018, "date or action expected.");
            }
            CoordinatorActionInfo coordInfo = coordEngine.reRun(jobId, rerunType, scope, Boolean.valueOf(refresh),
                    Boolean.valueOf(noCleanup), Boolean.valueOf(failed), conf);
            List<CoordinatorActionBean> actionBeans;
            if (coordInfo != null) {
                actionBeans = coordInfo.getCoordActions();
            }
            else {
                actionBeans = CoordUtils.getCoordActions(rerunType, jobId, scope, false);
            }
            List<CoordinatorAction> actions = new ArrayList<CoordinatorAction>();
            for (CoordinatorActionBean actionBean : actionBeans) {
                actions.add(actionBean);
            }
            return actions;
        }
        catch(CommandException ce){
            throw new OozieClientException(ce.getErrorCode().toString(), ce);
        }
        catch (BaseEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        }
    }

    /**
     * Suspend a coordinator job.
     *
     * @param jobId job Id.
     * @throws org.apache.oozie.client.OozieClientException thrown if the job
     *         could not be suspended.
     */
    @Override
    public void suspend(String jobId) throws OozieClientException {
        try {
            coordEngine.suspend(jobId);
        }
        catch (CoordinatorEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        }
    }

    /**
     * Resume a coordinator job.
     *
     * @param jobId job Id.
     * @throws org.apache.oozie.client.OozieClientException thrown if the job
     *         could not be resume.
     */
    @Override
    public void resume(String jobId) throws OozieClientException {
        try {
            coordEngine.resume(jobId);
        }
        catch (CoordinatorEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        }
    }

    /**
     * Kill a coordinator job.
     *
     * @param jobId job Id.
     * @throws org.apache.oozie.client.OozieClientException thrown if the job
     *         could not be killed.
     */
    @Override
    public void kill(String jobId) throws OozieClientException {
        try {
            coordEngine.kill(jobId);
        }
        catch (CoordinatorEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        }
    }

    /**
     * Get the info of a workflow job.
     *
     * @param jobId job Id.
     * @return the job info.
     * @throws org.apache.oozie.client.OozieClientException thrown if the job
     *         info could not be retrieved.
     */
    @Override
    @Deprecated
    public WorkflowJob getJobInfo(String jobId) throws OozieClientException {
        throw new OozieClientException(ErrorCode.E0301.toString(), "no-op");
    }

    /**
     * Get the info of a coordinator job.
     *
     * @param jobId job Id.
     * @return the job info.
     * @throws org.apache.oozie.client.OozieClientException thrown if the job
     *         info could not be retrieved.
     */
    @Override
    public CoordinatorJob getCoordJobInfo(String jobId) throws OozieClientException {
        try {
            return coordEngine.getCoordJob(jobId);
        }
        catch (CoordinatorEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        }
        catch (BaseEngineException bex) {
            throw new OozieClientException(bex.getErrorCode().toString(), bex);
        }
    }

    /**
     * Get the info of a coordinator action.
     *
     * @param actionId Id.
     * @return the coordinator action info.
     * @throws OozieClientException thrown if the job info could not be
     *         retrieved.
     */
    @Override
    public CoordinatorAction getCoordActionInfo(String actionId) throws OozieClientException {
        try {
            return coordEngine.getCoordAction(actionId);
        }
        catch (CoordinatorEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        }
        catch (BaseEngineException bex) {
            throw new OozieClientException(bex.getErrorCode().toString(), bex);
        }
    }

    /**
     * Return the info of the workflow jobs that match the filter.
     *
     * @param filter job filter. Refer to the {@link OozieClient} for the filter
     *        syntax.
     * @param start jobs offset, base 1.
     * @param len number of jobs to return.
     * @return a list with the workflow jobs info, without node details.
     * @throws OozieClientException thrown if the jobs info could not be
     *         retrieved.
     */
    @Override
    @Deprecated
    public List<WorkflowJob> getJobsInfo(String filter, int start, int len) throws OozieClientException {
        throw new OozieClientException(ErrorCode.E0301.toString(), "no-op");
    }

    /**
     * Return the info of the coordinator jobs that match the filter.
     *
     * @param filter job filter. Refer to the {@link OozieClient} for the filter
     *        syntax.
     * @param start jobs offset, base 1.
     * @param len number of jobs to return.
     * @return a list with the coordinator jobs info
     * @throws OozieClientException thrown if the jobs info could not be
     *         retrieved.
     */
    @Override
    public List<CoordinatorJob> getCoordJobsInfo(String filter, int start, int len) throws OozieClientException {
        try {
            CoordinatorJobInfo info = coordEngine.getCoordJobs(filter, start, len);
            List<CoordinatorJob> jobs = new ArrayList<CoordinatorJob>();
            List<CoordinatorJobBean> jobBeans = info.getCoordJobs();
            for (CoordinatorJobBean jobBean : jobBeans) {
                jobs.add(jobBean);
            }
            return jobs;

        }
        catch (CoordinatorEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        }
    }

    /**
     * Return the info of the workflow jobs that match the filter.
     * <p/>
     * It returns the first 100 jobs that match the filter.
     *
     * @param filter job filter. Refer to the {@link LocalOozieClient} for the
     *        filter syntax.
     * @return a list with the workflow jobs info, without node details.
     * @throws org.apache.oozie.client.OozieClientException thrown if the jobs
     *         info could not be retrieved.
     */
    @Override
    @Deprecated
    public List<WorkflowJob> getJobsInfo(String filter) throws OozieClientException {
        throw new OozieClientException(ErrorCode.E0301.toString(), "no-op");
    }

}
