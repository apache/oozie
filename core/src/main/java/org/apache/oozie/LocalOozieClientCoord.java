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
 * instance.
 * <p>
 * This class is thread safe.
 * <p>
 * Syntax for filter for the {@link #getJobsInfo(String)}
 * {@link #getJobsInfo(String, int, int)} methods:
 * <code>[NAME=VALUE][;NAME=VALUE]*</code>.
 * <p>
 * Valid filter names are:
 * <ul>
 * <li>name: the coordinator application name from the coordinator definition.</li>
 * <li>user: the user that submitted the job.</li>
 * <li>group: the group for the job.</li>
 * <li>status: the status of the job.</li>
 * </ul>
 * <p>
 * The query will do an AND among all the filter names. The query will do an OR
 * among all the filter values for the same name. Multiple values must be
 * specified as different name value pairs.
 */
public class LocalOozieClientCoord extends BaseLocalOozieClient {

    private final CoordinatorEngine coordEngine;

    /**
     * Create a coordinator client for Oozie local use.
     * <p>
     *
     * @param coordEngine the engine instance to use.
     */
    public LocalOozieClientCoord(CoordinatorEngine coordEngine) {
        super(coordEngine);
        this.coordEngine = coordEngine;
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
     * @throws OozieClientException if coordinators cannot be rerun
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
     * @throws OozieClientException  if coordinators cannot be rerun
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
     * Get the info of a coordinator job.
     *
     * @param jobId job Id.
     * @param filter filter the status filter
     * @param start starting index in the list of actions belonging to the job
     * @param len number of actions to be returned
     * @return the job info.
     * @throws org.apache.oozie.client.OozieClientException thrown if the job
     *         info could not be retrieved.
     */
    @Override
    public CoordinatorJob getCoordJobInfo(String jobId, String filter, int start, int len)
            throws OozieClientException {
        try {
            return coordEngine.getCoordJob(jobId, filter, start, len, false);
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
        catch (BaseEngineException bex) {
            throw new OozieClientException(bex.getErrorCode().toString(), bex);
        }
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

    @Override
    public String updateCoord(String jobId, Properties conf, String dryrun, String showDiff) throws OozieClientException {
        try {
            return coordEngine.updateJob(new XConfiguration(conf), jobId, Boolean.valueOf(dryrun), Boolean.valueOf(showDiff));
        } catch (CoordinatorEngineException e) {
            throw new OozieClientException(e.getErrorCode().toString(), e);
        }
    }

    @Override
    public String updateCoord(String jobId, String dryrun, String showDiff) throws OozieClientException {
        try {
            return coordEngine.updateJob(new XConfiguration(), jobId, Boolean.valueOf(dryrun), Boolean.valueOf(showDiff));
        } catch (CoordinatorEngineException e) {
            throw new OozieClientException(e.getErrorCode().toString(), e);
        }
    }

    @Override
    public CoordinatorJob getCoordJobInfo(String jobId, String filter, int start, int len, String order)
            throws OozieClientException {
        try {
            return coordEngine.getCoordJob(jobId, filter, start, len, order.equalsIgnoreCase("DESC"));
        } catch (BaseEngineException e) {
            throw new OozieClientException(e.getErrorCode().toString(), e);
        }
    }

    @Override
    public List<CoordinatorAction> kill(String jobId, String rangeType, String scope) throws OozieClientException {
        try {
            return (List) coordEngine.killActions(jobId, rangeType, scope).getCoordActions();
        } catch (CoordinatorEngineException e) {
            throw new OozieClientException(e.getErrorCode().toString(), e);
        }
    }

    @Override
    public List<CoordinatorAction> ignore(String jobId, String scope) throws OozieClientException {
        try {
            return (List) coordEngine.ignore(jobId, RestConstants.JOB_COORD_SCOPE_ACTION, scope).getCoordActions();
        } catch (CoordinatorEngineException e) {
            throw new OozieClientException(e.getErrorCode().toString(), e);
        }
    }

    @Override
    public List<WorkflowJob> getWfsForCoordAction(String coordActionId) throws OozieClientException {
        try {
            return (List) coordEngine.getReruns(coordActionId);
        } catch (CoordinatorEngineException e) {
            throw new OozieClientException(e.getErrorCode().toString(), e);
        }
    }
}
