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

import java.io.IOException;
import java.io.Writer;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JMSTopicService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.XLogStreamingService;
import org.apache.oozie.util.XLogFilter;

public abstract class BaseEngine {
    public static final String USE_XCOMMAND = "oozie.useXCommand";

    public enum LOG_TYPE {
        LOG, ERROR_LOG, AUDIT_LOG
    }

    protected String user;

    /**
     * Return the user name.
     *
     * @return the user name.
     */
    public String getUser() {
        return user;
    }

    /**
     * Submit a job.
     * <p/>
     * It validates configuration properties.
     *
     * @param conf job configuration.
     * @param startJob indicates if the job should be started or not.
     * @return the job Id.
     * @throws BaseEngineException thrown if the job could not be created.
     */
    public abstract String submitJob(Configuration conf, boolean startJob) throws BaseEngineException;

    /**
     * Start a job.
     *
     * @param jobId job Id.
     * @throws BaseEngineException thrown if the job could not be started.
     */
    public abstract void start(String jobId) throws BaseEngineException;

    /**
     * Resume a job.
     *
     * @param jobId job Id.
     * @throws BaseEngineException thrown if the job could not be resumed.
     */
    public abstract void resume(String jobId) throws BaseEngineException;

    /**
     * Suspend a job.
     *
     * @param jobId job Id.
     * @throws BaseEngineException thrown if the job could not be suspended.
     */
    public abstract void suspend(String jobId) throws BaseEngineException;

    /**
     * Kill a job.
     *
     * @param jobId job Id.
     * @throws BaseEngineException thrown if the job could not be killed.
     */
    public abstract void kill(String jobId) throws BaseEngineException;

    /**
     * Change a coordinator job.
     *
     * @param jobId job Id.
     * @param changeValue change value.
     * @throws BaseEngineException thrown if the job could not be changed.
     */
    public abstract void change(String jobId, String changeValue) throws BaseEngineException;

    /**
     * Rerun a job.
     *
     * @param jobId job Id to rerun.
     * @param conf configuration information for the rerun.
     * @throws BaseEngineException thrown if the job could not be rerun.
     */
    public abstract void reRun(String jobId, Configuration conf) throws BaseEngineException;

    /**
     * Return the info about a wf job.
     *
     * @param jobId job Id.
     * @return the workflow job info.
     * @throws DagEngineException thrown if the job info could not be obtained.
     */
    public abstract WorkflowJob getJob(String jobId) throws BaseEngineException;

    /**
     * Return the info about a wf job with actions subset.
     *
     * @param jobId job Id
     * @param start starting from this index in the list of actions belonging to the job
     * @param length number of actions to be returned
     * @return the workflow job info.
     * @throws DagEngineException thrown if the job info could not be obtained.
     */
    public abstract WorkflowJob getJob(String jobId, int start, int length) throws BaseEngineException;

    /**
     * Return the info about a coord job.
     *
     * @param jobId job Id.
     * @return the coord job info.
     * @throws BaseEngineException thrown if the job info could not be obtained.
     */
    public abstract CoordinatorJob getCoordJob(String jobId) throws BaseEngineException;

    /**
     * Return the info about a coord job with actions subset.
     *
     * @param jobId job Id.
     * @param filter the status filter
     * @param start starting from this index in the list of actions belonging to the job
     * @param length number of actions to be returned
     * @param order true if actions are sorted in a descending order of nominal time, false if asc order
     * @return the coord job info.
     * @throws BaseEngineException thrown if the job info could not be obtained.
     */
    public abstract CoordinatorJob getCoordJob(String jobId, String filter, int start, int length, boolean desc)
            throws BaseEngineException;

    /**
     * Return the a job definition.
     *
     * @param jobId job Id.
     * @return the job definition.
     * @throws BaseEngineException thrown if the job definition could no be obtained.
     */
    public abstract String getDefinition(String jobId) throws BaseEngineException;

    /**
     * Stream the log of a job.
     *
     * @param jobId job Id.
     * @param writer writer to stream the log to.
     * @param params additional parameters from the request
     * @throws IOException thrown if the log cannot be streamed.
     * @throws BaseEngineException thrown if there is error in getting the Workflow/Coordinator Job Information for
     *         jobId.
     */
    public abstract void streamLog(String jobId, Writer writer, Map<String, String[]> params)
            throws IOException, BaseEngineException;

    /**
     * Stream error log of a job.
     *
     * @param jobId job Id.
     * @param writer writer to stream the log to.
     * @param params additional parameters from the request
     * @throws IOException thrown if the log cannot be streamed.
     * @throws BaseEngineException thrown if there is error in getting the Workflow/Coordinator Job Information for
     *         jobId.
     */
    public abstract void streamErrorLog(String jobId, Writer writer, Map<String, String[]> params) throws IOException,
            BaseEngineException;
    /**
     * Stream Audit log of a job.
     *
     * @param jobId job Id.
     * @param writer writer to stream the log to.
     * @param params additional parameters from the request
     * @throws IOException thrown if the log cannot be streamed.
     * @throws BaseEngineException thrown if there is error in getting the Workflow/Coordinator Job Information for
     *         jobId.
     */
    public abstract void streamAuditLog(String jobId, Writer writer, Map<String, String[]> params) throws IOException,
            BaseEngineException;


    /**
     * Return the workflow Job ID for an external ID.
     * <p/>
     * This is reverse lookup for recovery purposes.
     *
     * @param externalId external ID provided at job submission time.
     * @return the associated workflow job ID if any, <code>null</code> if none.
     * @throws BaseEngineException thrown if the lookup could not be done.
     */
    public abstract String getJobIdForExternalId(String externalId) throws BaseEngineException;

    /**
     * Dry run a job; like {@link BaseEngine#submitJob(org.apache.hadoop.conf.Configuration, boolean) but doesn't actually execute
     * the job.
     * <p/>
     * It validates configuration properties.
     *
     * @param conf job configuration.
     * @return the result of the dryrun
     * @throws BaseEngineException thrown if there was a problem doing the dryrun
     */
    public abstract String dryRunSubmit(Configuration conf) throws BaseEngineException;


    /**
     * Return the jms topic name for the job.
     *
     * @param jobId job Id.
     * @return String the topic name
     * @throws DagEngineException thrown if the jms info could not be obtained.
     */
    public String getJMSTopicName(String jobId) throws DagEngineException {
        JMSTopicService jmsTopicService = Services.get().get(JMSTopicService.class);
        if (jmsTopicService != null) {
            try {
                return jmsTopicService.getTopic(jobId);
            }
            catch (JPAExecutorException e) {
               throw new DagEngineException(ErrorCode.E1602, e);
            }
        }
        else {
            throw new DagEngineException(ErrorCode.E1602,
                    "JMSTopicService is not initialized. JMS notification"
                            + "may not be enabled");
        }
    }

    /**
     * Return the status for a Job ID
     *
     * @param jobId job Id.
     * @return the job's status
     * @throws BaseEngineException thrown if the job's status could not be obtained
     */
    public abstract String getJobStatus(String jobId) throws BaseEngineException;

    /**
     * Return the status for a Job ID
     *
     * @param jobId job Id.
     * @return the job's status
     * @throws BaseEngineException thrown if the job's status could not be obtained
     */

    /**
     * Enable SLA alert for job
     * @param id
     * @param actions
     * @param dates
     * @param childIds
     * @throws BaseEngineException
     */
    public abstract void enableSLAAlert(String id, String actions, String dates, String childIds) throws BaseEngineException;

    /**
     * Disable SLA alert for job
     * @param id
     * @param actions
     * @param dates
     * @param childIds
     * @throws BaseEngineException
     */
    public abstract void disableSLAAlert(String id, String actions, String  dates, String childIds) throws BaseEngineException;

    /**
     * Change SLA properties for job
     * @param id
     * @param actions
     * @param childIds
     * @param newParams
     * @throws BaseEngineException
     */
    public abstract void changeSLA(String id, String actions, String  dates, String childIds, String newParams)
            throws BaseEngineException;

    protected void fetchLog(XLogFilter filter, Date startTime, Date endTime, Writer writer,
            Map<String, String[]> params, LOG_TYPE logType) throws IOException {

        switch (logType) {
            case LOG:
                Services.get().get(XLogStreamingService.class).streamLog(filter, startTime, endTime, writer, params);
                break;
            case ERROR_LOG:
                Services.get().get(XLogStreamingService.class)
                        .streamErrorLog(filter, startTime, endTime, writer, params);
                break;
            case AUDIT_LOG:
                Services.get().get(XLogStreamingService.class)
                        .streamAuditLog(filter, startTime, endTime, writer, params);
                break;
            default:
                throw new IOException("Unsupported log Type");
        }
    }


}
