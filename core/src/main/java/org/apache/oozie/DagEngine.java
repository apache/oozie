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

import org.apache.oozie.service.XLogService;
import org.apache.oozie.service.DagXLogInfoService;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.wf.CompletedActionXCommand;
import org.apache.oozie.command.wf.DefinitionXCommand;
import org.apache.oozie.command.wf.ExternalIdXCommand;
import org.apache.oozie.command.wf.JobXCommand;
import org.apache.oozie.command.wf.JobsXCommand;
import org.apache.oozie.command.wf.KillXCommand;
import org.apache.oozie.command.wf.ReRunXCommand;
import org.apache.oozie.command.wf.ResumeXCommand;
import org.apache.oozie.command.wf.StartXCommand;
import org.apache.oozie.command.wf.SubmitHiveXCommand;
import org.apache.oozie.command.wf.SubmitHttpXCommand;
import org.apache.oozie.command.wf.SubmitMRXCommand;
import org.apache.oozie.command.wf.SubmitPigXCommand;
import org.apache.oozie.command.wf.SubmitSqoopXCommand;
import org.apache.oozie.command.wf.SubmitXCommand;
import org.apache.oozie.command.wf.SuspendXCommand;
import org.apache.oozie.command.wf.WorkflowActionInfoXCommand;
import org.apache.oozie.command.OperationType;
import org.apache.oozie.command.wf.BulkWorkflowXCommand;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor.WorkflowJobQuery;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.CallableQueueService;
import org.apache.oozie.util.XLogAuditFilter;
import org.apache.oozie.util.XLogFilter;
import org.apache.oozie.util.XLogUserFilterParam;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XCallable;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.service.XLogStreamingService;

import java.io.StringReader;
import java.io.Writer;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.io.IOException;

/**
 * The DagEngine provides all the DAG engine functionality for WS calls.
 */
public class DagEngine extends BaseEngine {

    private static final int HIGH_PRIORITY = 2;
    private static XLog LOG = XLog.getLog(DagEngine.class);

    /**
     * Create a system Dag engine, with no user and no group.
     */
    public DagEngine() {

    }

    /**
     * Create a Dag engine to perform operations on behave of a user.
     *
     * @param user user name.
     */
    public DagEngine(String user) {
        this();

        this.user = ParamChecker.notEmpty(user, "user");
    }

    /**
     * Submit a workflow job. <p/> It validates configuration properties.
     *
     * @param conf job configuration.
     * @param startJob indicates if the job should be started or not.
     * @return the job Id.
     * @throws DagEngineException thrown if the job could not be created.
     */
    @Override
    public String submitJob(Configuration conf, boolean startJob) throws DagEngineException {
        validateSubmitConfiguration(conf);

        try {
            String jobId;
            SubmitXCommand submit = new SubmitXCommand(conf);
            jobId = submit.call();
            if (startJob) {
                start(jobId);
            }
            return jobId;
        }
        catch (CommandException ex) {
            throw new DagEngineException(ex);
        }
    }

    /**
     * Submit a workflow through a coordinator. It validates configuration properties.
     * @param conf job conf
     * @param parentId parent of workflow
     * @return
     * @throws DagEngineException
     */
    public String submitJobFromCoordinator(Configuration conf, String parentId) throws DagEngineException {
        validateSubmitConfiguration(conf);
        try {
            String jobId;
            SubmitXCommand submit = new SubmitXCommand(conf, parentId);
            jobId = submit.call();
            start(jobId);
            return jobId;
        }
        catch (CommandException ex) {
            throw new DagEngineException(ex);
        }
    }

    /**
     * Submit a pig/hive/mapreduce job through HTTP.
     * <p/>
     * It validates configuration properties.
     *
     * @param conf job configuration.
     * @param jobType job type - can be "pig", "hive", "sqoop" or "mapreduce".
     * @return the job Id.
     * @throws DagEngineException thrown if the job could not be created.
     */
    public String submitHttpJob(Configuration conf, String jobType) throws DagEngineException {
        validateSubmitConfiguration(conf);

        try {
            String jobId;
            SubmitHttpXCommand submit = null;
            if (jobType.equals("pig")) {
                submit = new SubmitPigXCommand(conf);
            }
            else if (jobType.equals("mapreduce")) {
                submit = new SubmitMRXCommand(conf);
            }
            else if (jobType.equals("hive")) {
                submit = new SubmitHiveXCommand(conf);
            }
            else if (jobType.equals("sqoop")) {
                submit = new SubmitSqoopXCommand(conf);
            }

            jobId = submit.call();
            start(jobId);
            return jobId;
        }
        catch (CommandException ex) {
            throw new DagEngineException(ex);
        }
    }

    private void validateSubmitConfiguration(Configuration conf) throws DagEngineException {
        if (conf.get(OozieClient.APP_PATH) == null) {
            throw new DagEngineException(ErrorCode.E0401, OozieClient.APP_PATH);
        }
    }

    /**
     * Start a job.
     *
     * @param jobId job Id.
     * @throws DagEngineException thrown if the job could not be started.
     */
    @Override
    public void start(String jobId) throws DagEngineException {
        // Changing to synchronous call from asynchronous queuing to prevent the
        // loss of command if the queue is full or the queue is lost in case of
        // failure.
        try {
                new StartXCommand(jobId).call();
        }
        catch (CommandException e) {
            throw new DagEngineException(e);
        }
    }

    /**
     * Resume a job.
     *
     * @param jobId job Id.
     * @throws DagEngineException thrown if the job could not be resumed.
     */
    @Override
    public void resume(String jobId) throws DagEngineException {
        // Changing to synchronous call from asynchronous queuing to prevent the
        // loss of command if the queue is full or the queue is lost in case of
        // failure.
        try {
                new ResumeXCommand(jobId).call();
        }
        catch (CommandException e) {
            throw new DagEngineException(e);
        }
    }

    /**
     * Suspend a job.
     *
     * @param jobId job Id.
     * @throws DagEngineException thrown if the job could not be suspended.
     */
    @Override
    public void suspend(String jobId) throws DagEngineException {
        // Changing to synchronous call from asynchronous queuing to prevent the
        // loss of command if the queue is full or the queue is lost in case of
        // failure.
        try {
			new SuspendXCommand(jobId).call();
        }
        catch (CommandException e) {
            throw new DagEngineException(e);
        }
    }

    /**
     * Kill a job.
     *
     * @param jobId job Id.
     * @throws DagEngineException thrown if the job could not be killed.
     */
    @Override
    public void kill(String jobId) throws DagEngineException {
        // Changing to synchronous call from asynchronous queuing to prevent the
        // loss of command if the queue is full or the queue is lost in case of
        // failure.
        try {
			new KillXCommand(jobId).call();
			LOG.info("User " + user + " killed the WF job " + jobId);
        }
        catch (CommandException e) {
            throw new DagEngineException(e);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#change(java.lang.String, java.lang.String)
     */
    @Override
    public void change(String jobId, String changeValue) throws DagEngineException {
        // This code should not be reached.
        throw new DagEngineException(ErrorCode.E1017);
    }

    /**
     * Rerun a job.
     *
     * @param jobId job Id to rerun.
     * @param conf configuration information for the rerun.
     * @throws DagEngineException thrown if the job could not be rerun.
     */
    @Override
    public void reRun(String jobId, Configuration conf) throws DagEngineException {
        try {
            WorkflowJobBean wfBean = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW, jobId);
            Configuration wfConf = new XConfiguration(new StringReader(wfBean.getConf()));
            XConfiguration.copy(conf, wfConf);
            validateReRunConfiguration(wfConf);
            new ReRunXCommand(jobId, wfConf).call();
        }
        catch (CommandException ex) {
            throw new DagEngineException(ex);
        }
        catch (JPAExecutorException ex) {
            throw new DagEngineException(ex);
        }
        catch (IOException ex) {
            throw new DagEngineException(ErrorCode.E0803, ex.getMessage());
        }
    }

    private void validateReRunConfiguration(Configuration conf) throws DagEngineException {
        if (conf.get(OozieClient.APP_PATH) == null) {
            throw new DagEngineException(ErrorCode.E0401, OozieClient.APP_PATH);
        }
        if (conf.get(OozieClient.RERUN_SKIP_NODES) == null && conf.get(OozieClient.RERUN_FAIL_NODES) == null) {
            throw new DagEngineException(ErrorCode.E0401, OozieClient.RERUN_SKIP_NODES + " OR "
                    + OozieClient.RERUN_FAIL_NODES);
        }
        if (conf.get(OozieClient.RERUN_SKIP_NODES) != null && conf.get(OozieClient.RERUN_FAIL_NODES) != null) {
            throw new DagEngineException(ErrorCode.E0404, OozieClient.RERUN_SKIP_NODES + " OR "
                    + OozieClient.RERUN_FAIL_NODES);
        }
    }

    /**
     * Process an action callback.
     *
     * @param actionId the action Id.
     * @param externalStatus the action external status.
     * @param actionData the action output data, <code>null</code> if none.
     * @throws DagEngineException thrown if the callback could not be processed.
     */
    public void processCallback(String actionId, String externalStatus, Properties actionData)
            throws DagEngineException {
        XLog.Info.get().clearParameter(XLogService.GROUP);
        XLog.Info.get().clearParameter(XLogService.USER);
        XCallable<Void> command = null;

		command = new CompletedActionXCommand(actionId, externalStatus,
				actionData, HIGH_PRIORITY);
        if (!Services.get().get(CallableQueueService.class).queue(command)) {
            LOG.warn(XLog.OPS, "queue is full or system is in SAFEMODE, ignoring callback");
        }
    }

    /**
     * Return the info about a job.
     *
     * @param jobId job Id.
     * @return the workflow job info.
     * @throws DagEngineException thrown if the job info could not be obtained.
     */
    @Override
    public WorkflowJob getJob(String jobId) throws DagEngineException {
        try {
                return new JobXCommand(jobId).call();
        }
        catch (CommandException ex) {
            throw new DagEngineException(ex);
        }
    }

    /**
     * Return the info about a job with actions subset.
     *
     * @param jobId job Id
     * @param start starting from this index in the list of actions belonging to the job
     * @param length number of actions to be returned
     * @return the workflow job info.
     * @throws DagEngineException thrown if the job info could not be obtained.
     */
    @Override
    public WorkflowJob getJob(String jobId, int start, int length) throws DagEngineException {
        try {
			return new JobXCommand(jobId, start, length).call();
        }
        catch (CommandException ex) {
            throw new DagEngineException(ex);
        }
    }

    /**
     * Return the a job definition.
     *
     * @param jobId job Id.
     * @return the job definition.
     * @throws DagEngineException thrown if the job definition could no be obtained.
     */
    @Override
    public String getDefinition(String jobId) throws DagEngineException {
        try {
            return new DefinitionXCommand(jobId).call();
        }
        catch (CommandException ex) {
            throw new DagEngineException(ex);
        }
    }

    /**
     * Stream the log of a job.
     *
     * @param jobId job Id.
     * @param writer writer to stream the log to.
     * @param params additional parameters from the request
     * @throws IOException thrown if the log cannot be streamed.
     * @throws DagEngineException thrown if there is error in getting the Workflow Information for jobId.
     */
    @Override
    public void streamLog(String jobId, Writer writer, Map<String, String[]> params) throws IOException,
            DagEngineException {
        streamJobLog(jobId, writer, params, LOG_TYPE.LOG);
    }

    /**
     * Stream the error log of a job.
     *
     * @param jobId job Id.
     * @param writer writer to stream the log to.
     * @param params additional parameters from the request
     * @throws IOException thrown if the log cannot be streamed.
     * @throws DagEngineException thrown if there is error in getting the Workflow Information for jobId.
     */
    @Override
    public void streamErrorLog(String jobId, Writer writer, Map<String, String[]> params) throws IOException,
            DagEngineException {
        streamJobLog(jobId, writer, params, LOG_TYPE.ERROR_LOG);
    }

    /**
     * Stream the audit log of a job.
     *
     * @param jobId job Id.
     * @param writer writer to stream the log to.
     * @param params additional parameters from the request
     * @throws IOException thrown if the log cannot be streamed.
     * @throws DagEngineException thrown if there is error in getting the Workflow Information for jobId.
     */
    @Override
    public void streamAuditLog(String jobId, Writer writer, Map<String, String[]> params) throws IOException,
            DagEngineException {
        try {
            streamJobLog(new XLogAuditFilter(new XLogUserFilterParam(params)),jobId, writer, params, LOG_TYPE.AUDIT_LOG);
        }
        catch (CommandException e) {
            throw new IOException(e);
        }
    }

    private void streamJobLog(String jobId, Writer writer, Map<String, String[]> params, LOG_TYPE logType)
            throws IOException, DagEngineException {
        try {
            streamJobLog(new XLogFilter(new XLogUserFilterParam(params)), jobId, writer, params, logType);
        }
        catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void streamJobLog(XLogFilter filter, String jobId, Writer writer, Map<String, String[]> params, LOG_TYPE logType)
            throws IOException, DagEngineException {
        try {
            filter.setParameter(DagXLogInfoService.JOB, jobId);
            WorkflowJob job = getJob(jobId);
            Date lastTime = job.getEndTime();
            if (lastTime == null) {
                lastTime = job.getLastModifiedTime();
            }
            fetchLog(filter, job.getCreatedTime(), lastTime, writer, params, logType);
        }
        catch (Exception e) {
            throw new IOException(e);
        }
    }

    private static final Set<String> FILTER_NAMES = new HashSet<String>();

    static {
        FILTER_NAMES.add(OozieClient.FILTER_USER);
        FILTER_NAMES.add(OozieClient.FILTER_NAME);
        FILTER_NAMES.add(OozieClient.FILTER_GROUP);
        FILTER_NAMES.add(OozieClient.FILTER_STATUS);
        FILTER_NAMES.add(OozieClient.FILTER_ID);
        FILTER_NAMES.add(OozieClient.FILTER_CREATED_TIME_START);
        FILTER_NAMES.add(OozieClient.FILTER_CREATED_TIME_END);
    }

    /**
     * Validate a jobs filter.
     *
     * @param filter filter to validate.
     * @return the parsed filter.
     * @throws DagEngineException thrown if the filter is invalid.
     */
    protected Map<String, List<String>> parseFilter(String filter) throws DagEngineException {
        Map<String, List<String>> map = new HashMap<String, List<String>>();
        if (filter != null) {
            StringTokenizer st = new StringTokenizer(filter, ";");
            while (st.hasMoreTokens()) {
                String token = st.nextToken();
                if (token.contains("=")) {
                    String[] pair = token.split("=");
                    if (pair.length != 2) {
                        throw new DagEngineException(ErrorCode.E0420, filter, "elements must be name=value pairs");
                    }
                    pair[0] = pair[0].toLowerCase();
                    if (!FILTER_NAMES.contains(pair[0])) {
                        throw new DagEngineException(ErrorCode.E0420, filter, XLog
                                .format("invalid name [{0}]", pair[0]));
                    }
                    if (pair[0].equals("status")) {
                        try {
                            WorkflowJob.Status.valueOf(pair[1]);
                        }
                        catch (IllegalArgumentException ex) {
                            throw new DagEngineException(ErrorCode.E0420, filter, XLog.format("invalid status [{0}]",
                                                                                              pair[1]));
                        }
                    }
                    List<String> list = map.get(pair[0]);
                    if (list == null) {
                        list = new ArrayList<String>();
                        map.put(pair[0], list);
                    }
                    list.add(pair[1]);
                }
                else {
                    throw new DagEngineException(ErrorCode.E0420, filter, "elements must be name=value pairs");
                }
            }
        }
        return map;
    }

    /**
     * Return the info about a set of jobs.
     *
     * @param filter job filter. Refer to the {@link org.apache.oozie.client.OozieClient} for the filter syntax.
     * @param start offset, base 1.
     * @param len number of jobs to return.
     * @return job info for all matching jobs, the jobs don't contain node action information.
     * @throws DagEngineException thrown if the jobs info could not be obtained.
     */
    public WorkflowsInfo getJobs(String filter, int start, int len) throws DagEngineException {
        Map<String, List<String>> filterList = parseFilter(filter);
        try {
			return new JobsXCommand(filterList, start, len).call();
        }
        catch (CommandException dce) {
            throw new DagEngineException(dce);
        }
    }

    /**
     * Return the workflow Job ID for an external ID. <p/> This is reverse lookup for recovery purposes.
     *
     * @param externalId external ID provided at job submission time.
     * @return the associated workflow job ID if any, <code>null</code> if none.
     * @throws DagEngineException thrown if the lookup could not be done.
     */
    @Override
    public String getJobIdForExternalId(String externalId) throws DagEngineException {
        try {
			return new ExternalIdXCommand(externalId).call();
        }
        catch (CommandException dce) {
            throw new DagEngineException(dce);
        }
    }

    @Override
    public CoordinatorJob getCoordJob(String jobId) throws BaseEngineException {
        throw new BaseEngineException(new XException(ErrorCode.E0301, "cannot get a coordinator job from DagEngine"));
    }

    @Override
    public CoordinatorJob getCoordJob(String jobId, String filter, int start, int length, boolean desc)
            throws BaseEngineException {
        throw new BaseEngineException(new XException(ErrorCode.E0301, "cannot get a coordinator job from DagEngine"));
    }

    public WorkflowActionBean getWorkflowAction(String actionId) throws BaseEngineException {
        try {
			return new WorkflowActionInfoXCommand(actionId).call();
        }
        catch (CommandException ex) {
            throw new BaseEngineException(ex);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.BaseEngine#dryRunSubmit(org.apache.hadoop.conf.Configuration)
     */
    @Override
    public String dryRunSubmit(Configuration conf) throws BaseEngineException {
        try {
            SubmitXCommand submit = new SubmitXCommand(true, conf);
            return submit.call();
        } catch (CommandException ex) {
            throw new DagEngineException(ex);
        }
    }

    /**
     * Return the status for a Job ID
     *
     * @param jobId job Id.
     * @return the job's status
     * @throws DagEngineException thrown if the job's status could not be obtained
     */
    @Override
    public String getJobStatus(String jobId) throws DagEngineException {
        try {
            WorkflowJobBean wfJob = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW_STATUS, jobId);
            return wfJob.getStatusStr();
        }
        catch (JPAExecutorException ex) {
            throw new DagEngineException(ex);
        }
    }

    @Override
    public void enableSLAAlert(String id, String actions, String dates, String childIds) throws BaseEngineException {
        throw new BaseEngineException(new XException(ErrorCode.E0301, "Not supported for workflow"));
    }

    @Override
    public void disableSLAAlert(String id, String actions, String dates, String childIds) throws BaseEngineException {
        throw new BaseEngineException(new XException(ErrorCode.E0301, "Not supported for workflow"));
    }

    @Override
    public void changeSLA(String id, String actions, String dates, String childIds, String newParams) throws BaseEngineException {
        throw new BaseEngineException(new XException(ErrorCode.E0301, "Not supported for workflow"));
    }

    /**
     * return the jobs that've been killed
     * @param filter Jobs that satisfy the filter will be killed
     * @param start start index in the database of jobs
     * @param len maximum number of jobs that will be killed
     * @return
     * @throws DagEngineException
     */
    public WorkflowsInfo killJobs(String filter, int start, int len) throws DagEngineException {
        try {
            Map<String, List<String>> filterList = parseFilter(filter);
            WorkflowsInfo workflowsInfo = new BulkWorkflowXCommand(filterList, start, len, OperationType.Kill).call();
            if (workflowsInfo == null) {
                return new WorkflowsInfo(new ArrayList<WorkflowJobBean>(), 0, 0, 0);
            }
            return workflowsInfo;
        }
        catch (CommandException ex) {
            throw new DagEngineException(ex);
        }
    }

    /**
     * return the jobs that've been suspended
     * @param filter Filter for jobs that will be suspended, can be name, user, group, status, id or combination of any
     * @param start Offset for the jobs that will be suspended
     * @param len maximum number of jobs that will be suspended
     * @return
     * @throws DagEngineException
     */
    public WorkflowsInfo suspendJobs(String filter, int start, int len) throws DagEngineException {
        try {
            Map<String, List<String>> filterList = parseFilter(filter);
            WorkflowsInfo workflowsInfo = new BulkWorkflowXCommand(filterList, start, len, OperationType.Suspend).call();
            if (workflowsInfo == null) {
                return new WorkflowsInfo(new ArrayList<WorkflowJobBean>(), 0, 0, 0);
            }
            return workflowsInfo;
        }
        catch (CommandException ex) {
            throw new DagEngineException(ex);
        }
    }

    /**
     * return the jobs that've been resumed
     * @param filter Filter for jobs that will be resumed, can be name, user, group, status, id or combination of any
     * @param start Offset for the jobs that will be resumed
     * @param len maximum number of jobs that will be resumed
     * @return
     * @throws DagEngineException
     */
    public WorkflowsInfo resumeJobs(String filter, int start, int len) throws DagEngineException {
        try {
            Map<String, List<String>> filterList = parseFilter(filter);
            WorkflowsInfo workflowsInfo = new BulkWorkflowXCommand(filterList, start, len, OperationType.Resume).call();
            if (workflowsInfo == null) {
                return new WorkflowsInfo(new ArrayList<WorkflowJobBean>(), 0, 0, 0);
            }
            return workflowsInfo;
        }
        catch (CommandException ex) {
            throw new DagEngineException(ex);
        }
    }
}
