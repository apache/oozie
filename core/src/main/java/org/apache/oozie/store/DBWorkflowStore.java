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

import static org.apache.oozie.util.db.SqlStatement.*;
import static org.apache.oozie.store.OozieSchema.OozieTable.*;
import static org.apache.oozie.store.OozieSchema.OozieColumn.*;

import org.apache.oozie.util.MemoryLocks.LockToken;
import org.apache.oozie.service.MemoryLocksService;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.WorkflowsInfo;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.store.OozieSchema.OozieColumn;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.WorkflowLib;
import org.apache.oozie.service.InstrumentationService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.db.SqlStatement;
import org.apache.oozie.util.db.SqlStatement.Condition;
import org.apache.oozie.util.db.SqlStatement.ResultSetReader;
import org.apache.oozie.util.db.SqlStatement.Select;

/**
 * DB Implementation of Workflow Store
 */
public class DBWorkflowStore implements WorkflowStore {
    private Connection conn;
    private WorkflowLib workflowLib;
    private boolean selectForUpdate;
    private List<LockToken> locks = null;
    private static final String INSTR_GROUP = "db";
    public static final int LOCK_TIMEOUT = 1000;

    // Following statements(INSERT_WORKFLOW, UPDATE_WORKFLOW)
    // follow the same numbering for place holders and
    // uses same function getJobValueMapfromBean for setting the values. So The
    // numbering is to be maintained if any change is made.
    private static final SqlStatement INSERT_WORKFLOW = insertInto(WORKFLOWS).value(WF_id, "1").value(WF_appName, "2")
            .value(WF_appPath, "3").value(WF_conf, "4").value(WF_protoActionConf, "5").value(WF_logToken, "6").value(
                    WF_status, "7").value(WF_run, "8").value(WF_createdTime, "9").value(WF_startTime, "10").value(
                    WF_endTime, "11").value(WF_user, "12").value(WF_groupName, "13").value(WF_authToken, "14").value(
                    WF_externalId, "15").value(WF_lastModTime, "16");

    private static final SqlStatement UPDATE_WORKFLOW = update(WORKFLOWS).set(WF_appName, "2").set(WF_appPath, "3")
            .set(WF_conf, "4").set(WF_protoActionConf, "5").set(WF_logToken, "6").set(WF_status, "7").set(WF_run, "8")
            .set(WF_createdTime, "9").set(WF_startTime, "10").set(WF_endTime, "11").set(WF_user, "12").set(
                    WF_groupName, "13").set(WF_authToken, "14").set(WF_externalId, "15").set(WF_lastModTime, "16")
            .where(isEqual(WF_id, "1"));

    private static final SqlStatement DELETE_WORKFLOW = deleteFrom(WORKFLOWS).where(isEqual(WF_id, "1"));

    private static final SqlStatement GET_WORKFLOWS = selectColumns(WF_id, WF_appName, WF_appPath, WF_conf,
            WF_protoActionConf, WF_logToken, WF_status, WF_run, WF_lastModTime, WF_createdTime, WF_startTime,
            WF_endTime, WF_user, WF_groupName, WF_authToken, WF_externalId);

    private static final SqlStatement GET_WORKFLOWS_COUNT = getCount(WORKFLOWS);

    private static final SqlStatement GET_COMPLETED_WORKFLOWS_OLDER_THAN = ((Select) GET_WORKFLOWS).where(lessThan(
            WF_endTime, "1"));

    private static final SqlStatement GET_WORKFLOW = ((Select) GET_WORKFLOWS).where(isEqual(WF_id, "1"));

    private static final SqlStatement GET_WORKFLOW_FOR_UPDATE = ((Select) GET_WORKFLOW).forUpdate();

    private static final SqlStatement GET_WORKFLOW_ID_FOR_EXTERNAL_ID = selectColumns(WF_id).where(
            isEqual(WF_externalId, "1"));

    private static final SqlStatement GET_WORKFLOWS_COUNT_WITH_STATUS = ((Select) GET_WORKFLOWS_COUNT).where(isEqual(
            WF_status, "1"));

    private static final SqlStatement GET_WORKFLOWS_COUNT_WITH_STATUS_IN_LAST_N_SECS = ((Select) GET_WORKFLOWS_COUNT).where(
            and(isEqual(WF_status, "1"), greaterThan(WF_lastModTime, "2")));

    // Following statements(INSERT_ACTION, UPDATE_ACTION) follow the same
    // numbering for place holders and uses same function
    // getActionValueMapFromBean for setting the values. So The numbering is to
    // be maintained if any change is made.
    private static final SqlStatement INSERT_ACTION = insertInto(ACTIONS).value(ACTIONS_id, "1").value(ACTIONS_name,
            "2").value(ACTIONS_type, "3").value(ACTIONS_wfId, "4").value(ACTIONS_conf, "5").value(ACTIONS_status, "6")
            .value(ACTIONS_externalStatus, "7").value(ACTIONS_transition, "8").value(ACTIONS_retries, "9").value(
                    ACTIONS_startTime, "10").value(ACTIONS_data, "11").value(ACTIONS_externalId, "12").value(
                    ACTIONS_trackerUri, "13").value(ACTIONS_consoleUrl, "14").value(ACTIONS_executionPath, "15").value(
                    ACTIONS_pending, "16").value(ACTIONS_pendingAge, "17").value(ACTIONS_signalValue, "18").value(
                    ACTIONS_logToken, "19").value(ACTIONS_errorCode, "20").value(ACTIONS_errorMessage, "21").value(
                    ACTIONS_endTime, "22").value(ACTIONS_lastCheckTime, "23");

    private static final SqlStatement UPDATE_ACTION = update(ACTIONS).set(ACTIONS_name, "2").set(ACTIONS_type, "3")
            .set(ACTIONS_wfId, "4").set(ACTIONS_conf, "5").set(ACTIONS_status, "6").set(ACTIONS_externalStatus, "7")
            .set(ACTIONS_transition, "8").set(ACTIONS_retries, "9").set(ACTIONS_startTime, "10")
            .set(ACTIONS_data, "11").set(ACTIONS_externalId, "12").set(ACTIONS_trackerUri, "13").set(
                    ACTIONS_consoleUrl, "14").set(ACTIONS_executionPath, "15").set(ACTIONS_pending, "16").set(
                    ACTIONS_pendingAge, "17").set(ACTIONS_signalValue, "18").set(ACTIONS_logToken, "19").set(
                    ACTIONS_errorCode, "20").set(ACTIONS_errorMessage, "21").set(ACTIONS_endTime, "22").set(
                    ACTIONS_lastCheckTime, "23").where(isEqual(ACTIONS_id, "1"));

    private static final SqlStatement DELETE_ACTION = deleteFrom(ACTIONS).where(isEqual(ACTIONS_id, "1"));

    private static final SqlStatement DELETE_ACTIONS_FOR_WORKFLOW = deleteFrom(ACTIONS).where(
            isEqual(ACTIONS_wfId, "1"));

    private static final SqlStatement GET_ACTIONS = selectColumns(ACTIONS_id, ACTIONS_name, ACTIONS_type, ACTIONS_wfId,
            ACTIONS_conf, ACTIONS_status, ACTIONS_externalStatus, ACTIONS_transition, ACTIONS_errorCode,
            ACTIONS_errorMessage, ACTIONS_retries, ACTIONS_startTime, ACTIONS_data, ACTIONS_externalId,
            ACTIONS_trackerUri, ACTIONS_consoleUrl, ACTIONS_executionPath, ACTIONS_pending, ACTIONS_pendingAge,
            ACTIONS_signalValue, ACTIONS_logToken, ACTIONS_endTime, ACTIONS_lastCheckTime);

    private static final SqlStatement GET_ACTION = ((Select) GET_ACTIONS).where(isEqual(ACTIONS_id, "1"));

    private static final SqlStatement GET_ACTION_FOR_UPDATE = ((Select) GET_ACTION).forUpdate();

    private static final SqlStatement GET_ACTIONS_FOR_WORKFLOW = ((Select) GET_ACTIONS).where(
            isEqual(ACTIONS_wfId, "1")).orderBy(ACTIONS_startTime, false);

    private static final SqlStatement GET_ACTIONS_OF_WORKFLOW_FOR_UPDATE = ((Select) GET_ACTIONS_FOR_WORKFLOW)
            .forUpdate();

    private static final SqlStatement GET_PENDING_ACTIONS = ((Select) GET_ACTIONS).where(and(isEqual(ACTIONS_pending,
            true), lessThan(ACTIONS_pendingAge, "1")));

    private static final SqlStatement GET_RUNNING_ACTIONS = ((Select) GET_ACTIONS).where(and(isEqual(ACTIONS_pending,
            true), isEqual(ACTIONS_status, WorkflowActionBean.Status.RUNNING.toString()), lessThan(ACTIONS_lastCheckTime, "1")));
    
    private PreparedStatement prepInsertWorkflow;
    private PreparedStatement prepUpdateWorkflow;
    private PreparedStatement prepDeleteWorkflow;
    private PreparedStatement prepGetWorkflow;
    private PreparedStatement prepGetWorkflowForUpdate;
    private PreparedStatement prepGetCompletedWorkflowsOlderThan;
    private PreparedStatement prepGetWorkflowIdForExtId;
    private PreparedStatement prepGetWorkflowsCountWithStatus;
    private PreparedStatement prepGetWorkflowsCountWithStatusInLastNSecs;

    private PreparedStatement prepInsertAction;
    private PreparedStatement prepUpdateAction;
    private PreparedStatement prepDeleteAction;
    private PreparedStatement prepDeleteActionsForWorkflow;
    private PreparedStatement prepGetAction;
    private PreparedStatement prepGetActionForUpdate;
    private PreparedStatement prepGetActionsForWorkflow;
    private PreparedStatement prepGetActionsForWorkflowForUpdate;
    private PreparedStatement prepGetPendingActions;
    private PreparedStatement prepGetRunningActions;

    public DBWorkflowStore(Connection connection, WorkflowLib wfLib, boolean selectForUpdate) throws StoreException {
        conn = ParamChecker.notNull(connection, "conn");
        workflowLib = wfLib;
        this.selectForUpdate = selectForUpdate;
        if (!selectForUpdate) {
            locks = new ArrayList<LockToken>();
        }
    }

    /**
     * Create a Workflow and return a WorkflowBean. It also creates the process
     * instance for the job.
     * 
     * @param workflow workflow bean
     * @throws StoreException
     */
    public void insertWorkflow(final WorkflowJobBean workflow) throws StoreException {
        ParamChecker.notNull(workflow, "workflow");

        doOperation("insertWorkflow", new Callable<Void>() {
                    public Void call() throws SQLException, StoreException, WorkflowException {
                        if (prepInsertWorkflow == null) {
                            prepInsertWorkflow = INSERT_WORKFLOW.prepare(conn);
                        }
                        INSERT_WORKFLOW.getNewStatementWithValues(getJobValueMapfromBean(workflow)).prepare(
                                prepInsertWorkflow).executeUpdate();
                        workflowLib.insert(workflow.getWorkflowInstance());
                        return null;
                    }
                });
    }

    /**
     * Load the Workflow into a Bean and return it. Also load the Workflow
     * Instance into the bean. And lock the Workflow depending on the locking
     * parameter.
     * 
     * @param id Workflow ID
     * @param locking true if Workflow is to be locked
     * @return
     * @throws StoreException
     */
    public WorkflowJobBean getWorkflow(final String id, final boolean locking) throws StoreException {
        ParamChecker.notEmpty(id, "WorkflowID");
        WorkflowJobBean wfBean = doOperation("getWorkflow", new Callable<WorkflowJobBean>() {
                    public WorkflowJobBean call() throws SQLException, StoreException, WorkflowException,
                            InterruptedException {
                        WorkflowJobBean wfBean = null;
                        wfBean = getWorkflowOnly(id, locking);
                        if (wfBean == null) {
                            throw new StoreException(ErrorCode.E0604, id);
                        }
                        WorkflowInstance wfInstance;
                        wfInstance = workflowLib.get(id);
                        wfBean.setWorkflowInstance(wfInstance);
                        return wfBean;
                    }
                });
        return wfBean;
    }

    /**
     * Get the number of Workflows with the given status.
     * 
     * @param status Workflow Status.
     * @return number of Workflows with given status.
     * @throws StoreException
     */
    @Override
    public int getWorkflowCountWithStatus(final String status) throws StoreException {
        ParamChecker.notEmpty(status, "status");
        Integer cnt = doOperation("getWorkflowCountWithStatus", new Callable<Integer>() {
            public Integer call() throws SQLException {
                if (prepGetWorkflowsCountWithStatus == null) {
                    prepGetWorkflowsCountWithStatus = GET_WORKFLOWS_COUNT_WITH_STATUS.prepare(conn);
                }
                Map<Object, Object> values = new HashMap<Object, Object>();
                values.put("1", status);
                ResultSet rsCount = GET_WORKFLOWS_COUNT_WITH_STATUS.getNewStatementWithValues(values).prepare(
                        prepGetWorkflowsCountWithStatus).executeQuery();
                if (rsCount.next()) {
                    return rsCount.getInt(1);
                }
                return 0;
            }
        });
        return cnt.intValue();
    }

    /**
     * Get the number of Workflows with the given status which was modified in given time limit.
     * 
     * @param status Workflow Status.
     * @param secs No. of seconds within which the workflow got modified.
     * @return number of Workflows modified within given time with given status.
     * @throws StoreException
     */
    @Override
    public int getWorkflowCountWithStatusInLastNSeconds(final String status, final int secs) throws StoreException {
        ParamChecker.notEmpty(status, "status");
        ParamChecker.notEmpty(status, "secs");
        Integer cnt = doOperation("getWorkflowCountWithStatusInLastNSecs", new Callable<Integer>() {
            public Integer call() throws SQLException {
                if (prepGetWorkflowsCountWithStatusInLastNSecs == null) {
                    prepGetWorkflowsCountWithStatusInLastNSecs = GET_WORKFLOWS_COUNT_WITH_STATUS_IN_LAST_N_SECS
                            .prepare(conn);
                }
                Map<Object, Object> values = new HashMap<Object, Object>();
                values.put("1", status);
                values.put("2", new Timestamp(System.currentTimeMillis() - (secs * 1000)));
                ResultSet rsCount = GET_WORKFLOWS_COUNT_WITH_STATUS_IN_LAST_N_SECS.getNewStatementWithValues(values)
                        .prepare(prepGetWorkflowsCountWithStatusInLastNSecs).executeQuery();
                if (rsCount.next()) {
                    return rsCount.getInt(1);
                }
                return 0;
            }
        });
        return cnt.intValue();
    }

    /**
     * Update the data from Workflow Bean to DB along with the workflow instance
     * data. Action table is not updated
     * 
     * @param wfBean Workflow Bean
     * @throws StoreException If Workflow doesn't exist
     */
    public void updateWorkflow(final WorkflowJobBean wfBean) throws StoreException {
        ParamChecker.notNull(wfBean, "wfBean");
        doOperation("updateWorkflow", new Callable<Void>() {
            public Void call() throws SQLException, StoreException, WorkflowException {
                if (prepUpdateWorkflow == null) {
                    prepUpdateWorkflow = UPDATE_WORKFLOW.prepare(conn);
                }
                if (UPDATE_WORKFLOW.getNewStatementWithValues(getJobValueMapfromBean(wfBean)).prepare(
                        prepUpdateWorkflow).executeUpdate() != 1) {
                    throw new StoreException(ErrorCode.E0604, wfBean.getId());
                }
                workflowLib.update(wfBean.getWorkflowInstance());
                return null;
            }
        });
    }

    /**
     * Create a new Action record in the ACTIONS table with the given Bean.
     * 
     * @param action ActionBean
     * @throws StoreException If the action is already present
     */
    public void insertAction(final WorkflowActionBean action) throws StoreException {
        ParamChecker.notNull(action, "WorkflowActionBean");
        doOperation("insertAction", new Callable<Void>() {
            public Void call() throws SQLException, StoreException, WorkflowException {
                if (prepInsertAction == null) {
                    prepInsertAction = INSERT_ACTION.prepare(conn);
                }
                INSERT_ACTION.getNewStatementWithValues(getActionValueMapFromBean(action)).prepare(prepInsertAction)
                        .executeUpdate();
                return null;
            }
        });
    }

    /**
     * Load the action data and returns a bean.
     * 
     * @param id Action Id
     * @param locking true if the action is to be locked
     * @return Action Bean
     * @throws StoreException If action doesn't exist
     */
    public WorkflowActionBean getAction(final String id, final boolean locking) throws StoreException {
        ParamChecker.notEmpty(id, "ActionID");
        WorkflowActionBean action = doOperation("getAction", new Callable<WorkflowActionBean>() {
            public WorkflowActionBean call() throws SQLException, StoreException, WorkflowException, InterruptedException {
                WorkflowActionBean action = new WorkflowActionBean();
                ResultSet rs = null;
                Map<Object, Object> values = new HashMap<Object, Object>();
                values.put("1", id);
                if (selectForUpdate && locking) {
                    if (prepGetActionForUpdate == null) {
                        prepGetActionForUpdate = GET_ACTION_FOR_UPDATE.prepare(conn);
                    }
                    rs = GET_ACTION_FOR_UPDATE.getNewStatementWithValues(values).prepare(prepGetActionForUpdate)
                            .executeQuery();
                }
                else {
                    if (locking) {
                        LockToken token = Services.get().get(MemoryLocksService.class).getWriteLock(id, LOCK_TIMEOUT);
                        if(token == null) {
                            throw new StoreException(ErrorCode.E0606, id);
                        }
                        locks.add(token);
                    }
                    if (prepGetAction == null) {
                        prepGetAction = GET_ACTION.prepare(conn);
                    }
                    rs = GET_ACTION.getNewStatementWithValues(values).prepare(prepGetAction).executeQuery();
                }
                ResultSetReader rsReader;
                if (rs.next()) {
                    rsReader = parse(rs);
                    action = getBeanForRunningAction(rsReader);
                    rsReader.close();
                }
                else {
                    rs.close();
                    throw new StoreException(ErrorCode.E0605, id);
                }
                return action;
            }
        });
        return action;
    }

    /**
     * Update the given action bean to DB.
     * 
     * @param action Action Bean
     * @throws StoreException if action doesn't exist
     */
    public void updateAction(final WorkflowActionBean action) throws StoreException {
        ParamChecker.notNull(action, "WorkflowActionBean");
        doOperation("updateAction", new Callable<Void>() {
            public Void call() throws SQLException, StoreException, WorkflowException {
                if (prepUpdateAction == null) {
                    prepUpdateAction = UPDATE_ACTION.prepare(conn);
                }
                if (UPDATE_ACTION.getNewStatementWithValues(getActionValueMapFromBean(action))
                        .prepare(prepUpdateAction).executeUpdate() != 1) {
                    throw new StoreException(ErrorCode.E0605, action.getId());
                }
                return null;
            }
        });
    }

    /**
     * Delete the Action with given id.
     * 
     * @param id Action ID
     * @throws StoreException if Action doesn't exist
     */
    public void deleteAction(final String id) throws StoreException {
        ParamChecker.notEmpty(id, "ActionID");
        doOperation("deleteAction", new Callable<Void>() {
            public Void call() throws SQLException, StoreException, WorkflowException {
                Map<Object, Object> values = new HashMap<Object, Object>();
                values.put("1", id);
                if (prepDeleteAction == null) {
                    prepDeleteAction = DELETE_ACTION.prepare(conn);
                }
                if (DELETE_ACTION.getNewStatementWithValues(values).prepare(prepDeleteAction).executeUpdate() != 1) {
                    throw new StoreException(ErrorCode.E0605, id);
                }
                return null;
            }
        });
    }

    /**
     * Loads all the actions for the given Workflow. Also locks all the actions
     * if locking is true.
     * 
     * @param wfId Workflow ID
     * @param locking true if Actions are to be locked
     * @return A List of ActionBeans
     * @throws StoreException
     */
    public List<WorkflowActionBean> getActionsForWorkflow(final String wfId, final boolean locking) throws StoreException {
        ParamChecker.notEmpty(wfId, "WorkflowID");
        List<WorkflowActionBean> actions = doOperation("getActionsForWorkflow", new Callable<List<WorkflowActionBean>>() {
                    public List<WorkflowActionBean> call() throws SQLException, StoreException, WorkflowException,
                            InterruptedException {
                        List<WorkflowActionBean> actions = new ArrayList<WorkflowActionBean>();
                        actions.addAll(getActionsOnlyForWorkflow(wfId, locking));
                        return actions;
                    }
                });
        return actions;
    }

    /**
     * Load All the actions that are pending for more than given time.
     * 
     * @param minimumPendingAgeSecs Minimum Pending age in seconds
     * @return List of action beans
     * @throws StoreException
     */
    public List<WorkflowActionBean> getPendingActions(final long minimumPendingAgeSecs) throws StoreException {
        List<WorkflowActionBean> actions = doOperation("getPendingActions", new Callable<List<WorkflowActionBean>>() {
                    public List<WorkflowActionBean> call() throws SQLException, StoreException, WorkflowException {
                        List<WorkflowActionBean> actions = new ArrayList<WorkflowActionBean>();
                        Map<Object, Object> values = new HashMap<Object, Object>();
                        values.put("1", new Timestamp(System.currentTimeMillis() - minimumPendingAgeSecs * 1000));
                        ResultSet rs;
                        if (prepGetPendingActions == null) {
                            prepGetPendingActions = GET_PENDING_ACTIONS.prepare(conn);
                        }
                        rs = GET_PENDING_ACTIONS.getNewStatementWithValues(values).prepare(prepGetPendingActions)
                                .executeQuery();
                        ResultSetReader rsReader = parse(rs);
                        while (rsReader.next()) {
                            WorkflowActionBean action = getBeanForRunningAction(rsReader);
                            actions.add(action);
                        }
                        rsReader.close();
                        return actions;
                    }
                });
        return actions;
    }
    
    /**
     * Load All the actions that are running and were last checked after now - miminumCheckAgeSecs
     *
     * @param checkAgeSecs check age in seconds.
     * @return List of action beans.
     * @throws StoreException
     */
    public List<WorkflowActionBean> getRunningActions(final long checkAgeSecs) throws StoreException {
        List<WorkflowActionBean> actions = doOperation("getRunningActions", new Callable<List<WorkflowActionBean>>() {

                    public List<WorkflowActionBean> call() throws SQLException, StoreException, WorkflowException {
                        List<WorkflowActionBean> actions = new ArrayList<WorkflowActionBean>();
                        Map<Object, Object> values = new HashMap<Object, Object>();
                        values.put("1", new Timestamp(System.currentTimeMillis() - checkAgeSecs * 1000));
                        ResultSet rs;
                        if (prepGetRunningActions == null) {
                            prepGetRunningActions = GET_RUNNING_ACTIONS.prepare(conn);
                        }
                        rs = GET_RUNNING_ACTIONS.getNewStatementWithValues(values).prepare(prepGetRunningActions)
                                .executeQuery();
                        ResultSetReader rsReader = parse(rs);
                        while (rsReader.next()) {
                            WorkflowActionBean action = getBeanForRunningAction(rsReader);
                            actions.add(action);
                        }
                        rsReader.close();
                        return actions;
                    }
                });
        return actions;
    }

    /**
     * Loads all the jobs that are satisfying the given filter condition.
     * Filters can be applied on user, group, appName, status.
     * 
     * @param filter Filter condition
     * @param start offset for select statement
     * @param len number of Workflows to be returned
     * @return A list of workflows
     * @throws StoreException
     */
    public WorkflowsInfo getWorkflowsInfo(final Map<String, List<String>> filter, final int start, final int len)
            throws StoreException {

        WorkflowsInfo workFlowsInfo = doOperation("getWorkflowsInfo", new Callable<WorkflowsInfo>() {
            public WorkflowsInfo call() throws SQLException, StoreException {
                List<WorkflowJobBean> wfBeans = new ArrayList<WorkflowJobBean>();

                SqlStatement s = GET_WORKFLOWS;
                SqlStatement countQuery = GET_WORKFLOWS_COUNT;
                List<Condition> andArray = new ArrayList<Condition>();
                for (Map.Entry<String, List<String>> entry : filter.entrySet()) {
                    OozieColumn col = null;
                    if (entry.getKey().equals(OozieClient.FILTER_GROUP)) {
                        col = OozieColumn.WF_groupName;
                    }
                    else if (entry.getKey().equals(OozieClient.FILTER_NAME)) {
                        col = OozieColumn.WF_appName;
                    }
                    else if (entry.getKey().equals(OozieClient.FILTER_STATUS)) {
                        col = OozieColumn.WF_status;
                    }
                    else if (entry.getKey().equals(OozieClient.FILTER_USER)) {
                        col = OozieColumn.WF_user;
                    }
                    if (col != null) {
                        List<Condition> orArray = new ArrayList<Condition>();
                        for (String val : entry.getValue()) {
                            if (col.equals(OozieColumn.WF_status)) {
                                val = val.toUpperCase();
                            }
                            orArray.add(isEqual(col, val));
                        }
                        if (orArray.size() > 1) {
                            andArray.add(or(orArray.toArray(new Condition[orArray.size()])));
                        }
                        else if (orArray.size() == 1) {
                            andArray.add(orArray.get(0));
                        }
                    }
                }
                if (andArray.size() > 0) {
                    s = ((Select) s).where(and(andArray.toArray(new Condition[andArray.size()])));
                    countQuery = ((Select) countQuery).where(and(andArray.toArray(new Condition[andArray.size()])));
                }
                s = ((Select) s).orderBy(WF_startTime, false, WF_endTime, false);
                if ((start > 0) && (len > 0)) {
                    s = ((Select) s).limit(start-1, len);
                }

                int count = 0;
                ResultSet rsCount = countQuery.prepareAndSetValues(conn).executeQuery();
                if (rsCount.next()) {
                    count = rsCount.getInt(1);
                }

                int realLen = 0;
                ResultSetReader rsReader = parse(s.prepareAndSetValues(conn).executeQuery());
                while (rsReader.next()) {
                    WorkflowJobBean wf = getBeanForRunningWorkflow(rsReader);
                    wfBeans.add(wf);
                    realLen++;
                }
                return new WorkflowsInfo(wfBeans, start, realLen, count);
            }
        });
        return workFlowsInfo;
    }


    /**
     * Load the Workflow and Action details and return a WorkflowBean. Workflow
     * Instance is not loaded
     * 
     * @param id Workflow Id
     * @return Workflow Bean
     * @throws StoreException If Workflow doesn't exist
     */
    public WorkflowJobBean getWorkflowInfo(final String id) throws StoreException {
        ParamChecker.notEmpty(id, "WorkflowID");
        WorkflowJobBean wfBean = doOperation("getWorkflowInfo", new Callable<WorkflowJobBean>() {
                    public WorkflowJobBean call() throws SQLException, StoreException, InterruptedException {
                        WorkflowJobBean wfBean = null;
                        wfBean = getWorkflowOnly(id, false);
                        if (wfBean == null) {
                            throw new StoreException(ErrorCode.E0604, id);
                        }
                        else {
                            wfBean.setActions(getActionsOnlyForWorkflow(id, false));
                        }
                        return wfBean;
                    }
                });
        return wfBean;
    }

    /**
     * Get the Workflow ID with given external ID which will be assigned for the
     * subworkflows.
     * 
     * @param externalId external ID
     * @return Workflow ID
     * @throws StoreException if there is no job with external ID
     */
    public String getWorkflowIdForExternalId(final String externalId) throws StoreException {
        ParamChecker.notEmpty(externalId, "externalId");
        String wfId = doOperation("getWorkflowIdForExternalId", new Callable<String>() {
                    public String call() throws SQLException, StoreException {
                        String id = "";
                        if (prepGetWorkflowIdForExtId == null) {
                            prepGetWorkflowIdForExtId = GET_WORKFLOW_ID_FOR_EXTERNAL_ID.prepare(conn);
                        }
                        Map<Object, Object> values = new HashMap<Object, Object>();
                        //TODO add current user to the where clause
                        values.put("1", externalId);
                        ResultSetReader rsReader = parse(GET_WORKFLOW_ID_FOR_EXTERNAL_ID.getNewStatementWithValues(
                                values).prepare(prepGetWorkflowIdForExtId).executeQuery());
                        if (rsReader.next()) {
                            id = rsReader.getString(WF_id);
                        }
                        return id;
                    }
                });
        return wfId;
    }

    private static final long DAY_IN_MS = 24 * 60 * 60 * 1000;

    /**
     * Purge the Workflows Completed older than given days.
     * 
     * @param olderThanDays number of days for which to preserve the workflows
     * @throws StoreException
     */
    public void purge(final long olderThanDays) throws StoreException {
        doOperation("purge", new Callable<Void>() {
            public Void call() throws SQLException, StoreException, WorkflowException {
                Timestamp maxEndTime = new Timestamp(System.currentTimeMillis() - (olderThanDays * DAY_IN_MS));
                Map<Object, Object> values = new HashMap<Object, Object>();
                values.put("1", maxEndTime);
                if (prepGetCompletedWorkflowsOlderThan == null) {
                    prepGetCompletedWorkflowsOlderThan = GET_COMPLETED_WORKFLOWS_OLDER_THAN.prepare(conn);
                }
                if (prepDeleteWorkflow == null) {
                    prepDeleteWorkflow = DELETE_WORKFLOW.prepare(conn);
                }
                if (prepDeleteActionsForWorkflow == null) {
                    prepDeleteActionsForWorkflow = DELETE_ACTIONS_FOR_WORKFLOW.prepare(conn);
                }
                ResultSetReader rsReader = parse(GET_COMPLETED_WORKFLOWS_OLDER_THAN.getNewStatementWithValues(values)
                        .prepare(prepGetCompletedWorkflowsOlderThan).executeQuery());
                while (rsReader.next()) {
                    Map<Object, Object> wfIdMap = new HashMap<Object, Object>();
                    wfIdMap.put("1", rsReader.getString(WF_id));
                    DELETE_WORKFLOW.getNewStatementWithValues(wfIdMap).prepare(prepDeleteWorkflow).executeUpdate();
                    DELETE_ACTIONS_FOR_WORKFLOW.getNewStatementWithValues(wfIdMap)
                            .prepare(prepDeleteActionsForWorkflow).executeUpdate();
                }
                rsReader.close();
                return null;
            }
        });
    }

    /**
     * Commit the DB changes made.
     * 
     * @throws StoreException
     */
    public void commit() throws StoreException {
        try {
            workflowLib.commit();
            conn.commit();
        }
        catch (SQLException ex) {
            throw new StoreException(ErrorCode.E0602, ex.getMessage(), ex);
        }
        catch (WorkflowException ex) {
            throw new StoreException(ex);
        }
        finally {
            if (locks != null) {
                for (LockToken lock : locks) {
                    lock.release();
                }
                locks.clear();
            }
        }
    }

    /**
     * Close the connection.
     * 
     * @throws StoreException
     */
    public void close() throws StoreException {
        try {
            workflowLib.close();
            conn.close();

        }
        catch (SQLException ex) {
            throw new StoreException(ErrorCode.E0601, ex.getMessage(), ex);
        }
        catch (WorkflowException ex) {
            throw new StoreException(ex);
        }
        finally {
            if (locks != null) {
                for (LockToken lock : locks) {
                    lock.release();
                }
                locks.clear();
            }
        }
    }

    private <V> V doOperation(String name, Callable<V> command) throws StoreException {
        try {
            Instrumentation.Cron cron = new Instrumentation.Cron();
            cron.start();
            V retVal;
            try {
                retVal = command.call();
            }
            finally {
                cron.stop();
            }
            Services.get().get(InstrumentationService.class).get().addCron(INSTR_GROUP, name, cron);
            return retVal;
        }
        catch (StoreException ex) {
            throw ex;
        }
        catch (SQLException ex) {
            throw new StoreException(ErrorCode.E0603, name, ex.getMessage(), ex);
        }
        catch (Exception e) {
            throw new StoreException(ErrorCode.E0607, name, e.getMessage(), e);
        }
    }

    private WorkflowJobBean getWorkflowOnly(final String id, boolean locking) throws SQLException, InterruptedException,
            StoreException {
        ResultSet rs;
        Map<Object, Object> values = new HashMap<Object, Object>();
        values.put("1", id);
        if (selectForUpdate && locking) {
            if (prepGetWorkflowForUpdate == null) {
                prepGetWorkflowForUpdate = GET_WORKFLOW_FOR_UPDATE.prepare(conn);
            }
            rs = GET_WORKFLOW_FOR_UPDATE.getNewStatementWithValues(values).prepare(prepGetWorkflowForUpdate)
                    .executeQuery();
        }
        else {
            if (locking) {
                LockToken token = Services.get().get(MemoryLocksService.class).getWriteLock(id, LOCK_TIMEOUT);
                if(token == null) {
                    throw new StoreException(ErrorCode.E0606, id);
                }
                locks.add(token);
            }
            if (prepGetWorkflow == null) {
                prepGetWorkflow = GET_WORKFLOW.prepare(conn);
            }
            rs = GET_WORKFLOW.getNewStatementWithValues(values).prepare(prepGetWorkflow).executeQuery();
        }
        if (!rs.next()) {
            rs.close();
            return null;
        }
        ResultSetReader rsReader = parse(rs);
        WorkflowJobBean wfBean = getBeanForRunningWorkflow(rsReader);
        rsReader.close();
        return wfBean;
    }

    private WorkflowJobBean getBeanForRunningWorkflow(ResultSetReader rsReader) throws SQLException {
        WorkflowJobBean wfBean = new WorkflowJobBean();
        wfBean.setId(rsReader.getString(WF_id));
        wfBean.setExternalId(rsReader.getString(WF_externalId));
        wfBean.setAppName(rsReader.getString(WF_appName));
        wfBean.setAppPath(rsReader.getString(WF_appPath));
        wfBean.setConf(rsReader.getString(WF_conf));
        wfBean.setProtoActionConf(rsReader.getString(WF_protoActionConf));
        wfBean.setLogToken(rsReader.getString(WF_logToken));
        wfBean.setStatus(WorkflowJob.Status.valueOf(rsReader.getString(WF_status)));
        wfBean.setRun(rsReader.getLong(WF_run).intValue());
        wfBean.setLastModTime(rsReader.getTimestamp(WF_lastModTime));
        wfBean.setCreatedTime(rsReader.getTimestamp(WF_createdTime));
        wfBean.setStartTime(rsReader.getTimestamp(WF_startTime));
        wfBean.setEndTime(rsReader.getTimestamp(WF_endTime));
        wfBean.setUser(rsReader.getString(WF_user));
        wfBean.setGroup(rsReader.getString(WF_groupName));
        wfBean.setAuthToken(rsReader.getString(WF_authToken));
        return wfBean;
    }

    private List<WorkflowActionBean> getActionsOnlyForWorkflow(String wfId, boolean locking) throws SQLException,
            InterruptedException, StoreException {
        List<WorkflowActionBean> actions = new ArrayList<WorkflowActionBean>();
        Map<Object, Object> values = new HashMap<Object, Object>();
        values.put("1", wfId);
        ResultSet rs = null;
        if (selectForUpdate && locking) {
            if (prepGetActionsForWorkflowForUpdate == null) {
                prepGetActionsForWorkflowForUpdate = GET_ACTIONS_OF_WORKFLOW_FOR_UPDATE.prepare(conn);
            }
            rs = GET_ACTIONS_OF_WORKFLOW_FOR_UPDATE.getNewStatementWithValues(values).prepare(
                    prepGetActionsForWorkflowForUpdate).executeQuery();
        }
        else {
            if (prepGetActionsForWorkflow == null) {
                prepGetActionsForWorkflow = GET_ACTIONS_FOR_WORKFLOW.prepare(conn);
            }
            rs = GET_ACTIONS_FOR_WORKFLOW.getNewStatementWithValues(values).prepare(prepGetActionsForWorkflow)
                    .executeQuery();
        }
        ResultSetReader rsReader = parse(rs);
        while (rsReader.next()) {
            WorkflowActionBean action = getBeanForRunningAction(rsReader);
            if (locking && !selectForUpdate) {
                LockToken token = Services.get().get(MemoryLocksService.class).getWriteLock(action.getId(),
                        LOCK_TIMEOUT);
                if(token == null) {
                    throw new StoreException(ErrorCode.E0606, action.getId());
                }
                locks.add(token);
            }
            actions.add(action);
        }
        rsReader.close();
        return actions;
    }

    private WorkflowActionBean getBeanForRunningAction(ResultSetReader rsReader) throws SQLException {
        if (rsReader != null) {
            WorkflowActionBean action = new WorkflowActionBean();
            action.setId(rsReader.getString(ACTIONS_id));
            action.setName(rsReader.getString(ACTIONS_name));
            action.setType(rsReader.getString(ACTIONS_type));
            action.setJobId(rsReader.getString(ACTIONS_wfId));
            action.setConf(rsReader.getString(ACTIONS_conf));
            action.setStatus(WorkflowAction.Status.valueOf(rsReader.getString(ACTIONS_status)));
            action.setExternalStatus(rsReader.getString(ACTIONS_externalStatus));
            action.setTransition(rsReader.getString(ACTIONS_transition));
            action.setRetries(rsReader.getLong(ACTIONS_retries).intValue());
            action.setStartTime(rsReader.getTimestamp(ACTIONS_startTime));
            action.setData(rsReader.getString(ACTIONS_data));
            action.setExternalId(rsReader.getString(ACTIONS_externalId));
            action.setTrackerUri(rsReader.getString(ACTIONS_trackerUri));
            action.setConsoleUrl(rsReader.getString(ACTIONS_consoleUrl));
            action.setExecutionPath(rsReader.getString(ACTIONS_executionPath));
            if (rsReader.getBoolean(ACTIONS_pending).booleanValue() == true) {
                action.setPending();
            }
            action.setPendingAge(rsReader.getTimestamp(ACTIONS_pendingAge));
            action.setSignalValue(rsReader.getString(ACTIONS_signalValue));
            action.setLogToken(rsReader.getString(ACTIONS_logToken));
            action.setErrorInfo(rsReader.getString(ACTIONS_errorCode), rsReader.getString(ACTIONS_errorMessage));
            action.setEndTime(rsReader.getTimestamp(ACTIONS_endTime));
            action.setLastCheckTime(rsReader.getTimestamp(ACTIONS_lastCheckTime));
            return action;
        }
        return null;
    }

    private Map<Object, Object> getJobValueMapfromBean(WorkflowJobBean wfBean) throws SQLException {
        Map<Object, Object> values = new HashMap<Object, Object>();
        values.put("1", wfBean.getId());
        values.put("2", wfBean.getAppName());
        values.put("3", wfBean.getAppPath());
        values.put("4", wfBean.getConf());
        values.put("5", wfBean.getProtoActionConf());
        values.put("6", wfBean.getLogToken());
        values.put("7", wfBean.getStatus().toString());
        values.put("8", wfBean.getRun());
        values.put("9", convertDateToTimeStamp(wfBean.getCreatedTime()));
        values.put("10", convertDateToTimeStamp(wfBean.getStartTime()));
        values.put("11", convertDateToTimeStamp(wfBean.getEndTime()));
        values.put("12", wfBean.getUser());
        values.put("13", wfBean.getGroup());
        values.put("14", wfBean.getAuthToken());
        values.put("15", wfBean.getExternalId());
        values.put("16", new Timestamp(System.currentTimeMillis()));
        return values;
    }

    private Map<Object, Object> getActionValueMapFromBean(WorkflowActionBean action) throws SQLException {
        Map<Object, Object> values = new HashMap<Object, Object>();
        values.put("1", action.getId());
        values.put("2", action.getName());
        values.put("3", action.getType());
        values.put("4", action.getJobId());
        values.put("5", action.getConf());
        values.put("6", action.getStatus().toString());
        values.put("7", action.getExternalStatus());
        values.put("8", action.getTransition());
        values.put("9", action.getRetries());
        values.put("10", convertDateToTimeStamp(action.getStartTime()));
        values.put("11", action.getData());
        values.put("12", action.getExternalId());
        values.put("13", action.getTrackerUri());
        values.put("14", action.getConsoleUrl());
        values.put("15", action.getExecutionPath());
        values.put("16", action.isPending());
        values.put("17", convertDateToTimeStamp(action.getPendingAge()));
        values.put("18", action.getSignalValue());
        values.put("19", action.getLogToken());
        values.put("20", action.getErrorCode());
        values.put("21", action.getErrorMessage());
        values.put("22", convertDateToTimeStamp(action.getEndTime()));
        values.put("23", convertDateToTimeStamp(action.getLastCheckTime()));
        return values;
    }

    private Timestamp convertDateToTimeStamp(Date d) {
        if (d != null) {
            return new Timestamp(d.getTime());
        }
        return null;
    }
}
