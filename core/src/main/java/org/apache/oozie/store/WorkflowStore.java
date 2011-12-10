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
package org.apache.oozie.store;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.WorkflowsInfo;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob.Status;
import org.apache.oozie.service.InstrumentationService;
import org.apache.oozie.service.SchemaService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.SchemaService.SchemaName;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.openjpa.persistence.OpenJPAEntityManager;
import org.apache.openjpa.persistence.OpenJPAPersistence;
import org.apache.openjpa.persistence.OpenJPAQuery;
import org.apache.openjpa.persistence.jdbc.FetchDirection;
import org.apache.openjpa.persistence.jdbc.JDBCFetchPlan;
import org.apache.openjpa.persistence.jdbc.LRSSizeAlgorithm;
import org.apache.openjpa.persistence.jdbc.ResultSetType;

/**
 * DB Implementation of Workflow Store
 */
public class WorkflowStore extends Store {
    private Connection conn;
    private EntityManager entityManager;
    private boolean selectForUpdate;
    private static final String INSTR_GROUP = "db";
    public static final int LOCK_TIMEOUT = 50000;
    private static final String seletStr = "Select w.id, w.appName, w.status, w.run, w.user, w.group, w.createdTimestamp, "
            + "w.startTimestamp, w.lastModifiedTimestamp, w.endTimestamp from WorkflowJobBean w";
    private static final String countStr = "Select count(w) from WorkflowJobBean w";

    public WorkflowStore() {
    }

    public WorkflowStore(Connection connection, boolean selectForUpdate) throws StoreException {
        super();
        conn = ParamChecker.notNull(connection, "conn");
        entityManager = getEntityManager();
        this.selectForUpdate = selectForUpdate;
    }

    public WorkflowStore(Connection connection, Store store, boolean selectForUpdate) throws StoreException {
        super(store);
        conn = ParamChecker.notNull(connection, "conn");
        entityManager = getEntityManager();
        this.selectForUpdate = selectForUpdate;
    }

    public WorkflowStore(boolean selectForUpdate) throws StoreException {
        super();
        entityManager = getEntityManager();
        javax.xml.validation.Schema schema = Services.get().get(SchemaService.class).getSchema(SchemaName.WORKFLOW);
        OpenJPAEntityManager kem = OpenJPAPersistence.cast(entityManager);
        conn = (Connection) kem.getConnection();
        this.selectForUpdate = selectForUpdate;
    }

    public WorkflowStore(Store store, boolean selectForUpdate) throws StoreException {
        super(store);
        entityManager = getEntityManager();
        this.selectForUpdate = selectForUpdate;
    }

    /**
     * Create a Workflow and return a WorkflowJobBean. It also creates the process instance for the job.
     *
     * @param workflow workflow bean
     * @throws StoreException
     */

    public void insertWorkflow(final WorkflowJobBean workflow) throws StoreException {
        ParamChecker.notNull(workflow, "workflow");

        doOperation("insertWorkflow", new Callable<Void>() {
            public Void call() throws SQLException, StoreException, WorkflowException {
                entityManager.persist(workflow);
                return null;
            }
        });
    }

    /**
     * Load the Workflow into a Bean and return it. Also load the Workflow Instance into the bean. And lock the Workflow
     * depending on the locking parameter.
     *
     * @param id Workflow ID
     * @param locking true if Workflow is to be locked
     * @return WorkflowJobBean
     * @throws StoreException
     */
    public WorkflowJobBean getWorkflow(final String id, final boolean locking) throws StoreException {
        ParamChecker.notEmpty(id, "WorkflowID");
        WorkflowJobBean wfBean = doOperation("getWorkflow", new Callable<WorkflowJobBean>() {
            public WorkflowJobBean call() throws SQLException, StoreException, WorkflowException, InterruptedException {
                WorkflowJobBean wfBean = null;
                wfBean = getWorkflowOnly(id, locking);
                if (wfBean == null) {
                    throw new StoreException(ErrorCode.E0604, id);
                }
                /*
                 * WorkflowInstance wfInstance; //krishna and next line
                 * wfInstance = workflowLib.get(id); wfInstance =
                 * wfBean.get(wfBean.getWfInstance());
                 * wfBean.setWorkflowInstance(wfInstance);
                 * wfBean.setWfInstance(wfInstance);
                 */
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
    public int getWorkflowCountWithStatus(final String status) throws StoreException {
        ParamChecker.notEmpty(status, "status");
        Integer cnt = doOperation("getWorkflowCountWithStatus", new Callable<Integer>() {
            public Integer call() throws SQLException {
                Query q = entityManager.createNamedQuery("GET_WORKFLOWS_COUNT_WITH_STATUS");
                q.setParameter("status", status);
                Long count = (Long) q.getSingleResult();
                return Integer.valueOf(count.intValue());
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
    public int getWorkflowCountWithStatusInLastNSeconds(final String status, final int secs) throws StoreException {
        ParamChecker.notEmpty(status, "status");
        ParamChecker.notEmpty(status, "secs");
        Integer cnt = doOperation("getWorkflowCountWithStatusInLastNSecs", new Callable<Integer>() {
            public Integer call() throws SQLException {
                Query q = entityManager.createNamedQuery("GET_WORKFLOWS_COUNT_WITH_STATUS_IN_LAST_N_SECS");
                Timestamp ts = new Timestamp(System.currentTimeMillis() - (secs * 1000));
                q.setParameter("status", status);
                q.setParameter("lastModTime", ts);
                Long count = (Long) q.getSingleResult();
                return Integer.valueOf(count.intValue());
            }
        });
        return cnt.intValue();
    }

    /**
     * Update the data from Workflow Bean to DB along with the workflow instance data. Action table is not updated
     *
     * @param wfBean Workflow Bean
     * @throws StoreException If Workflow doesn't exist
     */
    public void updateWorkflow(final WorkflowJobBean wfBean) throws StoreException {
        ParamChecker.notNull(wfBean, "WorkflowJobBean");
        doOperation("updateWorkflow", new Callable<Void>() {
            public Void call() throws SQLException, StoreException, WorkflowException {
                Query q = entityManager.createNamedQuery("UPDATE_WORKFLOW");
                q.setParameter("id", wfBean.getId());
                setWFQueryParameters(wfBean, q);
                q.executeUpdate();
                return null;
            }
        });
    }

    /**
     * Create a new Action record in the ACTIONS table with the given Bean.
     *
     * @param action WorkflowActionBean
     * @throws StoreException If the action is already present
     */
    public void insertAction(final WorkflowActionBean action) throws StoreException {
        ParamChecker.notNull(action, "WorkflowActionBean");
        doOperation("insertAction", new Callable<Void>() {
            public Void call() throws SQLException, StoreException, WorkflowException {
                entityManager.persist(action);
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
            public WorkflowActionBean call() throws SQLException, StoreException, WorkflowException,
                    InterruptedException {
                Query q = entityManager.createNamedQuery("GET_ACTION");
                /*
                 * if (locking) { OpenJPAQuery oq = OpenJPAPersistence.cast(q);
                 * FetchPlan fetch = oq.getFetchPlan();
                 * fetch.setReadLockMode(LockModeType.WRITE);
                 * fetch.setLockTimeout(1000); // 1 seconds }
                 */
                WorkflowActionBean action = null;
                q.setParameter("id", id);
                List<WorkflowActionBean> actions = q.getResultList();
                // action = (WorkflowActionBean) q.getSingleResult();
                if (actions.size() > 0) {
                    action = actions.get(0);
                }
                else {
                    throw new StoreException(ErrorCode.E0605, id);
                }

                /*
                 * if (locking) return action; else
                 */
                // return action;
                return getBeanForRunningAction(action);
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
                Query q = entityManager.createNamedQuery("UPDATE_ACTION");
                q.setParameter("id", action.getId());
                setActionQueryParameters(action, q);
                q.executeUpdate();
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
                /*
                 * Query q = entityManager.createNamedQuery("DELETE_ACTION");
                 * q.setParameter("id", id); q.executeUpdate();
                 */
                WorkflowActionBean action = entityManager.find(WorkflowActionBean.class, id);
                if (action != null) {
                    entityManager.remove(action);
                }
                return null;
            }
        });
    }

    /**
     * Loads all the actions for the given Workflow. Also locks all the actions if locking is true.
     *
     * @param wfId Workflow ID
     * @param locking true if Actions are to be locked
     * @return A List of WorkflowActionBean
     * @throws StoreException
     */
    public List<WorkflowActionBean> getActionsForWorkflow(final String wfId, final boolean locking)
            throws StoreException {
        ParamChecker.notEmpty(wfId, "WorkflowID");
        List<WorkflowActionBean> actions = doOperation("getActionsForWorkflow",
                                                       new Callable<List<WorkflowActionBean>>() {
                                                           public List<WorkflowActionBean> call() throws SQLException, StoreException, WorkflowException,
                                                                   InterruptedException {
                                                               List<WorkflowActionBean> actions;
                                                               List<WorkflowActionBean> actionList = new ArrayList<WorkflowActionBean>();
                                                               try {
                                                                   Query q = entityManager.createNamedQuery("GET_ACTIONS_FOR_WORKFLOW");

                                                                   /*
                                                                   * OpenJPAQuery oq = OpenJPAPersistence.cast(q);
                                                                   * if (locking) { //
                                                                   * q.setHint("openjpa.FetchPlan.ReadLockMode"
                                                                   * ,"WRITE"); FetchPlan fetch = oq.getFetchPlan();
                                                                   * fetch.setReadLockMode(LockModeType.WRITE);
                                                                   * fetch.setLockTimeout(1000); // 1 seconds }
                                                                   */
                                                                   q.setParameter("wfId", wfId);
                                                                   actions = q.getResultList();
                                                                   for (WorkflowActionBean a : actions) {
                                                                       WorkflowActionBean aa = getBeanForRunningAction(a);
                                                                       actionList.add(aa);
                                                                   }
                                                               }
                                                               catch (IllegalStateException e) {
                                                                   throw new StoreException(ErrorCode.E0601, e.getMessage(), e);
                                                               }
                                                               /*
                                                               * if (locking) { return actions; } else {
                                                               */
                                                               return actionList;
                                                               // }
                                                           }
                                                       });
        return actions;
    }

    /**
     * Loads given number of actions for the given Workflow. Also locks all the actions if locking is true.
     *
     * @param wfId Workflow ID
     * @param start offset for select statement
     * @param len number of Workflow Actions to be returned
     * @param locking true if Actions are to be locked
     * @return A List of WorkflowActionBean
     * @throws StoreException
     */
    public List<WorkflowActionBean> getActionsSubsetForWorkflow(final String wfId, final int start, final int len)
            throws StoreException {
        ParamChecker.notEmpty(wfId, "WorkflowID");
        List<WorkflowActionBean> actions = doOperation("getActionsForWorkflow",
                                                       new Callable<List<WorkflowActionBean>>() {
                                                           public List<WorkflowActionBean> call() throws SQLException, StoreException, WorkflowException,
                                                                   InterruptedException {
                                                               List<WorkflowActionBean> actions;
                                                               List<WorkflowActionBean> actionList = new ArrayList<WorkflowActionBean>();
                                                               try {
                                                                   Query q = entityManager.createNamedQuery("GET_ACTIONS_FOR_WORKFLOW");
                                                                   OpenJPAQuery oq = OpenJPAPersistence.cast(q);
                                                                   q.setParameter("wfId", wfId);
                                                                   q.setFirstResult(start - 1);
                                                                   q.setMaxResults(len);
                                                                   actions = q.getResultList();
                                                                   for (WorkflowActionBean a : actions) {
                                                                       WorkflowActionBean aa = getBeanForRunningAction(a);
                                                                       actionList.add(aa);
                                                                   }
                                                               }
                                                               catch (IllegalStateException e) {
                                                                   throw new StoreException(ErrorCode.E0601, e.getMessage(), e);
                                                               }
                                                               return actionList;
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
                Timestamp ts = new Timestamp(System.currentTimeMillis() - minimumPendingAgeSecs * 1000);
                List<WorkflowActionBean> actionList = null;
                try {
                    Query q = entityManager.createNamedQuery("GET_PENDING_ACTIONS");
                    q.setParameter("pendingAge", ts);
                    actionList = q.getResultList();
                }
                catch (IllegalStateException e) {
                    throw new StoreException(ErrorCode.E0601, e.getMessage(), e);
                }
                return actionList;
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
                Timestamp ts = new Timestamp(System.currentTimeMillis() - checkAgeSecs * 1000);
                try {
                    Query q = entityManager.createNamedQuery("GET_RUNNING_ACTIONS");
                    q.setParameter("lastCheckTime", ts);
                    actions = q.getResultList();
                }
                catch (IllegalStateException e) {
                    throw new StoreException(ErrorCode.E0601, e.getMessage(), e);
                }

                return actions;
            }
        });
        return actions;
    }

    /**
     * Load All the actions that are START_RETRY or START_MANUAL or END_RETRY or END_MANUAL.
     * 
     * @param wfId String
     * @return List of action beans
     * @throws StoreException
     */
    public List<WorkflowActionBean> getRetryAndManualActions(final String wfId) throws StoreException {
        List<WorkflowActionBean> actions = doOperation("GET_RETRY_MANUAL_ACTIONS",
                new Callable<List<WorkflowActionBean>>() {
                    public List<WorkflowActionBean> call() throws SQLException, StoreException, WorkflowException {
                        List<WorkflowActionBean> actionList = null;
                        try {
                            Query q = entityManager.createNamedQuery("GET_RETRY_MANUAL_ACTIONS");
                            q.setParameter("wfId", wfId);
                            actionList = q.getResultList();
                        }
                        catch (IllegalStateException e) {
                            throw new StoreException(ErrorCode.E0601, e.getMessage(), e);
                        }

                        return actionList;
                    }
                });
        return actions;
    }

    /**
     * Loads all the jobs that are satisfying the given filter condition. Filters can be applied on user, group,
     * appName, status.
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
            @SuppressWarnings("unchecked")
            public WorkflowsInfo call() throws SQLException, StoreException {

                List<String> orArray = new ArrayList<String>();
                List<String> colArray = new ArrayList<String>();
                List<String> valArray = new ArrayList<String>();
                StringBuilder sb = new StringBuilder("");
                boolean isStatus = false;
                boolean isGroup = false;
                boolean isAppName = false;
                boolean isUser = false;
                boolean isEnabled = false;
                int index = 0;
                for (Map.Entry<String, List<String>> entry : filter.entrySet()) {
                    String colName = null;
                    String colVar = null;
                    if (entry.getKey().equals(OozieClient.FILTER_GROUP)) {
                        List<String> values = filter.get(OozieClient.FILTER_GROUP);
                        colName = "group";
                        for (int i = 0; i < values.size(); i++) {
                            colVar = "group";
                            colVar = colVar + index;
                            if (!isEnabled && !isGroup) {
                                sb.append(seletStr).append(" where w.group IN (:group" + index);
                                isGroup = true;
                                isEnabled = true;
                            }
                            else {
                                if (isEnabled && !isGroup) {
                                    sb.append(" and w.group IN (:group" + index);
                                    isGroup = true;
                                }
                                else {
                                    if (isGroup) {
                                        sb.append(", :group" + index);
                                    }
                                }
                            }
                            if (i == values.size() - 1) {
                                sb.append(")");
                            }
                            index++;
                            valArray.add(values.get(i));
                            orArray.add(colName);
                            colArray.add(colVar);
                        }
                    }
                    else {
                        if (entry.getKey().equals(OozieClient.FILTER_STATUS)) {
                            List<String> values = filter.get(OozieClient.FILTER_STATUS);
                            colName = "status";
                            for (int i = 0; i < values.size(); i++) {
                                colVar = "status";
                                colVar = colVar + index;
                                if (!isEnabled && !isStatus) {
                                    sb.append(seletStr).append(" where w.status IN (:status" + index);
                                    isStatus = true;
                                    isEnabled = true;
                                }
                                else {
                                    if (isEnabled && !isStatus) {
                                        sb.append(" and w.status IN (:status" + index);
                                        isStatus = true;
                                    }
                                    else {
                                        if (isStatus) {
                                            sb.append(", :status" + index);
                                        }
                                    }
                                }
                                if (i == values.size() - 1) {
                                    sb.append(")");
                                }
                                index++;
                                valArray.add(values.get(i));
                                orArray.add(colName);
                                colArray.add(colVar);
                            }
                        }
                        else {
                            if (entry.getKey().equals(OozieClient.FILTER_NAME)) {
                                List<String> values = filter.get(OozieClient.FILTER_NAME);
                                colName = "appName";
                                for (int i = 0; i < values.size(); i++) {
                                    colVar = "appName";
                                    colVar = colVar + index;
                                    if (!isEnabled && !isAppName) {
                                        sb.append(seletStr).append(" where w.appName IN (:appName" + index);
                                        isAppName = true;
                                        isEnabled = true;
                                    }
                                    else {
                                        if (isEnabled && !isAppName) {
                                            sb.append(" and w.appName IN (:appName" + index);
                                            isAppName = true;
                                        }
                                        else {
                                            if (isAppName) {
                                                sb.append(", :appName" + index);
                                            }
                                        }
                                    }
                                    if (i == values.size() - 1) {
                                        sb.append(")");
                                    }
                                    index++;
                                    valArray.add(values.get(i));
                                    orArray.add(colName);
                                    colArray.add(colVar);
                                }
                            }
                            else {
                                if (entry.getKey().equals(OozieClient.FILTER_USER)) {
                                    List<String> values = filter.get(OozieClient.FILTER_USER);
                                    colName = "user";
                                    for (int i = 0; i < values.size(); i++) {
                                        colVar = "user";
                                        colVar = colVar + index;
                                        if (!isEnabled && !isUser) {
                                            sb.append(seletStr).append(" where w.user IN (:user" + index);
                                            isUser = true;
                                            isEnabled = true;
                                        }
                                        else {
                                            if (isEnabled && !isUser) {
                                                sb.append(" and w.user IN (:user" + index);
                                                isUser = true;
                                            }
                                            else {
                                                if (isUser) {
                                                    sb.append(", :user" + index);
                                                }
                                            }
                                        }
                                        if (i == values.size() - 1) {
                                            sb.append(")");
                                        }
                                        index++;
                                        valArray.add(values.get(i));
                                        orArray.add(colName);
                                        colArray.add(colVar);
                                    }
                                }
                            }
                        }
                    }
                }

                int realLen = 0;

                Query q = null;
                Query qTotal = null;
                if (orArray.size() == 0) {
                    q = entityManager.createNamedQuery("GET_WORKFLOWS_COLUMNS");
                    q.setFirstResult(start - 1);
                    q.setMaxResults(len);
                    qTotal = entityManager.createNamedQuery("GET_WORKFLOWS_COUNT");
                }
                else {
                    if (orArray.size() > 0) {
                        StringBuilder sbTotal = new StringBuilder(sb);
                        sb.append(" order by w.startTimestamp desc ");
                        XLog.getLog(getClass()).debug("Created String is **** " + sb.toString());
                        q = entityManager.createQuery(sb.toString());
                        q.setFirstResult(start - 1);
                        q.setMaxResults(len);
                        qTotal = entityManager.createQuery(sbTotal.toString().replace(seletStr, countStr));
                        for (int i = 0; i < orArray.size(); i++) {
                            q.setParameter(colArray.get(i), valArray.get(i));
                            qTotal.setParameter(colArray.get(i), valArray.get(i));
                        }
                    }
                }

                OpenJPAQuery kq = OpenJPAPersistence.cast(q);
                JDBCFetchPlan fetch = (JDBCFetchPlan) kq.getFetchPlan();
                fetch.setFetchBatchSize(20);
                fetch.setResultSetType(ResultSetType.SCROLL_INSENSITIVE);
                fetch.setFetchDirection(FetchDirection.FORWARD);
                fetch.setLRSSizeAlgorithm(LRSSizeAlgorithm.LAST);
                List<?> resultList = q.getResultList();
                List<Object[]> objectArrList = (List<Object[]>) resultList;
                List<WorkflowJobBean> wfBeansList = new ArrayList<WorkflowJobBean>();

                for (Object[] arr : objectArrList) {
                    WorkflowJobBean ww = getBeanForWorkflowFromArray(arr);
                    wfBeansList.add(ww);
                }

                realLen = ((Long) qTotal.getSingleResult()).intValue();

                return new WorkflowsInfo(wfBeansList, start, len, realLen);
            }
        });
        return workFlowsInfo;

    }

    /**
     * Load the Workflow and all Action details and return a WorkflowJobBean. Workflow Instance is not loaded
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
                wfBean = getWorkflowforInfo(id, false);
                if (wfBean == null) {
                    throw new StoreException(ErrorCode.E0604, id);
                }
                else {
                    wfBean.setActions(getActionsForWorkflow(id, false));
                }
                return wfBean;
            }
        });
        return wfBean;
    }

    /**
     * Load the Workflow and subset Actions details and return a WorkflowJobBean. Workflow Instance is not loaded
     *
     * @param id Workflow Id
     * @param start offset for select statement for actions
     * @param len number of Workflow Actions to be returned
     * @return Workflow Bean
     * @throws StoreException If Workflow doesn't exist
     */
    public WorkflowJobBean getWorkflowInfoWithActionsSubset(final String id, final int start, final int len) throws StoreException {
        ParamChecker.notEmpty(id, "WorkflowID");
        WorkflowJobBean wfBean = doOperation("getWorkflowInfo", new Callable<WorkflowJobBean>() {
            public WorkflowJobBean call() throws SQLException, StoreException, InterruptedException {
                WorkflowJobBean wfBean = null;
                wfBean = getWorkflowforInfo(id, false);
                if (wfBean == null) {
                    throw new StoreException(ErrorCode.E0604, id);
                }
                else {
                    wfBean.setActions(getActionsSubsetForWorkflow(id, start, len));
                }
                return wfBean;
            }
        });
        return wfBean;
    }

    /**
     * Get the Workflow ID with given external ID which will be assigned for the subworkflows.
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
                Query q = entityManager.createNamedQuery("GET_WORKFLOW_ID_FOR_EXTERNAL_ID");
                q.setParameter("externalId", externalId);
                List<String> w = q.getResultList();
                if (w.size() == 0) {
                    id = "";
                }
                else {
                    int index = w.size() - 1;
                    id = w.get(index);
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
    public void purge(final long olderThanDays, final int limit) throws StoreException {
        doOperation("purge", new Callable<Void>() {
            public Void call() throws SQLException, StoreException, WorkflowException {
                Timestamp maxEndTime = new Timestamp(System.currentTimeMillis() - (olderThanDays * DAY_IN_MS));
                Query q = entityManager.createNamedQuery("GET_COMPLETED_WORKFLOWS_OLDER_THAN");
                q.setParameter("endTime", maxEndTime);
                q.setMaxResults(limit);
                List<WorkflowJobBean> workflows = q.getResultList();
                int actionDeleted = 0;
                if (workflows.size() != 0) {
                    for (WorkflowJobBean w : workflows) {
                        String wfId = w.getId();
                        entityManager.remove(w);
                        Query g = entityManager.createNamedQuery("DELETE_ACTIONS_FOR_WORKFLOW");
                        g.setParameter("wfId", wfId);
                        actionDeleted += g.executeUpdate();
                    }
                }
                XLog.getLog(getClass()).debug("ENDED Workflow Purge deleted jobs :" + workflows.size() + " and actions " + actionDeleted);
                return null;
            }
        });
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

    private WorkflowJobBean getWorkflowOnly(final String id, boolean locking) throws SQLException,
            InterruptedException, StoreException {
        WorkflowJobBean wfBean = null;
        Query q = entityManager.createNamedQuery("GET_WORKFLOW");
        /*
         * if (locking) { // q.setHint("openjpa.FetchPlan.ReadLockMode","READ");
         * OpenJPAQuery oq = OpenJPAPersistence.cast(q); FetchPlan fetch =
         * oq.getFetchPlan(); fetch.setReadLockMode(LockModeType.WRITE);
         * fetch.setLockTimeout(-1); // unlimited }
         */
        q.setParameter("id", id);
        List<WorkflowJobBean> w = q.getResultList();
        if (w.size() > 0) {
            wfBean = w.get(0);
        }
        return wfBean;
        // return getBeanForRunningWorkflow(wfBean);
    }

    private WorkflowJobBean getWorkflowforInfo(final String id, boolean locking) throws SQLException,
            InterruptedException, StoreException {
        WorkflowJobBean wfBean = null;
        Query q = entityManager.createNamedQuery("GET_WORKFLOW");
        q.setParameter("id", id);
        List<WorkflowJobBean> w = q.getResultList();
        if (w.size() > 0) {
            wfBean = w.get(0);
            return getBeanForRunningWorkflow(wfBean);
        }
        return null;
    }

    private WorkflowJobBean getBeanForRunningWorkflow(WorkflowJobBean w) throws SQLException {
        WorkflowJobBean wfBean = new WorkflowJobBean();
        wfBean.setId(w.getId());
        wfBean.setAppName(w.getAppName());
        wfBean.setAppPath(w.getAppPath());
        wfBean.setConf(w.getConf());
        wfBean.setGroup(w.getGroup());
        wfBean.setRun(w.getRun());
        wfBean.setUser(w.getUser());
        wfBean.setAuthToken(w.getAuthToken());
        wfBean.setCreatedTime(w.getCreatedTime());
        wfBean.setEndTime(w.getEndTime());
        wfBean.setExternalId(w.getExternalId());
        wfBean.setLastModifiedTime(w.getLastModifiedTime());
        wfBean.setLogToken(w.getLogToken());
        wfBean.setProtoActionConf(w.getProtoActionConf());
        wfBean.setSlaXml(w.getSlaXml());
        wfBean.setStartTime(w.getStartTime());
        wfBean.setStatus(w.getStatus());
        wfBean.setWfInstance(w.getWfInstance());
        return wfBean;
    }

    private WorkflowJobBean getBeanForWorkflowFromArray(Object[] arr) {

        WorkflowJobBean wfBean = new WorkflowJobBean();
        wfBean.setId((String) arr[0]);
        if (arr[1] != null) {
            wfBean.setAppName((String) arr[1]);
        }
        if (arr[2] != null) {
            wfBean.setStatus(Status.valueOf((String) arr[2]));
        }
        if (arr[3] != null) {
            wfBean.setRun((Integer) arr[3]);
        }
        if (arr[4] != null) {
            wfBean.setUser((String) arr[4]);
        }
        if (arr[5] != null) {
            wfBean.setGroup((String) arr[5]);
        }
        if (arr[6] != null) {
            wfBean.setCreatedTime((Timestamp) arr[6]);
        }
        if (arr[7] != null) {
            wfBean.setStartTime((Timestamp) arr[7]);
        }
        if (arr[8] != null) {
            wfBean.setLastModifiedTime((Timestamp) arr[8]);
        }
        if (arr[9] != null) {
            wfBean.setEndTime((Timestamp) arr[9]);
        }
        return wfBean;
    }

    private WorkflowActionBean getBeanForRunningAction(WorkflowActionBean a) throws SQLException {
        if (a != null) {
            WorkflowActionBean action = new WorkflowActionBean();
            action.setId(a.getId());
            action.setConf(a.getConf());
            action.setConsoleUrl(a.getConsoleUrl());
            action.setData(a.getData());
            action.setStats(a.getStats());
            action.setExternalChildIDs(a.getExternalChildIDs());
            action.setErrorInfo(a.getErrorCode(), a.getErrorMessage());
            action.setExternalId(a.getExternalId());
            action.setExternalStatus(a.getExternalStatus());
            action.setName(a.getName());
            action.setCred(a.getCred());
            action.setRetries(a.getRetries());
            action.setTrackerUri(a.getTrackerUri());
            action.setTransition(a.getTransition());
            action.setType(a.getType());
            action.setEndTime(a.getEndTime());
            action.setExecutionPath(a.getExecutionPath());
            action.setLastCheckTime(a.getLastCheckTime());
            action.setLogToken(a.getLogToken());
            if (a.getPending() == true) {
                action.setPending();
            }
            action.setPendingAge(a.getPendingAge());
            action.setSignalValue(a.getSignalValue());
            action.setSlaXml(a.getSlaXml());
            action.setStartTime(a.getStartTime());
            action.setStatus(a.getStatus());
            action.setJobId(a.getWfId());
            action.setUserRetryCount(a.getUserRetryCount());
            action.setUserRetryInterval(a.getUserRetryInterval());
            action.setUserRetryMax(a.getUserRetryMax());
            return action;
        }
        return null;
    }

    private void setWFQueryParameters(WorkflowJobBean wfBean, Query q) {
        q.setParameter("appName", wfBean.getAppName());
        q.setParameter("appPath", wfBean.getAppPath());
        q.setParameter("conf", wfBean.getConf());
        q.setParameter("groupName", wfBean.getGroup());
        q.setParameter("run", wfBean.getRun());
        q.setParameter("user", wfBean.getUser());
        q.setParameter("authToken", wfBean.getAuthToken());
        q.setParameter("createdTime", wfBean.getCreatedTimestamp());
        q.setParameter("endTime", wfBean.getEndTimestamp());
        q.setParameter("externalId", wfBean.getExternalId());
        q.setParameter("lastModTime", new Date());
        q.setParameter("logToken", wfBean.getLogToken());
        q.setParameter("protoActionConf", wfBean.getProtoActionConf());
        q.setParameter("slaXml", wfBean.getSlaXml());
        q.setParameter("startTime", wfBean.getStartTimestamp());
        q.setParameter("status", wfBean.getStatusStr());
        q.setParameter("wfInstance", wfBean.getWfInstance());
    }

    private void setActionQueryParameters(WorkflowActionBean aBean, Query q) {
        q.setParameter("conf", aBean.getConf());
        q.setParameter("consoleUrl", aBean.getConsoleUrl());
        q.setParameter("data", aBean.getData());
        q.setParameter("stats", aBean.getStats());
        q.setParameter("externalChildIDs", aBean.getExternalChildIDs());
        q.setParameter("errorCode", aBean.getErrorCode());
        q.setParameter("errorMessage", aBean.getErrorMessage());
        q.setParameter("externalId", aBean.getExternalId());
        q.setParameter("externalStatus", aBean.getExternalStatus());
        q.setParameter("name", aBean.getName());
        q.setParameter("cred", aBean.getCred());
        q.setParameter("retries", aBean.getRetries());
        q.setParameter("trackerUri", aBean.getTrackerUri());
        q.setParameter("transition", aBean.getTransition());
        q.setParameter("type", aBean.getType());
        q.setParameter("endTime", aBean.getEndTimestamp());
        q.setParameter("executionPath", aBean.getExecutionPath());
        q.setParameter("lastCheckTime", aBean.getLastCheckTimestamp());
        q.setParameter("logToken", aBean.getLogToken());
        q.setParameter("pending", aBean.isPending() ? 1 : 0);
        q.setParameter("pendingAge", aBean.getPendingAgeTimestamp());
        q.setParameter("signalValue", aBean.getSignalValue());
        q.setParameter("slaXml", aBean.getSlaXml());
        q.setParameter("startTime", aBean.getStartTimestamp());
        q.setParameter("status", aBean.getStatusStr());
        q.setParameter("wfId", aBean.getWfId());
    }
}
