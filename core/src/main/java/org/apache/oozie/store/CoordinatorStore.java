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

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.CoordinatorJobInfo;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.Job.Status;
import org.apache.oozie.client.CoordinatorJob.Timeunit;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.InstrumentationService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.openjpa.persistence.OpenJPAPersistence;
import org.apache.openjpa.persistence.OpenJPAQuery;
import org.apache.openjpa.persistence.jdbc.FetchDirection;
import org.apache.openjpa.persistence.jdbc.JDBCFetchPlan;
import org.apache.openjpa.persistence.jdbc.LRSSizeAlgorithm;
import org.apache.openjpa.persistence.jdbc.ResultSetType;

/**
 * DB Implementation of Coord Store
 */
public class CoordinatorStore extends Store {
    private final XLog log = XLog.getLog(getClass());

    private EntityManager entityManager;
    private static final String INSTR_GROUP = "db";
    public static final int LOCK_TIMEOUT = 50000;
    private static final long DAY_IN_MS = 24 * 60 * 60 * 1000;

    public CoordinatorStore(boolean selectForUpdate) throws StoreException {
        super();
        entityManager = getEntityManager();
    }

    public CoordinatorStore(Store store, boolean selectForUpdate) throws StoreException {
        super(store);
        entityManager = getEntityManager();
    }

    /**
     * Create a CoordJobBean. It also creates the process instance for the job.
     *
     * @param workflow workflow bean
     * @throws StoreException
     */

    public void insertCoordinatorJob(final CoordinatorJobBean coordinatorJob) throws StoreException {
        ParamChecker.notNull(coordinatorJob, "coordinatorJob");

        doOperation("insertCoordinatorJob", new Callable<Void>() {
            public Void call() throws StoreException {
                entityManager.persist(coordinatorJob);
                return null;
            }
        });
    }

    /**
     * Load the CoordinatorJob into a Bean and return it. Also load the Workflow Instance into the bean. And lock the
     * Workflow depending on the locking parameter.
     *
     * @param id Job ID
     * @param locking Flag for Table Lock
     * @return CoordinatorJobBean
     * @throws StoreException
     */
    public CoordinatorJobBean getCoordinatorJob(final String id, final boolean locking) throws StoreException {
        ParamChecker.notEmpty(id, "CoordJobId");
        CoordinatorJobBean cjBean = doOperation("getCoordinatorJob", new Callable<CoordinatorJobBean>() {
            @SuppressWarnings("unchecked")
            public CoordinatorJobBean call() throws StoreException {
                Query q = entityManager.createNamedQuery("GET_COORD_JOB");
                q.setParameter("id", id);
                /*
                 * if (locking) { OpenJPAQuery oq = OpenJPAPersistence.cast(q);
                 * // q.setHint("openjpa.FetchPlan.ReadLockMode","WRITE");
                 * FetchPlan fetch = oq.getFetchPlan();
                 * fetch.setReadLockMode(LockModeType.WRITE);
                 * fetch.setLockTimeout(-1); // 1 second }
                 */
                List<CoordinatorJobBean> cjBeans = q.getResultList();

                if (cjBeans.size() > 0) {
                    return cjBeans.get(0);
                }
                else {
                    throw new StoreException(ErrorCode.E0604, id);
                }
            }
        });

        cjBean.setStatus(cjBean.getStatus());
        return cjBean;
    }

    /**
     * Get a list of Coordinator Jobs that should be materialized. Jobs with a 'last materialized time' older than the
     * argument will be returned.
     *
     * @param d Date
     * @return List of Coordinator Jobs that have a last materialized time older than input date
     * @throws StoreException
     */
    public List<CoordinatorJobBean> getCoordinatorJobsToBeMaterialized(final Date d, final int limit)
            throws StoreException {

        ParamChecker.notNull(d, "Coord Job Materialization Date");
        List<CoordinatorJobBean> cjBeans = doOperation("getCoordinatorJobsToBeMaterialized",
                                                                                  new Callable<List<CoordinatorJobBean>>() {
                                                                                      public List<CoordinatorJobBean> call() throws StoreException {

                                                                                          List<CoordinatorJobBean> cjBeans;
                                                                                          List<CoordinatorJobBean> jobList = new ArrayList<CoordinatorJobBean>();
                                                                                          try {
                                                                                              Query q = entityManager.createNamedQuery("GET_COORD_JOBS_OLDER_THAN");
                                                                                              q.setParameter("matTime", new Timestamp(d.getTime()));
                                                                                              if (limit > 0) {
                                                                                                  q.setMaxResults(limit);
                                                                                              }
                                                                                              /*
                                                                                              OpenJPAQuery oq = OpenJPAPersistence.cast(q);
                                                                                              FetchPlan fetch = oq.getFetchPlan();
                                                                                              fetch.setReadLockMode(LockModeType.WRITE);
                                                                                              fetch.setLockTimeout(-1); // no limit
                                                                                              */
                                                                                              cjBeans = q.getResultList();
                                                                                              // copy results to a new object
                                                                                              for (CoordinatorJobBean j : cjBeans) {
                                                                                                  jobList.add(j);
                                                                                              }
                                                                                          }
                                                                                          catch (IllegalStateException e) {
                                                                                              throw new StoreException(ErrorCode.E0601, e.getMessage(), e);
                                                                                          }
                                                                                          return jobList;

                                                                                      }
                                                                                  });
        return cjBeans;
    }

    /**
     * A list of Coordinator Jobs that are matched with the status and have last materialized time' older than
     * checkAgeSecs will be returned.
     *
     * @param checkAgeSecs Job age in Seconds
     * @param status Coordinator Job Status
     * @param limit Number of results to return
     * @param locking Flag for Table Lock
     * @return List of Coordinator Jobs that are matched with the parameters.
     * @throws StoreException
     */
    public List<CoordinatorJobBean> getCoordinatorJobsOlderThanStatus(final long checkAgeSecs, final String status,
                                                                      final int limit, final boolean locking) throws StoreException {

        ParamChecker.notNull(status, "Coord Job Status");
        List<CoordinatorJobBean> cjBeans = doOperation("getCoordinatorJobsOlderThanStatus",
                                                                                  new Callable<List<CoordinatorJobBean>>() {
                                                                                      public List<CoordinatorJobBean> call() throws StoreException {

                                                                                          List<CoordinatorJobBean> cjBeans;
                                                                                          List<CoordinatorJobBean> jobList = new ArrayList<CoordinatorJobBean>();
                                                                                          try {
                                                                                              Query q = entityManager.createNamedQuery("GET_COORD_JOBS_OLDER_THAN_STATUS");
                                                                                              Timestamp ts = new Timestamp(System.currentTimeMillis() - checkAgeSecs * 1000);
                                                                                              q.setParameter("lastModTime", ts);
                                                                                              q.setParameter("status", status);
                                                                                              if (limit > 0) {
                                                                                                  q.setMaxResults(limit);
                                                                                              }
                                                                                              /*
                                                                                              * if (locking) { OpenJPAQuery oq =
                                                                                              * OpenJPAPersistence.cast(q); FetchPlan fetch =
                                                                                              * oq.getFetchPlan();
                                                                                              * fetch.setReadLockMode(LockModeType.WRITE);
                                                                                              * fetch.setLockTimeout(-1); // no limit }
                                                                                              */
                                                                                              cjBeans = q.getResultList();
                                                                                              for (CoordinatorJobBean j : cjBeans) {
                                                                                                  jobList.add(j);
                                                                                              }
                                                                                          }
                                                                                          catch (Exception e) {
                                                                                              throw new StoreException(ErrorCode.E0603, e.getMessage(), e);
                                                                                          }
                                                                                          return jobList;

                                                                                      }
                                                                                  });
        return cjBeans;
    }

    /**
     * Load the CoordinatorAction into a Bean and return it.
     *
     * @param id action ID
     * @return CoordinatorActionBean
     * @throws StoreException
     */
    public CoordinatorActionBean getCoordinatorAction(final String id, final boolean locking) throws StoreException {
        ParamChecker.notEmpty(id, "actionID");
        CoordinatorActionBean caBean = doOperation("getCoordinatorAction", new Callable<CoordinatorActionBean>() {
            public CoordinatorActionBean call() throws StoreException {
                Query q = entityManager.createNamedQuery("GET_COORD_ACTION");
                q.setParameter("id", id);
                OpenJPAQuery oq = OpenJPAPersistence.cast(q);
                /*
                 * if (locking) { //q.setHint("openjpa.FetchPlan.ReadLockMode",
                 * "WRITE"); FetchPlan fetch = oq.getFetchPlan();
                 * fetch.setReadLockMode(LockModeType.WRITE);
                 * fetch.setLockTimeout(-1); // no limit }
                 */

                CoordinatorActionBean action = null;
                List<CoordinatorActionBean> actions = q.getResultList();
                if (actions.size() > 0) {
                    action = actions.get(0);
                }
                else {
                    throw new StoreException(ErrorCode.E0605, id);
                }

                /*
                 * if (locking) return action; else
                 */
                return getBeanForRunningCoordAction(action);
            }
        });
        return caBean;
    }

    /**
     * Return CoordinatorActions for a jobID. Action should be in READY state. Number of returned actions should be <=
     * concurrency number. Sort returned actions based on execution order (FIFO, LIFO, LAST_ONLY, NONE)
     *
     * @param id job ID
     * @param numResults number of results to return
     * @param executionOrder execution for this job - FIFO, LIFO, LAST_ONLY, NONE
     * @return List of CoordinatorActionBean
     * @throws StoreException
     */
    public List<CoordinatorActionBean> getCoordinatorActionsForJob(final String id, final int numResults,
                                                                   final String executionOrder) throws StoreException {
        ParamChecker.notEmpty(id, "jobID");
        List<CoordinatorActionBean> caBeans = doOperation("getCoordinatorActionsForJob",
                                                          new Callable<List<CoordinatorActionBean>>() {
                                                              public List<CoordinatorActionBean> call() throws StoreException {

                                                                  List<CoordinatorActionBean> caBeans;
                                                                  Query q;
                                                                  // check if executionOrder is FIFO, LIFO, NONE or LAST_ONLY
                                                                  if (executionOrder.equalsIgnoreCase("FIFO")) {
                                                                      q = entityManager.createNamedQuery("GET_COORD_ACTIONS_FOR_JOB_FIFO");
                                                                  }
                                                                  else {
                                                                      q = entityManager.createNamedQuery("GET_COORD_ACTIONS_FOR_JOB_LIFO");
                                                                  }
                                                                  q.setParameter("jobId", id);
                                                                  // if executionOrder is LAST_ONLY, only retrieve first
                                                                  // record in LIFO,
                                                                  // otherwise, use numResults if it is positive.
                                                                  if (executionOrder.equalsIgnoreCase("LAST_ONLY")) {
                                                                      q.setMaxResults(1);
                                                                  }
                                                                  else {
                                                                      if (numResults > 0) {
                                                                          q.setMaxResults(numResults);
                                                                      }
                                                                  }
                                                                  caBeans = q.getResultList();
                                                                  return caBeans;
                                                              }
                                                          });
        return caBeans;
    }

    /**
     * Return CoordinatorActions for a jobID. Action should be in READY state. Number of returned actions should be <=
     * concurrency number.
     *
     * @param id job ID
     * @return Number of running actions
     * @throws StoreException
     */
    public int getCoordinatorRunningActionsCount(final String id) throws StoreException {
        ParamChecker.notEmpty(id, "jobID");
        Integer cnt = doOperation("getCoordinatorRunningActionsCount", new Callable<Integer>() {
            public Integer call() throws SQLException {

                Query q = entityManager.createNamedQuery("GET_COORD_RUNNING_ACTIONS_COUNT");

                q.setParameter("jobId", id);
                Long count = (Long) q.getSingleResult();
                return Integer.valueOf(count.intValue());
            }
        });
        return cnt.intValue();
    }

    /**
     * Create a new Action record in the ACTIONS table with the given Bean.
     *
     * @param action WorkflowActionBean
     * @throws StoreException If the action is already present
     */
    public void insertCoordinatorAction(final CoordinatorActionBean action) throws StoreException {
        ParamChecker.notNull(action, "CoordinatorActionBean");
        doOperation("insertCoordinatorAction", new Callable<Void>() {
            public Void call() throws StoreException {
                entityManager.persist(action);
                return null;
            }
        });
    }

    /**
     * Update the given action bean to DB.
     *
     * @param action Action Bean
     * @throws StoreException if action doesn't exist
     */
    public void updateCoordinatorAction(final CoordinatorActionBean action) throws StoreException, JPAExecutorException {
        ParamChecker.notNull(action, "CoordinatorActionBean");
        doOperation("updateCoordinatorAction", new Callable<Void>() {
            public Void call() throws StoreException, JPAExecutorException {
                CoordActionQueryExecutor.getInstance().executeUpdate(
                        CoordActionQueryExecutor.CoordActionQuery.UPDATE_COORD_ACTION, action);
                return null;
            }
        });
    }

    /**
     * Update the given action bean to DB.
     *
     * @param action Action Bean
     * @throws StoreException if action doesn't exist
     */
    public void updateCoordActionMin(final CoordinatorActionBean action) throws StoreException, JPAExecutorException {
        ParamChecker.notNull(action, "CoordinatorActionBean");
        doOperation("updateCoordinatorAction", new Callable<Void>() {
            public Void call() throws StoreException, JPAExecutorException {
                CoordActionQueryExecutor.getInstance().executeUpdate(
                        CoordActionQueryExecutor.CoordActionQuery.UPDATE_COORD_ACTION_FOR_INPUTCHECK, action);
                return null;
            }
        });
    }

    /**
     * Update the given coordinator job bean to DB.
     *
     * @param jobbean Coordinator Job Bean
     * @throws StoreException if action doesn't exist
     */
    public void updateCoordinatorJob(final CoordinatorJobBean job) throws StoreException {
        ParamChecker.notNull(job, "CoordinatorJobBean");
        doOperation("updateJob", new Callable<Void>() {
            public Void call() throws StoreException, JPAExecutorException {
                CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, job);
                return null;
            }
        });
    }

    public void updateCoordinatorJobStatus(final CoordinatorJobBean job) throws StoreException {
        ParamChecker.notNull(job, "CoordinatorJobBean");
        doOperation("updateJobStatus", new Callable<Void>() {
            public Void call() throws StoreException, JPAExecutorException {
                CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB_STATUS_MODTIME, job);
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
            throw new StoreException(ErrorCode.E0611, name, ex.getMessage(), ex);
        }
        catch (Exception e) {
            throw new StoreException(ErrorCode.E0607, name, e.getMessage(), e);
        }
    }

    /**
     * Purge the coordinators completed older than given days.
     *
     * @param olderThanDays number of days for which to preserve the coordinators
     * @param limit maximum number of coordinator jobs to be purged
     * @throws StoreException
     */
    public void purge(final long olderThanDays, final int limit) throws StoreException {
        doOperation("coord-purge", new Callable<Void>() {
            public Void call() throws SQLException, StoreException, WorkflowException {
                Timestamp lastModTm = new Timestamp(System.currentTimeMillis() - (olderThanDays * DAY_IN_MS));
                Query jobQ = entityManager.createNamedQuery("GET_COMPLETED_COORD_JOBS_OLDER_THAN_STATUS");
                jobQ.setParameter("lastModTime", lastModTm);
                jobQ.setMaxResults(limit);
                List<CoordinatorJobBean> coordJobs = jobQ.getResultList();

                int actionDeleted = 0;
                if (coordJobs.size() != 0) {
                    for (CoordinatorJobBean coord : coordJobs) {
                        String jobId = coord.getId();
                        entityManager.remove(coord);
                        Query g = entityManager.createNamedQuery("DELETE_COMPLETED_ACTIONS_FOR_COORDINATOR");
                        g.setParameter("jobId", jobId);
                        actionDeleted += g.executeUpdate();
                    }
                }

                XLog.getLog(getClass()).debug("ENDED Coord Purge deleted jobs :" + coordJobs.size() + " and actions " + actionDeleted);
                return null;
            }
        });
    }

    public void commit() throws StoreException {
    }

    public void close() throws StoreException {
    }

    public CoordinatorJobBean getCoordinatorJobs(String id) {
        // TODO Auto-generated method stub
        return null;
    }

    public CoordinatorJobInfo getCoordinatorInfo(final Map<String, List<String>> filter, final int start, final int len)
            throws StoreException {

        CoordinatorJobInfo coordJobInfo = doOperation("getCoordinatorJobInfo", new Callable<CoordinatorJobInfo>() {
            public CoordinatorJobInfo call() throws SQLException, StoreException {
                List<String> orArray = new ArrayList<String>();
                List<String> colArray = new ArrayList<String>();
                List<String> valArray = new ArrayList<String>();
                StringBuilder sb = new StringBuilder("");

                StoreStatusFilter.filter(filter, orArray, colArray, valArray, sb, StoreStatusFilter.coordSeletStr,
                                         StoreStatusFilter.coordCountStr);

                int realLen = 0;

                Query q = null;
                Query qTotal = null;
                if (orArray.size() == 0) {
                    q = entityManager.createNamedQuery("GET_COORD_JOBS_COLUMNS");
                    q.setFirstResult(start - 1);
                    q.setMaxResults(len);
                    qTotal = entityManager.createNamedQuery("GET_COORD_JOBS_COUNT");
                }
                else {
                    StringBuilder sbTotal = new StringBuilder(sb);
                    sb.append(" order by w.createdTimestamp desc ");
                    XLog.getLog(getClass()).debug("Created String is **** " + sb.toString());
                    q = entityManager.createQuery(sb.toString());
                    q.setFirstResult(start - 1);
                    q.setMaxResults(len);
                    qTotal = entityManager.createQuery(sbTotal.toString().replace(StoreStatusFilter.coordSeletStr,
                                                                                  StoreStatusFilter.coordCountStr));
                }

                for (int i = 0; i < orArray.size(); i++) {
                    q.setParameter(colArray.get(i), valArray.get(i));
                    qTotal.setParameter(colArray.get(i), valArray.get(i));
                }

                OpenJPAQuery kq = OpenJPAPersistence.cast(q);
                JDBCFetchPlan fetch = (JDBCFetchPlan) kq.getFetchPlan();
                fetch.setFetchBatchSize(20);
                fetch.setResultSetType(ResultSetType.SCROLL_INSENSITIVE);
                fetch.setFetchDirection(FetchDirection.FORWARD);
                fetch.setLRSSizeAlgorithm(LRSSizeAlgorithm.LAST);
                List<?> resultList = q.getResultList();
                List<Object[]> objectArrList = (List<Object[]>) resultList;
                List<CoordinatorJobBean> coordBeansList = new ArrayList<CoordinatorJobBean>();

                for (Object[] arr : objectArrList) {
                    CoordinatorJobBean ww = getBeanForCoordinatorJobFromArray(arr);
                    coordBeansList.add(ww);
                }

                realLen = ((Long) qTotal.getSingleResult()).intValue();

                return new CoordinatorJobInfo(coordBeansList, start, len, realLen);
            }
        });
        return coordJobInfo;
    }

    private CoordinatorJobBean getBeanForCoordinatorJobFromArray(Object[] arr) {
        CoordinatorJobBean bean = new CoordinatorJobBean();
        bean.setId((String) arr[0]);
        if (arr[1] != null) {
            bean.setAppName((String) arr[1]);
        }
        if (arr[2] != null) {
            bean.setStatus(Status.valueOf((String) arr[2]));
        }
        if (arr[3] != null) {
            bean.setUser((String) arr[3]);
        }
        if (arr[4] != null) {
            bean.setGroup((String) arr[4]);
        }
        if (arr[5] != null) {
            bean.setStartTime((Timestamp) arr[5]);
        }
        if (arr[6] != null) {
            bean.setEndTime((Timestamp) arr[6]);
        }
        if (arr[7] != null) {
            bean.setAppPath((String) arr[7]);
        }
        if (arr[8] != null) {
            bean.setConcurrency(((Integer) arr[8]).intValue());
        }
        if (arr[9] != null) {
            bean.setFrequency((String) arr[9]);
        }
        if (arr[10] != null) {
            bean.setLastActionTime((Timestamp) arr[10]);
        }
        if (arr[11] != null) {
            bean.setNextMaterializedTime((Timestamp) arr[11]);
        }
        if (arr[13] != null) {
            bean.setTimeUnit(Timeunit.valueOf((String) arr[13]));
        }
        if (arr[14] != null) {
            bean.setTimeZone((String) arr[14]);
        }
        if (arr[15] != null) {
            bean.setTimeout((Integer) arr[15]);
        }
        return bean;
    }

    /**
     * Loads all actions for the given Coordinator job.
     *
     * @param jobId coordinator job id
     * @param locking true if Actions are to be locked
     * @return A List of CoordinatorActionBean
     * @throws StoreException
     */
    public Integer getActionsForCoordinatorJob(final String jobId, final boolean locking)
            throws StoreException {
        ParamChecker.notEmpty(jobId, "CoordinatorJobID");
        Integer actionsCount = doOperation("getActionsForCoordinatorJob",
                                                          new Callable<Integer>() {
                                                              @SuppressWarnings("unchecked")
                                                              public Integer call() throws StoreException {
                                                                  List<CoordinatorActionBean> actions;
                                                                  List<CoordinatorActionBean> actionList = new ArrayList<CoordinatorActionBean>();
                                                                  try {
                                                                      Query q = entityManager.createNamedQuery("GET_ACTIONS_FOR_COORD_JOB");
                                                                      q.setParameter("jobId", jobId);
                                                                      /*
                                                                      * if (locking) { //
                                                                      * q.setHint("openjpa.FetchPlan.ReadLockMode", //
                                                                      * "READ"); OpenJPAQuery oq =
                                                                      * OpenJPAPersistence.cast(q); JDBCFetchPlan fetch =
                                                                      * (JDBCFetchPlan) oq.getFetchPlan();
                                                                      * fetch.setReadLockMode(LockModeType.WRITE);
                                                                      * fetch.setLockTimeout(-1); // 1 second }
                                                                      */
                                                                      Long count = (Long) q.getSingleResult();
                                                                      return Integer.valueOf(count.intValue());
                                                                      /*actions = q.getResultList();
                                                                      for (CoordinatorActionBean a : actions) {
                                                                          CoordinatorActionBean aa = getBeanForRunningCoordAction(a);
                                                                          actionList.add(aa);
                                                                      }*/
                                                                  }
                                                                  catch (IllegalStateException e) {
                                                                      throw new StoreException(ErrorCode.E0601, e.getMessage(), e);
                                                                  }
                                                                  /*
                                                                  * if (locking) { return actions; } else {
                                                                  */

                                                                  // }
                                                              }
                                                          });
        return actionsCount;
    }

    /**
     * Loads given number of actions for the given Coordinator job.
     *
     * @param jobId coordinator job id
     * @param start offset for select statement
     * @param len number of Workflow Actions to be returned
     * @return A List of CoordinatorActionBean
     * @throws StoreException
     */
    public List<CoordinatorActionBean> getActionsSubsetForCoordinatorJob(final String jobId, final int start,
            final int len) throws StoreException {
        ParamChecker.notEmpty(jobId, "CoordinatorJobID");
        List<CoordinatorActionBean> actions = doOperation("getActionsForCoordinatorJob",
                new Callable<List<CoordinatorActionBean>>() {
                    @SuppressWarnings("unchecked")
                    public List<CoordinatorActionBean> call() throws StoreException {
                        List<CoordinatorActionBean> actions;
                        List<CoordinatorActionBean> actionList = new ArrayList<CoordinatorActionBean>();
                        try {
                            Query q = entityManager.createNamedQuery("GET_ACTIONS_FOR_COORD_JOB");
                            q.setParameter("jobId", jobId);
                            q.setFirstResult(start - 1);
                            q.setMaxResults(len);
                            actions = q.getResultList();
                            for (CoordinatorActionBean a : actions) {
                                CoordinatorActionBean aa = getBeanForRunningCoordAction(a);
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

    protected CoordinatorActionBean getBeanForRunningCoordAction(CoordinatorActionBean a) {
        if (a != null) {
            CoordinatorActionBean action = new CoordinatorActionBean();
            action.setId(a.getId());
            action.setActionNumber(a.getActionNumber());
            action.setActionXmlBlob(a.getActionXmlBlob());
            action.setConsoleUrl(a.getConsoleUrl());
            action.setCreatedConfBlob(a.getCreatedConfBlob());
            action.setErrorCode(a.getErrorCode());
            action.setErrorMessage(a.getErrorMessage());
            action.setExternalStatus(a.getExternalStatus());
            action.setMissingDependenciesBlob(a.getMissingDependenciesBlob());
            action.setRunConfBlob(a.getRunConfBlob());
            action.setTimeOut(a.getTimeOut());
            action.setTrackerUri(a.getTrackerUri());
            action.setType(a.getType());
            action.setCreatedTime(a.getCreatedTime());
            action.setExternalId(a.getExternalId());
            action.setJobId(a.getJobId());
            action.setLastModifiedTime(a.getLastModifiedTime());
            action.setNominalTime(a.getNominalTime());
            action.setSlaXmlBlob(a.getSlaXmlBlob());
            action.setStatus(a.getStatus());
            return action;
        }
        return null;
    }

    public CoordinatorActionBean getAction(String id, boolean b) {
        return null;
    }


    public List<CoordinatorActionBean> getRunningActionsForCoordinatorJob(final String jobId, final boolean locking)
            throws StoreException {
        ParamChecker.notEmpty(jobId, "CoordinatorJobID");
        List<CoordinatorActionBean> actions = doOperation("getRunningActionsForCoordinatorJob",
                                                          new Callable<List<CoordinatorActionBean>>() {
                                                              @SuppressWarnings("unchecked")
                                                              public List<CoordinatorActionBean> call() throws StoreException {
                                                                  List<CoordinatorActionBean> actions;
                                                                  try {
                                                                      Query q = entityManager.createNamedQuery("GET_RUNNING_ACTIONS_FOR_COORD_JOB");
                                                                      q.setParameter("jobId", jobId);
                                                                      /*
                                                                      * if (locking) {
                                                                      * q.setHint("openjpa.FetchPlan.ReadLockMode",
                                                                      * "READ"); OpenJPAQuery oq =
                                                                      * OpenJPAPersistence.cast(q); FetchPlan fetch =
                                                                      * oq.getFetchPlan();
                                                                      * fetch.setReadLockMode(LockModeType.WRITE);
                                                                      * fetch.setLockTimeout(-1); // no limit }
                                                                      */
                                                                      actions = q.getResultList();
                                                                      return actions;
                                                                  }
                                                                  catch (IllegalStateException e) {
                                                                      throw new StoreException(ErrorCode.E0601, e.getMessage(), e);
                                                                  }
                                                              }
                                                          });
        return actions;
    }

    public List<CoordinatorActionBean> getRunningActionsOlderThan(final long checkAgeSecs, final boolean locking)
            throws StoreException {
        List<CoordinatorActionBean> actions = doOperation("getRunningActionsOlderThan",
                                                          new Callable<List<CoordinatorActionBean>>() {
                                                              @SuppressWarnings("unchecked")
                                                              public List<CoordinatorActionBean> call() throws StoreException {
                                                                  List<CoordinatorActionBean> actions;
                                                                  Timestamp ts = new Timestamp(System.currentTimeMillis() - checkAgeSecs * 1000);
                                                                  try {
                                                                      Query q = entityManager.createNamedQuery("GET_RUNNING_ACTIONS_OLDER_THAN");
                                                                      q.setParameter("lastModifiedTime", ts);
                                                                      /*
                                                                      * if (locking) { OpenJPAQuery oq =
                                                                      * OpenJPAPersistence.cast(q); FetchPlan fetch =
                                                                      * oq.getFetchPlan();
                                                                      * fetch.setReadLockMode(LockModeType.WRITE);
                                                                      * fetch.setLockTimeout(-1); // no limit }
                                                                      */
                                                                      actions = q.getResultList();
                                                                      return actions;
                                                                  }
                                                                  catch (IllegalStateException e) {
                                                                      throw new StoreException(ErrorCode.E0601, e.getMessage(), e);
                                                                  }
                                                              }
                                                          });
        return actions;
    }

    public List<CoordinatorActionBean> getRecoveryActionsOlderThan(final long checkAgeSecs, final boolean locking)
            throws StoreException {
        List<CoordinatorActionBean> actions = doOperation("getRunningActionsOlderThan",
                                                          new Callable<List<CoordinatorActionBean>>() {
                                                              @SuppressWarnings("unchecked")
                                                              public List<CoordinatorActionBean> call() throws StoreException {
                                                                  List<CoordinatorActionBean> actions;
                                                                  try {
                                                                      Query q = entityManager.createNamedQuery("GET_COORD_ACTIONS_FOR_RECOVERY_OLDER_THAN");
                                                                      Timestamp ts = new Timestamp(System.currentTimeMillis() - checkAgeSecs * 1000);
                                                                      q.setParameter("lastModifiedTime", ts);
                                                                      /*
                                                                      * if (locking) { OpenJPAQuery oq =
                                                                      * OpenJPAPersistence.cast(q); FetchPlan fetch =
                                                                      * oq.getFetchPlan();
                                                                      * fetch.setReadLockMode(LockModeType.WRITE);
                                                                      * fetch.setLockTimeout(-1); // no limit }
                                                                      */
                                                                      actions = q.getResultList();
                                                                      return actions;
                                                                  }
                                                                  catch (IllegalStateException e) {
                                                                      throw new StoreException(ErrorCode.E0601, e.getMessage(), e);
                                                                  }
                                                              }
                                                          });
        return actions;
    }

    /**
     * Get coordinator action beans for given start date and end date
     *
     * @param startDate
     * @param endDate
     * @return list of coordinator action beans
     * @throws StoreException
     */
    public List<CoordinatorActionBean> getCoordActionsForDates(final String jobId, final Date startDate,
            final Date endDate) throws StoreException {
        List<CoordinatorActionBean> actions = doOperation("getCoordActionsForDates",
                new Callable<List<CoordinatorActionBean>>() {
                    @SuppressWarnings("unchecked")
                    public List<CoordinatorActionBean> call() throws StoreException {
                        List<CoordinatorActionBean> actions;
                        try {
                            Query q = entityManager.createNamedQuery("GET_ACTIONS_FOR_DATES");
                            q.setParameter("jobId", jobId);
                            q.setParameter("startTime", new Timestamp(startDate.getTime()));
                            q.setParameter("endTime", new Timestamp(endDate.getTime()));
                            actions = q.getResultList();
                            List<CoordinatorActionBean> actionList = new ArrayList<CoordinatorActionBean>();
                            for (CoordinatorActionBean a : actions) {
                                CoordinatorActionBean aa = getBeanForRunningCoordAction(a);
                                actionList.add(aa);
                            }
                            return actionList;
                        }
                        catch (IllegalStateException e) {
                            throw new StoreException(ErrorCode.E0601, e.getMessage(), e);
                        }
                    }
                });
        return actions;
    }

    /**
     * Get coordinator action bean for given date
     *
     * @param nominalTime
     * @return CoordinatorActionBean
     * @throws StoreException
     */
    public CoordinatorActionBean getCoordActionForNominalTime(final String jobId, final Date nominalTime)
            throws StoreException {
        CoordinatorActionBean action = doOperation("getCoordActionForNominalTime",
                new Callable<CoordinatorActionBean>() {
            @SuppressWarnings("unchecked")
            public CoordinatorActionBean call() throws StoreException {
                List<CoordinatorActionBean> actions;
                Query q = entityManager.createNamedQuery("GET_ACTION_FOR_NOMINALTIME");
                q.setParameter("jobId", jobId);
                q.setParameter("nominalTime", new Timestamp(nominalTime.getTime()));
                actions = q.getResultList();

                CoordinatorActionBean action = null;
                if (actions.size() > 0) {
                    action = actions.get(0);
                }
                else {
                    throw new StoreException(ErrorCode.E0605, DateUtils.formatDateOozieTZ(nominalTime));
                }
                return getBeanForRunningCoordAction(action);
            }
        });
        return action;
    }

    public List<String> getRecoveryActionsGroupByJobId(final long checkAgeSecs) throws StoreException {
        List<String> jobids = doOperation("getRecoveryActionsGroupByJobId", new Callable<List<String>>() {
            @SuppressWarnings("unchecked")
            public List<String> call() throws StoreException {
                List<String> jobids = new ArrayList<String>();
                try {
                    Query q = entityManager.createNamedQuery("GET_READY_ACTIONS_GROUP_BY_JOBID");
                    Timestamp ts = new Timestamp(System.currentTimeMillis() - checkAgeSecs * 1000);
                    q.setParameter("lastModifiedTime", ts);
                    List<Object[]> list = q.getResultList();

                    for (Object[] arr : list) {
                        if (arr != null && arr[0] != null) {
                            jobids.add((String) arr[0]);
                        }
                    }

                    return jobids;
                }
                catch (IllegalStateException e) {
                    throw new StoreException(ErrorCode.E0601, e.getMessage(), e);
                }
            }
        });
        return jobids;
    }
}
