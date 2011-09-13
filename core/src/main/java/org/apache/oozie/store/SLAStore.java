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
import java.util.concurrent.Callable;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.SLAEventBean;
import org.apache.oozie.XException;
import org.apache.oozie.service.InstrumentationService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.ParamChecker;

public class SLAStore extends Store {
    private EntityManager entityManager;
    private static final String INSTR_GROUP = "db";

    public SLAStore() throws StoreException {
        super();
        entityManager = getEntityManager();

    }

    public SLAStore(Store store) throws StoreException {
        super(store);
        entityManager = getEntityManager();
    }

    /**
     * Create a CoordJobBean. It also creates the process instance for the job.
     *
     * @param workflow workflow bean
     * @throws StoreException
     */

    public void insertSLAEvent(final SLAEventBean slaEvent) throws StoreException {
        ParamChecker.notNull(slaEvent, "sLaEvent");

        doOperation("insertSLAEvent", new Callable<Void>() {
            public Void call() throws StoreException {
                entityManager.persist(slaEvent);
                return null;
            }
        });
    }

    /**
     * Get a list of SLA Events newer than a specific sequence with limit clause.
     *
     * @param seqId sequence id
     * @return List of SLA Events
     * @throws StoreException
     */
    public List<SLAEventBean> getSLAEventListNewerSeqLimited(final long seqId, final int limitLen, long[] lastSeqId)
            throws StoreException {
        ParamChecker.notNull(seqId, "SLAEventListNewerSeqLimited");
        ParamChecker.checkGTZero(limitLen, "SLAEventListNewerSeqLimited");

        lastSeqId[0] = seqId;

        List<SLAEventBean> seBeans = (List<SLAEventBean>) doOperation("getSLAEventListNewerSeqLimited",
                                                                      new Callable<List<SLAEventBean>>() {

                                                                          public List<SLAEventBean> call() throws StoreException {

                                                                              List<SLAEventBean> seBeans;
                                                                              try {
                                                                                  Query q = entityManager.createNamedQuery("GET_SLA_EVENT_NEWER_SEQ_LIMITED");
                                                                                  q.setParameter("id", seqId);
                                                                                  // q.setFirstResult(0);
                                                                                  q.setMaxResults(limitLen);
                                                                                  seBeans = q.getResultList();
                                                                              }
                                                                              catch (IllegalStateException e) {
                                                                                  throw new StoreException(ErrorCode.E0601, e.getMessage(), e);
                                                                              }
                                                                              return seBeans;
                                                                          }
                                                                      });
        List<SLAEventBean> eventList = new ArrayList<SLAEventBean>();
        for (SLAEventBean j : seBeans) {
            lastSeqId[0] = Math.max(lastSeqId[0], j.getEvent_id());
            eventList.add(j);
        }
        return eventList;
    }

    private SLAEventBean copyEventBean(SLAEventBean e) {
        SLAEventBean event = new SLAEventBean();
        event.setAlertContact(e.getAlertContact());
        event.setAlertFrequency(e.getAlertFrequency());
        event.setAlertPercentage(e.getAlertPercentage());
        event.setAppName(e.getAppName());
        event.setAppType(e.getAppType());
        event.setAppTypeStr(e.getAppTypeStr());
        event.setDevContact(e.getDevContact());
        event.setEvent_id(e.getEvent_id());
        event.setEventType(e.getEventType());
        event.setExpectedEnd(e.getExpectedEnd());
        event.setExpectedStart(e.getExpectedStart());
        event.setGroupName(e.getGroupName());
        event.setJobData(e.getJobData());
        event.setJobStatus(e.getJobStatus());
        event.setJobStatusStr(e.getJobStatusStr());
        event.setNotificationMsg(e.getNotificationMsg());
        event.setParentClientId(e.getParentClientId());
        event.setParentSlaId(e.getParentSlaId());
        event.setQaContact(e.getQaContact());
        event.setSeContact(e.getSeContact());
        event.setSlaId(e.getSlaId());
        event.setStatusTimestamp(e.getStatusTimestamp());
        event.setUpstreamApps(e.getUpstreamApps());
        event.setUser(e.getUser());
        return event;
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

}
