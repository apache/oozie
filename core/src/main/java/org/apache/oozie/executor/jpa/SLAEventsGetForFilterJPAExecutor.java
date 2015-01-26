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

package org.apache.oozie.executor.jpa;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.SLAEventBean;
import org.apache.oozie.client.OozieClient;

/**
 * Load the list of SLAEventBean for a seqId and return the list.
 */
@Deprecated
public class SLAEventsGetForFilterJPAExecutor implements JPAExecutor<List<SLAEventBean>> {

    private static final String selectStr = "SELECT OBJECT(w) FROM SLAEventBean w WHERE w.event_id > :seqid";
    private long seqId = -1;
    private int len;
    private long[] lastSeqId;
    private Map<String, List<String>> filter;
    private StringBuilder sb;

    public SLAEventsGetForFilterJPAExecutor(long seqId, int len, Map<String, List<String>> filter, long[] lastSeqId) {
        this.seqId = seqId;
        this.len = len;
        this.filter = filter;
        this.lastSeqId = lastSeqId;
        this.lastSeqId[0] = seqId;
    }

    @Override
    public String getName() {
        return "SLAEventsGetForFilterJPAExecutor";
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<SLAEventBean> execute(EntityManager em) throws JPAExecutorException {

        List<SLAEventBean> seBeans;
        StringBuilder sb = new StringBuilder(selectStr);
        Map<String, String> keyVal = new HashMap<String, String>();

        for (Map.Entry<String, List<String>> entry : filter.entrySet()) {

            if (entry.getKey().equals(OozieClient.FILTER_JOBID) || entry.getKey().equals(OozieClient.FILTER_APPNAME)) {
                sb.append(" AND ");
            }

            if (entry.getKey().equals(OozieClient.FILTER_JOBID)) {
                List<String> vals = entry.getValue();
                if (vals.size() == 1) {
                    sb.append("w.slaId = :jobid");
                    keyVal.put("jobid", vals.get(0));
                }
                else {
                    for (int i = 0; i < vals.size(); i++) {
                        String val = vals.get(i);
                        keyVal.put("jobid" + i, val);
                        if (i == 0) {
                            sb.append("w.slaId IN (:jobid" + i);
                        }
                        else {
                            sb.append(",:jobid" + i);
                        }
                    }
                    sb.append(")");
                }
            }
            else if (entry.getKey().equals(OozieClient.FILTER_APPNAME)) {
                List<String> vals = entry.getValue();
                if (vals.size() == 1) {
                    sb.append("w.appName = :appname");
                    keyVal.put("appname", vals.get(0));
                }
                else {
                    for (int i = 0; i < vals.size(); i++) {
                        String val = vals.get(i);
                        keyVal.put("appname" + i, val);
                        if (i == 0) {
                            sb.append("w.appName IN (:appname" + i);
                        }
                        else {
                            sb.append(",:appname" + i);
                        }
                    }
                    sb.append(")");
                }
            }
        }
        sb.append(" ORDER BY w.event_id ");

        try {
            Query q = em.createQuery(sb.toString());
            q.setMaxResults(len);
            q.setParameter("seqid", seqId);
            for (Map.Entry<String, String> entry : keyVal.entrySet()) {
                q.setParameter(entry.getKey(), entry.getValue());
            }
            seBeans = q.getResultList();
            for (SLAEventBean j : seBeans) {
                lastSeqId[0] = Math.max(lastSeqId[0], j.getEvent_id());
            }

        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
        return seBeans;
    }

}
