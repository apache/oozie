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
package org.apache.oozie.sla;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;

import org.apache.oozie.client.rest.JsonBean;
import org.json.simple.JSONObject;

@Entity
@Table(name = "SLA_CALCULATOR")
@NamedQueries({ @NamedQuery(name = "GET_SLA_CALC", query = "select OBJECT(w) from SLACalculatorBean w where w.jobId = :id") })
public class SLACalculatorBean implements JsonBean {

    @Id
    @Basic
    @Column(name = "job_id")
    private String jobId;

    @Basic
    @Column(name = "start_processed")
    private boolean startProcessed = false;

    @Basic
    @Column(name = "end_processed")
    private boolean endProcessed = false;

    @Basic
    @Column(name = "duration_processed")
    private boolean durationProcessed = false;

    public SLACalculatorBean() {
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public boolean isStartProcessed() {
        return startProcessed;
    }

    public void setStartProcessed(boolean startProcessed) {
        this.startProcessed = startProcessed;
    }

    public boolean isEndProcessed() {
        return endProcessed;
    }

    public void setEndProcessed(boolean endProcessed) {
        this.endProcessed = endProcessed;
    }

    public boolean isDurationProcessed() {
        return durationProcessed;
    }

    public void setDurationProcessed(boolean durationProcessed) {
        this.durationProcessed = durationProcessed;
    }

    @Override
    public JSONObject toJSONObject() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public JSONObject toJSONObject(String timeZoneId) {
        // TODO Auto-generated method stub
        return null;
    }

}
