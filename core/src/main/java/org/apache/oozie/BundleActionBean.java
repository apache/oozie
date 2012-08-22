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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.DiscriminatorColumn;
import javax.persistence.DiscriminatorType;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;

import org.apache.hadoop.io.Writable;
import org.apache.oozie.client.Job.Status;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.WritableUtils;
import org.json.simple.JSONObject;

@Entity
@Table(name = "BUNDLE_ACTIONS")
@DiscriminatorColumn(name = "bean_type", discriminatorType = DiscriminatorType.STRING)
@NamedQueries( {
        @NamedQuery(name = "DELETE_BUNDLE_ACTION", query = "delete from BundleActionBean w where w.bundleActionId = :bundleActionId"),

        @NamedQuery(name = "GET_BUNDLE_ACTIONS_FOR_BUNDLE", query = "select OBJECT(w) from BundleActionBean w where w.bundleId = :bundleId"),

        @NamedQuery(name = "GET_BUNDLE_ACTION_STATUS_PENDING_FOR_BUNDLE", query = "select w.coordId, w.status, w.pending from BundleActionBean w where w.bundleId = :bundleId"),

        @NamedQuery(name = "GET_BUNDLE_ACTIONS", query = "select OBJECT(w) from BundleActionBean w"),

        @NamedQuery(name = "GET_BUNDLE_ACTIONS_BY_LAST_MODIFIED_TIME", query = "select OBJECT(w) from BundleActionBean w where w.lastModifiedTimestamp >= :lastModifiedTime"),

        @NamedQuery(name = "GET_BUNDLE_WAITING_ACTIONS_OLDER_THAN", query = "select OBJECT(a) from BundleActionBean a where a.pending > 0 AND a.lastModifiedTimestamp <= :lastModifiedTime"),

        @NamedQuery(name = "GET_BUNDLE_ACTION", query = "select OBJECT(w) from BundleActionBean w where w.bundleActionId = :bundleActionId"),

        @NamedQuery(name = "GET_BUNDLE_ACTIONS_COUNT", query = "select count(w) from BundleActionBean w"),

        @NamedQuery(name = "GET_BUNDLE_ACTIONS_COUNT_BY_JOB", query = "select count(w) from BundleActionBean w where w.bundleId = :bundleId"),

        @NamedQuery(name = "GET_BUNDLE_ACTIONS_PENDING_TRUE_COUNT", query = "select count(w) from BundleActionBean w where w.bundleId = :bundleId AND w.pending > 0"),

        @NamedQuery(name = "GET_BUNDLE_ACTIONS_NOT_EQUAL_STATUS_COUNT", query = "select count(w) from BundleActionBean w where w.bundleId = :bundleId AND w.status <> :status"),

        @NamedQuery(name = "GET_BUNDLE_ACTIONS_NOT_TERMINATE_STATUS_COUNT", query = "select count(w) from BundleActionBean w where w.bundleId = :bundleId AND (w.status = 'PREP' OR w.status = 'RUNNING' OR w.status = 'RUNNINGWITHERROR' OR w.status = 'SUSPENDED' OR w.status = 'SUSPENDEDWITHERROR' OR w.status = 'PREPSUSPENDED' OR w.status = 'PAUSED' OR  w.status = 'PAUSEDWITHERROR' OR w.status = 'PREPPAUSED')"),

        @NamedQuery(name = "GET_BUNDLE_ACTIONS_FAILED_NULL_COORD_COUNT", query = "select count(w) from BundleActionBean w where w.bundleId = :bundleId AND w.status = 'FAILED' AND w.coordId IS NULL"),

        @NamedQuery(name = "GET_BUNDLE_ACTIONS_OLDER_THAN", query = "select OBJECT(w) from BundleActionBean w order by w.lastModifiedTimestamp"),

        @NamedQuery(name = "DELETE_COMPLETED_ACTIONS_FOR_BUNDLE", query = "delete from BundleActionBean a where a.bundleId = :bundleId and (a.status = 'SUCCEEDED' OR a.status = 'FAILED' OR a.status= 'KILLED' OR a.status = 'DONEWITHERROR')")})
public class BundleActionBean implements Writable, JsonBean {

    @Id
    @Column(name = "bundle_action_id")
    private String bundleActionId = null;

    @Column(name = "bundle_id")
    private String bundleId = null;

    @Column(name = "coord_name")
    private String coordName = null;

    @Basic
    @Column(name = "coord_id")
    private String coordId = null;

    @Basic
    @Column(name = "status")
    private String status = null;

    @Basic
    @Column(name = "critical")
    private int critical = 0;

    @Basic
    @Column(name = "pending")
    private int pending = 0;

    @Basic
    @Column(name = "last_modified_time")
    private java.sql.Timestamp lastModifiedTimestamp = null;

    /**
     * bundleActionId to set
     *
     * @param bundleActionId the bundleActionId to set
     */
    public void setBundleActionId(String bundleActionId) {
        this.bundleActionId = bundleActionId;
    }

    /**
     * Get the Bundle Action Id.
     *
     * @return the bundleActionId
     */
    public String getBundleActionId() {
        return bundleActionId;
    }

    /**
     * Get the BundleId
     *
     * @return bundleId
     */
    public String getBundleId() {
        return bundleId;
    }

    /**
     * Set the Bundle Id.
     *
     * @param bundleId
     */
    public void setBundleId(String bundleId) {
        this.bundleId = bundleId;
    }

    /**
     * Get the Coordinator name.
     *
     * @return coordName
     */
    public String getCoordName() {
        return coordName;
    }

    /**
     * Set the Coordinator name.
     *
     * @param coordName
     */
    public void setCoordName(String coordName) {
        this.coordName = coordName;
    }

    /**
     * Get the coordinator Id.
     *
     * @return the coordId
     */
    public String getCoordId() {
        return coordId;
    }

    /**
     * Set the coordinator Id.
     *
     * @param coordId
     */
    public void setCoordId(String coordId) {
        this.coordId = coordId;
    }

    /**
     * Get the Status of the Bundle Action
     *
     * @return status object
     */
    public Status getStatus() {
        return Status.valueOf(this.status);
    }

    /**
     * Get the Status of the Bundle Action
     *
     * @return status string
     */
    public String getStatusStr() {
        return status;
    }

    /**
     * Set the Status of the Bundle Action
     *
     * @param val
     */
    public void setStatus(Status val) {
        this.status = val.toString();
    }

    /**
     * Set Whether this bundle action is critical or not.
     *
     * @param critical set critical to true
     */
    public void setCritical() {
        this.critical = 1;
    }

    /**
     * Reseset Whether this bundle action is critical or not.
     *
     * @param critical set critical to false
     */
    public void resetCritical() {
        this.critical = 0;
    }

    /**
     * Return if the action is critical.
     *
     * @return if the action is critical.
     */
    public boolean isCritical() {
        return critical == 1 ? true : false;
    }

    /**
     * Set some actions are in progress for particular bundle action.
     *
     * @param pending set pending to true
     */
    public void setPending(int pending) {
        this.pending = pending;
    }

    /**
     * increment pending and return it
     *
     * @return pending
     */
    public int incrementAndGetPending() {
        this.pending++;
        return pending;
    }

    /**
     * decrement pending and return it
     *
     * @return pending
     */
    public int decrementAndGetPending() {
        this.pending = Math.max(this.pending-1, 0);
        return pending;
    }

    /**
     * Get some actions are in progress for particular bundle action.
     *
     * @return pending
     */
    public int getPending() {
        return this.pending;
    }

    /**
     * Return if the action is pending.
     *
     * @return if the action is pending.
     */
    public boolean isPending() {
        return pending > 0 ? true : false;
    }

    /**
     * @return true if in terminal status
     */
    public boolean isTerminalStatus() {
        boolean isTerminal = false;
        switch (getStatus()) {
            case SUCCEEDED:
            case FAILED:
            case KILLED:
            case DONEWITHERROR:
                isTerminal = true;
                break;
            default:
                isTerminal = false;
                break;
        }
        return isTerminal;
    }

    /**
     * Set Last modified time.
     *
     * @param lastModifiedTimestamp the lastModifiedTimestamp to set
     */
    public void setLastModifiedTimestamp(java.sql.Timestamp lastModifiedTimestamp) {
        this.lastModifiedTimestamp = lastModifiedTimestamp;
    }

    /**
     * Set Last modified time.
     *
     * @param lastModifiedTime the lastModifiedTime to set
     */
    public void setLastModifiedTime(Date lastModifiedTime) {
        this.lastModifiedTimestamp = DateUtils.convertDateToTimestamp(lastModifiedTime);
    }

    /**
     * Get Last modified time.
     *
     * @return lastModifiedTime
     */
    public Date getLastModifiedTime() {
        return DateUtils.toDate(lastModifiedTimestamp);
    }

    /**
     * Get Last modified time.
     *
     * @return lastModifiedTimestamp
     */
    public Timestamp getLastModifiedTimestamp() {
        return lastModifiedTimestamp;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeStr(dataOutput, getBundleActionId());
        WritableUtils.writeStr(dataOutput, getBundleId());
        WritableUtils.writeStr(dataOutput, getCoordName());
        WritableUtils.writeStr(dataOutput, getCoordId());
        WritableUtils.writeStr(dataOutput, getStatusStr());
        dataOutput.writeInt(critical);
        dataOutput.writeInt(pending);
        dataOutput.writeLong((getLastModifiedTimestamp() != null) ? getLastModifiedTimestamp().getTime() : -1);
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        setBundleActionId(WritableUtils.readStr(dataInput));
        setBundleId(WritableUtils.readStr(dataInput));
        setCoordName(WritableUtils.readStr(dataInput));
        setCoordId(WritableUtils.readStr(dataInput));
        setStatus(Status.valueOf(WritableUtils.readStr(dataInput)));
        critical = dataInput.readInt();
        pending = dataInput.readInt();
        long d = dataInput.readLong();
        if (d != -1) {
            setLastModifiedTime(new Date(d));
        }
    }

    @Override
    public JSONObject toJSONObject() {
        return null;
    }

    @Override
    public JSONObject toJSONObject(String timeZoneId) {
        return null;
    }
}
