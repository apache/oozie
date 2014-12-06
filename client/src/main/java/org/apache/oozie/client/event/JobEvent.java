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

package org.apache.oozie.client.event;

import java.io.Serializable;
import java.util.Date;

import org.apache.oozie.AppType;

/**
 * An abstract implementation of the Event interface, related to
 * notification events after job status changes
 */
public abstract class JobEvent extends Event implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Coarse-grained status of the job event
     */
    public static enum EventStatus {
        WAITING, STARTED, SUCCESS, SUSPEND, FAILURE
    }

    private String id;
    private String parentId;
    private String user;
    private AppType appType;
    private String appName;
    private EventStatus eventStatus;
    private Date startTime;
    private Date endTime;

    public JobEvent(String id, String parentId, String user, AppType appType, String appName) {
        super(MessageType.JOB);
        this.id = id;
        this.parentId = parentId;
        this.user = user;
        this.appType = appType;
        this.appName = appName;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getParentId() {
        return parentId;
    }

    public void setParentId(String id) {
        parentId = id;
    }

    /**
     * Get the AppType of the event
     *
     * @return AppType
     */
    public AppType getAppType() {
        return appType;
    }

    public void setAppType(AppType type) {
        appType = type;
    }

    /**
     * Get the app-name of the job generating this event
     *
     * @return String app-name
     */
    public String getAppName() {
        return appName;
    }

    public void setAppName(String name) {
        appName = name;
    }

    /**
     * Get user name
     *
     * @return user
     */
    public String getUser() {
        return user;
    }

    public void setUser(String euser) {
        user = euser;
    }

    /**
     * Get the coarse status
     *
     * @return EventStatus
     */
    public EventStatus getEventStatus() {
        return eventStatus;
    }

    public void setEventStatus(EventStatus eStatus) {
        eventStatus = eStatus;
    }


    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date time) {
        startTime = time;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date time) {
        endTime = time;
    }

    @Override
    public String toString() {
        return "ID: " + getId() + ", MsgType:" + getMsgType() + ", AppType: " + getAppType() + ", Appname: "
                + getAppName() + ", Status: " + getEventStatus();
    }

}
