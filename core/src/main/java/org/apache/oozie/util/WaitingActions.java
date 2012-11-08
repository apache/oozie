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
package org.apache.oozie.util;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * The object to store list of waiting actions and timestamp corresponding to a
 * particular hcat partition
 */
public class WaitingActions {
    private List<String> actions;
    private Timestamp lastModified;

    /**
     * Empty (default) constructor
     */
    public WaitingActions() {
        this(new CopyOnWriteArrayList<String>());
    }

    /**
     * Constructor that sets list directly
     *
     * @param List newActions
     */
    WaitingActions(List<String> newActions) {
        this.setActions(newActions);
        this.setLastModified(DateUtils.convertDateToTimestamp(new Date()));
    }

    /**
     * constructor that sets single action in list
     *
     * @param newActionId
     */
    public WaitingActions(String actionId) {
        this();
        this.getActions().add(actionId);
    }

    /**
     * @return the actions
     */
    public List<String> getActions() {
        return actions;
    }

    /**
     * @param actions the actions to set
     */
    public void setActions(List<String> actions) {
        this.actions = actions;
    }

    /**
     * @return the lastModified
     */
    public Timestamp getLastModified() {
        return lastModified;
    }

    /**
     * @param lastModified the lastModified to set
     */
    public void setLastModified(Timestamp lastModified) {
        this.lastModified = lastModified;
    }

    /**
     * @param lastModified the lastModified to set
     */
    public void setLastModified(Date lastModified) {
        this.lastModified = DateUtils.convertDateToTimestamp(lastModified);
    }

    /**
     * Add actionId to list and update lastModifiedTime
     *
     * @param actionId
     */
    public void addAndUpdate(String actionId) {
        getActions().add(actionId);
        setLastModified(new Date());
    }
}
