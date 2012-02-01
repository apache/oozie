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
package org.apache.oozie.coord;

import java.util.Date;
import java.util.TimeZone;

/**
 * This class represents a Coordinator action.
 */
public class SyncCoordAction {
    private String actionId;
    private String name;
    private Date nominalTime;
    private Date actualTime;
    private TimeZone timeZone;
    private int frequency;
    private TimeUnit timeUnit;
    private TimeUnit endOfDuration; // End of Month or End of Days

    public String getActionId() {
        return this.actionId;
    }

    public void setActionId(String id) {
        this.actionId = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }

    public int getFrequency() {
        return frequency;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public void setTimeUnit(TimeUnit timeUnit) {
        this.timeUnit = timeUnit;
    }

    /**
     * @return the nominalTime
     */
    public Date getNominalTime() {
        return nominalTime;
    }

    /**
     * @param nominalTime the nominalTime to set
     */
    public void setNominalTime(Date nominalTime) {
        this.nominalTime = nominalTime;
    }

    /**
     * @return the actualTime
     */
    public Date getActualTime() {
        return actualTime;
    }

    /**
     * @param actualTime the actualTime to set
     */
    public void setActualTime(Date actualTime) {
        this.actualTime = actualTime;
    }

    public TimeUnit getEndOfDuration() {
        return endOfDuration;
    }

    public void setEndOfDuration(TimeUnit endOfDuration) {
        this.endOfDuration = endOfDuration;
    }

}
