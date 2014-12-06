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
 * This class is a bean to represent a dataset.
 */
public class SyncCoordDataset {
    protected String name;
    protected String type;
    protected int frequency;
    private TimeUnit timeUnit;
    private TimeZone timeZone;
    private TimeUnit endOfDuration; // End of Month or End of Days
    protected Date initInstance;
    protected String uriTemplate;
    protected String doneFlag;

    /**
     * @return the name
     */
    public String getDoneFlag() {
        return doneFlag;
    }

    /**
     * @param name the name to set
     */
    public void setDoneFlag(String doneFlag) {
        this.doneFlag = doneFlag;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the frequency
     */
    public int getFrequency() {
        return frequency;
    }

    /**
     * @param frequency the frequency to set
     */
    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    /**
     * @return the uriTemplate
     */
    public String getUriTemplate() {
        return uriTemplate;
    }

    /**
     * @param uriTemplate the uriTemplate to set
     */
    public void setUriTemplate(String uriTemplate) {
        this.uriTemplate = uriTemplate;
    }

    /**
     * @return the type
     */
    public String getType() {
        return type;
    }

    /**
     * @param type the type to set
     */
    public void setType(String type) {
        this.type = type;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public void setTimeUnit(TimeUnit timeUnit) {
        this.timeUnit = timeUnit;
    }

    public Date getInitInstance() {
        return initInstance;
    }

    public void setInitInstance(Date initInstance) {
        this.initInstance = initInstance;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }

    public TimeUnit getEndOfDuration() {
        return endOfDuration;
    }

    public void setEndOfDuration(TimeUnit endOfDuration) {
        this.endOfDuration = endOfDuration;
    }

}
