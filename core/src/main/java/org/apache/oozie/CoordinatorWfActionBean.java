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

import org.apache.oozie.client.CoordinatorWfAction;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.client.rest.JsonTags;
import org.json.simple.JSONObject;

public class CoordinatorWfActionBean implements CoordinatorWfAction, JsonBean{

    private int actionNumber;

    private WorkflowActionBean action;

    private String strNullReason;

    public CoordinatorWfActionBean(int actionNumber) {
        this(actionNumber, null, null);
    }

    public CoordinatorWfActionBean(int actionNumber, WorkflowActionBean action, String nullReason) {
        this.actionNumber = actionNumber;
        this.action = action;
        this.strNullReason = nullReason;
    }

    public int getActionNumber() {
        return actionNumber;
    }

    public WorkflowActionBean getAction() {
        return action;
    }

    public String getNullReason() {
        return strNullReason;
    }

    public void setActionNumber(int actionNumber) {
        this.actionNumber = actionNumber;
    }

    public void setAction(WorkflowActionBean action) {
        this.action = action;
    }

    public void setNullReason(String nullReason) {
        this.strNullReason = nullReason;
    }

    @Override
    public JSONObject toJSONObject() {
        return toJSONObject("GMT");
    }

    @Override
    public JSONObject toJSONObject(String timeZoneId) {
        JSONObject json = new JSONObject();
        json.put(JsonTags.COORDINATOR_WF_ACTION_NUMBER, actionNumber);
        json.put(JsonTags.COORDINATOR_WF_ACTION_NULL_REASON, strNullReason);
        if (action != null) {
            json.put(JsonTags.COORDINATOR_WF_ACTION, action.toJSONObject(timeZoneId));
        }
        else {
            json.put(JsonTags.COORDINATOR_WF_ACTION, action);
        }
        return json;
    }
}
