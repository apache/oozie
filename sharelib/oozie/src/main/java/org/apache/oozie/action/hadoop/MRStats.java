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

package org.apache.oozie.action.hadoop;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.json.simple.JSONObject;

/**
 * Class to collect statistics for Map-Reduce action.
 */
public class MRStats extends ActionStats {
    public static final String ACTION_TYPE_LABEL = "ACTION_TYPE";
    private Counters counters = null;

    public MRStats(Counters groups) {
        this.currentActionType = ActionType.MAP_REDUCE;
        this.counters = groups;
    }

    @SuppressWarnings("unchecked")
    @Override
    public String toJSON() {
        if (counters == null) {
            return null;
        }

        JSONObject groups = new JSONObject();
        groups.put(ACTION_TYPE_LABEL, getCurrentActionType().toString());
        for (String gName : counters.getGroupNames()) {
            JSONObject group = new JSONObject();
            for (Counter counter : counters.getGroup(gName)) {
                String cName = counter.getName();
                Long cValue = counter.getValue();
                group.put(cName, cValue);
            }
            groups.put(gName, group);
        }
        return groups.toJSONString();
    }
}
