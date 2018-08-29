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

package org.apache.oozie.util.db;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;

public enum CompositeIndex {
    I_WF_JOBS_STATUS_CREATED_TIME ("WF_JOBS", "status", "created_time"),
    I_COORD_ACTIONS_JOB_ID_STATUS ("COORD_ACTIONS", "job_id", "status"),
    I_COORD_JOBS_STATUS_CREATED_TIME ("COORD_JOBS", "status", "created_time"),
    I_COORD_JOBS_STATUS_LAST_MODIFIED_TIME ("COORD_JOBS", "status", "last_modified_time"),
    I_COORD_JOBS_PENDING_DONE_MATERIALIZATION_LAST_MODIFIED_TIME
            ("COORD_JOBS", "pending", "done_materialization", "last_modified_time"),
    I_COORD_JOBS_PENDING_LAST_MODIFIED_TIME ("COORD_JOBS", "pending", "last_modified_time"),
    I_BUNLDE_JOBS_STATUS_CREATED_TIME ("BUNDLE_JOBS", "status", "created_time"),
    I_BUNLDE_JOBS_STATUS_LAST_MODIFIED_TIME ("BUNDLE_JOBS", "status", "last_modified_time"),
    I_BUNLDE_ACTIONS_PENDING_LAST_MODIFIED_TIME ("BUNDLE_ACTIONS", "pending", "last_modified_time");

    private final String createStatement;

    CompositeIndex(String tableName, String ... columnNames) {
        final String columns = Joiner.on(", ").join(columnNames);
        this.createStatement = String.format("CREATE INDEX %s ON %s (%s)",
                name(), tableName.toUpperCase(), columns);
    }

    public static List<String> getIndexStatements() {
        Function<CompositeIndex, String> compositeIndexToString = new Function<CompositeIndex, String>() {
            public String apply(CompositeIndex i) { return i != null ? i.createStatement : null; }
        };

        List<CompositeIndex> indexList = Arrays.asList(values());
        return Lists.transform(indexList, compositeIndexToString);
    }

    public static boolean find(String indexName) {
        try {
            valueOf(indexName.toUpperCase());
        } catch (IllegalArgumentException | NullPointerException ex) {
            return false;
        }
        return true;
    }
}
