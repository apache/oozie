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

import java.util.List;

/**
 * Bean that contains the result for a workflows query.
 */
public class WorkflowsInfo {
    private int start;
    private int len;
    private int total;
    private List<WorkflowJobBean> workflows;

    /**
     * Create  a workflows info bean.
     *
     * @param workflows workflows being returned.
     * @param start workflows offset.
     * @param len number of workflows.
     * @param total total workflows.
     */
    public WorkflowsInfo(List<WorkflowJobBean> workflows, int start, int len, int total) {
        this.start = start;
        this.len = len;
        this.total = total;
        this.workflows = workflows;
    }

    /**
     * Return the workflows being returned.
     *
     * @return the workflows being returned.
     */
    public List<WorkflowJobBean> getWorkflows() {
        return workflows;
    }

    /**
     * Return the offset of the workflows being returned. <p/> For pagination purposes.
     *
     * @return the offset of the workflows being returned.
     */
    public int getStart() {
        return start;
    }

    /**
     * Return the number of the workflows being returned. <p/> For pagination purposes.
     *
     * @return the number of the workflows being returned.
     */
    public int getLen() {
        return len;
    }

    /**
     * Return the total number of workflows. <p/> For pagination purposes.
     *
     * @return the total number of workflows.
     */
    public int getTotal() {
        return total;
    }

}
