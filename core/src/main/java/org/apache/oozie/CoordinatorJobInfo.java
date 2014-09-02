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

public class CoordinatorJobInfo {
    private int start;
    private int len;
    private int total;
    private List<CoordinatorJobBean> jobs;

    /**
     * Create a coordinator info bean.
     * 
     * @param coordiantor jobs being returned.
     * @param start coordiantor jobs offset.
     * @param len number of coordiantor jobs.
     * @param total total coordiantor jobs.
     */
    public CoordinatorJobInfo(List<CoordinatorJobBean> jobs, int start, int len, int total) {
        this.start = start;
        this.len = len;
        this.total = total;
        this.jobs = jobs;
    }

    /**
     * Return the coordiantor jobs being returned.
     *
     * @return the coordiantor jobs being returned.
     */
    public List<CoordinatorJobBean> getCoordJobs() {
        return jobs;
    }

    /**
     * Return the offset of the workflows being returned. <p/> For pagination purposes.
     *
     * @return the offset of the coordiantor jobs being returned.
     */
    public int getStart() {
        return start;
    }

    /**
     * Return the number of the workflows being returned. <p/> For pagination purposes.
     *
     * @return the number of the coordiantor jobs being returned.
     */
    public int getLen() {
        return len;
    }

    /**
     * Return the total number of workflows. <p/> For pagination purposes.
     *
     * @return the total number of coordiantor jobs.
     */
    public int getTotal() {
        return total;
    }

}
