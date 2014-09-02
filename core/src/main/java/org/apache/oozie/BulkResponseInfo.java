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

import org.apache.oozie.client.rest.BulkResponseImpl;

/**
 * Class that stores the entire retrieved info from the bulk request command
 * along with params for pagination of the results
 * Query execution in BulkJPAExecutor returns an object of BulkResponseInfo class
 */
public class BulkResponseInfo {
    private int start = 1;
    private int len = 50;
    private long total;
    private List<BulkResponseImpl> responses;

    /**
     * Create a bulk response info bean.
     *
     * @param bundle job being returned.
     * @param start bulk entries offset.
     * @param len number of bulk entries.
     * @param total total bulk entries.
     */
    public BulkResponseInfo(List<BulkResponseImpl> responses, int start, int len, long total) {
        this.start = start;
        this.len = len;
        this.total = total;
        this.responses = responses;
    }

    /**
     * Return the coordinator actions being returned.
     *
     * @return the coordinator actions being returned.
     */
    public List<BulkResponseImpl> getResponses() {
        return responses;
    }

    /**
     * Return the offset of the bulk entries being returned.
     * <p/>
     * For pagination purposes.
     *
     * @return the offset of the bulk entries being returned.
     */
    public int getStart() {
        return start;
    }

    /**
     * Return the number of the bulk entries being returned.
     * <p/>
     * For pagination purposes.
     *
     * @return the number of the bulk entries being returned.
     */
    public int getLen() {
        return len;
    }

    /**
     * Return the total number of bulk entries.
     * <p/>
     * For pagination purposes.
     *
     * @return the total number of bulk entries.
     */
    public long getTotal() {
        return total;
    }
}
