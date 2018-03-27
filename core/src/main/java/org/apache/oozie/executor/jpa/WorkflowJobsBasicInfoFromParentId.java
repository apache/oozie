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

package org.apache.oozie.executor.jpa;

import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.util.DateUtils;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class WorkflowJobsBasicInfoFromParentId {
    protected final String parentId;
    protected final int limit;
    protected final int offset;

    WorkflowJobsBasicInfoFromParentId(final String parentId, final int offset, final int limit) {
        this.parentId = parentId;
        this.offset = offset;
        this.limit = limit;
    }

    List<WorkflowJobBean> getBeanFromArray(final List resultList) {
        final List<WorkflowJobBean> wfActionBeanList = new ArrayList<>();

        for (final Object element : resultList) {
            final WorkflowJobBean wfBean = new WorkflowJobBean();
            final Object[] arr = (Object[])element;

            final String id = (String) arr[0];
            if (id != null) {
                wfBean.setId(id);
            }

            final String status = (String) arr[1];
            if (status != null) {
                wfBean.setStatus(WorkflowJob.Status.valueOf(status));
            }

            final Timestamp endTime = (Timestamp) arr[2];
            if (endTime != null) {
                wfBean.setEndTime(DateUtils.toDate(endTime));
            }

            final Timestamp lastModifiedTime = (Timestamp) arr[3];
            if (lastModifiedTime != null) {
                final Date d = DateUtils.toDate(lastModifiedTime);
                wfBean.setLastModifiedTime(d);
            }
            wfActionBeanList.add(wfBean);
        }

        return wfActionBeanList;
    }
}
