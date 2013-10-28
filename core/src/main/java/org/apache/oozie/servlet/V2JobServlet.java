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
package org.apache.oozie.servlet;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.oozie.DagEngine;
import org.apache.oozie.DagEngineException;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.service.DagEngineService;
import org.apache.oozie.service.Services;

@SuppressWarnings("serial")
public class V2JobServlet extends V1JobServlet {

    private static final String INSTRUMENTATION_NAME = "v2job";

    public V2JobServlet() {
        super(INSTRUMENTATION_NAME);
    }

    @Override
    protected JsonBean getWorkflowJob(HttpServletRequest request, HttpServletResponse response) throws XServletException {
        JsonBean jobBean = super.getWorkflowJobBean(request, response);
        return jobBean;
    }

    @Override
    protected JsonBean getWorkflowAction(HttpServletRequest request, HttpServletResponse response) throws XServletException {
        JsonBean actionBean = super.getWorkflowActionBean(request, response);
        return actionBean;
    }

    @Override
    protected int getCoordinatorJobLength(int defaultLen, int len) {
        return (len < 0) ? defaultLen : len;
    }

    @Override
    protected String getJMSTopicName(HttpServletRequest request, HttpServletResponse response) throws XServletException,
            IOException {
        String topicName;
        String jobId = getResourceName(request);
        DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(getUser(request));
        try {
            topicName = dagEngine.getJMSTopicName(jobId);
        }
        catch (DagEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
        return topicName;
    }
}
