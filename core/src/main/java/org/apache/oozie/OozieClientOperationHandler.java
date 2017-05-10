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

import org.apache.oozie.client.rest.RestConstants;
import org.json.simple.JSONObject;

class OozieClientOperationHandler {

    private static final String WF = "wf";
    private static final String BUNDLE = "bundle";
    private static final String COORD = "coord";
    private static final String GMT = "GMT";

    private BaseEngine engine;

    OozieClientOperationHandler(BaseEngine engine) {
        this.engine = engine;
    }

    interface OozieOperationJob {
        JSONObject BundleJob() throws BundleEngineException;

        JSONObject CoordinatorJob() throws CoordinatorEngineException;

        JSONObject WorkflowsJob() throws DagEngineException;
    }

    abstract class AbstractOozieOperationJob implements OozieOperationJob {
        protected final String filter;
        protected final int start;
        protected final int len;

        AbstractOozieOperationJob(final String filter, final int start, final int len) {
            this.filter = filter;
            this.start = start;
            this.len = len;
        }

        @Override
        public JSONObject BundleJob() throws BundleEngineException {
            return OozieJsonFactory.getBundleJSONObject(createBundleJobInfo(), GMT);
        }

        abstract BundleJobInfo createBundleJobInfo() throws BundleEngineException;

        @Override
        public JSONObject CoordinatorJob() throws CoordinatorEngineException {
            return OozieJsonFactory.getCoordJSONObject(createCoordinatorJobInfo(), GMT);
        }

        abstract CoordinatorJobInfo createCoordinatorJobInfo() throws CoordinatorEngineException;

        @Override
        public JSONObject WorkflowsJob() throws DagEngineException {
            return OozieJsonFactory.getWFJSONObject(createWorkflowsInfo(), GMT);
        }

        abstract WorkflowsInfo createWorkflowsInfo() throws DagEngineException;
    }


    static class OozieJobOperationCaller {
        JSONObject call(final String jobType, final OozieOperationJob job)
                throws DagEngineException, BundleEngineException, CoordinatorEngineException {
            switch (jobType) {
                case WF:
                    return job.WorkflowsJob();
                case COORD:
                    return job.CoordinatorJob();
                case BUNDLE:
                    return job.BundleJob();
                default:
                    throw new IllegalArgumentException("Invalid jobType passed. jobType: " + jobType);
            }
        }
    }

    class KillOperation extends AbstractOozieOperationJob {

        KillOperation(final String filter, final int start, final int len) {
            super(filter, start, len);
        }

        @Override
        BundleJobInfo createBundleJobInfo() throws BundleEngineException {
            return ((BundleEngine) engine).killJobs(filter, start, len);
        }

        @Override
        CoordinatorJobInfo createCoordinatorJobInfo() throws CoordinatorEngineException {
            return ((CoordinatorEngine) engine).killJobs(filter, start, len);
        }

        @Override
        WorkflowsInfo createWorkflowsInfo() throws DagEngineException {
            return ((DagEngine) engine).killJobs(filter, start, len);
        }
    }

    class SuspendingOperation extends AbstractOozieOperationJob {

        SuspendingOperation(final String filter, final int start, final int len) {
            super(filter, start, len);
        }

        @Override
        BundleJobInfo createBundleJobInfo() throws BundleEngineException {
            return ((BundleEngine) engine).suspendJobs(filter, start, len);
        }

        @Override
        CoordinatorJobInfo createCoordinatorJobInfo() throws CoordinatorEngineException {
            return ((CoordinatorEngine) engine).suspendJobs(filter, start, len);
        }

        @Override
        WorkflowsInfo createWorkflowsInfo() throws DagEngineException {
            return ((DagEngine) engine).suspendJobs(filter, start, len);
        }
    }

    class ResumingOperation extends AbstractOozieOperationJob {

        ResumingOperation(final String filter, final int start, final int len) {
            super(filter, start, len);
        }

        @Override
        BundleJobInfo createBundleJobInfo() throws BundleEngineException {
            return ((BundleEngine) engine).resumeJobs(filter, start, len);
        }

        @Override
        CoordinatorJobInfo createCoordinatorJobInfo() throws CoordinatorEngineException {
            return ((CoordinatorEngine) engine).resumeJobs(filter, start, len);
        }

        @Override
        WorkflowsInfo createWorkflowsInfo() throws DagEngineException {
            return ((DagEngine) engine).resumeJobs(filter, start, len);
        }
    }

    OozieOperationJob getOperationHandler(String operation, final String filter, final int start, final int len) {
        switch (operation) {
            case RestConstants.JOB_ACTION_KILL:
                return new KillOperation(filter, start, len);
            case RestConstants.JOB_ACTION_SUSPEND:
                return new SuspendingOperation(filter, start, len);
            case RestConstants.JOB_ACTION_RESUME:
                return new ResumingOperation(filter, start, len);
            default:
                throw new IllegalArgumentException(operation);
        }
    }
}
