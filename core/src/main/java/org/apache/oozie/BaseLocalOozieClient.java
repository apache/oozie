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

import com.google.common.base.Preconditions;
import org.apache.oozie.client.BulkResponse;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.JMSConnectionInfo;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.servlet.V2ValidateServlet;
import org.apache.oozie.util.XConfiguration;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.oozie.BaseEngine.BaseEngineCallable;
import static org.apache.oozie.BaseEngine.callOrRethrow;
import static org.apache.oozie.BaseEngine.throwNoOp;
import static org.apache.oozie.OozieClientOperationHandler.OozieOperationJob;
import static org.apache.oozie.OozieClientOperationHandler.OozieJobOperationCaller;

abstract class BaseLocalOozieClient extends OozieClient {

    private final BaseEngine engine;

    BaseLocalOozieClient(final BaseEngine engine) {
        Preconditions.checkNotNull(engine);
        this.engine = engine;
    }

    @Override
    public String getOozieUrl() {
        return "localoozie";
    }

    @Override
    public String getProtocolUrl() throws OozieClientException {
        return "localoozie";
    }

    @Override
    public synchronized void validateWSVersion() throws OozieClientException {
    }

    @Override
    public Properties createConfiguration() {
        final Properties conf = new Properties();
        if (engine != null) {
            conf.setProperty(USER_NAME, engine.getUser());
        }
        conf.setProperty(GROUP_NAME, "users");
        return conf;
    }

    // no-operation
    @Override
    public void setHeader(final String name, final String value) {
    }

    // no-operation
    @Override
    public String getHeader(final String name) {
        return "";
    }

    // no-operation
    @Override
    public void removeHeader(final String name) {
    }

    // no-operation
    @Override
    public Iterator<String> getHeaderNames() {
        return Collections.<String> emptySet().iterator();
    }

    @Override
    public Map<String, String> getHeaders() {
        return Collections.emptyMap();
    }

    @Override
    public String submit(final Properties conf) throws OozieClientException {
        return callOrRethrow(new BaseEngineCallable<String>() {
            @Override
            public String callOrThrow() throws BaseEngineException {
                return engine.submitJob(new XConfiguration(conf), false);
            }
        });
    }

    @Override
    public void start(final String jobId) throws OozieClientException {
        callOrRethrow(new BaseEngineCallable<Void>() {
            @Override
            public Void callOrThrow() throws BaseEngineException {
                engine.start(jobId);
                return null;
            }
        });
    }

    @Override
    public String run(final Properties conf) throws OozieClientException {
        return callOrRethrow(new BaseEngineCallable<String>() {
            @Override
            public String callOrThrow() throws BaseEngineException {
                return engine.submitJob(new XConfiguration(conf), true);
            }
        });
    }

    @Override
    public void reRun(final String jobId, final Properties conf) throws OozieClientException {
        callOrRethrow(new BaseEngineCallable<Void>() {
            @Override
            public Void callOrThrow() throws BaseEngineException {
                engine.reRun(jobId, new XConfiguration(conf));
                return null;
            }
        });
    }

    @Override
    public void suspend(final String jobId) throws OozieClientException {
        callOrRethrow(new BaseEngineCallable<Void>() {
            @Override
            public Void callOrThrow() throws BaseEngineException {
                engine.suspend(jobId);
                return null;
            }
        });
    }

    @Override
    public void resume(final String jobId) throws OozieClientException {
        callOrRethrow(new BaseEngineCallable<Void>() {
            @Override
            public Void callOrThrow() throws BaseEngineException {
                engine.resume(jobId);
                return null;
            }
        });

    }

    @Override
    public void kill(final String jobId) throws OozieClientException {
        callOrRethrow(new BaseEngineCallable<Void>() {
            @Override
            public Void callOrThrow() throws BaseEngineException {
                engine.kill(jobId);
                return null;
            }
        });
    }

    @Override
    public String dryrun(final Properties conf) throws OozieClientException {
        return callOrRethrow(new BaseEngineCallable<String>() {
            @Override
            public String callOrThrow() throws BaseEngineException {
                return engine.dryRunSubmit(new XConfiguration(conf));
            }
        });
    }

    @Override
    public String getStatus(final String jobId) throws OozieClientException {
        return callOrRethrow(new BaseEngineCallable<String>() {
            @Override
            public String callOrThrow() throws BaseEngineException {
                return engine.getJobStatus(jobId);
            }
        });
    }

    @Override
    public String getJobDefinition(final String jobId) throws OozieClientException {
        return callOrRethrow(new BaseEngineCallable<String>() {
            @Override
            public String callOrThrow() throws BaseEngineException {
                return engine.getDefinition(jobId);
            }
        });
    }

    @Override
    public String getJobId(final String externalId) throws OozieClientException {
        return callOrRethrow(new BaseEngineCallable<String>() {
            @Override
            public String callOrThrow() throws BaseEngineException {
                return engine.getJobIdForExternalId(externalId);
            }
        });
    }

    @Override
    public void slaEnableAlert(final String bundleId, final String actions, final String dates, final String coords)
            throws OozieClientException {
        callOrRethrow(new BaseEngineCallable<Void>() {
            @Override
            public Void callOrThrow() throws BaseEngineException {
                engine.enableSLAAlert(bundleId, actions, dates, coords);
                return null;
            }
        });
    }

    @Override
    public void slaEnableAlert(String jobIds, String actions, String dates) throws OozieClientException {
        slaEnableAlert(jobIds, actions, dates, null);
    }

    @Override
    public void slaDisableAlert(final String bundleId, final String actions, final String dates, final String coords)
            throws OozieClientException {
        callOrRethrow(new BaseEngineCallable<Void>() {
            @Override
            public Void callOrThrow() throws BaseEngineException {
                engine.disableSLAAlert(bundleId, actions, dates, coords);
                return null;
            }
        });
    }

    @Override
    public void slaDisableAlert(String jobIds, String actions, String dates) throws OozieClientException {
        slaDisableAlert(jobIds, actions, dates, null);
    }

    @Override
    public void slaChange(final String bundleId, final String actions, final String dates, final String coords,
                          final String newSlaParams)
            throws OozieClientException {
        callOrRethrow(new BaseEngineCallable<Void>() {
            @Override
            public Void callOrThrow() throws BaseEngineException {
                engine.changeSLA(bundleId, actions, dates, coords, newSlaParams);
                return null;
            }
        });
    }

    @Override
    public void slaChange(String jobIds, String actions, String dates, String newSlaParams) throws OozieClientException {
        slaChange(jobIds, actions, dates, null, newSlaParams);
    }

    @Override
    public void slaChange(String bundleId, String actions, String dates, String coords, Map<String, String> newSlaParams)
            throws OozieClientException {
        slaChange(bundleId, actions, dates, coords, mapToString(newSlaParams));
    }

    @Override
    public void change(final String jobId, final String changeValue) throws OozieClientException {
        callOrRethrow(new BaseEngineCallable<Void>() {
            @Override
            public Void callOrThrow() throws BaseEngineException {
                engine.change(jobId, changeValue);
                return null;
            }
        });
    }

    @Override
    public JSONObject bulkModifyJobs(final String actionType, final String filter, final String jobType, final int start,
                                     final int len)
            throws OozieClientException {
        final JSONObject jsonObject;
        switch (actionType) {
            case RestConstants.JOB_ACTION_KILL:
                jsonObject = killJobs(filter, jobType, start, len);
                break;
            case RestConstants.JOB_ACTION_SUSPEND:
                jsonObject = suspendJobs(filter, jobType, start, len);
                break;
            case RestConstants.JOB_ACTION_RESUME:
                jsonObject = resumeJobs(filter, jobType, start, len);
                break;
            default:
                throw new IllegalArgumentException("Invalid actionType passed. actionType: " + actionType);
        }
        return jsonObject;
    }

    @Override
    public JSONObject killJobs(final String filter, final String jobType, final int start, final int len)
            throws OozieClientException {
        OozieClientOperationHandler handler = new OozieClientOperationHandler(engine);
        final OozieOperationJob operation = handler.getOperationHandler(RestConstants.JOB_ACTION_KILL, filter, start, len);


        return callOrRethrow(new BaseEngineCallable<JSONObject>() {
            @Override
            public JSONObject callOrThrow() throws BaseEngineException {
                return new OozieJobOperationCaller().call(jobType, operation);
            }
        });
    }

    @Override
    public JSONObject suspendJobs(final String filter, final String jobType, final int start, final int len)
            throws OozieClientException {
        OozieClientOperationHandler handler = new OozieClientOperationHandler(engine);
        final OozieOperationJob operation = handler.getOperationHandler(RestConstants.JOB_ACTION_SUSPEND, filter, start, len);

        return callOrRethrow(new BaseEngineCallable<JSONObject>() {
            @Override
            public JSONObject callOrThrow() throws BaseEngineException {
                return new OozieJobOperationCaller().call(jobType, operation);
            }
        });
    }

    @Override
    public JSONObject resumeJobs(final String filter, final String jobType, final int start, final int len)
            throws OozieClientException {
        OozieClientOperationHandler handler = new OozieClientOperationHandler(engine);
        final OozieOperationJob operation = handler.getOperationHandler(RestConstants.JOB_ACTION_RESUME, filter, start, len);

        return callOrRethrow(new BaseEngineCallable<JSONObject>() {
            @Override
            public JSONObject callOrThrow() throws BaseEngineException {
                return new OozieJobOperationCaller().call(jobType, operation);
            }
        });
    }


    @Override
    public WorkflowJob getJobInfo(final String jobId) throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public WorkflowJob getJobInfo(final String jobId, final int start, final int len) throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public List<WorkflowJob> getJobsInfo(final String filter, final int start, final int len) throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public List<WorkflowJob> getJobsInfo(final String filter) throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public List<CoordinatorAction> kill(final String jobId, final String rangeType, final String scope)
            throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public List<BulkResponse> getBulkInfo(final String filter, final int start, final int len) throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public String updateCoord(String jobId, Properties conf, String dryrun, String showDiff) throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public String updateCoord(String jobId, String dryrun, String showDiff) throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public WorkflowAction getWorkflowActionInfo(String actionId) throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public BundleJob getBundleJobInfo(String jobId) throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public CoordinatorJob getCoordJobInfo(String jobId) throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public CoordinatorJob getCoordJobInfo(String jobId, String filter, int start, int len) throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public CoordinatorJob getCoordJobInfo(String jobId, String filter, int start, int len, String order)
            throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public List<WorkflowJob> getWfsForCoordAction(String coordActionId) throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public CoordinatorAction getCoordActionInfo(String actionId) throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public List<CoordinatorAction> reRunCoord(String jobId, String rerunType, String scope, boolean refresh, boolean noCleanup)
            throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public List<CoordinatorAction> reRunCoord(String jobId, String rerunType, String scope, boolean refresh,
                                              boolean noCleanup, boolean failed, Properties props) throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public Void reRunBundle(String jobId, String coordScope, String dateScope, boolean refresh, boolean noCleanup)
            throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public List<CoordinatorJob> getCoordJobsInfo(String filter, int start, int len) throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public List<BundleJob> getBundleJobsInfo(String filter, int start, int len) throws OozieClientException {
        return throwNoOp();
    }

    @Override
    protected HttpURLConnection createRetryableConnection(URL url, String method) throws IOException {
        try {
            return throwNoOp();
        } catch (OozieClientException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected HttpURLConnection createConnection(URL url, String method) throws IOException, OozieClientException {
        return throwNoOp();
    }

    @Override
    public List<CoordinatorAction> ignore(String jobId, String scope) throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public JMSConnectionInfo getJMSConnectionInfo() throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public String getJobLog(String jobId) throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public void getJobAuditLog(String jobId, PrintStream ps) throws OozieClientException {
        throwNoOp();
    }

    @Override
    public void getJobLog(String jobId, String logRetrievalType, String logRetrievalScope, String logFilter, PrintStream ps)
            throws OozieClientException {
        throwNoOp();
    }

    @Override
    public void getJobErrorLog(String jobId, PrintStream ps) throws OozieClientException {
        throwNoOp();
    }

    @Override
    public void getJobLog(String jobId, String logRetrievalType, String logRetrievalScope, PrintStream ps)
            throws OozieClientException {
        throwNoOp();
    }

    @Override
    public String getJMSTopicName(String jobId) throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public void getSlaInfo(int start, int len, String filter) throws OozieClientException {
        throwNoOp();
    }

    @Override
    public void setSystemMode(SYSTEM_MODE status) throws OozieClientException {
        throwNoOp();
    }

    @Override
    public SYSTEM_MODE getSystemMode() throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public String updateShareLib() throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public String listShareLib(String sharelibKey) throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public String getServerBuildVersion() throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public String getClientBuildVersion() {
        throw new UnsupportedOperationException("Operation not supported.");
    }

    @Override
    public String validateXML(final String xmlContent) throws OozieClientException {
        final V2ValidateServlet validateServlet = new V2ValidateServlet();

        try {
            validateServlet.validate(xmlContent);
            return V2ValidateServlet.VALID_WORKFLOW_APP;
        }
        catch (final Exception e) {
            throw new OozieClientException("Cannot validate XML.", e);
        }
    }

    @Override
    public void pollJob(String id, int timeout, int interval, boolean verbose) throws OozieClientException {
        throwNoOp();
    }

    @Override
    public List<String> getQueueDump() throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public Map<String, String> getAvailableOozieServers() throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public Map<String, String> getServerConfiguration() throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public Map<String, String> getJavaSystemProperties() throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public Map<String, String> getOSEnv() throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public Metrics getMetrics() throws OozieClientException {
        return throwNoOp();
    }

    @Override
    public Instrumentation getInstrumentation() throws OozieClientException {
        return throwNoOp();
    }
}