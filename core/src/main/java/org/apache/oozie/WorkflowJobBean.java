/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie;

import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.client.rest.JsonWorkflowJob;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.util.WritableUtils;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.IOException;
import java.io.DataOutput;
import java.util.Date;

public class WorkflowJobBean extends JsonWorkflowJob implements Writable {
    private String authToken;
    private String logToken;
    private WorkflowInstance workflowInstance;
    private String protoActionConf;

    /**
     * Default constructor.
     */
    public WorkflowJobBean() {
    }

    /**
     * Serialize the workflow bean to a data output.
     *
     * @param dataOutput data output.
     * @throws IOException thrown if the workflow bean could not be serialized.
     */
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeStr(dataOutput, getAppPath());
        WritableUtils.writeStr(dataOutput, getAppName());
        WritableUtils.writeStr(dataOutput, getId());
        WritableUtils.writeStr(dataOutput, getConf());
        WritableUtils.writeStr(dataOutput, getStatus().toString());
        dataOutput.writeLong((getCreatedTime() != null) ? getCreatedTime().getTime() : -1);
        dataOutput.writeLong((getStartTime() != null) ? getStartTime().getTime() : -1);
        dataOutput.writeLong((getLastModTime() != null) ? getLastModTime().getTime() : -1);
        dataOutput.writeLong((getEndTime() != null) ? getEndTime().getTime() : -1);
        WritableUtils.writeStr(dataOutput, getUser());
        WritableUtils.writeStr(dataOutput, getGroup());
        dataOutput.writeInt(getRun());
        WritableUtils.writeStr(dataOutput, authToken);
        WritableUtils.writeStr(dataOutput, logToken);
        WritableUtils.writeStr(dataOutput, protoActionConf);
    }

    /**
     * Deserialize a workflow bean from a data input.
     *
     * @param dataInput data input.
     * @throws IOException thrown if the workflow bean could not be deserialized.
     */
    public void readFields(DataInput dataInput) throws IOException {
        setAppPath(WritableUtils.readStr(dataInput));
        setAppName(WritableUtils.readStr(dataInput));
        setId(WritableUtils.readStr(dataInput));
        setConf(WritableUtils.readStr(dataInput));
        setStatus(WorkflowJob.Status.valueOf(WritableUtils.readStr(dataInput)));
        long d = dataInput.readLong();
        if (d != -1) {
            setCreatedTime(new Date(d));
        }
        d = dataInput.readLong();
        if (d != -1) {
            setStartTime(new Date(d));
        }
        d = dataInput.readLong();
        if(d != -1) {
            setLastModTime(new Date(d));
        }
        d = dataInput.readLong();
        if (d != -1) {
            setEndTime(new Date(d));
        }
        setUser(WritableUtils.readStr(dataInput));
        setGroup(WritableUtils.readStr(dataInput));
        setRun(dataInput.readInt());
        authToken = WritableUtils.readStr(dataInput);
        logToken = WritableUtils.readStr(dataInput);
        protoActionConf = WritableUtils.readStr(dataInput);
    }

    public String getAuthToken() {
        return authToken;
    }

    public void setAuthToken(String authToken) {
        this.authToken = authToken;
    }

    public String getLogToken() {
        return logToken;
    }

    public void setLogToken(String logToken) {
        this.logToken = logToken;
    }

    public WorkflowInstance getWorkflowInstance() {
        return workflowInstance;
    }

    public void setWorkflowInstance(WorkflowInstance workflowInstance) {
        this.workflowInstance = workflowInstance;
    }

    public String getProtoActionConf() {
        return protoActionConf;
    }

    public void setProtoActionConf(String protoActionConf) {
        this.protoActionConf = protoActionConf;
    }
}
