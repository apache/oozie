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

import java.util.List;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;

// Note: methods which modify/read the state of errorHolder are synchronized to avoid data races when LauncherAM invokes getError()
public class AMRMCallBackHandler implements AMRMClientAsync.CallbackHandler {
    private ErrorHolder errorHolder;

    @Override
    public void onContainersCompleted(List<ContainerStatus> containerStatuses) {
        //noop
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
        //noop
    }

    @Override
    public synchronized void onShutdownRequest() {
        System.out.println("Resource manager requested AM Shutdown");
        errorHolder = new ErrorHolder();
        errorHolder.setErrorCode(0);
        errorHolder.setErrorMessage("ResourceManager requested AM Shutdown");
    }

    @Override
    public void onNodesUpdated(List<NodeReport> nodeReports) {
        //noop
    }

    @Override
    public float getProgress() {
        return 0.5f;
    }

    @Override
    public synchronized void onError(final Throwable ex) {
        System.out.println("Received asynchronous error");
        ex.printStackTrace();
        errorHolder = new ErrorHolder();
        errorHolder.setErrorCode(0);
        errorHolder.setErrorMessage(ex.getMessage());
        errorHolder.setErrorCause(ex);
    }

    public synchronized ErrorHolder getError() {
        return errorHolder;
    }
}