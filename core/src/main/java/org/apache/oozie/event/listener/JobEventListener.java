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
package org.apache.oozie.event.listener;

import org.apache.oozie.event.BundleJobEvent;
import org.apache.oozie.event.CoordinatorActionEvent;
import org.apache.oozie.event.CoordinatorJobEvent;
import org.apache.oozie.event.WorkflowActionEvent;
import org.apache.oozie.event.WorkflowJobEvent;

/**
 * Event listener for Job notification events, defining methods corresponding to
 * job status changes
 */
public abstract class JobEventListener {

    /**
     * Initialize the listener
     */
    public abstract void init();

    /**
     * Destroy the listener
     */
    public abstract void destroy();

    /**
     * On workflow job transition to start state
     * @param WorkflowJobEvent
     */
    public abstract void onWorkflowJobStart(WorkflowJobEvent wje);

    /**
     * On workflow job transition to success state
     * @param WorkflowJobEvent
     */
    public abstract void onWorkflowJobSuccess(WorkflowJobEvent wje);

    /**
     * On workflow job transition to failure state
     * @param WorkflowJobEvent
     */
    public abstract void onWorkflowJobFailure(WorkflowJobEvent wje);

    /**
     * On workflow job transition to suspend state
     * @param WorkflowJobEvent
     */
    public abstract void onWorkflowJobSuspend(WorkflowJobEvent wje);

    /**
     * On workflow action transition to start state
     * @param WorkflowActionEvent
     */
    public abstract void onWorkflowActionStart(WorkflowActionEvent wae);

    /**
     * On workflow action transition to success state
     * @param WorkflowActionEvent
     */
    public abstract void onWorkflowActionSuccess(WorkflowActionEvent wae);

    /**
     * On workflow action transition to failure state
     * @param WorkflowActionEvent
     */
    public abstract void onWorkflowActionFailure(WorkflowActionEvent wae);

    /**
     * On workflow action transition to suspend state
     * @param WorkflowActionEvent
     */
    public abstract void onWorkflowActionSuspend(WorkflowActionEvent wae);

    /**
     * On coord job transition to start state
     * @param CoordinatorJobEvent
     */
    public abstract void onCoordinatorJobStart(CoordinatorJobEvent wje);

    /**
     * On coord job transition to success state
     * @param CoordinatorJobEvent
     */
    public abstract void onCoordinatorJobSuccess(CoordinatorJobEvent wje);

    /**
     * On coord job transition to failure state
     * @param CoordinatorJobEvent
     */
    public abstract void onCoordinatorJobFailure(CoordinatorJobEvent wje);

    /**
     * On coord job transition to suspend state
     * @param CoordinatorJobEvent
     */
    public abstract void onCoordinatorJobSuspend(CoordinatorJobEvent wje);

    /**
     * On coord action transition to waiting state
     * @param CoordinatorActionEvent
     */
    public abstract void onCoordinatorActionWaiting(CoordinatorActionEvent wae);

    /**
     * On coord action transition to start state
     * @param CoordinatorActionEvent
     */
    public abstract void onCoordinatorActionStart(CoordinatorActionEvent wae);

    /**
     * On coord action transition to success state
     * @param CoordinatorActionEvent
     */
    public abstract void onCoordinatorActionSuccess(CoordinatorActionEvent wae);

    /**
     * On coord action transition to failure state
     * @param CoordinatorActionEvent
     */
    public abstract void onCoordinatorActionFailure(CoordinatorActionEvent wae);

    /**
     * On coord action transition to suspend state
     * @param CoordinatorActionEvent
     */
    public abstract void onCoordinatorActionSuspend(CoordinatorActionEvent wae);

    /**
     * On bundle job transition to start state
     * @param BundleJobEvent
     */
    public abstract void onBundleJobStart(BundleJobEvent wje);

    /**
     * On bundle job transition to success state
     * @param BundleJobEvent
     */
    public abstract void onBundleJobSuccess(BundleJobEvent wje);

    /**
     * On bundle job transition to failure state
     * @param BundleJobEvent
     */
    public abstract void onBundleJobFailure(BundleJobEvent wje);

    /**
     * On bundle job transition to suspend state
     * @param BundleJobEvent
     */
    public abstract void onBundleJobSuspend(BundleJobEvent wje);

}
