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

import org.apache.hadoop.conf.Configuration;
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
     * @param conf
     */
    public abstract void init(Configuration conf);

    /**
     * Destroy the listener
     */
    public abstract void destroy();

    /**
     * On workflow job transition
     * @param WorkflowJobEvent
     */
    public abstract void onWorkflowJobEvent(WorkflowJobEvent wje);

    /**
     * On workflow action transition
     * @param WorkflowActionEvent
     */
    public abstract void onWorkflowActionEvent(WorkflowActionEvent wae);

    /**
     * On coordinator job transition
     * @param CoordinatorJobEvent
     */
    public abstract void onCoordinatorJobEvent(CoordinatorJobEvent cje);

    /**
     * On coordinator action transition
     * @param CoordinatorActionEvent
     */
    public abstract void onCoordinatorActionEvent(CoordinatorActionEvent cae);

    /**
     * On bundle job transition
     * @param BundleJobEvent
     */
    public abstract void onBundleJobEvent(BundleJobEvent bje);

}
