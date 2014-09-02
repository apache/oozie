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

package org.apache.oozie.sla.listener;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.event.SLAEvent;

/**
 * Event listener for SLA related events, defining methods corresponding to
 * SLA mets/misses
 */
public abstract class SLAEventListener {

    /**
     * Initialize the listener
     * @param conf
     */
    public abstract void init(Configuration conf) throws Exception;

    /**
     * Destroy the listener
     */
    public abstract void destroy();

    /**
     * on SLA job start-time limit met
     * @param SLAEvent
     */
    public abstract void onStartMet(SLAEvent work);

    /**
     * on SLA job start-time limit missed
     * @param SLAEvent
     */
    public abstract void onStartMiss(SLAEvent event);

    /**
     * on SLA job end-time limit met
     * @param SLAEvent
     */
    public abstract void onEndMet(SLAEvent work);

    /**
     * on SLA job end-time limit missed
     * @param SLAEvent
     */
    public abstract void onEndMiss(SLAEvent event);

    /**
     * on SLA job duration limit met
     * @param SLAEvent
     */
    public abstract void onDurationMet(SLAEvent work);

    /**
     * on SLA job duration limit missed
     * @param SLAEvent
     */
    public abstract void onDurationMiss(SLAEvent event);

}
