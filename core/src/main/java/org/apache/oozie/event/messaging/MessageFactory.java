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

package org.apache.oozie.event.messaging;

import org.apache.hadoop.util.ReflectionUtils;
import org.apache.oozie.client.event.SLAEvent;
import org.apache.oozie.client.event.message.CoordinatorActionMessage;
import org.apache.oozie.client.event.message.WorkflowJobMessage;
import org.apache.oozie.client.event.message.SLAMessage;
import org.apache.oozie.event.CoordinatorActionEvent;
import org.apache.oozie.event.WorkflowJobEvent;
import org.apache.oozie.service.Services;

/**
 * Factory for constructing messages and retrieving the serializer
 */
public class MessageFactory {

    public static final String OOZIE_MESSAGE_FORMAT = Services.get().getConf().get("message.format", "json");
    public static final String OOZIE_MESSAGE_SERIALIZE = "oozie.jms.serialize.";

    private static class MessageSerializerHolder {
        private static String messageSerializerInstance = Services
                .get()
                .getConf()
                .get(OOZIE_MESSAGE_SERIALIZE + OOZIE_MESSAGE_FORMAT,
                        "org.apache.oozie.event.messaging.JSONMessageSerializer");
        public static final MessageSerializer INSTANCE;
        static {
            try {
                INSTANCE = (MessageSerializer) ReflectionUtils.newInstance(Class.forName(messageSerializerInstance),
                        null);
            }
            catch (ClassNotFoundException cnfe) {
                throw new IllegalStateException("Could not construct the serializer ", cnfe);
            }
        }
    }

    /**
     * Gets the configured serializer
     *
     * @return
     */
    public static MessageSerializer getMessageSerializer() {
        return MessageSerializerHolder.INSTANCE;
    }

    /**
     * Constructs and returns the workflow job message for workflow job event
     *
     * @param wfJobEvent the workflow job event
     * @return
     */
    public static WorkflowJobMessage createWorkflowJobMessage(WorkflowJobEvent wfJobEvent) {
        WorkflowJobMessage wfJobMessage = new WorkflowJobMessage(wfJobEvent.getEventStatus(), wfJobEvent.getId(),
                wfJobEvent.getParentId(), wfJobEvent.getStartTime(), wfJobEvent.getEndTime(), wfJobEvent.getStatus(),
                wfJobEvent.getUser(), wfJobEvent.getAppName(), wfJobEvent.getErrorCode(), wfJobEvent.getErrorMessage());
        return wfJobMessage;
    }

    /**
     * Constructs and returns the coordinator action message for coordinator
     * action event
     *
     * @param coordActionEvent the coordinator action event
     * @return
     */
    public static CoordinatorActionMessage createCoordinatorActionMessage(CoordinatorActionEvent coordActionEvent) {
        CoordinatorActionMessage coordActionMessage = new CoordinatorActionMessage(coordActionEvent.getEventStatus(),
                coordActionEvent.getId(), coordActionEvent.getParentId(), coordActionEvent.getStartTime(),
                coordActionEvent.getEndTime(), coordActionEvent.getNominalTime(), coordActionEvent.getStatus(),
                coordActionEvent.getUser(), coordActionEvent.getAppName(), coordActionEvent.getMissingDeps(),
                coordActionEvent.getErrorCode(), coordActionEvent.getErrorMessage());
        return coordActionMessage;
    }

    /**
     * Constructs and returns SLA notification message
     * @param event SLA event
     * @return
     */
    public static SLAMessage createSLAMessage(SLAEvent event) {
        SLAMessage slaMessage = new SLAMessage(event.getEventStatus(), event.getSLAStatus(), event.getAppType(),
                event.getAppName(), event.getUser(), event.getId(), event.getParentId(), event.getNominalTime(),
                event.getExpectedStart(), event.getActualStart(), event.getExpectedEnd(), event.getActualEnd(),
                event.getExpectedDuration(), event.getActualDuration(), event.getNotificationMsg(), event.getUpstreamApps());
        return slaMessage;
    }
}
