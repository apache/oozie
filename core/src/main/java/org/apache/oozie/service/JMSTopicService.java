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
package org.apache.oozie.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.event.CoordinatorActionEvent;
import org.apache.oozie.event.WorkflowJobEvent;
import org.apache.oozie.executor.jpa.BundleJobGetForUserJPAExecutor;
import org.apache.oozie.executor.jpa.CoordinatorJobGetForUserJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowJobGetForUserJPAExecutor;
import org.apache.oozie.util.XLog;

/**
 * JMS Topic service to retrieve topic names from events or job id
 *
 */
public class JMSTopicService implements Service {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "JMSTopicService.";
    public static final String TOPIC_NAME = CONF_PREFIX + "topic.name";
    public static final String TOPIC_PREFIX = CONF_PREFIX + "topic.prefix";
    private static XLog LOG;
    private Configuration conf;
    private final Map<String, String> topicMap = new HashMap<String, String>();
    private static final List<String> JOB_TYPE_CONSTANTS = new ArrayList<String>();
    private static final List<String> ALLOWED_TOPIC_NAMES = new ArrayList<String>();
    private JPAService jpaService = Services.get().get(JPAService.class);
    private String defaultTopicName = TopicType.USER.value;
    private String topicPrefix;

    static {
        ALLOWED_TOPIC_NAMES.add(TopicType.USER.value);
        ALLOWED_TOPIC_NAMES.add(TopicType.JOBID.value);
    }

    static {
        JOB_TYPE_CONSTANTS.add(JobType.WORKFLOW.value);
        JOB_TYPE_CONSTANTS.add(JobType.COORDINATOR.value);
        JOB_TYPE_CONSTANTS.add(JobType.BUNDLE.value);
    }

    public static enum JobType {
        WORKFLOW("WORKFLOW"), COORDINATOR("COORDINATOR"), BUNDLE("BUNDLE");
        private String value;

        JobType(String value) {
            this.value = value;
        }

        String getValue() {
            return value;
        }

    }

    public static enum TopicType {
        USER("${username}"), JOBID("${jobId}");

        private String value;

        TopicType(String value) {
            this.value = value;
        }

        String getValue() {
            return value;
        }

    }

    @Override
    public void init(Services services) throws ServiceException {
        LOG = XLog.getLog(getClass());
        conf = services.getConf();
        parseTopicConfiguration();
        topicPrefix = conf.get(TOPIC_PREFIX, "");
    }

    private void parseTopicConfiguration() throws ServiceException {
        String topicName = conf.get(TOPIC_NAME, "default=" + TopicType.USER.value);
        if (topicName == null) {
            throw new ServiceException(ErrorCode.E0100, getClass().getName(), "JMS topic cannot be null ");
        }
        LOG.info("Topic Name is [{0}]", topicName);
        String[] topic = topicName.trim().split(",");
        for (int i = 0; i < topic.length; i++) {
            String[] split = topic[i].trim().split("=");
            if (split.length == 2) {
                split[0] = split[0].trim();
                split[1] = split[1].trim();
                if (split[0].equals("default")) {
                    if (!ALLOWED_TOPIC_NAMES.contains(split[1])) {
                        throw new ServiceException(ErrorCode.E0100, getClass().getName(), "Topic name " + split[1]
                                + " not allowed in default; allowed" + "topics are " + ALLOWED_TOPIC_NAMES);
                    }
                    defaultTopicName = split[1];
                }
                else if (!JOB_TYPE_CONSTANTS.contains(split[0])) {
                    throw new ServiceException(ErrorCode.E0100, getClass().getName(),
                            "Incorrect job type for defining JMS topic: " + split[0] + " ;" + "allowed job types are "
                                    + JOB_TYPE_CONSTANTS);
                }
                else if (!ALLOWED_TOPIC_NAMES.contains(split[1]) && split[1].contains("$")) {
                    throw new ServiceException(ErrorCode.E0100, getClass().getName(), "JMS topic value " + split[1]
                            + " " + "for a job type is incorrect " + "Correct values are " + ALLOWED_TOPIC_NAMES);
                }
                else {
                    topicMap.put(split[0], split[1]);
                }
            }
            else {
                throw new ServiceException(ErrorCode.E0100, getClass().getName(), "Property " + topic[i]
                        + "has incorrect syntax; It should be specified as key value pair");
            }
        }
    }

    /**
     * Retrieve topic from Job id
     * @param jobId
     * @return
     * @throws JPAExecutorException
     */
    public String getTopic(String jobId) throws JPAExecutorException {
        String topicName = null;
        if (jobId.contains("-W@")) {
            jobId = jobId.substring(0, jobId.indexOf('@'));
            topicName = getTopicForWorkflow(jobId);
        }
        else if (jobId.endsWith("-W")) {
            topicName = getTopicForWorkflow(jobId);
        }
        else if (jobId.contains("-C@")) {
            jobId = jobId.substring(0, jobId.indexOf('@'));
            topicName = getTopicForCoordinator(jobId);
        }
        else if (jobId.endsWith("C")) {
            topicName = getTopicForCoordinator(jobId);
        }
        else if (jobId.contains("-B_")) {
            jobId = jobId.substring(0, jobId.indexOf('_'));
            topicName = getTopicForBundle(jobId);
        }

        else if (jobId.endsWith("B")) {
            topicName = getTopicForBundle(jobId);
        }
        return topicPrefix + topicName;
    }

    /**
     * Retrieve topic from coord event
     * @param event the coordinatorAction event
     * @return
     */
    public String getTopic(CoordinatorActionEvent event) {
        String topicName = topicMap.get(JobType.COORDINATOR.value);
        if (topicName == null) {
            if (defaultTopicName.equals(TopicType.USER.value)) {
                topicName = event.getUser();
            }
            else if (defaultTopicName.equals(TopicType.JOBID.value)) {
                topicName = event.getParentId();
            }
        }
        return topicPrefix + topicName;

    }

    /**
     * Retrieve topic for workflow job event
     * @param event the wf job event
     * @return
     */
    public String getTopic(WorkflowJobEvent event) {
        String topicName = topicMap.get(JobType.WORKFLOW.value);
        if (topicName == null) {
            if (defaultTopicName.equals(TopicType.USER.value)) {
                topicName = event.getUser();
            }
            else if (defaultTopicName.equals(TopicType.JOBID.value)) {
                topicName = event.getId();
            }
        }
        return topicPrefix + topicName;

    }

    private String getTopicForWorkflow(String jobId) throws JPAExecutorException {
        String topicName = topicMap.get(JobType.WORKFLOW.value);
        if (topicName == null) {
            topicName = defaultTopicName;
        }
        if (topicName.equals(TopicType.USER.value)) {
            topicName = jpaService.execute(new WorkflowJobGetForUserJPAExecutor(jobId));
        }
        else if (topicName.equals(TopicType.JOBID.value)) {
            topicName = jobId;
        }
        return topicName;

    }

    private String getTopicForCoordinator(String jobId) throws JPAExecutorException {
        String topicName = topicMap.get(JobType.COORDINATOR.value);

        if (topicName == null) {
            topicName = defaultTopicName;
        }
        if (topicName.equals(TopicType.USER.value)) {
            topicName = jpaService.execute(new CoordinatorJobGetForUserJPAExecutor(jobId));
        }
        else if (topicName.equals(TopicType.JOBID.value)) {
            topicName = jobId;
        }
        return topicName;
    }

    private String getTopicForBundle(String jobId) throws JPAExecutorException {
        String topicName = topicMap.get(JobType.BUNDLE.value);
        if (topicName == null) {
            topicName = defaultTopicName;
        }
        if (topicName.equals(TopicType.USER.value)) {
            topicName = jpaService.execute(new BundleJobGetForUserJPAExecutor(jobId));
        }
        else if (topicName.equals(TopicType.JOBID.value)) {
            topicName = jobId;
        }
        return topicName;
    }

    @Override
    public void destroy() {
        topicMap.clear();

    }

    @Override
    public Class<? extends Service> getInterface() {
        return JMSTopicService.class;
    }

}
