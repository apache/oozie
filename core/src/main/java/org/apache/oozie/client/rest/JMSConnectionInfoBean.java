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

package org.apache.oozie.client.rest;

import java.util.Properties;

import org.apache.oozie.client.JMSConnectionInfoWrapper;
import org.apache.oozie.client.rest.JsonTags;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
/**
 * JMS connection info bean representing the JMS related information for a job
 *
 */
public class JMSConnectionInfoBean implements JsonBean, JMSConnectionInfoWrapper {

    private Properties JNDIProperties;
    private String topicPrefix;
    private Properties topicProperties;


    @Override
    public JSONObject toJSONObject() {
        return toJSONObject("GMT");
    }

    /**
     * Set the JNDI properties for jms connection
     * @param JNDIProperties
     */
    public void setJNDIProperties(Properties JNDIProperties) {
        this.JNDIProperties = JNDIProperties;
    }

    public Properties getJNDIProperties() {
        return JNDIProperties;
    }

    @SuppressWarnings("unchecked")
    @Override
    public JSONObject toJSONObject(String timeZoneId) {
        JSONObject json = new JSONObject();
        json.put(JsonTags.JMS_JNDI_PROPERTIES, JSONValue.toJSONString(JNDIProperties));
        json.put(JsonTags.JMS_TOPIC_PATTERN, JSONValue.toJSONString(topicProperties));
        json.put(JsonTags.JMS_TOPIC_PREFIX, topicPrefix);
        return json;
    }

    @Override
    public String getTopicPrefix() {
       return topicPrefix;
    }

    /**
     * Sets the topic prefix
     * @param topicPrefix
     */
    public void setTopicPrefix(String topicPrefix) {
        this.topicPrefix = topicPrefix;
    }

    /**
     * Set the topic pattern properties
     * @param topicProperties
     */
    public void setTopicPatternProperties(Properties topicProperties) {
        this.topicProperties = topicProperties;
    }

    @Override
    public Properties getTopicPatternProperties() {
        return topicProperties;
    }

}
