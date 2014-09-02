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

package org.apache.oozie.client.event.jms;

/**
 *
 * Class holding constants used in JMS selectors
 */
public final class JMSHeaderConstants {
    // JMS Application specific properties for selectors
    public static final String EVENT_STATUS = "eventStatus";
    public static final String SLA_STATUS = "slaStatus";
    public static final String APP_NAME = "appName";
    public static final String USER = "user";
    public static final String MESSAGE_TYPE = "msgType";
    public static final String APP_TYPE = "appType";
    // JMS Header property
    public static final String MESSAGE_FORMAT = "msgFormat";
}
