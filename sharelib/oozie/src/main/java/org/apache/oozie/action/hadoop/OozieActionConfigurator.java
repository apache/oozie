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

import org.apache.hadoop.mapred.JobConf;

/**
 * Users can implement this interface to provide a class for Oozie to configure the MapReduce action with.  Make sure that the jar
 * with this class is available with the action and then simply specify the class name in the "config-class" field in the MapReduce
 * action XML.
 */
public interface OozieActionConfigurator {
    /**
     * This method should update the passed in configuration with additional changes; it will be used by the action.  If any
     * Exceptions need to be thrown, they should be wrapped in an OozieActionConfiguratorException
     *
     * @param actionConf The action configuration
     * @throws OozieActionConfiguratorException
     */
    public void configure(JobConf actionConf) throws OozieActionConfiguratorException;
}
