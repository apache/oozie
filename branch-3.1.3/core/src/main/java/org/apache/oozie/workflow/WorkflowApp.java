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
package org.apache.oozie.workflow;

/**
 * A workflow definition is a programmatic definition of a workflow application.
 */
public interface WorkflowApp {

    /**
     * Return the name of teh workflow application.
     *
     * @return the name of teh workflow application.
     */
    public String getName();

    /**
     * Return the original workflow definition (normally XML).
     *
     * @return the original workflow definition (normally XML).
     */
    public String getDefinition();
}
