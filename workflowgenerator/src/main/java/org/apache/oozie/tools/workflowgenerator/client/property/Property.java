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

package org.apache.oozie.tools.workflowgenerator.client.property;

/**
 * class to provide key-value pair for property, mostly used in property table
 */
public class Property {

    private String name;
    private String value;

    /**
     * Constructor which records name and value
     *
     * @param name name
     * @param value value
     */
    public Property(String name, String value) {
        this.name = name;
        this.value = value;
    }

    /**
     * Return a name of property
     *
     * @return
     */
    public String getName() {
        return name;
    }

    /**
     * Set a name of property
     *
     * @param name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Return a value of property
     *
     * @return
     */
    public String getValue() {
        return value;
    }

    /**
     * Set a value of property
     *
     * @param value
     */
    public void setValue(String value) {
        this.value = value;
    }
}
