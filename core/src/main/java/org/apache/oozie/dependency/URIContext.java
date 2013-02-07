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
package org.apache.oozie.dependency;

import org.apache.hadoop.conf.Configuration;

public abstract class URIContext {

    private Configuration conf;
    private String user;

    /**
     * Create a URIContext that can be used to access a URI
     * @param conf Configuration to access the URI
     * @param user name of the user the URI should be accessed as
     */
    public URIContext(Configuration conf, String user) {
        this.conf = conf;
        this.user = user;
    }

    /**
     * Get the Configuration to access the URI
     * @return Configuration to access the URI
     */
    public Configuration getConfiguration() {
        return conf;
    }

    /**
     * Get the name of the user the URI will be accessed as
     * @return the user name the URI will be accessed as
     */
    public String getUser() {
        return user;
    }

    /**
     * Destroy the URIContext
     */
    public void destroy() {
    }

}
