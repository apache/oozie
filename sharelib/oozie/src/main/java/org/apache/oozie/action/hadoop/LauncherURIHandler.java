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

import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

public interface LauncherURIHandler {

    /**
     * Create the resource identified by the URI
     *
     * @param uri URI of the dependency
     * @param conf Configuration to access the URI
     *
     * @return <code>true</code> if the URI did not exist and was successfully
     *         created; <code>false</code> if the URI already existed
     *
     * @throws LauncherException
     */
    public boolean create(URI uri, Configuration conf) throws LauncherException;

    /**
     * Delete the resource identified by the URI
     *
     * @param uri URI of the dependency
     * @param conf Configuration to access the URI
     *
     * @return <code>true</code> if the URI exists and was successfully deleted;
     *         <code>false</code> if the URI does not exist
     * @throws LauncherException
     */
    public boolean delete(URI uri, Configuration conf) throws LauncherException;


    /**
     * Get list of classes to ship to launcher for LauncherURIHandler
     *
     * @return list of classes to ship to launcher
     */
    public List<Class<?>> getClassesForLauncher();

}
