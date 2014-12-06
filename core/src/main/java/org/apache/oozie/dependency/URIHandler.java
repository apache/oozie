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

import java.net.URI;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.action.hadoop.LauncherURIHandler;

public interface URIHandler {

    /**
     * Type of the dependency. PULL dependencies are those whose availability is determined by
     * polling and PUSH dependencies are those whose availability is known from notifications
     */
    public enum DependencyType {
        PULL,
        PUSH;
    }

    /**
     * Initialize the URIHandler
     *
     * @param conf Configuration for initialization
     */
    public void init(Configuration conf);

    /**
     * Get the list of uri schemes supported by this URIHandler
     *
     * @return supported list of uri schemes
     */
    public Set<String> getSupportedSchemes();

    /**
     * Get the URIHandler that will be used to handle the supported schemes in launcher
     *
     * @return LauncherURIHandler that handles URI in the launcher
     */
    public Class<? extends LauncherURIHandler> getLauncherURIHandlerClass();

    /**
     * Get list of classes to ship to launcher for LauncherURIHandler
     *
     * @return list of classes to ship to launcher
     */
    public List<Class<?>> getClassesForLauncher();

    /**
     * Get the dependency type of the URI. When the availability of the
     * URI is to be determined by polling the type is DependencyType.PULL, and
     * when the availability is received through notifications from a external
     * entity like a JMS server the type is DependencyType.PUSH
     *
     * @return dependency type of URI
     */
    public DependencyType getDependencyType(URI uri) throws URIHandlerException;

    /**
     * Register for notifications in case of a push dependency
     *
     * @param uri  The URI to check for availability
     * @param conf Configuration to access the URI
     * @param user name of the user the URI should be accessed as
     * @param actionID The id of action which depends on the availability of the uri
     *
     * @throws URIHandlerException
     */
    public void registerForNotification(URI uri, Configuration conf, String user, String actionID)
            throws URIHandlerException;

    /**
     * Unregister from notifications in case of a push dependency
     *
     * @param uri The URI to be removed from missing dependency
     * @param actionID The id of action which was dependent on the uri.
     *
     * @throws URIHandlerException
     */
    public boolean unregisterFromNotification(URI uri, String actionID);

    /**
     * Get the Context which can be used to access URI of the same scheme and
     * host
     *
     * @param uri URI which identifies the scheme and host
     * @param conf Configuration to access the URI
     * @param user name of the user the URI should be accessed as
     * @param readOnly indicate if operation is read-only
     * @return Context to access URIs with same scheme and host
     *
     * @throws URIHandlerException
     */
    public Context getContext(URI uri, Configuration conf, String user, boolean readOnly) throws URIHandlerException;

    /**
     * Check if the dependency identified by the URI is available
     *
     * @param uri URI of the dependency
     * @param context Context to access the URI
     *
     * @return <code>true</code> if the URI exists; <code>false</code> if the
     *         URI does not exist
     *
     * @throws URIHandlerException
     */
    public boolean exists(URI uri, Context context) throws URIHandlerException;

    /**
     * Check if the dependency identified by the URI is available
     *
     * @param uri URI of the dependency
     * @param conf Configuration to access the URI
     * @param user name of the user the URI should be accessed as. If null the
     *        logged in user is used.
     *
     * @return <code>true</code> if the URI exists; <code>false</code> if the
     *         URI does not exist
     *
     * @throws URIHandlerException
     */
    public boolean exists(URI uri, Configuration conf, String user) throws URIHandlerException;

    /**
     * Delete a URI
     *
     * @param uri URI
     * @param context Context to access the URI
     * @throws URIHandlerException
     */
    public void delete(URI uri, Context context) throws URIHandlerException;

    /**
     * Delete a URI
     *
     * @param uri URI
     * @param conf Configuration to access the URI
     * @param user name of the user the URI should be accessed as
     * @throws URIHandlerException
     */
    public void delete(URI uri, Configuration conf, String user) throws URIHandlerException;

    /**
     * Get the URI based on the done flag
     *
     * @param uri URI of the dependency
     * @param doneFlag flag that determines URI availability
     *
     * @return the final URI with the doneFlag incorporated
     *
     * @throws URIHandlerException
     */
    public String getURIWithDoneFlag(String uri, String doneFlag) throws URIHandlerException;

    /**
     * Check whether the URI is valid or not
     * @param uri
     * @return
     * @throws URIHandlerException
     */
    public void validate(String uri) throws URIHandlerException;

    /**
     * Destroy the URIHandler
     */
    public void destroy();

    public static abstract class Context {

        private Configuration conf;
        private String user;

        /**
         * Create a Context that can be used to access a URI
         *
         * @param conf Configuration to access the URI
         * @param user name of the user the URI should be accessed as
         */
        public Context(Configuration conf, String user) {
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
         * Destroy the Context
         */
        public void destroy() {
        }

    }

}
