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
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.service.URIAccessorException;
import org.jdom.Element;

public abstract class URIHandler {

    public abstract void init(Configuration conf);

    /**
     * Get the list of uri schemes supported by this URIHandler
     *
     * @return supported list of uri schemes
     */
    public abstract Set<String> getSupportedSchemes();

    /**
     * Get the type of dependency type of the URI. When the availability of the
     * URI is to be determined by polling the type is DependencyType.PULL, and
     * when the availability is received through notifications from a external
     * entity like a JMS server the type is DependencyType.PUSH
     *
     * @return dependency type of URI
     */
    public abstract DependencyType getDependencyType(URI uri) throws URIAccessorException;

    /**
     * Register for notifications in case of a push dependency
     *
     * @param uri The URI to check for availability
     * @param actionID The id of action which depends on the availability of the
     *        uri.
     */
    public abstract void registerForNotification(URI uri, String actionID) throws URIAccessorException;

    /**
     * Get the URIContext which can be used to access URI of the same scheme and
     * host
     *
     * @param uri URI which identifies the scheme and host
     * @param conf Configuration to access the URI
     * @param user name of the user the URI should be accessed as
     *
     * @return Context to access URIs with same scheme and host
     *
     * @throws URIAccessorException
     */
    public abstract URIContext getURIContext(URI uri, Configuration conf, String user) throws URIAccessorException;

    /**
     * Create the resource identified by the URI
     *
     * @param uri URI of the dependency
     * @param conf Configuration to access the URI
     * @param user name of the user the URI should be accessed as
     *
     * @return <code>true</code> if the URI did not exist and was successfully
     *         created; <code>false</code> if the URI already existed
     *
     * @throws URIAccessorException
     */
    public abstract boolean create(URI uri, Configuration conf, String user) throws URIAccessorException;

    /**
     * Check if the dependency identified by the URI is available
     *
     * @param uri URI of the dependency
     * @param uriContext Context to access the URI
     *
     * @return <code>true</code> if the URI exists; <code>false</code> if the
     *         URI does not exist
     *
     * @throws URIAccessorException
     */
    public abstract boolean exists(URI uri, URIContext uriContext) throws URIAccessorException;

    /**
     * Check if the dependency identified by the URI is available
     *
     * @param uri URI of the dependency
     * @param conf Configuration to access the URI
     *
     * @return <code>true</code> if the URI exists; <code>false</code> if the
     *         URI does not exist
     *
     * @throws URIAccessorException
     */
    public abstract boolean exists(URI uri, Configuration conf, String user) throws URIAccessorException;

    /**
     * Delete the resource identified by the URI
     *
     * @param uri URI of the dependency
     * @param conf Configuration to access the URI
     * @param user name of the user the URI should be accessed as
     *
     * @return <code>true</code> if the URI exists and was successfully deleted;
     *         <code>false</code> if the URI does not exist
     *
     * @throws URIAccessorException
     */
    public abstract boolean delete(URI uri, Configuration conf, String user) throws URIAccessorException;

    /**
     * Get the URI based on the done flag
     *
     * @param uri URI of the dependency
     * @param doneFlag flag that determines URI availability
     *
     * @return the final URI with the doneFlag incorporated
     *
     * @throws URIAccessorException
     */
    public abstract String getURIWithDoneFlag(String uri, Element doneFlagElement) throws URIAccessorException;

    /**
     * Get the URI based on the done flag
     *
     * @param uri URI of the dependency
     * @param doneFlag flag that determines URI availability
     *
     * @return the final URI with the doneFlag incorporated
     *
     * @throws URIAccessorException
     */
    public abstract String getURIWithDoneFlag(String uri, String doneFlag) throws URIAccessorException;

    /**
     * Check whether the URI is valid or not
     * @param uri
     * @return
     * @throws URIAccessorException
     */
    public abstract void validate(String uri) throws URIAccessorException;

    public abstract void destroy();

}
