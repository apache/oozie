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

package org.apache.oozie.fluentjob.api.action;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.List;
import java.util.Map;

/**
 * A class representing the Oozie map-reduce action.
 * Instances of this class should be built using the builder {@link MapReduceActionBuilder}.
 *
 * The properties of the builder can only be set once, an attempt to set them a second time will trigger
 * an {@link IllegalStateException}.
 *
 * Builder instances can be used to build several elements, although properties already set cannot be changed after
 * a call to {@link MapReduceActionBuilder#build} either.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class MapReduceAction extends Node implements HasAttributes {
    private final ActionAttributes attributes;

    MapReduceAction(final ConstructionData constructionData,
                    final ActionAttributes attributes) {
        super(constructionData);

        this.attributes = attributes;
    }

    public String getResourceManager() {
        return attributes.getResourceManager();
    }

    /**
     * Returns the name node stored in this {@link MapReduceAction} object.
     * @return The name node stored in this {@link MapReduceAction} object.
     */
    public String getNameNode() {
        return attributes.getNameNode();
    }

    /**
     * Returns the {@link Prepare} object stored in this {@link MapReduceAction} object.
     * @return The {@link Prepare} object stored in this {@link MapReduceAction} object.
     */
    public Prepare getPrepare() {
        return attributes.getPrepare();
    }

    /**
     * Returns the {@link Streaming} object stored in this {@link MapReduceAction} object.
     * @return The {@link Streaming} object stored in this {@link MapReduceAction} object.
     */
    public Streaming getStreaming() {
        return attributes.getStreaming();
    }

    /**
     * Returns the {@link Pipes} object stored in this {@link MapReduceAction} object.
     * @return The {@link Pipes} object stored in this {@link MapReduceAction} object.
     */
    public Pipes getPipes() {
        return attributes.getPipes();
    }

    /**
     * Returns the list of job XMLs stored in this {@link MapReduceAction} object.
     * @return The list of job XMLs stored in this {@link MapReduceAction} object.
     */
    public List<String> getJobXmls() {
        return attributes.getJobXmls();
    }


    /**
     * Returns the value associated with the provided configuration property name.
     * @param property The name of the configuration property for which the value will be returned.
     * @return The value associated with the provided configuration property name.
     */
    public String getConfigProperty(final String property) {
        return attributes.getConfiguration().get(property);
    }

    /**
     * Returns an immutable map of the configuration key-value pairs stored in this {@link MapReduceAction} object.
     * @return An immutable map of the configuration key-value pairs stored in this {@link MapReduceAction} object.
     */
    public Map<String, String> getConfiguration() {
        return attributes.getConfiguration();
    }

    /**
     * Returns the configuration class property of this {@link MapReduceAction} object.
     * @return The configuration class property of this {@link MapReduceAction} object.
     */
    public String getConfigClass() {
        return attributes.getConfigClass();
    }

    /**
     * Returns an immutable list of the names of the files associated with this {@link MapReduceAction} object.
     * @return An immutable list of the names of the files associated with this {@link MapReduceAction} object.
     */
    public List<String> getFiles() {
        return attributes.getFiles();
    }

    /**
     * Returns an immutable list of the names of the archives associated with this {@link MapReduceAction} object.
     * @return An immutable list of the names of the archives associated with this {@link MapReduceAction} object.
     */
    public List<String> getArchives() {
        return attributes.getArchives();
    }

    public ActionAttributes getAttributes() {
        return attributes;
    }
}
