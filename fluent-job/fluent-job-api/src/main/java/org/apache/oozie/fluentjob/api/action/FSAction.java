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
 * A class representing the Oozie file system action.
 * Instances of this class should be built using the builder {@link FSActionBuilder}.
 *
 * The properties of the builder can only be set once, an attempt to set them a second time will trigger
 * an {@link IllegalStateException}.
 *
 * Builder instances can be used to build several elements, although properties already set cannot be changed after
 * a call to {@link FSActionBuilder#build} either.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class FSAction extends Node implements HasAttributes {
    private final ActionAttributes attributes;

    FSAction(final Node.ConstructionData constructionData,
             final ActionAttributes attributes) {
        super(constructionData);

        this.attributes = attributes;
    }

    /**
     * Returns the name node used by this {@link FSAction}.
     * @return The name node used by this {@link FSAction}.
     */
    public String getNameNode() {
        return attributes.getNameNode();
    }

    /**
     * Returns the list of job XMLs used by this {@link FSAction}.
     * @return The list of job XMLs used by this {@link FSAction}.
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
     * Returns all configuration properties of this {@link FSAction} as a map.
     * @return All configuration properties of this {@link FSAction} as a map.
     */
    public Map<String, String> getConfiguration() {
        return attributes.getConfiguration();
    }

    /**
     * Returns the {@link Delete} objects that specify which directories or files will be deleted when running this action.
     * @return The {@link Delete} objects that specify which directories or files will be deleted when running this action.
     */
    public List<Delete> getDeletes() {
        return attributes.getDeletes();
    }

    /**
     * Returns the {@link Mkdir} objects that specify which directories will be created when running this action.
     * @return The {@link Mkdir} objects that specify which directories will be created when running this action.
     */
    public List<Mkdir> getMkdirs() {
        return attributes.getMkdirs();
    }

    /**
     * Returns the {@link Move} objects that specify which directories or files will be moved and where when running this action.
     * @return The {@link Move} objects that specify which directories or files will be moved and where when running this action.
     */
    public List<Move> getMoves() {
        return attributes.getMoves();
    }

    /**
     * Returns the {@link Chmod} objects that specify the directories or files the permission of which will be changed
     * and to what when running this action.
     * @return The {@link Chmod} objects that specify the directories or files the permission of which will be changed
     *         and to what when running this action.
     */
    public List<Chmod> getChmods() {
        return attributes.getChmods();
    }

    /**
     * Returns the {@link Touchz} objects that specify which files will be touched when running this action.
     * @return The {@link Touchz} objects that specify which files will be touched when running this action.
     */
    public List<Touchz> getTouchzs() {
        return attributes.getTouchzs();
    }

    /**
     * Returns the {@link Chgrp} objects that specify the directories or files the owner / group of which will be changed
     * and to what when running this action.
     * @return The {@link Chgrp} objects that specify the directories or files the owner / group of which will be changed
     *         and to what when running this action.
     */
    public List<Chgrp> getChgrps() {
        return attributes.getChgrps();
    }

    public ActionAttributes getAttributes() {
        return attributes;
    }
}
