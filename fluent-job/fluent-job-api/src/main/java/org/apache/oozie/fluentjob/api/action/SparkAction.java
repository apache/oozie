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
 * A class representing the Oozie Spark action.
 * Instances of this class should be built using the builder {@link SparkActionBuilder}.
 *
 * The properties of the builder can only be set once, an attempt to set them a second time will trigger
 * an {@link IllegalStateException}.
 *
 * Builder instances can be used to build several elements, although properties already set cannot be changed after
 * a call to {@link SparkActionBuilder#build} either.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SparkAction extends Node implements HasAttributes {
    private final ActionAttributes attributes;
    private final String master;
    private final String mode;
    private final String actionName;
    private final String actionClass;
    private final String jar;
    private final String sparkOpts;

    public SparkAction(final ConstructionData constructionData,
                       final ActionAttributes attributes,
                       final String master,
                       final String mode,
                       final String actionName,
                       final String actionClass,
                       final String jar,
                       final String sparkOpts) {
        super(constructionData);

        this.attributes = attributes;
        this.master = master;
        this.mode = mode;
        this.actionName = actionName;
        this.actionClass = actionClass;
        this.jar = jar;
        this.sparkOpts = sparkOpts;
    }

    public String getResourceManager() {
        return attributes.getResourceManager();
    }

    public String getNameNode() {
        return attributes.getNameNode();
    }

    public Prepare getPrepare() {
        return attributes.getPrepare();
    }

    public Launcher getLauncher() {
        return attributes.getLauncher();
    }

    public List<String> getJobXmls() {
        return attributes.getJobXmls();
    }

    public String getConfigProperty(final String property) {
        return attributes.getConfiguration().get(property);
    }

    public Map<String, String> getConfiguration() {
        return attributes.getConfiguration();
    }

    public String getMaster() {
        return master;
    }

    public String getMode() {
        return mode;
    }

    public String getActionName() {
        return actionName;
    }

    public String getActionClass() {
        return actionClass;
    }

    public String getJar() {
        return jar;
    }

    public String getSparkOpts() {
        return sparkOpts;
    }

    public List<String> getArgs() {
        return attributes.getArgs();
    }

    public List<String> getFiles() {
        return attributes.getFiles();
    }

    public List<String> getArchives() {
        return attributes.getArchives();
    }

    public ActionAttributes getAttributes() {
        return attributes;
    }
}