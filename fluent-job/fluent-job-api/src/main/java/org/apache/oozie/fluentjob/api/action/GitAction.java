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

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.List;
import java.util.Map;

/**
 * A class representing the Oozie Git action.
 * Instances of this class should be built using the builder {@link GitActionBuilder}.
 *
 * The properties of the builder can only be set once, an attempt to set them a second time will trigger
 * an {@link IllegalStateException}.
 *
 * Builder instances can be used to build several elements, although properties already set cannot be changed after
 * a call to {@link GitActionBuilder#build} either.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class GitAction extends Node implements HasAttributes {
    protected final ActionAttributes attributes;
    protected final String gitUri;
    protected final String branch;
    protected final String keyPath;
    protected final String destinationUri;

    GitAction(final ConstructionData constructionData,
              final ActionAttributes attributes,
              final String gitUri,
              final String branch,
              final String keyPath,
              final String destinationUri) {
        super(constructionData);

        this.attributes = attributes;
        this.gitUri = gitUri;
        this.branch = branch;
        this.keyPath = keyPath;
        this.destinationUri = destinationUri;
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

    public String getConfigProperty(final String property) {
        return attributes.getConfiguration().get(property);
    }

    public Map<String, String> getConfiguration() {
        return attributes.getConfiguration();
    }

    public String getGitUri() {
        return gitUri;
    }

    public String getBranch() {
        return branch;
    }

    public String getKeyPath() {
        return keyPath;
    }

    public String getDestinationUri() {
        return destinationUri;
    }

    public ActionAttributes getAttributes() {
        return attributes;
    }
}
