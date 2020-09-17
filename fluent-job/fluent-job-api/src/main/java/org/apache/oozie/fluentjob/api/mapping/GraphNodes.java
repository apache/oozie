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

package org.apache.oozie.fluentjob.api.mapping;

import org.apache.oozie.fluentjob.api.workflow.Credentials;
import org.apache.oozie.fluentjob.api.workflow.Global;
import org.apache.oozie.fluentjob.api.workflow.Parameters;
import org.apache.oozie.fluentjob.api.dag.End;
import org.apache.oozie.fluentjob.api.dag.NodeBase;
import org.apache.oozie.fluentjob.api.dag.Start;

import java.util.Collection;

/**
 * We use this class for better testability of the conversion from a Graph to {@code WORKFLOWAPP} - we don't have to build
 * a Workflow to turn it into a {@code Graph}, we can generate the NodeBase's directly in the tests.
 */
public class GraphNodes {
    private final String name;
    private final Parameters parameters;
    private final Global global;
    private final Credentials credentials;
    private final Start start;
    private final End end;
    private final Collection<NodeBase> nodes;

    GraphNodes(final String name,
               final Parameters parameters,
               final Global global,
               final Credentials credentials,
               final Start start,
               final End end,
               final Collection<NodeBase> nodes) {
        this.name = name;
        this.parameters = parameters;
        this.global = global;
        this.credentials = credentials;
        this.start = start;
        this.end = end;
        this.nodes = nodes;
    }

    public String getName() {
        return name;
    }

    public Start getStart() {
        return start;
    }

    public End getEnd() {
        return end;
    }

    public Collection<NodeBase> getNodes() {
        return nodes;
    }

    public Parameters getParameters() {
        return parameters;
    }

    public Global getGlobal() {
        return global;
    }

    public Credentials getCredentials() {
        return credentials;
    }
}
