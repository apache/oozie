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

import com.google.common.base.Preconditions;
import org.apache.oozie.fluentjob.api.generated.workflow.WORKFLOWAPP;
import org.apache.oozie.fluentjob.api.dag.Graph;
import org.dozer.DozerConverter;
import org.dozer.Mapper;
import org.dozer.MapperAware;

/**
 * A {@link DozerConverter} converting from {@link Graph} to JAXB {@link WORKFLOWAPP}.
 * <p>
 * Delegates to {@link GraphNodesToWORKFLOWAPPConverter}.
 */
public class GraphToWORKFLOWAPPConverter extends DozerConverter<Graph, WORKFLOWAPP> implements MapperAware {
    private Mapper mapper;

    public GraphToWORKFLOWAPPConverter() {
        super(Graph.class, WORKFLOWAPP.class);
    }

    @Override
    public WORKFLOWAPP convertTo(final Graph graph, final WORKFLOWAPP workflowapp) {
        final GraphNodes graphNodes = new GraphNodes(graph.getName(),
                graph.getParameters(),
                graph.getGlobal(),
                graph.getCredentials(),
                graph.getStart(),
                graph.getEnd(),
                graph.getNodes());

        return checkAndGetMapper().map(graphNodes, WORKFLOWAPP.class);
    }

    @Override
    public Graph convertFrom(final WORKFLOWAPP workflowapp, final Graph graph) {
        throw new UnsupportedOperationException("This mapping is not bidirectional.");
    }

    private Mapper checkAndGetMapper() {
        Preconditions.checkNotNull(mapper, "mapper should be set");
        return mapper;
    }

    @Override
    public void setMapper(final Mapper mapper) {
        this.mapper = mapper;
    }
}
