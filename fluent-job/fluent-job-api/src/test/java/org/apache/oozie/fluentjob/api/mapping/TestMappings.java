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

import org.apache.oozie.fluentjob.api.generated.workflow.WORKFLOWAPP;
import org.apache.oozie.fluentjob.api.action.MapReduceAction;
import org.apache.oozie.fluentjob.api.action.MapReduceActionBuilder;
import org.apache.oozie.fluentjob.api.dag.Graph;
import org.apache.oozie.fluentjob.api.workflow.Workflow;
import org.apache.oozie.fluentjob.api.workflow.WorkflowBuilder;
import org.dozer.DozerBeanMapper;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestMappings {

    @Test
    public void whenWorkflowWithOneMRActionIsCreatedWORKFLOWAPPIsMappedCorrectly() {
        final MapReduceAction mr1 = MapReduceActionBuilder.create().withName("mr1").build();

        final Workflow workflow = new WorkflowBuilder()
                .withName("Workflow_to_map")
                .withDagContainingNode(mr1)
                .build();
        final Graph graph = new Graph(workflow);

        final List<String> mappingFiles = new ArrayList<>();
        mappingFiles.add("dozer_config.xml");
        mappingFiles.add("mappingGraphToWORKFLOWAPP.xml");
        mappingFiles.add("action_mappings.xml");

        final DozerBeanMapper mapper = new DozerBeanMapper();
        mapper.setMappingFiles(mappingFiles);

        final WORKFLOWAPP workflowapp = mapper.map(graph, WORKFLOWAPP.class);

        assertEquals("API and JAXB workflows should have the same names", workflow.getName(), workflowapp.getName());
    }
}
