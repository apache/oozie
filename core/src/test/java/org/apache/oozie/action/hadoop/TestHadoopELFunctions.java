/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.action.hadoop;

import org.apache.oozie.DagELFunctions;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.command.wf.ActionCommand;
import org.apache.oozie.service.ELService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.lite.EndNodeDef;
import org.apache.oozie.workflow.lite.LiteWorkflowApp;
import org.apache.oozie.workflow.lite.LiteWorkflowInstance;
import org.apache.oozie.workflow.lite.StartNodeDef;

public class TestHadoopELFunctions extends ActionExecutorTestCase {


    public void testCountersEL() throws Exception {
        String counters = "{\"g\":{\"c\":10},\"org.apache.hadoop.mapred.JobInProgress$Counter\":" +
                          "{\"TOTAL_LAUNCHED_REDUCES\":1,\"TOTAL_LAUNCHED_MAPS\":2,\"DATA_LOCAL_MAPS\":2}," +
                          "\"FileSystemCounters\":{\"FILE_BYTES_READ\":38,\"HDFS_BYTES_READ\":19," +
                          "\"FILE_BYTES_WRITTEN\":146,\"HDFS_BYTES_WRITTEN\":16}," +
                          "\"org.apache.hadoop.mapred.Task$Counter\":{\"REDUCE_INPUT_GROUPS\":2," +
                          "\"COMBINE_OUTPUT_RECORDS\":0,\"MAP_INPUT_RECORDS\":2,\"REDUCE_SHUFFLE_BYTES\":22," +
                          "\"REDUCE_OUTPUT_RECORDS\":2,\"SPILLED_RECORDS\":4,\"MAP_OUTPUT_BYTES\":28," +
                          "\"MAP_INPUT_BYTES\":12,\"MAP_OUTPUT_RECORDS\":2,\"COMBINE_INPUT_RECORDS\":0," +
                          "\"REDUCE_INPUT_RECORDS\":2}}";


        WorkflowJobBean workflow = new WorkflowJobBean();
        workflow.setProtoActionConf("<configuration/>");
        LiteWorkflowApp wfApp = new LiteWorkflowApp("x", "<workflow-app/>", new StartNodeDef("a"));
        wfApp.addNode(new EndNodeDef("a"));
        WorkflowInstance wi = new LiteWorkflowInstance(wfApp, new XConfiguration(), "1");

        workflow.setWorkflowInstance(wi);
        workflow.setId(Services.get().get(UUIDService.class).generateId());
        final WorkflowActionBean action = new WorkflowActionBean();
        action.setName("H");

        ActionCommand.ActionExecutorContext context = new ActionCommand.ActionExecutorContext(workflow, action, false);
        context.setVar(MapReduceActionExecutor.HADOOP_COUNTERS, counters);

        ELEvaluator eval = Services.get().get(ELService.class).createEvaluator();
        DagELFunctions.configureEvaluator(eval, workflow, action);

        String group = "g";
        String name = "c";
        assertEquals(new Long(10),
                     eval.evaluate("${hadoop:counters('H')['" + group + "']['" + name + "']}", Long.class));

        assertEquals(new Long(2), eval.evaluate("${hadoop:counters('H')[RECORDS][GROUPS]}", Long.class));
        assertEquals(new Long(2), eval.evaluate("${hadoop:counters('H')[RECORDS][REDUCE_IN]}", Long.class));
        assertEquals(new Long(2), eval.evaluate("${hadoop:counters('H')[RECORDS][REDUCE_OUT]}", Long.class));
        assertEquals(new Long(2), eval.evaluate("${hadoop:counters('H')[RECORDS][MAP_IN]}", Long.class));
        assertEquals(new Long(2), eval.evaluate("${hadoop:counters('H')[RECORDS][MAP_OUT]}", Long.class));
    }

}
