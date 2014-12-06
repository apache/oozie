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
package org.apache.oozie.action.hadoop;

import org.apache.oozie.DagELFunctions;
import org.apache.oozie.util.ELEvaluationException;
import org.apache.oozie.util.XLog;
import org.apache.oozie.workflow.WorkflowInstance;
import org.json.simple.JSONValue;

import java.util.Map;

/**
 * Hadoop EL functions.
 */
public class HadoopELFunctions {

    private static final String HADOOP_COUNTERS = "oozie.el.action.hadoop.counters";

    public static final String RECORDS = "org.apache.hadoop.mapred.Task$Counter";
    public static final String MAP_IN = "MAP_INPUT_RECORDS";
    public static final String MAP_OUT = "MAP_OUTPUT_RECORDS";
    public static final String REDUCE_IN = "REDUCE_INPUT_RECORDS";
    public static final String REDUCE_OUT = "REDUCE_OUTPUT_RECORDS";
    public static final String GROUPS = "REDUCE_INPUT_GROUPS";

    private static final String RECORDS_023 = "org.apache.hadoop.mapreduce.TaskCounter";

    @SuppressWarnings("unchecked")
    public static Map<String, Map<String, Long>> hadoop_counters(String nodeName) throws ELEvaluationException {
        WorkflowInstance instance = DagELFunctions.getWorkflow().getWorkflowInstance();
        Object obj = instance.getTransientVar(nodeName + WorkflowInstance.NODE_VAR_SEPARATOR + HADOOP_COUNTERS);
        Map<String, Map<String, Long>> counters = (Map<String, Map<String, Long>>) obj;
        if (counters == null) {
            counters = getCounters(nodeName);
            // In Hadoop 0.23 they deprecated 'org.apache.hadoop.mapred.Task$Counter' and they REMOVED IT
            // Here we are getting the new Name and inserting it using the old name if the old name is not found
            if (counters.get(RECORDS) == null) {
                counters.put(RECORDS, counters.get(RECORDS_023));
            }
            instance.setTransientVar(nodeName + WorkflowInstance.NODE_VAR_SEPARATOR + HADOOP_COUNTERS, counters);
        }
        return counters;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Map<String, Long>> getCounters(String nodeName) throws ELEvaluationException {
        String jsonCounters = DagELFunctions.getActionVar(nodeName, MapReduceActionExecutor.HADOOP_COUNTERS);
        if (jsonCounters == null) {
            throw new IllegalArgumentException(XLog.format("Hadoop counters not available for action [{0}]", nodeName));
        }
        return (Map) JSONValue.parse(jsonCounters);
    }

}
