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
package org.apache.oozie.workflow.lite;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.oozie.workflow.WorkflowApp;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;
import org.apache.oozie.ErrorCode;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

//TODO javadoc
public class LiteWorkflowApp implements Writable, WorkflowApp {
    private String name;
    private String definition;
    private Map<String, NodeDef> nodesMap = new LinkedHashMap<String, NodeDef>();
    private boolean complete = false;

    LiteWorkflowApp() {
    }

    public LiteWorkflowApp(String name, String definition, StartNodeDef startNode) {
        this.name = ParamChecker.notEmpty(name, "name");
        this.definition = ParamChecker.notEmpty(definition, "definition");
        nodesMap.put(StartNodeDef.START, startNode);
    }

    public boolean equals(LiteWorkflowApp other) {
        return !(other == null || getClass() != other.getClass() || !getName().equals(other.getName()));
    }

    public int hashCode() {
        return name.hashCode();
    }

    public LiteWorkflowApp addNode(NodeDef node) throws WorkflowException {
        ParamChecker.notNull(node, "node");
        if (complete) {
            throw new WorkflowException(ErrorCode.E0704, name);
        }
        if (nodesMap.containsKey(node.getName())) {
            throw new WorkflowException(ErrorCode.E0705, node.getName());
        }
        if (node.getTransitions().contains(node.getName())) {
            throw new WorkflowException(ErrorCode.E0706, node.getName());
        }
        nodesMap.put(node.getName(), node);
        if (node instanceof EndNodeDef) {
            complete = true;
        }
        return this;
    }

    public String getName() {
        return name;
    }

    public String getDefinition() {
        return definition;
    }

    public Collection<NodeDef> getNodeDefs() {
        return Collections.unmodifiableCollection(nodesMap.values());
    }

    public NodeDef getNode(String name) {
        return nodesMap.get(name);
    }

    public void validateWorkflowIntegrity() {
        //TODO traverse wf, ensure there are not cycles, no open paths, and one END
    }

    public void validateTransition(String name, String transition) {
        ParamChecker.notEmpty(name, "name");
        ParamChecker.notEmpty(transition, "transition");
        NodeDef node = getNode(name);
        if (!node.getTransitions().contains(transition)) {
            throw new IllegalArgumentException("invalid transition");
        }
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(name);
        //dataOutput.writeUTF(definition);
        //writeUTF() has limit 65535, so split long string to multiple short strings
        List<String> defList = divideStr(definition);
        dataOutput.writeInt(defList.size());
        for (String d : defList) {
            dataOutput.writeUTF(d);
        }
        dataOutput.writeInt(nodesMap.size());
        for (NodeDef n : getNodeDefs()) {
            dataOutput.writeUTF(n.getClass().getName());
            n.write(dataOutput);
        }
    }

    /**
     * To split long string to a list of smaller strings.
     *
     * @param str
     * @return List
     */
    private List<String> divideStr(String str) {
        List<String> list = new ArrayList<String>();
        int len = 20000;
        int strlen = str.length();
        int start = 0;
        int end = len;

        while (end < strlen) {
            list.add(str.substring(start, end));
            start = end;
            end += len;
        }

        if (strlen <= end) {
            list.add(str.substring(start, strlen));
        }
        return list;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        name = dataInput.readUTF();
        //definition = dataInput.readUTF();
        //read the full definition back
        int defListSize = dataInput.readInt();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < defListSize; i++) {
            sb.append(dataInput.readUTF());
        }
        definition = sb.toString();

        int numNodes = dataInput.readInt();
        for (int x = 0; x < numNodes; x++) {
            try {
                String nodeDefClass = dataInput.readUTF();
                NodeDef node = (NodeDef) ReflectionUtils.newInstance(Class.forName(nodeDefClass), null);
                node.readFields(dataInput);
                addNode(node);
            }
            catch (WorkflowException ex) {
                throw new IOException(ex);
            }
            catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
        }
    }

}
