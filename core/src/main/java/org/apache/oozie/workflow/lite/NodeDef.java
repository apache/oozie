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
package org.apache.oozie.workflow.lite;

import org.apache.hadoop.io.Writable;
import org.apache.oozie.util.ParamChecker;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

//TODO javadoc
public class NodeDef implements Writable {
    private String name;
    private Class<? extends NodeHandler> handlerClass;
    private String conf;
    private List<String> transitions = new ArrayList<String>();

    NodeDef() {
    }

    NodeDef(String name, String conf, Class<? extends NodeHandler> handlerClass, List<String> transitions) {
        this.name = ParamChecker.notEmpty(name, "name");
        this.conf = conf;
        this.handlerClass = ParamChecker.notNull(handlerClass, "handlerClass");
        this.transitions = Collections.unmodifiableList(ParamChecker.notEmptyElements(transitions, "transitions"));
    }

    public boolean equals(NodeDef other) {
        return !(other == null || getClass() != other.getClass() || !getName().equals(other.getName()));
    }

    public int hashCode() {
        return name.hashCode();
    }

    public String getName() {
        return name;
    }

    public Class<? extends NodeHandler> getHandlerClass() {
        return handlerClass;
    }

    public List<String> getTransitions() {
        return transitions;
    }

    public String getConf() {
        return conf;
    }

    @Override @SuppressWarnings("unchecked")
    public void readFields(DataInput dataInput) throws IOException {
        name = dataInput.readUTF();
        String handlerClassName = dataInput.readUTF();
        if ((handlerClassName != null) && (handlerClassName.length() > 0)) {
            try {
                handlerClass = (Class<? extends NodeHandler>) Class.forName(handlerClassName);
            }
            catch (ClassNotFoundException ex) {
                throw new IOException(ex);
            }
        }
        conf = dataInput.readUTF();
        if (conf.equals("null")) {
            conf = null;
        }
        int numTrans = dataInput.readInt();
        transitions = new ArrayList<String>(numTrans);
        for (int i = 0; i < numTrans; i++) {
            transitions.add(dataInput.readUTF());
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(name);
        dataOutput.writeUTF(handlerClass.getName());
        if (conf != null) {
            dataOutput.writeUTF(conf);
        }
        else {
            dataOutput.writeUTF("null");
        }
        dataOutput.writeInt(transitions.size());
        for (String transition : transitions) {
            dataOutput.writeUTF(transition);
        }
    }

}
