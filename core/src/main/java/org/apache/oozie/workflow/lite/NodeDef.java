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
import org.apache.oozie.service.LiteWorkflowStoreService;
import org.apache.oozie.util.StringSerializationUtil;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.workflow.WorkflowException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This node definition is serialized object and should provide readFields() and write() for read and write of fields in
 * this class.
 */
public class NodeDef implements Writable {
    private String nodeDefVersion = null;
    private String name = null;
    private Class<? extends NodeHandler> handlerClass;
    private String conf = null;
    private List<String> transitions = new ArrayList<String>();
    private String cred = null;
    private String userRetryMax = "null";
    private String userRetryInterval = "null";
    private String userRetryPolicy = "null";

    NodeDef() {
    }

    NodeDef(String name, String conf, Class<? extends NodeHandler> handlerClass, List<String> transitions) {
        this.name = ParamChecker.notEmpty(name, "name");
        this.conf = conf;
        this.handlerClass = ParamChecker.notNull(handlerClass, "handlerClass");
        this.transitions = Collections.unmodifiableList(ParamChecker.notEmptyElements(transitions, "transitions"));
    }

    NodeDef(String name, String conf, Class<? extends NodeHandler> handlerClass, List<String> transitions, String cred) {
        this(name, conf, handlerClass, transitions);
        if (cred != null) {
            this.cred = cred;
        }
    }

    NodeDef(String name, String conf, Class<? extends NodeHandler> handlerClass, List<String> transitions, String cred,
            String userRetryMax, String userRetryInterval, String userRetryPolicy) {
        this(name, conf, handlerClass, transitions, cred);
        if (userRetryMax != null) {
            this.userRetryMax = userRetryMax;
        }
        if (userRetryInterval != null) {
            this.userRetryInterval = userRetryInterval;
        }
        if (userRetryPolicy != null) {
            this.userRetryPolicy = userRetryPolicy;
        }
    }

    public boolean equals(NodeDef other) {
        return !(other == null || getClass() != other.getClass() || !getName().equals(other.getName()));
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    public String getName() {
        return name;
    }

    public String getCred() {
        return cred;
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

    public String getUserRetryMax() {
        return userRetryMax;
    }

    public String getUserRetryInterval() {
        return userRetryInterval;
    }

    public String getNodeDefVersion() {
        if (nodeDefVersion == null) {
            try {
                nodeDefVersion = LiteWorkflowStoreService.getNodeDefDefaultVersion();
            }
            catch (WorkflowException e) {
                nodeDefVersion = LiteWorkflowStoreService.NODE_DEF_VERSION_2;
            }
        }
        return nodeDefVersion;
    }

    public String getUserRetryPolicy() {
        return userRetryPolicy;
    }

    public void setUserRetryPolicy(String userRetryPolicy) {
        this.userRetryPolicy = userRetryPolicy;
    }

    @SuppressWarnings("unchecked")
    private void readVersionZero(DataInput dataInput, String firstField) throws IOException {
        if (firstField.equals(LiteWorkflowStoreService.NODE_DEF_VERSION_0)) {
            name = dataInput.readUTF();
        } else {
            name = firstField;
        }
        nodeDefVersion = LiteWorkflowStoreService.NODE_DEF_VERSION_0;
        cred = dataInput.readUTF();
        String handlerClassName = dataInput.readUTF();
        if ((handlerClassName != null) && (handlerClassName.length() > 0)) {
            try {
                handlerClass = (Class<? extends NodeHandler>) Class.forName(handlerClassName);
            }
            catch (ClassNotFoundException ex) {
                throw new IOException(ex);
            }
        }
        conf = readString(dataInput);
        if (conf.equals("null")) {
            conf = null;
        }
        int numTrans = dataInput.readInt();
        transitions = new ArrayList<String>(numTrans);
        for (int i = 0; i < numTrans; i++) {
            transitions.add(dataInput.readUTF());
        }
    }
    @SuppressWarnings("unchecked")
    private void readVersionOne(DataInput dataInput, String firstField) throws IOException {
        readCommon(dataInput, firstField, LiteWorkflowStoreService.NODE_DEF_VERSION_1);
    }

    /*
     * Reads according to version 2
     */
    @SuppressWarnings("unchecked")
    private void readVersionTwo(DataInput dataInput, String firstField) throws IOException {
        readCommon(dataInput, firstField, LiteWorkflowStoreService.NODE_DEF_VERSION_2);
        userRetryPolicy = dataInput.readUTF();
    }

    /*
     * Reads common part
     */
    @SuppressWarnings("unchecked")
    private void readCommon(DataInput dataInput, String firstField, String nodeDefVer) throws IOException {
        nodeDefVersion = nodeDefVer;
        name = dataInput.readUTF();
        cred = dataInput.readUTF();
        if (cred.equals("null")) {
            cred = null;
        }
        String handlerClassName = dataInput.readUTF();
        if ((handlerClassName != null) && (handlerClassName.length() > 0)) {
            try {
                handlerClass = (Class<? extends NodeHandler>) Class.forName(handlerClassName);
            }
            catch (ClassNotFoundException ex) {
                throw new IOException(ex);
            }
        }
        conf = readString(dataInput);
        if (conf.equals("null")) {
            conf = null;
        }
        int numTrans = dataInput.readInt();
        transitions = new ArrayList<String>(numTrans);
        for (int i = 0; i < numTrans; i++) {
            transitions.add(dataInput.readUTF());
        }
        userRetryMax = dataInput.readUTF();
        userRetryInterval = dataInput.readUTF();
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String firstField = dataInput.readUTF();
        if (firstField.equals(LiteWorkflowStoreService.NODE_DEF_VERSION_1)) {
            // since oozie version 3.1
            readVersionOne(dataInput, firstField);
        }
        else if (firstField.equals(LiteWorkflowStoreService.NODE_DEF_VERSION_2)) {
            readVersionTwo(dataInput, firstField);
        }
        else {
            readVersionZero(dataInput, firstField);
        }
    }

    private void writeVersionZero(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(nodeDefVersion);
        dataOutput.writeUTF(name);
        if (cred != null) {
            dataOutput.writeUTF(cred);
        }
        else {
            dataOutput.writeUTF("null");
        }
        dataOutput.writeUTF(handlerClass.getName());
        if (conf != null) {
            writeString(dataOutput, conf);
        }
        else {
            dataOutput.writeUTF("null");
        }
        dataOutput.writeInt(transitions.size());
        for (String transition : transitions) {
            dataOutput.writeUTF(transition);
        }
    }

    private void writeString(DataOutput dataOutput, String value) throws IOException {
        StringSerializationUtil.writeString(dataOutput, value);
    }

    private String readString(DataInput dataInput) throws IOException {
        return StringSerializationUtil.readString(dataInput);
    }

    /**
     * Write as version one format, this version was since 3.1.
     *
     * @param dataOutput data output to serialize node def
     * @throws IOException thrown if fail to write
     */
    private void writeVersionOne(DataOutput dataOutput) throws IOException {
        writeCommon(dataOutput);
    }

    /**
     * Write as version two format, this version was since 4.4.4.1.
     *
     * @param dataOutput data output to serialize node def
     * @throws IOException thrown if fail to write
     */
    private void writeVersionTwo(DataOutput dataOutput) throws IOException {
        writeCommon(dataOutput);
        if (userRetryPolicy != null) {
            dataOutput.writeUTF(userRetryPolicy);
        }
        else {
            dataOutput.writeUTF("null");
        }
    }

    /*
     * Write the common part
     */
    private void writeCommon(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(nodeDefVersion);
        dataOutput.writeUTF(name);
        if (cred != null) {
            writeString(dataOutput, cred);
        }
        else {
            dataOutput.writeUTF("null");
        }
        writeString(dataOutput, handlerClass.getName());
        if (conf != null) {
            writeString(dataOutput, conf);
        }
        else {
            dataOutput.writeUTF("null");
        }
        dataOutput.writeInt(transitions.size());
        for (String transition : transitions) {
            dataOutput.writeUTF(transition);
        }
        if (userRetryMax != null) {
            dataOutput.writeUTF(userRetryMax);
        }
        else {
            dataOutput.writeUTF("null");
        }
        if (userRetryInterval != null) {
            dataOutput.writeUTF(userRetryInterval);
        }
        else {
            dataOutput.writeUTF("null");
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        if (getNodeDefVersion().equals(LiteWorkflowStoreService.NODE_DEF_VERSION_1)) {
            // since oozie version 3.1
            writeVersionOne(dataOutput);
        }
        else if (getNodeDefVersion().equals(LiteWorkflowStoreService.NODE_DEF_VERSION_2)) {
            writeVersionTwo(dataOutput);
        }
        else {
            writeVersionZero(dataOutput);
        }
    }

}
