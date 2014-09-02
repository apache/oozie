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

package org.apache.oozie.workflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.workflow.lite.NodeDef;

import java.util.Map;

/**
 * A workflow instance is an executable instance of a {@link WorkflowApp}.
 */
public interface WorkflowInstance {

    /**
     * Separator to qualify variables belonging to a node. <p/> Variables names should be compossed as <code>nodeName +
     * {@link #NODE_VAR_SEPARATOR} + varName</code>.
     */
    public final static String NODE_VAR_SEPARATOR = "#";

    /**
     * Defines the possible stati of a {@link WorkflowInstance}.
     */
    public static enum Status {
        PREP(false),
        RUNNING(false),
        SUSPENDED(false),
        SUCCEEDED(true),
        FAILED(true),
        KILLED(true);

        private boolean isEndState;

        private Status(boolean isEndState) {
            this.isEndState = isEndState;
        }

        /**
         * Return if the status if an end state (it cannot change anymore).
         *
         * @return if the status if an end state (it cannot change anymore).
         */
        public boolean isEndState() {
            return isEndState;
        }

    }

    /**
     * Return the configuration of the instance.
     *
     * @return the configuration of the instance.
     */
    public Configuration getConf();

    /**
     * Return the ID of the instance.
     *
     * @return the ID of the instance.
     */
    public String getId();

    /**
     * Return the workflow application that defines the instance.
     *
     * @return the workflow application that defines the instance.
     */
    public WorkflowApp getApp();

    /**
     * Start the instance.
     *
     * @throws WorkflowException thrown if the instance could not be started.
     */
    public boolean start() throws WorkflowException;

    /**
     * Signal the instance that a node has completed.
     *
     * @param path execution path of the node that has completed.
     * @param signaValue signal value for the node.
     * @return <code>true</code> if the instance has completed its execution, <code>false</code> otherwise.
     */
    public boolean signal(String path, String signaValue) throws WorkflowException;

    /**
     * Fail the instance. <p/> All executing nodes will be be signaled for fail.
     *
     * @param nodeName the name of the node to be failed.
     * @throws WorkflowException thrown if the instance could not be failed.
     */
    public void fail(String nodeName) throws WorkflowException;

    /**
     * Kill the instance. <p/> All executing nodes will be be signaled for kill.
     *
     * @throws WorkflowException thrown if the instance could not be killed.
     */
    public void kill() throws WorkflowException;

    /**
     * Suspend the instance.
     *
     * @throws WorkflowException thrown if the instance could not be suspended.
     */
    public void suspend() throws WorkflowException;

    /**
     * Resume the instance.
     *
     * @throws WorkflowException thrown if the instance could not be resume.
     */
    public void resume() throws WorkflowException;

    /**
     * Return the current status of the instance.
     *
     * @return the current status of the instance.
     */
    public Status getStatus();

    /**
     * Set a variable in the context of the instance. <p/> Variables are persisted with the instance.
     *
     * @param name variable name.
     * @param value variable value, setting a <code>null</code> value removes the variable.
     */
    public void setVar(String name, String value);

    /**
     * Return a variable from the context of the instance.
     *
     * @param name name of the variable.
     * @return variable value, <code>null</code> if the variable is not in the context.
     */
    public String getVar(String name);

    /**
     * Return a map with all the variables in the context of the instance.
     *
     * @return a map with all the variables in the context of the instance.
     */
    public Map<String, String> getAllVars();

    /**
     * Add a set of variables in the context of the instance. <p/> Variables are persisted with the instance.
     *
     * @param varMap map with the variables to add.
     */
    public void setAllVars(Map<String, String> varMap);

    /**
     * Set a transient variable in the context of the instance. <p/> Transient variables are not persisted with the
     * instance.
     *
     * @param name transient variable name.
     * @param value transient variable value, setting a <code>null</code> value removes the variable.
     */
    public void setTransientVar(String name, Object value);

    /**
     * Return a transient variable from the context of the instance.
     *
     * @param name name of the transient variable.
     * @return transient variable value, <code>null</code> if the variable is not in the context.
     */
    public Object getTransientVar(String name);

    /**
     * Return the transition a node did. <p/> This is meaninful only for action and decision nodes.
     *
     * @param node the node name.
     * @return the transition the node did, <code>null</code> if the node didn't execute yet.
     */
    public String getTransition(String node);

    /**
     * Get NodeDef from workflow instance
     * @param executionPath execution path
     * @return node def
     */
    public NodeDef getNodeDef(String executionPath);

}
