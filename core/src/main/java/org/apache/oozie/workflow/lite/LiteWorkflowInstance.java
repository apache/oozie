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

import org.apache.oozie.service.XLogService;
import org.apache.oozie.service.DagXLogInfoService;
import org.apache.oozie.client.OozieClient;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.workflow.WorkflowApp;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.ErrorCode;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//TODO javadoc
public class LiteWorkflowInstance implements Writable, WorkflowInstance {
    private static final String TRANSITION_TO = "transition.to";

    private final Date FAR_INTO_THE_FUTURE = new Date(Long.MAX_VALUE);

    private XLog log;

    private static String PATH_SEPARATOR = "/";
    private static String ROOT = PATH_SEPARATOR;
    private static String TRANSITION_SEPARATOR = "#";

    // Using unique string to indicate version. This is to make sure that it
    // doesn't match with user data.
    private static final String DATA_VERSION = "V==1";

    private static class NodeInstance {
        String nodeName;
        boolean started = false;

        private NodeInstance(String nodeName) {
            this.nodeName = nodeName;
        }
    }

    private class Context implements NodeHandler.Context {
        private NodeDef nodeDef;
        private String executionPath;
        private String exitState;
        private Status status = Status.RUNNING;

        private Context(NodeDef nodeDef, String executionPath, String exitState) {
            this.nodeDef = nodeDef;
            this.executionPath = executionPath;
            this.exitState = exitState;
        }

        public NodeDef getNodeDef() {
            return nodeDef;
        }

        public String getExecutionPath() {
            return executionPath;
        }

        public String getParentExecutionPath(String executionPath) {
            return LiteWorkflowInstance.getParentPath(executionPath);
        }

        public String getSignalValue() {
            return exitState;
        }

        public String createExecutionPath(String name) {
            return LiteWorkflowInstance.createChildPath(executionPath, name);
        }

        public String createFullTransition(String executionPath, String transition) {
            return LiteWorkflowInstance.createFullTransition(executionPath, transition);
        }

        public void deleteExecutionPath() {
            if (!executionPaths.containsKey(executionPath)) {
                throw new IllegalStateException();
            }
            executionPaths.remove(executionPath);
            executionPath = LiteWorkflowInstance.getParentPath(executionPath);
        }

        public void failJob() {
            status = Status.FAILED;
        }

        public void killJob() {
            status = Status.KILLED;
        }

        public void completeJob() {
            status = Status.SUCCEEDED;
        }

        @Override
        public Object getTransientVar(String name) {
            return LiteWorkflowInstance.this.getTransientVar(name);
        }

        @Override
        public String getVar(String name) {
            return LiteWorkflowInstance.this.getVar(name);
        }

        @Override
        public void setTransientVar(String name, Object value) {
            LiteWorkflowInstance.this.setTransientVar(name, value);
        }

        @Override
        public void setVar(String name, String value) {
            LiteWorkflowInstance.this.setVar(name, value);
        }

        @Override
        public LiteWorkflowInstance getProcessInstance() {
            return LiteWorkflowInstance.this;
        }

    }

    private LiteWorkflowApp def;
    private Configuration conf;
    private String instanceId;
    private Status status;
    private Map<String, NodeInstance> executionPaths = new HashMap<String, NodeInstance>();
    private Map<String, String> persistentVars = new HashMap<String, String>();
    private Map<String, Object> transientVars = new HashMap<String, Object>();
    private ActionEndTimesComparator actionEndTimesComparator = null;

    protected LiteWorkflowInstance() {
        log = XLog.getLog(getClass());
    }

    public LiteWorkflowInstance(LiteWorkflowApp def, Configuration conf, String instanceId) {
        this();
        this.def = ParamChecker.notNull(def, "def");
        this.instanceId = ParamChecker.notNull(instanceId, "instanceId");
        this.conf = ParamChecker.notNull(conf, "conf");
        refreshLog();
        status = Status.PREP;
    }

    public LiteWorkflowInstance(LiteWorkflowApp def, Configuration conf, String instanceId, Map<String, Date> actionEndTimes) {
        this(def, conf, instanceId);
        actionEndTimesComparator = new ActionEndTimesComparator(actionEndTimes);
    }

    public synchronized boolean start() throws WorkflowException {
        if (status != Status.PREP) {
            throw new WorkflowException(ErrorCode.E0719);
        }
        log.debug(XLog.STD, "Starting job");
        status = Status.RUNNING;
        executionPaths.put(ROOT, new NodeInstance(StartNodeDef.START));
        return signal(ROOT, StartNodeDef.START);
    }

    //todo if suspended store signal and use when resuming

    public synchronized boolean signal(String executionPath, String signalValue) throws WorkflowException {
        ParamChecker.notEmpty(executionPath, "executionPath");
        ParamChecker.notNull(signalValue, "signalValue");
        log.debug(XLog.STD, "Signaling job execution path [{0}] signal value [{1}]", executionPath, signalValue);
        if (status != Status.RUNNING) {
            throw new WorkflowException(ErrorCode.E0716);
        }
        NodeInstance nodeJob = executionPaths.get(executionPath);
        if (nodeJob == null) {
            status = Status.FAILED;
            log.error("invalid execution path [{0}]", executionPath);
        }
        NodeDef nodeDef = null;
        if (!status.isEndState()) {
            nodeDef = def.getNode(nodeJob.nodeName);
            if (nodeDef == null) {
                status = Status.FAILED;
                log.error("invalid transition [{0}]", nodeJob.nodeName);
            }
        }
        if (!status.isEndState()) {
            NodeHandler nodeHandler = newInstance(nodeDef.getHandlerClass());
            boolean exiting = true;

            Context context = new Context(nodeDef, executionPath, signalValue);
            if (!nodeJob.started) {
                try {
                    nodeHandler.loopDetection(context);
                    exiting = nodeHandler.enter(context);
                    nodeJob.started = true;
                }
                catch (WorkflowException ex) {
                    status = Status.FAILED;
                    List<String> killedNodes = terminateNodes(Status.KILLED);
                    if (killedNodes.size() > 1) {
                        log.warn(XLog.STD, "Workflow completed [{0}], killing [{1}] running nodes", status, killedNodes
                                .size());
                    }
                    throw ex;
                }
            }

            if (exiting) {
                List<String> pathsToStart = new ArrayList<String>();
                List<String> fullTransitions;
                try {
                    fullTransitions = nodeHandler.multiExit(context);
                    int last = fullTransitions.size() - 1;
                    // TEST THIS
                    if (last >= 0) {
                        String transitionTo = getTransitionNode(fullTransitions.get(last));
                        if (nodeDef instanceof ForkNodeDef) {
                            transitionTo = "*"; // WF action cannot hold all transitions for a fork.
                                                // transitions are hardcoded in the WF app.
                        }
                        persistentVars.put(nodeDef.getName() + WorkflowInstance.NODE_VAR_SEPARATOR + TRANSITION_TO,
                                           transitionTo);
                    }
                }
                catch (WorkflowException ex) {
                    status = Status.FAILED;
                    throw ex;
                }

                if (context.status == Status.KILLED) {
                    status = Status.KILLED;
                    log.debug(XLog.STD, "Completing job, kill node [{0}]", nodeJob.nodeName);
                }
                else {
                    if (context.status == Status.FAILED) {
                        status = Status.FAILED;
                        log.debug(XLog.STD, "Completing job, fail node [{0}]", nodeJob.nodeName);
                    }
                    else {
                        if (context.status == Status.SUCCEEDED) {
                            status = Status.SUCCEEDED;
                            log.debug(XLog.STD, "Completing job, end node [{0}]", nodeJob.nodeName);
                        }
/*
                else if (context.status == Status.SUSPENDED) {
                    status = Status.SUSPENDED;
                    log.debug(XLog.STD, "Completing job, end node [{0}]", nodeJob.nodeName);
                }
*/
                        else {
                            for (String fullTransition : fullTransitions) {
                                // this is the whole trick for forking, we need the
                                // executionpath and the transition
                                // in the case of no forking last element of
                                // executionpath is different from transition
                                // in the case of forking they are the same

                                log.debug(XLog.STD, "Exiting node [{0}] with transition[{1}]", nodeJob.nodeName,
                                          fullTransition);

                                String execPathFromTransition = getExecutionPath(fullTransition);
                                String transition = getTransitionNode(fullTransition);
                                def.validateTransition(nodeJob.nodeName, transition);

                                NodeInstance nodeJobInPath = executionPaths.get(execPathFromTransition);
                                if ((nodeJobInPath == null) || (!transition.equals(nodeJobInPath.nodeName))) {
                                    // TODO explain this IF better
                                    // If the WfJob is signaled with the parent
                                    // execution executionPath again
                                    // The Fork node will execute again.. and replace
                                    // the Node WorkflowJobBean
                                    // so this is required to prevent that..
                                    // Question : Should we throw an error in this case
                                    // ??
                                    executionPaths.put(execPathFromTransition, new NodeInstance(transition));
                                    pathsToStart.add(execPathFromTransition);
                                }

                            }

                            // If we're doing a rerun, then we need to make sure to put the actions in pathToStart into the order
                            // that they ended in.  Otherwise, it could result in an error later on in some edge cases.
                            // e.g. You have a fork with two nodes, A and B, that both succeeded, followed by a join and some more
                            // nodes, some of which failed.  If you do the rerun, it will always signal A and then B, even if in the
                            // original run B signaled first and then A.  By sorting this, we maintain the proper signal ordering.
                            if (actionEndTimesComparator != null && pathsToStart.size() > 1) {
                                Collections.sort(pathsToStart, actionEndTimesComparator);
                            }

                            // signal all new synch transitions
                            for (String pathToStart : pathsToStart) {
                                signal(pathToStart, "::synch::");
                            }
                        }
                    }
                }
            }
        }
        if (status.isEndState()) {
            if (status == Status.FAILED) {
                List<String> failedNodes = terminateNodes(status);
                log.warn(XLog.STD, "Workflow completed [{0}], failing [{1}] running nodes", status, failedNodes
                        .size());
            }
            else {
                List<String> killedNodes = terminateNodes(Status.KILLED);

                if (killedNodes.size() > 1) {
                    log.warn(XLog.STD, "Workflow completed [{0}], killing [{1}] running nodes", status, killedNodes
                            .size());
                }
            }
        }
        return status.isEndState();
    }

    /**
     * Get NodeDef from workflow instance
     * @param executionPath execution path
     * @return node def
     */
    public NodeDef getNodeDef(String executionPath) {
        NodeInstance nodeJob = executionPaths.get(executionPath);
        NodeDef nodeDef = null;
        if (nodeJob == null) {
            log.error("invalid execution path [{0}]", executionPath);
        }
        else {
            nodeDef = def.getNode(nodeJob.nodeName);
            if (nodeDef == null) {
                log.error("invalid transition [{0}]", nodeJob.nodeName);
            }
        }
        return nodeDef;
    }

    public synchronized void fail(String nodeName) throws WorkflowException {
        if (status.isEndState()) {
            throw new WorkflowException(ErrorCode.E0718);
        }
        String failedNode = failNode(nodeName);
        if (failedNode != null) {
            log.warn(XLog.STD, "Workflow Failed. Failing node [{0}]", failedNode);
        }
        else {
            //TODO failed attempting to fail the action. EXCEPTION
        }
        List<String> killedNodes = killNodes();
        if (killedNodes.size() > 1) {
            log.warn(XLog.STD, "Workflow Failed, killing [{0}] nodes", killedNodes.size());
        }
        status = Status.FAILED;
    }

    public synchronized void kill() throws WorkflowException {
        if (status.isEndState()) {
            throw new WorkflowException(ErrorCode.E0718);
        }
        log.debug(XLog.STD, "Killing job");
        List<String> killedNodes = killNodes();
        if (killedNodes.size() > 1) {
            log.warn(XLog.STD, "workflow killed, killing [{0}] nodes", killedNodes.size());
        }
        status = Status.KILLED;
    }

    public synchronized void suspend() throws WorkflowException {
        if (status != Status.RUNNING) {
            throw new WorkflowException(ErrorCode.E0716);
        }
        log.debug(XLog.STD, "Suspending job");
        this.status = Status.SUSPENDED;
    }

    public boolean isSuspended() {
        return (status == Status.SUSPENDED);
    }

    public synchronized void resume() throws WorkflowException {
        if (status != Status.SUSPENDED) {
            throw new WorkflowException(ErrorCode.E0717);
        }
        log.debug(XLog.STD, "Resuming job");
        status = Status.RUNNING;
    }

    public void setVar(String name, String value) {
        if (value != null) {
            persistentVars.put(name, value);
        }
        else {
            persistentVars.remove(name);
        }
    }

    @Override
    public Map<String, String> getAllVars() {
        return persistentVars;
    }

    @Override
    public void setAllVars(Map<String, String> varMap) {
        persistentVars.putAll(varMap);
    }

    public String getVar(String name) {
        return persistentVars.get(name);
    }


    public void setTransientVar(String name, Object value) {
        if (value != null) {
            transientVars.put(name, value);
        }
        else {
            transientVars.remove(name);
        }
    }

    public boolean hasTransientVar(String name) {
        return transientVars.containsKey(name);
    }

    public Object getTransientVar(String name) {
        return transientVars.get(name);
    }

    public boolean hasEnded() {
        return status.isEndState();
    }

    private List<String> terminateNodes(Status endStatus) {
        List<String> endNodes = new ArrayList<String>();
        for (Map.Entry<String, NodeInstance> entry : executionPaths.entrySet()) {
            if (entry.getValue().started) {
                NodeDef nodeDef = def.getNode(entry.getValue().nodeName);
                if (!(nodeDef instanceof ControlNodeDef)) {
                    NodeHandler nodeHandler = newInstance(nodeDef.getHandlerClass());
                    try {
                        if (endStatus == Status.KILLED) {
                            nodeHandler.kill(new Context(nodeDef, entry.getKey(), null));
                        }
                        else {
                            if (endStatus == Status.FAILED) {
                                nodeHandler.fail(new Context(nodeDef, entry.getKey(), null));
                            }
                        }
                        endNodes.add(nodeDef.getName());
                    }
                    catch (Exception ex) {
                        log.warn(XLog.STD, "Error Changing node state to [{0}] for Node [{1}]", endStatus.toString(),
                                 nodeDef.getName(), ex);
                    }
                }
            }
        }
        return endNodes;
    }

    private String failNode(String nodeName) {
        String failedNode = null;
        for (Map.Entry<String, NodeInstance> entry : executionPaths.entrySet()) {
            String node = entry.getKey();
            NodeInstance nodeInstance = entry.getValue();
            if (nodeInstance.started && nodeInstance.nodeName.equals(nodeName)) {
                NodeDef nodeDef = def.getNode(nodeInstance.nodeName);
                NodeHandler nodeHandler = newInstance(nodeDef.getHandlerClass());
                try {
                    nodeHandler.fail(new Context(nodeDef, node, null));
                    failedNode = nodeDef.getName();
                    nodeInstance.started = false;
                }
                catch (Exception ex) {
                    log.warn(XLog.STD, "Error failing node [{0}]", nodeDef.getName(), ex);
                }
                return failedNode;
            }
        }
        return failedNode;
    }

    private List<String> killNodes() {
        List<String> killedNodes = new ArrayList<String>();
        for (Map.Entry<String, NodeInstance> entry : executionPaths.entrySet()) {
            String node = entry.getKey();
            NodeInstance nodeInstance = entry.getValue();
            if (nodeInstance.started) {
                NodeDef nodeDef = def.getNode(nodeInstance.nodeName);
                NodeHandler nodeHandler = newInstance(nodeDef.getHandlerClass());
                try {
                    nodeHandler.kill(new Context(nodeDef, node, null));
                    killedNodes.add(nodeDef.getName());
                }
                catch (Exception ex) {
                    log.warn(XLog.STD, "Error killing node [{0}]", nodeDef.getName(), ex);
                }
            }
        }
        return killedNodes;
    }

    public LiteWorkflowApp getProcessDefinition() {
        return def;
    }

    private static String createChildPath(String path, String child) {
        return path + child + PATH_SEPARATOR;
    }

    private static String getParentPath(String path) {
        path = path.substring(0, path.length() - 1);
        return (path.length() == 0) ? null : path.substring(0, path.lastIndexOf(PATH_SEPARATOR) + 1);
    }

    private static String createFullTransition(String executionPath, String transition) {
        return executionPath + TRANSITION_SEPARATOR + transition;
    }

    private static String getExecutionPath(String fullTransition) {
        int index = fullTransition.indexOf(TRANSITION_SEPARATOR);
        if (index == -1) {
            throw new IllegalArgumentException("Invalid fullTransition");
        }
        return fullTransition.substring(0, index);
    }

    private static String getTransitionNode(String fullTransition) {
        int index = fullTransition.indexOf(TRANSITION_SEPARATOR);
        if (index == -1) {
            throw new IllegalArgumentException("Invalid fullTransition");
        }
        return fullTransition.substring(index + 1);
    }

    private NodeHandler newInstance(Class<? extends NodeHandler> handler) {
        return (NodeHandler) ReflectionUtils.newInstance(handler, null);
    }

    private void refreshLog() {
        XLog.Info.get().setParameter(XLogService.USER, conf.get(OozieClient.USER_NAME));
        XLog.Info.get().setParameter(XLogService.GROUP, conf.get(OozieClient.GROUP_NAME));
        XLog.Info.get().setParameter(DagXLogInfoService.APP, def.getName());
        XLog.Info.get().setParameter(DagXLogInfoService.TOKEN, conf.get(OozieClient.LOG_TOKEN, ""));
        XLog.Info.get().setParameter(DagXLogInfoService.JOB, instanceId);
        log = XLog.getLog(getClass());
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    @Override
    public void write(DataOutput dOut) throws IOException {
        dOut.writeUTF(instanceId);

        //Hadoop Configuration has to get its act right
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        conf.writeXml(baos);
        baos.close();
        byte[] array = baos.toByteArray();
        dOut.writeInt(array.length);
        dOut.write(array);

        def.write(dOut);
        dOut.writeUTF(status.toString());
        dOut.writeInt(executionPaths.size());
        for (Map.Entry<String, NodeInstance> entry : executionPaths.entrySet()) {
            dOut.writeUTF(entry.getKey());
            dOut.writeUTF(entry.getValue().nodeName);
            dOut.writeBoolean(entry.getValue().started);
        }
        dOut.writeInt(persistentVars.size());
        for (Map.Entry<String, String> entry : persistentVars.entrySet()) {
            dOut.writeUTF(entry.getKey());
            writeStringAsBytes(entry.getValue(), dOut);
        }
        if (actionEndTimesComparator != null) {
            Map<String, Date> actionEndTimes = actionEndTimesComparator.getActionEndTimes();
            dOut.writeInt(actionEndTimes.size());
            for (Map.Entry<String, Date> entry : actionEndTimes.entrySet()) {
                dOut.writeUTF(entry.getKey());
                dOut.writeLong(entry.getValue().getTime());
            }
        }
    }

    @Override
    public void readFields(DataInput dIn) throws IOException {
        instanceId = dIn.readUTF();

        //Hadoop Configuration has to get its act right
        int len = dIn.readInt();
        byte[] array = new byte[len];
        dIn.readFully(array);
        ByteArrayInputStream bais = new ByteArrayInputStream(array);
        conf = new XConfiguration(bais);

        def = new LiteWorkflowApp();
        def.readFields(dIn);
        status = Status.valueOf(dIn.readUTF());
        int numExPaths = dIn.readInt();
        for (int x = 0; x < numExPaths; x++) {
            String path = dIn.readUTF();
            String nodeName = dIn.readUTF();
            boolean isStarted = dIn.readBoolean();
            NodeInstance nodeInstance = new NodeInstance(nodeName);
            nodeInstance.started = isStarted;
            executionPaths.put(path, nodeInstance);
        }
        int numVars = dIn.readInt();
        for (int x = 0; x < numVars; x++) {
            String vName = dIn.readUTF();
            String vVal = readBytesAsString(dIn);
            persistentVars.put(vName, vVal);
        }
        int numActionEndTimes = -1;
        try {
            numActionEndTimes = dIn.readInt();
        } catch (IOException ioe) {
            // This means that there isn't an actionEndTimes, so just ignore
        }
        if (numActionEndTimes > 0) {
            Map<String, Date> actionEndTimes = new HashMap<String, Date>(numActionEndTimes);
            for (int x = 0; x < numActionEndTimes; x++) {
                String name = dIn.readUTF();
                long endTime = dIn.readLong();
                actionEndTimes.put(name, new Date(endTime));
            }
            actionEndTimesComparator = new ActionEndTimesComparator(actionEndTimes);
        }
        refreshLog();
    }

    private void writeStringAsBytes(String value, DataOutput dOut) throws IOException {
        if (value == null) {
            dOut.writeUTF(null);
            return;
        }
        dOut.writeUTF(DATA_VERSION);
        byte[] data = value.getBytes("UTF-8");
        dOut.writeInt(data.length);
        dOut.write(data);
    }

    private String readBytesAsString(DataInput dIn) throws IOException {
        String value = dIn.readUTF();
        if (value != null && value.equals(DATA_VERSION)) {
            int length = dIn.readInt();
            byte[] data = new byte[length];
            dIn.readFully(data);
            value = new String(data, "UTF-8");
        }
        return value;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public WorkflowApp getApp() {
        return def;
    }

    @Override
    public String getId() {
        return instanceId;
    }

    @Override
    public String getTransition(String node) {
        return persistentVars.get(node + WorkflowInstance.NODE_VAR_SEPARATOR + TRANSITION_TO);
    }

    @Override
    public boolean equals(Object o) {
        return (o != null) && (getClass().isInstance(o)) && ((WorkflowInstance) o).getId().equals(instanceId);
    }

    @Override
    public int hashCode() {
        return instanceId.hashCode();
    }

    private class ActionEndTimesComparator implements Comparator<String> {

        private final Map<String, Date> actionEndTimes;

        public ActionEndTimesComparator(Map<String, Date> actionEndTimes) {
            this.actionEndTimes = actionEndTimes;
        }

        @Override
        public int compare(String node1, String node2) {
            Date date1 = null;
            Date date2 = null;
            NodeInstance node1Instance = executionPaths.get(node1);
            if (node1Instance != null) {
                date1 = this.actionEndTimes.get(node1Instance.nodeName);
            }
            NodeInstance node2Instance = executionPaths.get(node2);
            if (node2Instance != null) {
                date2 = this.actionEndTimes.get(node2Instance.nodeName);
            }
            date1 = (date1 == null) ? FAR_INTO_THE_FUTURE : date1;
            date2 = (date2 == null) ? FAR_INTO_THE_FUTURE : date2;
            return date1.compareTo(date2);
        }

        public Map<String, Date> getActionEndTimes() {
            return actionEndTimes;
        }
    }
}
