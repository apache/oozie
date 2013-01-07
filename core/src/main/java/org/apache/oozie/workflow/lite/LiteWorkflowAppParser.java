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

import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.ParameterVerifier;
import org.apache.oozie.util.ParameterVerifierException;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.ActionService;
import org.apache.hadoop.conf.Configuration;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.xml.sax.SAXException;

import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.Validator;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Class to parse and validate workflow xml
 */
public class LiteWorkflowAppParser {

    private static final String DECISION_E = "decision";
    private static final String ACTION_E = "action";
    private static final String END_E = "end";
    private static final String START_E = "start";
    private static final String JOIN_E = "join";
    private static final String FORK_E = "fork";
    private static final Object KILL_E = "kill";

    private static final String SLA_INFO = "info";
    private static final String CREDENTIALS = "credentials";
    private static final String GLOBAL = "global";
    private static final String PARAMETERS = "parameters";

    private static final String NAME_A = "name";
    private static final String CRED_A = "cred";
    private static final String USER_RETRY_MAX_A = "retry-max";
    private static final String USER_RETRY_INTERVAL_A = "retry-interval";
    private static final String TO_A = "to";

    private static final String FORK_PATH_E = "path";
    private static final String FORK_START_A = "start";

    private static final String ACTION_OK_E = "ok";
    private static final String ACTION_ERROR_E = "error";

    private static final String DECISION_SWITCH_E = "switch";
    private static final String DECISION_CASE_E = "case";
    private static final String DECISION_DEFAULT_E = "default";

    private static final String KILL_MESSAGE_E = "message";
    public static final String VALIDATE_FORK_JOIN = "oozie.validate.ForkJoin";
    public static final String WF_VALIDATE_FORK_JOIN = "oozie.wf.validate.ForkJoin";

    private Schema schema;
    private Class<? extends ControlNodeHandler> controlNodeHandler;
    private Class<? extends DecisionNodeHandler> decisionHandlerClass;
    private Class<? extends ActionNodeHandler> actionHandlerClass;

    private static enum VisitStatus {
        VISITING, VISITED
    }

    private List<String> forkList = new ArrayList<String>();
    private List<String> joinList = new ArrayList<String>();
    private StartNodeDef startNode;
    private List<String> visitedOkNodes = new ArrayList<String>();
    private List<String> visitedJoinNodes = new ArrayList<String>();

    public LiteWorkflowAppParser(Schema schema,
                                 Class<? extends ControlNodeHandler> controlNodeHandler,
                                 Class<? extends DecisionNodeHandler> decisionHandlerClass,
                                 Class<? extends ActionNodeHandler> actionHandlerClass) throws WorkflowException {
        this.schema = schema;
        this.controlNodeHandler = controlNodeHandler;
        this.decisionHandlerClass = decisionHandlerClass;
        this.actionHandlerClass = actionHandlerClass;
    }

    /**
     * Parse and validate xml to {@link LiteWorkflowApp}
     *
     * @param reader
     * @return LiteWorkflowApp
     * @throws WorkflowException
     */
    public LiteWorkflowApp validateAndParse(Reader reader, Configuration jobConf) throws WorkflowException {
        try {
            StringWriter writer = new StringWriter();
            IOUtils.copyCharStream(reader, writer);
            String strDef = writer.toString();

            if (schema != null) {
                Validator validator = schema.newValidator();
                validator.validate(new StreamSource(new StringReader(strDef)));
            }

            Element wfDefElement = XmlUtils.parseXml(strDef);
            ParameterVerifier.verifyParameters(jobConf, wfDefElement);
            LiteWorkflowApp app = parse(strDef, wfDefElement);
            Map<String, VisitStatus> traversed = new HashMap<String, VisitStatus>();
            traversed.put(app.getNode(StartNodeDef.START).getName(), VisitStatus.VISITING);
            validate(app, app.getNode(StartNodeDef.START), traversed);
            //Validate whether fork/join are in pair or not
            if (jobConf.getBoolean(WF_VALIDATE_FORK_JOIN, true) && Services.get().getConf().getBoolean(VALIDATE_FORK_JOIN, true)) {
                validateForkJoin(app);
            }
            return app;
        }
        catch (ParameterVerifierException ex) {
            throw new WorkflowException(ex);
        }
        catch (JDOMException ex) {
            throw new WorkflowException(ErrorCode.E0700, ex.getMessage(), ex);
        }
        catch (SAXException ex) {
            throw new WorkflowException(ErrorCode.E0701, ex.getMessage(), ex);
        }
        catch (IOException ex) {
            throw new WorkflowException(ErrorCode.E0702, ex.getMessage(), ex);
        }
    }

    /**
     * Validate whether fork/join are in pair or not
     * @param app LiteWorkflowApp
     * @throws WorkflowException
     */
    private void validateForkJoin(LiteWorkflowApp app) throws WorkflowException {
        // Make sure the number of forks and joins in wf are equal
        if (forkList.size() != joinList.size()) {
            throw new WorkflowException(ErrorCode.E0730);
        }

        // No need to bother going through all of this if there are no fork/join nodes
        if (!forkList.isEmpty()) {
            visitedOkNodes.clear();
            visitedJoinNodes.clear();
            validateForkJoin(startNode, app, new LinkedList<String>(), new LinkedList<String>(), new LinkedList<String>(), true);
        }
    }

    /*
     * Recursively walk through the DAG and make sure that all fork paths are valid.
     * This should be called from validateForkJoin(LiteWorkflowApp app).  It assumes that visitedOkNodes and visitedJoinNodes are
     * both empty ArrayLists on the first call.
     *
     * @param node the current node; use the startNode on the first call
     * @param app the WorkflowApp
     * @param forkNodes a stack of the current fork nodes
     * @param joinNodes a stack of the current join nodes
     * @param path a stack of the current path
     * @param okTo false if node (or an ancestor of node) was gotten to via an "error to" transition or via a join node that has
     * already been visited at least once before
     * @throws WorkflowException
     */
    private void validateForkJoin(NodeDef node, LiteWorkflowApp app, Deque<String> forkNodes, Deque<String> joinNodes,
            Deque<String> path, boolean okTo) throws WorkflowException {
        if (path.contains(node.getName())) {
            // cycle
            throw new WorkflowException(ErrorCode.E0741, node.getName(), Arrays.toString(path.toArray()));
        }
        path.push(node.getName());

        // Make sure that we're not revisiting a node (that's not a Kill, Join, or End type) that's been visited before from an
        // "ok to" transition; if its from an "error to" transition, then its okay to visit it multiple times.  Also, because we
        // traverse through join nodes multiple times, we have to make sure not to throw an exception here when we're really just
        // re-walking the same execution path (this is why we need the visitedJoinNodes list used later)
        if (okTo && !(node instanceof KillNodeDef) && !(node instanceof JoinNodeDef) && !(node instanceof EndNodeDef)) {
            if (visitedOkNodes.contains(node.getName())) {
                throw new WorkflowException(ErrorCode.E0743, node.getName());
            }
            visitedOkNodes.add(node.getName());
        }

        if (node instanceof StartNodeDef) {
            String transition = node.getTransitions().get(0);   // start always has only 1 transition
            NodeDef tranNode = app.getNode(transition);
            validateForkJoin(tranNode, app, forkNodes, joinNodes, path, okTo);
        }
        else if (node instanceof ActionNodeDef) {
            String transition = node.getTransitions().get(0);   // "ok to" transition
            NodeDef tranNode = app.getNode(transition);
            validateForkJoin(tranNode, app, forkNodes, joinNodes, path, okTo);  // propogate okTo
            transition = node.getTransitions().get(1);          // "error to" transition
            tranNode = app.getNode(transition);
            validateForkJoin(tranNode, app, forkNodes, joinNodes, path, false); // use false
        }
        else if (node instanceof DecisionNodeDef) {
            for(String transition : (new HashSet<String>(node.getTransitions()))) {
                NodeDef tranNode = app.getNode(transition);
                validateForkJoin(tranNode, app, forkNodes, joinNodes, path, okTo);
            }
        }
        else if (node instanceof ForkNodeDef) {
            forkNodes.push(node.getName());
            for(String transition : (new HashSet<String>(node.getTransitions()))) {
                NodeDef tranNode = app.getNode(transition);
                validateForkJoin(tranNode, app, forkNodes, joinNodes, path, okTo);
            }
            forkNodes.pop();
            if (!joinNodes.isEmpty()) {
                joinNodes.pop();
            }
        }
        else if (node instanceof JoinNodeDef) {
            if (forkNodes.isEmpty()) {
                // no fork for join to match with
                throw new WorkflowException(ErrorCode.E0742, node.getName());
            }
            if (forkNodes.size() > joinNodes.size() && (joinNodes.isEmpty() || !joinNodes.peek().equals(node.getName()))) {
                joinNodes.push(node.getName());
            }
            if (!joinNodes.peek().equals(node.getName())) {
                // join doesn't match fork
                throw new WorkflowException(ErrorCode.E0732, forkNodes.peek(), node.getName(), joinNodes.peek());
            }
            joinNodes.pop();
            String currentForkNode = forkNodes.pop();
            String transition = node.getTransitions().get(0);   // join always has only 1 transition
            NodeDef tranNode = app.getNode(transition);
            // If we're already under a situation where okTo is false, use false (propogate it)
            // Or if we've already visited this join node, use false (because we've already traversed this path before and we don't
            // want to throw an exception from the check against visitedOkNodes)
            if (!okTo || visitedJoinNodes.contains(node.getName())) {
                validateForkJoin(tranNode, app, forkNodes, joinNodes, path, false);
            // Else, use true because this is either the first time we've gone through this join node or okTo was already false
            } else {
                visitedJoinNodes.add(node.getName());
                validateForkJoin(tranNode, app, forkNodes, joinNodes, path, true);
            }
            forkNodes.push(currentForkNode);
            joinNodes.push(node.getName());
        }
        else if (node instanceof KillNodeDef) {
            // do nothing
        }
        else if (node instanceof EndNodeDef) {
            if (!forkNodes.isEmpty()) {
                path.pop();     // = node
                String parent = path.peek();
                // can't go to an end node in a fork
                throw new WorkflowException(ErrorCode.E0737, parent, node.getName());
            }
        }
        else {
            // invalid node type (shouldn't happen)
            throw new WorkflowException(ErrorCode.E0740, node.getName());
        }
        path.pop();
    }

    /**
     * Parse xml to {@link LiteWorkflowApp}
     *
     * @param strDef
     * @param root
     * @return LiteWorkflowApp
     * @throws WorkflowException
     */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    private LiteWorkflowApp parse(String strDef, Element root) throws WorkflowException {
        Namespace ns = root.getNamespace();
        LiteWorkflowApp def = null;
        Element global = null;
        for (Element eNode : (List<Element>) root.getChildren()) {
            if (eNode.getName().equals(START_E)) {
                def = new LiteWorkflowApp(root.getAttributeValue(NAME_A), strDef,
                                          new StartNodeDef(controlNodeHandler, eNode.getAttributeValue(TO_A)));
            }
            else {
                if (eNode.getName().equals(END_E)) {
                    def.addNode(new EndNodeDef(eNode.getAttributeValue(NAME_A), controlNodeHandler));
                }
                else {
                    if (eNode.getName().equals(KILL_E)) {
                        def.addNode(new KillNodeDef(eNode.getAttributeValue(NAME_A),
                                                    eNode.getChildText(KILL_MESSAGE_E, ns), controlNodeHandler));
                    }
                    else {
                        if (eNode.getName().equals(FORK_E)) {
                            List<String> paths = new ArrayList<String>();
                            for (Element tran : (List<Element>) eNode.getChildren(FORK_PATH_E, ns)) {
                                paths.add(tran.getAttributeValue(FORK_START_A));
                            }
                            def.addNode(new ForkNodeDef(eNode.getAttributeValue(NAME_A), controlNodeHandler, paths));
                        }
                        else {
                            if (eNode.getName().equals(JOIN_E)) {
                                def.addNode(new JoinNodeDef(eNode.getAttributeValue(NAME_A), controlNodeHandler,
                                                            eNode.getAttributeValue(TO_A)));
                            }
                            else {
                                if (eNode.getName().equals(DECISION_E)) {
                                    Element eSwitch = eNode.getChild(DECISION_SWITCH_E, ns);
                                    List<String> transitions = new ArrayList<String>();
                                    for (Element e : (List<Element>) eSwitch.getChildren(DECISION_CASE_E, ns)) {
                                        transitions.add(e.getAttributeValue(TO_A));
                                    }
                                    transitions.add(eSwitch.getChild(DECISION_DEFAULT_E, ns).getAttributeValue(TO_A));

                                    String switchStatement = XmlUtils.prettyPrint(eSwitch).toString();
                                    def.addNode(new DecisionNodeDef(eNode.getAttributeValue(NAME_A), switchStatement, decisionHandlerClass,
                                                                    transitions));
                                }
                                else {
                                    if (ACTION_E.equals(eNode.getName())) {
                                        String[] transitions = new String[2];
                                        Element eActionConf = null;
                                        for (Element elem : (List<Element>) eNode.getChildren()) {
                                            if (ACTION_OK_E.equals(elem.getName())) {
                                                transitions[0] = elem.getAttributeValue(TO_A);
                                            }
                                            else {
                                                if (ACTION_ERROR_E.equals(elem.getName())) {
                                                    transitions[1] = elem.getAttributeValue(TO_A);
                                                }
                                                else {
                                                    if (SLA_INFO.equals(elem.getName()) || CREDENTIALS.equals(elem.getName())) {
                                                        continue;
                                                    }
                                                    else {
                                                        eActionConf = elem;
                                                        handleGlobal(ns, global, elem);
                                                        }
                                                }
                                            }
                                        }

                                        String credStr = eNode.getAttributeValue(CRED_A);
                                        String userRetryMaxStr = eNode.getAttributeValue(USER_RETRY_MAX_A);
                                        String userRetryIntervalStr = eNode.getAttributeValue(USER_RETRY_INTERVAL_A);

                                        String actionConf = XmlUtils.prettyPrint(eActionConf).toString();
                                        def.addNode(new ActionNodeDef(eNode.getAttributeValue(NAME_A), actionConf, actionHandlerClass,
                                                                      transitions[0], transitions[1], credStr,
                                                                      userRetryMaxStr, userRetryIntervalStr));
                                    }
                                    else {
                                        if (SLA_INFO.equals(eNode.getName()) || CREDENTIALS.equals(eNode.getName())) {
                                            // No operation is required
                                        }
                                        else {
                                            if (eNode.getName().equals(GLOBAL)) {
                                                global = eNode;
                                            }
                                            else {
                                                if (eNode.getName().equals(PARAMETERS)) {
                                                    // No operation is required
                                                }
                                                else {
                                                    throw new WorkflowException(ErrorCode.E0703, eNode.getName());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return def;
    }

    /**
     * Validate workflow xml
     *
     * @param app
     * @param node
     * @param traversed
     * @throws WorkflowException
     */
    private void validate(LiteWorkflowApp app, NodeDef node, Map<String, VisitStatus> traversed) throws WorkflowException {
        if (node instanceof StartNodeDef) {
            startNode = (StartNodeDef) node;
        }
        else {
            try {
                ParamChecker.validateActionName(node.getName());
            }
            catch (IllegalArgumentException ex) {
                throw new WorkflowException(ErrorCode.E0724, ex.getMessage());
            }
        }
        if (node instanceof ActionNodeDef) {
            try {
                Element action = XmlUtils.parseXml(node.getConf());
                boolean supportedAction = Services.get().get(ActionService.class).getExecutor(action.getName()) != null;
                if (!supportedAction) {
                    throw new WorkflowException(ErrorCode.E0723, node.getName(), action.getName());
                }
            }
            catch (JDOMException ex) {
                throw new RuntimeException("It should never happen, " + ex.getMessage(), ex);
            }
        }

        if(node instanceof ForkNodeDef){
            forkList.add(node.getName());
        }

        if(node instanceof JoinNodeDef){
            joinList.add(node.getName());
        }

        if (node instanceof EndNodeDef) {
            traversed.put(node.getName(), VisitStatus.VISITED);
            return;
        }
        if (node instanceof KillNodeDef) {
            traversed.put(node.getName(), VisitStatus.VISITED);
            return;
        }
        for (String transition : node.getTransitions()) {

            if (app.getNode(transition) == null) {
                throw new WorkflowException(ErrorCode.E0708, node.getName(), transition);
            }

            //check if it is a cycle
            if (traversed.get(app.getNode(transition).getName()) == VisitStatus.VISITING) {
                throw new WorkflowException(ErrorCode.E0707, app.getNode(transition).getName());
            }
            //ignore validated one
            if (traversed.get(app.getNode(transition).getName()) == VisitStatus.VISITED) {
                continue;
            }

            traversed.put(app.getNode(transition).getName(), VisitStatus.VISITING);
            validate(app, app.getNode(transition), traversed);
        }
        traversed.put(node.getName(), VisitStatus.VISITED);
    }

    /**
     * Handle the global section
     *
     * @param ns
     * @param global
     * @param eActionConf
     * @throws WorkflowException
     */

    private void handleGlobal(Namespace ns, Element global, Element eActionConf) throws WorkflowException {

        // Use the action's namespace when getting children of the action (will be different than ns for extension actions)
        Namespace actionNs = eActionConf.getNamespace();

        if (global != null) {
            Element globalJobTracker = global.getChild("job-tracker", ns);
            Element globalNameNode = global.getChild("name-node", ns);
            List<Element> globalJobXml = global.getChildren("job-xml", ns);
            Element globalConfiguration = global.getChild("configuration", ns);

            if (globalJobTracker != null && eActionConf.getChild("job-tracker", actionNs) == null) {
                Element jobTracker = new Element("job-tracker", actionNs);
                jobTracker.setText(globalJobTracker.getText());
                eActionConf.addContent(jobTracker);
            }

            if (globalNameNode != null && eActionConf.getChild("name-node", actionNs) == null) {
                Element nameNode = new Element("name-node", actionNs);
                nameNode.setText(globalNameNode.getText());
                eActionConf.addContent(nameNode);
            }

            if (!globalJobXml.isEmpty()) {
                List<Element> actionJobXml = eActionConf.getChildren("job-xml", actionNs);
                for(Element jobXml: globalJobXml){
                    boolean alreadyExists = false;
                    for(Element actionXml: actionJobXml){
                        if(jobXml.getText().equals(actionXml.getText())){
                            alreadyExists = true;
                        }
                    }

                    if (!alreadyExists){
                        Element ejobXml = new Element("job-xml", actionNs);
                        ejobXml.setText(jobXml.getText());
                        eActionConf.addContent(ejobXml);
                    }

                }
            }

            if (globalConfiguration != null) {
                Element actionConfiguration = eActionConf.getChild("configuration", actionNs);
                if (actionConfiguration == null) {
                    actionConfiguration = new Element("configuration", actionNs);
                    eActionConf.addContent(actionConfiguration);
                }
                for (Element globalConfig : (List<Element>) globalConfiguration.getChildren()) {
                    boolean isSet = false;
                    String globalVarName = globalConfig.getChildText("name", ns);
                    for (Element local : (List<Element>) actionConfiguration.getChildren()) {
                        if (local.getChildText("name", actionNs).equals(globalVarName)) {
                            isSet = true;
                        }
                    }
                    if (!isSet) {
                        Element varToCopy = new Element("property", actionNs);
                        Element varName = new Element("name", actionNs);
                        Element varValue = new Element("value", actionNs);

                        varName.setText(globalConfig.getChildText("name", ns));
                        varValue.setText(globalConfig.getChildText("value", ns));

                        varToCopy.addContent(varName);
                        varToCopy.addContent(varValue);

                        actionConfiguration.addContent(varToCopy);
                    }
                }
            }
        }
        else {
            ActionExecutor ae = Services.get().get(ActionService.class).getExecutor(eActionConf.getName());
            if (ae == null) {
                throw new WorkflowException(ErrorCode.E0723, eActionConf.getName(), ActionService.class.getName());
            }
            if (ae.requiresNNJT) {

                if (eActionConf.getChild("name-node", actionNs) == null) {
                    throw new WorkflowException(ErrorCode.E0701, "No name-node defined");
                }
                if (eActionConf.getChild("job-tracker", actionNs) == null) {
                    throw new WorkflowException(ErrorCode.E0701, "No job-tracker defined");
                }
            }
        }
    }

}
