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
import org.apache.oozie.ErrorCode;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.ActionService;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

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

    private Schema schema;
    private Class<? extends DecisionNodeHandler> decisionHandlerClass;
    private Class<? extends ActionNodeHandler> actionHandlerClass;

    private static enum VisitStatus {
        VISITING, VISITED
    }

    private List<String> forkList = new ArrayList<String>();
    private List<String> joinList = new ArrayList<String>();

    public LiteWorkflowAppParser(Schema schema, Class<? extends DecisionNodeHandler> decisionHandlerClass,
                                 Class<? extends ActionNodeHandler> actionHandlerClass) throws WorkflowException {
        this.schema = schema;
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
    public LiteWorkflowApp validateAndParse(Reader reader) throws WorkflowException {
        try {
            StringWriter writer = new StringWriter();
            IOUtils.copyCharStream(reader, writer);
            String strDef = writer.toString();

            if (schema != null) {
                Validator validator = schema.newValidator();
                validator.validate(new StreamSource(new StringReader(strDef)));
            }

            Element wfDefElement = XmlUtils.parseXml(strDef);
            LiteWorkflowApp app = parse(strDef, wfDefElement);
            Map<String, VisitStatus> traversed = new HashMap<String, VisitStatus>();
            traversed.put(app.getNode(StartNodeDef.START).getName(), VisitStatus.VISITING);
            validate(app, app.getNode(StartNodeDef.START), traversed);
            //Validate whether fork/join are in pair or not
            if (Services.get().getConf().getBoolean(VALIDATE_FORK_JOIN, true)) {
                validateForkJoin(app);
            }
            return app;
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

        while(!forkList.isEmpty()){
            // Make sure each of the fork node has a corresponding join; start with the root fork Node first
            validateFork(app.getNode(forkList.remove(0)), app);
        }

    }

    /*
     * Test whether the fork node has a corresponding join
     * @param node - the fork node
     * @param app - the WorkflowApp
     * @return
     * @throws WorkflowException
     */
    private NodeDef validateFork(NodeDef forkNode, LiteWorkflowApp app) throws WorkflowException {
        List<String> transitions = new ArrayList<String>(forkNode.getTransitions());
        // list for keeping track of "error-to" transitions of Action Node
        List<String> errorToTransitions = new ArrayList<String>();
        String joinNode = null;
        for (int i = 0; i < transitions.size(); i++) {
            NodeDef node = app.getNode(transitions.get(i));
            if (node instanceof DecisionNodeDef) {
                Set<String> decisionSet = new HashSet<String>(node.getTransitions());
                for (String ds : decisionSet) {
                    if (transitions.contains(ds)) {
                        throw new WorkflowException(ErrorCode.E0734, node.getName(), ds);
                    } else {
                        transitions.add(ds);
                    }
                }
            } else if (node instanceof ActionNodeDef) {
                // Make sure the transition is valid
                validateTransition(errorToTransitions, transitions, app, node);
                // Add the "ok-to" transition of node
                transitions.add(node.getTransitions().get(0));
                String errorTo = node.getTransitions().get(1);
                // Add the "error-to" transition if the transition is a Action Node
                if (app.getNode(errorTo) instanceof ActionNodeDef) {
                    errorToTransitions.add(errorTo);
                }
            } else if (node instanceof ForkNodeDef) {
                forkList.remove(node.getName());
                // Make a recursive call to resolve this fork node
                NodeDef joinNd = validateFork(node, app);
                // Make sure the transition is valid
                validateTransition(errorToTransitions, transitions, app, node);
                // Add the "ok-to" transition of node
                transitions.add(joinNd.getTransitions().get(0));
            } else if (node instanceof JoinNodeDef) {
                // If joinNode encountered for the first time, remove it from the joinList and remember it
                String currentJoin = node.getName();
                if (joinList.contains(currentJoin)) {
                    joinList.remove(currentJoin);
                    joinNode = currentJoin;
                } else {
                    // Make sure this join is the same as the join seen from the first time
                    if (joinNode == null) {
                        throw new WorkflowException(ErrorCode.E0733, forkNode);
                    }
                    if (!joinNode.equals(currentJoin)) {
                        throw new WorkflowException(ErrorCode.E0732, forkNode, joinNode);
                    }
                }
            } else {
                throw new WorkflowException(ErrorCode.E0730);
            }
        }
        return app.getNode(joinNode);

    }

    private void validateTransition(List<String> errorToTransitions, List<String> transitions, LiteWorkflowApp app, NodeDef node)
            throws WorkflowException {
        for (String transition : node.getTransitions()) {
            // Make sure the transition node is either a join node or is not already visited
            if (transitions.contains(transition) && !(app.getNode(transition) instanceof JoinNodeDef)) {
                throw new WorkflowException(ErrorCode.E0734, node.getName(), transition);
            }
            // Make sure the transition node is not the same as an already visited 'error-to' transition
            if (errorToTransitions.contains(transition)) {
                throw new WorkflowException(ErrorCode.E0735, node.getName(), transition);
            }
        }

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
        for (Element eNode : (List<Element>) root.getChildren()) {
            if (eNode.getName().equals(START_E)) {
                def = new LiteWorkflowApp(root.getAttributeValue(NAME_A), strDef,
                                          new StartNodeDef(eNode.getAttributeValue(TO_A)));
            }
            else {
                if (eNode.getName().equals(END_E)) {
                    def.addNode(new EndNodeDef(eNode.getAttributeValue(NAME_A)));
                }
                else {
                    if (eNode.getName().equals(KILL_E)) {
                        def.addNode(new KillNodeDef(eNode.getAttributeValue(NAME_A), eNode.getChildText(KILL_MESSAGE_E, ns)));
                    }
                    else {
                        if (eNode.getName().equals(FORK_E)) {
                            List<String> paths = new ArrayList<String>();
                            for (Element tran : (List<Element>) eNode.getChildren(FORK_PATH_E, ns)) {
                                paths.add(tran.getAttributeValue(FORK_START_A));
                            }
                            def.addNode(new ForkNodeDef(eNode.getAttributeValue(NAME_A), paths));
                        }
                        else {
                            if (eNode.getName().equals(JOIN_E)) {
                                def.addNode(new JoinNodeDef(eNode.getAttributeValue(NAME_A), eNode.getAttributeValue(TO_A)));
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
        if (!(node instanceof StartNodeDef)) {
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
}
