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

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.hadoop.FsActionExecutor;
import org.apache.oozie.action.oozie.SubWorkflowActionExecutor;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.SchemaService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.ELUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.ParameterVerifier;
import org.apache.oozie.util.ParameterVerifierException;
import org.apache.oozie.util.WritableUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.workflow.WorkflowException;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.xml.sax.SAXException;

import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.Validator;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

/**
 * Class to parse and validate workflow xml
 */
public class LiteWorkflowAppParser {

    private static final String LAUNCHER_E = "launcher";
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
    private static final String USER_RETRY_POLICY_A = "retry-policy";

    private static final String FORK_PATH_E = "path";
    private static final String FORK_START_A = "start";

    private static final String ACTION_OK_E = "ok";
    private static final String ACTION_ERROR_E = "error";

    private static final String DECISION_SWITCH_E = "switch";
    private static final String DECISION_CASE_E = "case";
    private static final String DECISION_DEFAULT_E = "default";

    private static final String SUBWORKFLOW_E = "sub-workflow";

    private static final String KILL_MESSAGE_E = "message";
    public static final String VALIDATE_FORK_JOIN = "oozie.validate.ForkJoin";
    public static final String WF_VALIDATE_FORK_JOIN = "oozie.wf.validate.ForkJoin";

    public static final String DEFAULT_NAME_NODE = "oozie.actions.default.name-node";
    public static final String DEFAULT_JOB_TRACKER = "oozie.actions.default.job-tracker";
    public static final String DEFAULT_RESOURCE_MANAGER = "oozie.actions.default.resource-manager";
    public static final String OOZIE_GLOBAL = "oozie.wf.globalconf";

    private static final String JOB_TRACKER = "job-tracker";
    private static final String RESOURCE_MANAGER = "resource-manager";

    private static final String NAME_NODE = "name-node";
    private static final String JOB_XML = "job-xml";
    private static final String CONFIGURATION = "configuration";

    private Schema schema;
    private Class<? extends ControlNodeHandler> controlNodeHandler;
    private Class<? extends DecisionNodeHandler> decisionHandlerClass;
    private Class<? extends ActionNodeHandler> actionHandlerClass;

    private String defaultNameNode;
    private String defaultResourceManager;
    private String defaultJobTracker;
    private boolean isResourceManagerTagUsed;

    public LiteWorkflowAppParser(Schema schema,
                                 Class<? extends ControlNodeHandler> controlNodeHandler,
                                 Class<? extends DecisionNodeHandler> decisionHandlerClass,
                                 Class<? extends ActionNodeHandler> actionHandlerClass) throws WorkflowException {
        this.schema = schema;
        this.controlNodeHandler = controlNodeHandler;
        this.decisionHandlerClass = decisionHandlerClass;
        this.actionHandlerClass = actionHandlerClass;

        defaultNameNode = getPropertyFromConfig(DEFAULT_NAME_NODE);
        defaultResourceManager = getPropertyFromConfig(DEFAULT_RESOURCE_MANAGER);
        defaultJobTracker = getPropertyFromConfig(DEFAULT_JOB_TRACKER);
    }

    private String getPropertyFromConfig(final String configPropertyKey) {
        String property = ConfigurationService.get(configPropertyKey);
        if (property != null) {
            property = property.trim();
            if (property.isEmpty()) {
                property = null;
            }
        }

        return property;
    }

    public LiteWorkflowApp validateAndParse(Reader reader, Configuration jobConf) throws WorkflowException {
        return validateAndParse(reader, jobConf, null);
    }

    /**
     * Parse and validate xml to {@link LiteWorkflowApp}
     *
     * @param reader
     * @return LiteWorkflowApp
     * @throws WorkflowException
     */
    public LiteWorkflowApp validateAndParse(Reader reader, Configuration jobConf, Configuration configDefault)
            throws WorkflowException {
        try {
            StringWriter writer = new StringWriter();
            IOUtils.copyCharStream(reader, writer);
            String strDef = writer.toString();

            if (schema != null) {
                Validator validator = SchemaService.getValidator(schema);
                validator.validate(new StreamSource(new StringReader(strDef)));
            }

            Element wfDefElement = XmlUtils.parseXml(strDef);
            ParameterVerifier.verifyParameters(jobConf, wfDefElement);
            LiteWorkflowApp app = parse(strDef, wfDefElement, configDefault, jobConf);


            boolean validateForkJoin = false;

            if (jobConf.getBoolean(WF_VALIDATE_FORK_JOIN, true)
                    && ConfigurationService.getBoolean(VALIDATE_FORK_JOIN)) {
                validateForkJoin = true;
            }

            LiteWorkflowValidator validator = new LiteWorkflowValidator();
            validator.validateWorkflow(app, validateForkJoin);

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
     * Parse xml to {@link LiteWorkflowApp}
     *
     * @param strDef
     * @param root
     * @param configDefault
     * @param jobConf
     * @return LiteWorkflowApp
     * @throws WorkflowException
     */
    @SuppressWarnings({"unchecked"})
    private LiteWorkflowApp parse(String strDef, Element root, Configuration configDefault, Configuration jobConf)
            throws WorkflowException {
        Namespace ns = root.getNamespace();

        LiteWorkflowApp def = null;
        GlobalSectionData gData = jobConf.get(OOZIE_GLOBAL) == null ?
                null : getGlobalFromString(jobConf.get(OOZIE_GLOBAL));
        boolean serializedGlobalConf = false;
        for (Element eNode : (List<Element>) root.getChildren()) {
            if (eNode.getName().equals(START_E)) {
                def = new LiteWorkflowApp(root.getAttributeValue(NAME_A), strDef,
                                          new StartNodeDef(controlNodeHandler, eNode.getAttributeValue(TO_A)));
            } else if (eNode.getName().equals(END_E)) {
                def.addNode(new EndNodeDef(eNode.getAttributeValue(NAME_A), controlNodeHandler));
            } else if (eNode.getName().equals(KILL_E)) {
                def.addNode(new KillNodeDef(eNode.getAttributeValue(NAME_A),
                                            eNode.getChildText(KILL_MESSAGE_E, ns), controlNodeHandler));
            } else if (eNode.getName().equals(FORK_E)) {
                List<String> paths = new ArrayList<String>();
                for (Element tran : (List<Element>) eNode.getChildren(FORK_PATH_E, ns)) {
                    paths.add(tran.getAttributeValue(FORK_START_A));
                }
                def.addNode(new ForkNodeDef(eNode.getAttributeValue(NAME_A), controlNodeHandler, paths));
            } else if (eNode.getName().equals(JOIN_E)) {
                def.addNode(new JoinNodeDef(eNode.getAttributeValue(NAME_A), controlNodeHandler, eNode.getAttributeValue(TO_A)));
            } else if (eNode.getName().equals(DECISION_E)) {
                Element eSwitch = eNode.getChild(DECISION_SWITCH_E, ns);
                List<String> transitions = new ArrayList<String>();
                for (Element e : (List<Element>) eSwitch.getChildren(DECISION_CASE_E, ns)) {
                    transitions.add(e.getAttributeValue(TO_A));
                }
                transitions.add(eSwitch.getChild(DECISION_DEFAULT_E, ns).getAttributeValue(TO_A));

                String switchStatement = XmlUtils.prettyPrint(eSwitch).toString();
                def.addNode(new DecisionNodeDef(eNode.getAttributeValue(NAME_A), switchStatement, decisionHandlerClass,
                                                transitions));
            } else if (ACTION_E.equals(eNode.getName())) {
                String[] transitions = new String[2];
                Element eActionConf = null;
                for (Element elem : (List<Element>) eNode.getChildren()) {
                    if (ACTION_OK_E.equals(elem.getName())) {
                        transitions[0] = elem.getAttributeValue(TO_A);
                    } else if (ACTION_ERROR_E.equals(elem.getName())) {
                        transitions[1] = elem.getAttributeValue(TO_A);
                    } else if (SLA_INFO.equals(elem.getName()) || CREDENTIALS.equals(elem.getName())) {
                        continue;
                    } else {
                        if (!serializedGlobalConf && elem.getName().equals(SubWorkflowActionExecutor.ACTION_TYPE) &&
                                elem.getChild(("propagate-configuration"), ns) != null && gData != null) {
                            serializedGlobalConf = true;
                            jobConf.set(OOZIE_GLOBAL, getGlobalString(gData));
                        }
                        eActionConf = elem;
                        if (SUBWORKFLOW_E.equals(elem.getName())) {
                            handleDefaultsAndGlobal(gData, null, elem, ns);
                        }
                        else {
                            handleDefaultsAndGlobal(gData, configDefault, elem, ns);
                        }
                    }
                }

                String credStr = eNode.getAttributeValue(CRED_A);
                String userRetryMaxStr = eNode.getAttributeValue(USER_RETRY_MAX_A);
                String userRetryIntervalStr = eNode.getAttributeValue(USER_RETRY_INTERVAL_A);
                String userRetryPolicyStr = eNode.getAttributeValue(USER_RETRY_POLICY_A);
                try {
                    if (!StringUtils.isEmpty(userRetryMaxStr)) {
                        userRetryMaxStr = ELUtils.resolveAppName(userRetryMaxStr, jobConf);
                    }
                    if (!StringUtils.isEmpty(userRetryIntervalStr)) {
                        userRetryIntervalStr = ELUtils.resolveAppName(userRetryIntervalStr, jobConf);
                    }
                    if (!StringUtils.isEmpty(userRetryPolicyStr)) {
                        userRetryPolicyStr = ELUtils.resolveAppName(userRetryPolicyStr, jobConf);
                    }
                }
                catch (Exception e) {
                    throw new WorkflowException(ErrorCode.E0703, e.getMessage());
                }

                String actionConf = XmlUtils.prettyPrint(eActionConf).toString();
                def.addNode(new ActionNodeDef(eNode.getAttributeValue(NAME_A), actionConf, actionHandlerClass,
                        transitions[0], transitions[1], credStr, userRetryMaxStr, userRetryIntervalStr,
                        userRetryPolicyStr));
            } else if (SLA_INFO.equals(eNode.getName()) || CREDENTIALS.equals(eNode.getName())) {
                // No operation is required
            } else if (eNode.getName().equals(GLOBAL)) {
                if(jobConf.get(OOZIE_GLOBAL) != null) {
                    gData = getGlobalFromString(jobConf.get(OOZIE_GLOBAL));
                    handleDefaultsAndGlobal(gData, null, eNode, ns);
                }

                gData = parseGlobalSection(ns, eNode);

            } else if (eNode.getName().equals(PARAMETERS)) {
                // No operation is required
            } else {
                throw new WorkflowException(ErrorCode.E0703, eNode.getName());
            }
        }
        return def;
    }

    /**
     * Read the GlobalSectionData from Base64 string.
     * @param globalStr
     * @return GlobalSectionData
     * @throws WorkflowException
     */
    private GlobalSectionData getGlobalFromString(String globalStr) throws WorkflowException {
        GlobalSectionData globalSectionData = new GlobalSectionData();
        try {
            byte[] data = Base64.decodeBase64(globalStr);
            Inflater inflater = new Inflater();
            DataInputStream ois = new DataInputStream(new InflaterInputStream(new ByteArrayInputStream(data), inflater));
            globalSectionData.readFields(ois);
            ois.close();
        } catch (Exception ex) {
            throw new WorkflowException(ErrorCode.E0700, "Error while processing global section conf");
        }
        return globalSectionData;
    }


    /**
     * Write the GlobalSectionData to a Base64 string.
     * @param globalSectionData
     * @return String
     * @throws WorkflowException
     */
    private String getGlobalString(GlobalSectionData globalSectionData) throws WorkflowException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream oos = null;
        try {
            Deflater def = new Deflater();
            oos = new DataOutputStream(new DeflaterOutputStream(baos, def));
            globalSectionData.write(oos);
            oos.close();
        } catch (IOException e) {
            throw new WorkflowException(ErrorCode.E0700, "Error while processing global section conf");
        }
        return Base64.encodeBase64String(baos.toByteArray());
    }

    private void addChildElement(Element parent, Namespace ns, String childName, String childValue) {
        Element child = new Element(childName, ns);
        child.setText(childValue);
        parent.addContent(child);
    }

    private class GlobalSectionData implements Writable {
        String jobTracker;
        String nameNode;
        List<String> jobXmls;
        Configuration conf;

        public GlobalSectionData() {
        }

        public GlobalSectionData(String jobTracker, String nameNode, List<String> jobXmls, Configuration conf) {
            this.jobTracker = jobTracker;
            this.nameNode = nameNode;
            this.jobXmls = jobXmls;
            this.conf = conf;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            WritableUtils.writeStr(dataOutput, jobTracker);
            WritableUtils.writeStr(dataOutput, nameNode);

            if(jobXmls != null && !jobXmls.isEmpty()) {
                dataOutput.writeInt(jobXmls.size());
                for (String content : jobXmls) {
                    WritableUtils.writeStr(dataOutput, content);
                }
            } else {
                dataOutput.writeInt(0);
            }
            if(conf != null) {
                WritableUtils.writeStr(dataOutput, XmlUtils.prettyPrint(conf).toString());
            } else {
                WritableUtils.writeStr(dataOutput, null);
            }
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            jobTracker = WritableUtils.readStr(dataInput);
            nameNode = WritableUtils.readStr(dataInput);
            int length = dataInput.readInt();
            if (length > 0) {
                jobXmls = new ArrayList<String>();
                for (int i = 0; i < length; i++) {
                    jobXmls.add(WritableUtils.readStr(dataInput));
                }
            }
            String confString = WritableUtils.readStr(dataInput);
            if(confString != null) {
                conf = new XConfiguration(new StringReader(confString));
            }
        }
    }

    private GlobalSectionData parseGlobalSection(Namespace ns, Element global) throws WorkflowException {
        GlobalSectionData gData = null;
        if (global != null) {
            String globalJobTracker = null;
            Element globalJobTrackerElement = getResourceManager(ns, global);
            isResourceManagerTagUsed = globalJobTrackerElement != null
                    && globalJobTrackerElement.getName().equals(RESOURCE_MANAGER);
            if (globalJobTrackerElement != null) {
                globalJobTracker = globalJobTrackerElement.getValue();
            }


            String globalNameNode = null;
            Element globalNameNodeElement = global.getChild(NAME_NODE, ns);
            if (globalNameNodeElement != null) {
                globalNameNode = globalNameNodeElement.getValue();
            }

            List<String> globalJobXmls = null;
            @SuppressWarnings("unchecked")
            List<Element> globalJobXmlElements = global.getChildren(JOB_XML, ns);
            if (!globalJobXmlElements.isEmpty()) {
                globalJobXmls = new ArrayList<String>(globalJobXmlElements.size());
                for(Element jobXmlElement: globalJobXmlElements) {
                    globalJobXmls.add(jobXmlElement.getText());
                }
            }

            Configuration globalConf = new XConfiguration();
            Element globalConfigurationElement = global.getChild(CONFIGURATION, ns);
            if (globalConfigurationElement != null) {
                try {
                    globalConf = new XConfiguration(new StringReader(XmlUtils.prettyPrint(globalConfigurationElement).toString()));
                } catch (IOException ioe) {
                    throw new WorkflowException(ErrorCode.E0700, "Error while processing global section conf");
                }
            }

            Element globalLauncherElement = global.getChild(LAUNCHER_E, ns);
            if (globalLauncherElement != null) {
                LauncherConfigHandler launcherConfigHandler = new LauncherConfigHandler(globalConf, globalLauncherElement, ns);
                launcherConfigHandler.processSettings();
            }
            gData = new GlobalSectionData(globalJobTracker, globalNameNode, globalJobXmls, globalConf);
        }
        return gData;
    }

    private Element getResourceManager(final Namespace ns, final Element element) {
        final Element resourceManager = element.getChild(RESOURCE_MANAGER, ns);
        if (resourceManager != null) {
            return resourceManager;
        }
        return element.getChild(JOB_TRACKER, ns);
    }

    private void handleDefaultsAndGlobal(GlobalSectionData gData, Configuration configDefault, Element actionElement, Namespace ns)
            throws WorkflowException {

        ActionExecutor ae = Services.get().get(ActionService.class).getExecutor(actionElement.getName());
        if (ae == null && !GLOBAL.equals(actionElement.getName())) {
            throw new WorkflowException(ErrorCode.E0723, actionElement.getName(), ActionService.class.getName());
        }

        Namespace actionNs = actionElement.getNamespace();

        // If this is the global section or ActionExecutor.requiresNameNodeJobTracker() returns true, we parse the action's
        // <name-node> and <job-tracker> fields.  If those aren't defined, we take them from the <global> section.  If those
        // aren't defined, we take them from the oozie-site defaults.  If those aren't defined, we throw a WorkflowException.
        // However, for the SubWorkflow and FS Actions, as well as the <global> section, we don't throw the WorkflowException.
        // Also, we only parse the NN (not the JT) for the FS Action.
        if (SubWorkflowActionExecutor.ACTION_TYPE.equals(actionElement.getName()) ||
                FsActionExecutor.ACTION_TYPE.equals(actionElement.getName()) ||
                GLOBAL.equals(actionElement.getName()) || ae.requiresNameNodeJobTracker()) {
            if (actionElement.getChild(NAME_NODE, actionNs) == null) {
                if (gData != null && gData.nameNode != null) {
                    addChildElement(actionElement, actionNs, NAME_NODE, gData.nameNode);
                } else if (defaultNameNode != null) {
                    addChildElement(actionElement, actionNs, NAME_NODE, defaultNameNode);
                } else if (!(SubWorkflowActionExecutor.ACTION_TYPE.equals(actionElement.getName()) ||
                        FsActionExecutor.ACTION_TYPE.equals(actionElement.getName()) ||
                        GLOBAL.equals(actionElement.getName()))) {
                    throw new WorkflowException(ErrorCode.E0701, "No " + NAME_NODE + " defined");
                }
            }
            if (getResourceManager(actionNs, actionElement) == null &&
                    !FsActionExecutor.ACTION_TYPE.equals(actionElement.getName())) {
                if (gData != null && gData.jobTracker != null) {
                    addResourceManagerOrJobTracker(actionElement, actionNs, gData.jobTracker, isResourceManagerTagUsed);
                } else if (defaultResourceManager != null) {
                    addResourceManagerOrJobTracker(actionElement, actionNs, defaultResourceManager, true);
                } else if (defaultJobTracker != null) {
                    addResourceManagerOrJobTracker(actionElement, actionNs, defaultJobTracker, false);
                } else if (!(SubWorkflowActionExecutor.ACTION_TYPE.equals(actionElement.getName()) ||
                        GLOBAL.equals(actionElement.getName()))) {
                    throw new WorkflowException(ErrorCode.E0701, "No " + JOB_TRACKER + " or " + RESOURCE_MANAGER + " defined");
                }
            }
        }

        // If this is the global section or ActionExecutor.supportsConfigurationJobXML() returns true, we parse the action's
        // <configuration> and <job-xml> fields.  We also merge this with those from the <global> section, if given.  If none are
        // defined, empty values are placed.  Exceptions are thrown if there's an error parsing, but not if they're not given.
        if (GLOBAL.equals(actionElement.getName()) || ae.supportsConfigurationJobXML()) {
            @SuppressWarnings("unchecked")
            List<Element> actionJobXmls = actionElement.getChildren(JOB_XML, actionNs);
            if (gData != null && gData.jobXmls != null) {
                for(String gJobXml : gData.jobXmls) {
                    boolean alreadyExists = false;
                    for (Element actionXml : actionJobXmls) {
                        if (gJobXml.equals(actionXml.getText())) {
                            alreadyExists = true;
                            break;
                        }
                    }
                    if (!alreadyExists) {
                        Element ejobXml = new Element(JOB_XML, actionNs);
                        ejobXml.setText(gJobXml);
                        actionElement.addContent(ejobXml);
                    }
                }
            }

            try {
                XConfiguration actionConf = new XConfiguration();
                if (configDefault != null)
                    XConfiguration.copy(configDefault, actionConf);
                if (gData != null && gData.conf != null) {
                    XConfiguration.copy(gData.conf, actionConf);
                }

                Element launcherConfiguration = actionElement.getChild(LAUNCHER_E, actionNs);
                if (launcherConfiguration != null) {
                    LauncherConfigHandler launcherConfigHandler = new LauncherConfigHandler(
                            actionConf, launcherConfiguration, actionNs);
                    launcherConfigHandler.processSettings();
                }

                Element actionConfiguration = actionElement.getChild(CONFIGURATION, actionNs);
                if (actionConfiguration != null) {
                    //copy and override
                    XConfiguration.copy(new XConfiguration(new StringReader(XmlUtils.prettyPrint(actionConfiguration).toString())),
                            actionConf);
                }

                int position = actionElement.indexOf(actionConfiguration);
                actionElement.removeContent(actionConfiguration); //replace with enhanced one
                Element eConfXml = XmlUtils.parseXml(actionConf.toXmlString(false));
                eConfXml.detach();
                eConfXml.setNamespace(actionNs);
                if (position > 0) {
                    actionElement.addContent(position, eConfXml);
                }
                else {
                    actionElement.addContent(eConfXml);
                }
            }
            catch (IOException e) {
                throw new WorkflowException(ErrorCode.E0700, "Error while processing action conf");
            }
            catch (JDOMException e) {
                throw new WorkflowException(ErrorCode.E0700, "Error while processing action conf");
            }
        }
    }

    private void addResourceManagerOrJobTracker(final Element actionElement, final Namespace actionNs, final String jobTracker,
                                                boolean isResourceManagerUsed) {
        if (isResourceManagerUsed) {
            addChildElement(actionElement, actionNs, RESOURCE_MANAGER, jobTracker);
        } else {
            addChildElement(actionElement, actionNs, JOB_TRACKER, jobTracker);
        }
    }

    private void addResourceManager(final Element actionElement, final Namespace actionNs, final String jobTracker) {
            addChildElement(actionElement, actionNs, RESOURCE_MANAGER, jobTracker);
    }
}