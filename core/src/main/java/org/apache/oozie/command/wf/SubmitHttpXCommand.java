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

package org.apache.oozie.command.wf;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.WorkflowStoreService;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.DagXLogInfoService;
import org.apache.oozie.util.InstrumentUtils;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.WorkflowJobInsertJPAExecutor;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.workflow.WorkflowApp;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.WorkflowLib;
import org.apache.oozie.util.PropertiesUtils;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.XOozieClient;
import org.jdom.Element;
import org.jdom.Namespace;

import com.google.common.collect.ImmutableSet;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

public abstract class SubmitHttpXCommand extends WorkflowXCommand<String> {

    static final Set<String> MANDATORY_OOZIE_CONFS = ImmutableSet.of(XOozieClient.RM, XOozieClient.NN, OozieClient.LIBPATH);
    static final Set<String> OPTIONAL_OOZIE_CONFS = ImmutableSet.of(XOozieClient.FILES, XOozieClient.ARCHIVES);

    private Configuration conf;

    public SubmitHttpXCommand(String name, String type, Configuration conf) {
        super(name, type, 1);
        this.conf = ParamChecker.notNull(conf, "conf");
    }

    private static final Set<String> DISALLOWED_DEFAULT_PROPERTIES = new HashSet<String>();
    private static final Set<String> DISALLOWED_USER_PROPERTIES = new HashSet<String>();

    static {
        String[] badUserProps = { PropertiesUtils.DAYS, PropertiesUtils.HOURS, PropertiesUtils.MINUTES,
                PropertiesUtils.KB, PropertiesUtils.MB, PropertiesUtils.GB, PropertiesUtils.TB, PropertiesUtils.PB,
                PropertiesUtils.RECORDS, PropertiesUtils.MAP_IN, PropertiesUtils.MAP_OUT, PropertiesUtils.REDUCE_IN,
                PropertiesUtils.REDUCE_OUT, PropertiesUtils.GROUPS };
        PropertiesUtils.createPropertySet(badUserProps, DISALLOWED_USER_PROPERTIES);

        String[] badDefaultProps = { PropertiesUtils.HADOOP_USER};
        PropertiesUtils.createPropertySet(badUserProps, DISALLOWED_DEFAULT_PROPERTIES);
        PropertiesUtils.createPropertySet(badDefaultProps, DISALLOWED_DEFAULT_PROPERTIES);
    }

    abstract protected Element generateSection(Configuration conf, Namespace ns);

    abstract protected Namespace getSectionNamespace();

    abstract protected String getWorkflowName();

    protected void checkMandatoryConf(Configuration conf) {
        for (String key : MANDATORY_OOZIE_CONFS) {
            String value = conf.get(key);
            if (value == null) {
                throw new RuntimeException(key + " is not specified");
            }
        }
    }

    protected Namespace getWorkflowNamespace() {
        return Namespace.getNamespace("uri:oozie:workflow:0.2");
    }
    /**
     * Generate workflow xml from conf object
     *
     * @param conf the configuration object
     * @return workflow xml def string representation
     */
    protected String getWorkflowXml(Configuration conf) {
        checkMandatoryConf(conf);

        Namespace ns = getWorkflowNamespace();
        Element root = new Element("workflow-app", ns);
        String name = getWorkflowName();
        root.setAttribute("name", "oozie-" + name);

        Element start = new Element("start", ns);
        String nodeName = name + "1";
        start.setAttribute("to", nodeName);
        root.addContent(start);

        Element action = new Element("action", ns);
        action.setAttribute("name", nodeName);

        Element ele = generateSection(conf, getSectionNamespace());
        action.addContent(ele);

        Element ok = new Element("ok", ns);
        ok.setAttribute("to", "end");
        action.addContent(ok);

        Element error = new Element("error", ns);
        error.setAttribute("to", "fail");
        action.addContent(error);

        root.addContent(action);

        Element kill = new Element("kill", ns);
        kill.setAttribute("name", "fail");
        Element message = new Element("message", ns);
        message.addContent(name + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]");
        kill.addContent(message);
        root.addContent(kill);

        Element end = new Element("end", ns);
        end.setAttribute("name", "end");
        root.addContent(end);

        return XmlUtils.prettyPrint(root).toString();
    };

    protected Element generateConfigurationSection(List<String> Dargs, Namespace ns) {
        Element configuration = new Element("configuration", ns);
        for (String arg : Dargs) {
            String name = null, value = null;
            int pos = arg.indexOf("=");
            if (pos == -1) { // "-D<name>" or "-D" only
                name = arg.substring(2, arg.length());
                value = "";
            }
            else { // "-D<name>=<value>"
                name = arg.substring(2, pos);
                value = arg.substring(pos + 1, arg.length());
            }

            Element property = new Element("property", ns);
            Element nameElement = new Element("name", ns);
            nameElement.addContent(name);
            property.addContent(nameElement);
            Element valueElement = new Element("value", ns);
            valueElement.addContent(value);
            property.addContent(valueElement);
            configuration.addContent(property);
        }

        return configuration;
    }

    @Override
    protected String execute() throws CommandException {
        InstrumentUtils.incrJobCounter(getName(), 1, getInstrumentation());
        WorkflowAppService wps = Services.get().get(WorkflowAppService.class);
        try {
            XLog.Info.get().setParameter(DagXLogInfoService.TOKEN, conf.get(OozieClient.LOG_TOKEN));
            String wfXml = getWorkflowXml(conf);
            LOG.debug("workflow xml created on the server side is :\n");
            LOG.debug(wfXml);
            WorkflowApp app = wps.parseDef(wfXml, conf);
            XConfiguration protoActionConf = wps.createProtoActionConf(conf, false);
            WorkflowLib workflowLib = Services.get().get(WorkflowStoreService.class).getWorkflowLibWithNoDB();

            PropertiesUtils.checkDisallowedProperties(conf, DISALLOWED_USER_PROPERTIES);

            // Resolving all variables in the job properties.
            // This ensures the Hadoop Configuration semantics is preserved.
            XConfiguration resolvedVarsConf = new XConfiguration();
            for (Map.Entry<String, String> entry : conf) {
                resolvedVarsConf.set(entry.getKey(), conf.get(entry.getKey()));
            }
            conf = resolvedVarsConf;

            WorkflowInstance wfInstance;
            try {
                wfInstance = workflowLib.createInstance(app, conf);
            }
            catch (WorkflowException e) {
                throw new StoreException(e);
            }

            Configuration conf = wfInstance.getConf();

            WorkflowJobBean workflow = new WorkflowJobBean();
            workflow.setId(wfInstance.getId());
            workflow.setAppName(app.getName());
            workflow.setAppPath(conf.get(OozieClient.APP_PATH));
            workflow.setConf(XmlUtils.prettyPrint(conf).toString());
            workflow.setProtoActionConf(protoActionConf.toXmlString());
            workflow.setCreatedTime(new Date());
            workflow.setLastModifiedTime(new Date());
            workflow.setLogToken(conf.get(OozieClient.LOG_TOKEN, ""));
            workflow.setStatus(WorkflowJob.Status.PREP);
            workflow.setRun(0);
            workflow.setUser(conf.get(OozieClient.USER_NAME));
            workflow.setGroup(conf.get(OozieClient.GROUP_NAME));
            workflow.setWorkflowInstance(wfInstance);
            workflow.setExternalId(conf.get(OozieClient.EXTERNAL_ID));

            LogUtils.setLogInfo(workflow);
            JPAService jpaService = Services.get().get(JPAService.class);
            if (jpaService != null) {
                jpaService.execute(new WorkflowJobInsertJPAExecutor(workflow));
            }
            else {
                LOG.error(ErrorCode.E0610);
                return null;
            }

            return workflow.getId();
        }
        catch (WorkflowException ex) {
            throw new CommandException(ex);
        }
        catch (Exception ex) {
            throw new CommandException(ErrorCode.E0803, ex.getMessage(), ex);
        }
    }

    static private void addSection(Element X, Namespace ns, String filesStr, String tagName) {
        if (filesStr != null) {
            String[] files = filesStr.split(",");
            for (String f : files) {
                Element tagElement = new Element(tagName, ns);
                if (f.contains("#")) {
                    tagElement.addContent(f);
                }
                else {
                    String filename = f.substring(f.lastIndexOf("/") + 1, f.length());
                    if (filename == null || filename.isEmpty()) {
                        tagElement.addContent(f);
                    }
                    else {
                        tagElement.addContent(f + "#" + filename);
                    }
                }
                X.addContent(tagElement);
            }
        }
    }

    /**
     * Add file section in X.
     *
     * @param parent XML element to be appended
     * @param conf Configuration object
     * @param ns XML element namespace
     */
    static void addFileSection(Element X, Configuration conf, Namespace ns) {
        String filesStr = conf.get(XOozieClient.FILES);
        addSection(X, ns, filesStr, "file");
    }

    /**
     * Add archive section in X.
     *
     * @param parent XML element to be appended
     * @param conf Configuration object
     * @param ns XML element namespace
     */
    static void addArchiveSection(Element X, Configuration conf, Namespace ns) {
        String archivesStr = conf.get(XOozieClient.ARCHIVES);
        addSection(X, ns, archivesStr, "archive");
    }
}
