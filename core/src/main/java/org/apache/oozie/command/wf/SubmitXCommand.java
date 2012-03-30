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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.WorkflowStoreService;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.DagXLogInfoService;
import org.apache.oozie.util.ConfigUtils;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.WorkflowJobInsertJPAExecutor;
import org.apache.oozie.service.ELService;
import org.apache.oozie.service.SchemaService;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.workflow.WorkflowApp;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.WorkflowLib;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.InstrumentUtils;
import org.apache.oozie.util.PropertiesUtils;
import org.apache.oozie.util.db.SLADbOperations;
import org.apache.oozie.service.SchemaService.SchemaName;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.SLAEvent.SlaAppType;
import org.jdom.Element;
import org.jdom.Namespace;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.io.IOException;
import java.net.URI;

public class SubmitXCommand extends WorkflowXCommand<String> {
    public static final String CONFIG_DEFAULT = "config-default.xml";

    private Configuration conf;
    private String authToken;

    public SubmitXCommand(Configuration conf, String authToken) {
        super("submit", "submit", 1);
        this.conf = ParamChecker.notNull(conf, "conf");
        this.authToken = ParamChecker.notEmpty(authToken, "authToken");
    }

    private static final Set<String> DISALLOWED_DEFAULT_PROPERTIES = new HashSet<String>();
    private static final Set<String> DISALLOWED_USER_PROPERTIES = new HashSet<String>();

    static {
        String[] badUserProps = {PropertiesUtils.DAYS, PropertiesUtils.HOURS, PropertiesUtils.MINUTES,
                PropertiesUtils.KB, PropertiesUtils.MB, PropertiesUtils.GB, PropertiesUtils.TB, PropertiesUtils.PB,
                PropertiesUtils.RECORDS, PropertiesUtils.MAP_IN, PropertiesUtils.MAP_OUT, PropertiesUtils.REDUCE_IN,
                PropertiesUtils.REDUCE_OUT, PropertiesUtils.GROUPS};
        PropertiesUtils.createPropertySet(badUserProps, DISALLOWED_USER_PROPERTIES);

        String[] badDefaultProps = {PropertiesUtils.HADOOP_USER};
        PropertiesUtils.createPropertySet(badUserProps, DISALLOWED_DEFAULT_PROPERTIES);
        PropertiesUtils.createPropertySet(badDefaultProps, DISALLOWED_DEFAULT_PROPERTIES);
    }

    @Override
    protected String execute() throws CommandException {
        InstrumentUtils.incrJobCounter(getName(), 1, getInstrumentation());
        WorkflowAppService wps = Services.get().get(WorkflowAppService.class);
        try {
            XLog.Info.get().setParameter(DagXLogInfoService.TOKEN, conf.get(OozieClient.LOG_TOKEN));
            WorkflowApp app = wps.parseDef(conf, authToken);
            XConfiguration protoActionConf = wps.createProtoActionConf(conf, authToken, true);
            WorkflowLib workflowLib = Services.get().get(WorkflowStoreService.class).getWorkflowLibWithNoDB();

            String user = conf.get(OozieClient.USER_NAME);
            String group = ConfigUtils.getWithDeprecatedCheck(conf, OozieClient.JOB_ACL, OozieClient.GROUP_NAME, null);
            URI uri = new URI(conf.get(OozieClient.APP_PATH));
            HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
            Configuration fsConf = has.createJobConf(uri.getAuthority());
            FileSystem fs = has.createFileSystem(user, uri, fsConf);

            Path configDefault = null;
            // app path could be a directory
            Path path = new Path(uri.getPath());
            if (!fs.isFile(path)) {
                configDefault = new Path(path, CONFIG_DEFAULT);
            } else {
                configDefault = new Path(path.getParent(), CONFIG_DEFAULT);
            }

            if (fs.exists(configDefault)) {
                try {
                    Configuration defaultConf = new XConfiguration(fs.open(configDefault));
                    PropertiesUtils.checkDisallowedProperties(defaultConf, DISALLOWED_DEFAULT_PROPERTIES);
                    XConfiguration.injectDefaults(defaultConf, conf);
                }
                catch (IOException ex) {
                    throw new IOException("default configuration file, " + ex.getMessage(), ex);
                }
            }

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
            // System.out.println("WF INSTANCE CONF:");
            // System.out.println(XmlUtils.prettyPrint(conf).toString());

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
            workflow.setAuthToken(authToken);
            workflow.setWorkflowInstance(wfInstance);
            workflow.setExternalId(conf.get(OozieClient.EXTERNAL_ID));

            LogUtils.setLogInfo(workflow, logInfo);
            LOG = XLog.resetPrefix(LOG);
            LOG.debug("Workflow record created, Status [{0}]", workflow.getStatus());
            Element wfElem = XmlUtils.parseXml(app.getDefinition());
            ELEvaluator evalSla = createELEvaluatorForGroup(conf, "wf-sla-submit");
            String jobSlaXml = verifySlaElements(wfElem, evalSla);
            writeSLARegistration(jobSlaXml, workflow.getId(), workflow.getUser(), workflow.getGroup(), LOG);
            workflow.setSlaXml(jobSlaXml);
            // System.out.println("SlaXml :"+ slaXml);

            //store.insertWorkflow(workflow);
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
        catch (HadoopAccessorException ex) {
            throw new CommandException(ex);
        }
        catch (Exception ex) {
            throw new CommandException(ErrorCode.E0803, ex);
        }
    }

    private String verifySlaElements(Element eWfJob, ELEvaluator evalSla) throws CommandException {
        String jobSlaXml = "";
        // Validate WF job
        Element eSla = eWfJob.getChild("info", Namespace.getNamespace(SchemaService.SLA_NAME_SPACE_URI));
        if (eSla != null) {
            jobSlaXml = resolveSla(eSla, evalSla);
        }

        // Validate all actions
        for (Element action : (List<Element>) eWfJob.getChildren("action", eWfJob.getNamespace())) {
            eSla = action.getChild("info", Namespace.getNamespace(SchemaService.SLA_NAME_SPACE_URI));
            if (eSla != null) {
                resolveSla(eSla, evalSla);
            }
        }
        return jobSlaXml;
    }

    private void writeSLARegistration(String slaXml, String id, String user, String group, XLog log)
            throws CommandException {
        try {
            if (slaXml != null && slaXml.length() > 0) {
                Element eSla = XmlUtils.parseXml(slaXml);
                SLADbOperations.writeSlaRegistrationEvent(eSla, id, SlaAppType.WORKFLOW_JOB, user, group, log);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new CommandException(ErrorCode.E1007, "workflow " + id, e);
        }
    }

    /**
     * Resolve variables in sla xml element.
     *
     * @param eSla sla xml element
     * @param evalSla sla evaluator
     * @return sla xml string after evaluation
     * @throws CommandException
     */
    public static String resolveSla(Element eSla, ELEvaluator evalSla) throws CommandException {
        // EL evaluation
        String slaXml = XmlUtils.prettyPrint(eSla).toString();
        try {
            slaXml = XmlUtils.removeComments(slaXml);
            slaXml = evalSla.evaluate(slaXml, String.class);
            XmlUtils.validateData(slaXml, SchemaName.SLA_ORIGINAL);
            return slaXml;
        }
        catch (Exception e) {
            throw new CommandException(ErrorCode.E1004, "Validation erro :" + e.getMessage(), e);
        }
    }

    /**
     * Create an EL evaluator for a given group.
     *
     * @param conf configuration variable
     * @param group group variable
     * @return the evaluator created for the group
     */
    public static ELEvaluator createELEvaluatorForGroup(Configuration conf, String group) {
        ELEvaluator eval = Services.get().get(ELService.class).createEvaluator(group);
        for (Map.Entry<String, String> entry : conf) {
            eval.setVariable(entry.getKey(), entry.getValue());
        }
        return eval;
    }

    @Override
    public String getEntityKey() {
        return null;
    }

    @Override
    protected boolean isLockRequired() {
        return false;
    }

    @Override
    protected void loadState() {

    }

    @Override
    protected void verifyPrecondition() throws CommandException {

    }

}
