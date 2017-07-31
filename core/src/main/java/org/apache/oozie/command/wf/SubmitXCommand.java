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
import org.apache.oozie.AppType;
import org.apache.oozie.SLAEventBean;
import org.apache.oozie.command.wf.ActionXCommand.ActionExecutorContext;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.action.oozie.SubWorkflowActionExecutor;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.service.WorkflowStoreService;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.DagXLogInfoService;
import org.apache.oozie.util.ELUtils;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.sla.SLAOperations;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.ELService;
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
import org.apache.oozie.client.rest.JsonBean;
import org.jdom.Element;
import org.jdom.filter.ElementFilter;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.io.IOException;
import java.net.URI;

@SuppressWarnings("deprecation")
public class SubmitXCommand extends WorkflowXCommand<String> {
    public static final String CONFIG_DEFAULT = "config-default.xml";

    private Configuration conf;
    private List<JsonBean> insertList = new ArrayList<JsonBean>();
    private String parentId;

    /**
     * Constructor to create the workflow Submit Command.
     *
     * @param conf : Configuration for workflow job
     */
    public SubmitXCommand(Configuration conf) {
        super("submit", "submit", 1);
        this.conf = ParamChecker.notNull(conf, "conf");
    }

    /**
     * Constructor for submitting wf through coordinator
     *
     * @param conf : Configuration for workflow job
     * @param parentId the coord action id
     */
    public SubmitXCommand(Configuration conf, String parentId) {
        this(conf);
        this.parentId = parentId;
    }

    /**
     * Constructor to create the workflow Submit Command.
     *
     * @param dryrun : if dryrun
     * @param conf : Configuration for workflow job
     */
    public SubmitXCommand(boolean dryrun, Configuration conf) {
        this(conf);
        this.dryrun = dryrun;
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
            String user = conf.get(OozieClient.USER_NAME);
            URI uri = new URI(conf.get(OozieClient.APP_PATH));
            HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
            Configuration fsConf = has.createConfiguration(uri.getAuthority());
            FileSystem fs = has.createFileSystem(user, uri, fsConf);

            Path configDefault = null;
            Configuration defaultConf = null;
            // app path could be a directory
            Path path = new Path(uri.getPath());
            if (!fs.isFile(path)) {
                configDefault = new Path(path, CONFIG_DEFAULT);
            } else {
                configDefault = new Path(path.getParent(), CONFIG_DEFAULT);
            }

            if (fs.exists(configDefault)) {
                try {
                    defaultConf = new XConfiguration(fs.open(configDefault));
                    PropertiesUtils.checkDisallowedProperties(defaultConf, DISALLOWED_DEFAULT_PROPERTIES);
                    XConfiguration.injectDefaults(defaultConf, conf);
                }
                catch (IOException ex) {
                    throw new IOException("default configuration file, " + ex.getMessage(), ex);
                }
            }
            if (defaultConf != null) {
                defaultConf = resolveDefaultConfVariables(defaultConf);
            }

            WorkflowApp app = wps.parseDef(conf, defaultConf);
            XConfiguration protoActionConf = wps.createProtoActionConf(conf, true);
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
            // System.out.println("WF INSTANCE CONF:");
            // System.out.println(XmlUtils.prettyPrint(conf).toString());

            WorkflowJobBean workflow = new WorkflowJobBean();
            workflow.setId(wfInstance.getId());
            workflow.setAppName(ELUtils.resolveAppName(app.getName(), conf));
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
            // Set parent id if it doesn't already have one (for subworkflows)
            if (workflow.getParentId() == null) {
                workflow.setParentId(conf.get(SubWorkflowActionExecutor.PARENT_ID));
            }
            // Set to coord action Id if workflow submitted through coordinator
            if (workflow.getParentId() == null) {
                workflow.setParentId(parentId);
            }

            LogUtils.setLogInfo(workflow);
            LOG.debug("Workflow record created, Status [{0}]", workflow.getStatus());
            Element wfElem = XmlUtils.parseXml(app.getDefinition());
            ELEvaluator evalSla = createELEvaluatorForGroup(conf, "wf-sla-submit");
            String jobSlaXml = verifySlaElements(wfElem, evalSla);
            if (!dryrun) {
                writeSLARegistration(wfElem, jobSlaXml, workflow.getId(), workflow.getParentId(), workflow.getUser(),
                        workflow.getGroup(), workflow.getAppName(), LOG, evalSla);
                workflow.setSlaXml(jobSlaXml);
                // System.out.println("SlaXml :"+ slaXml);

                //store.insertWorkflow(workflow);
                insertList.add(workflow);
                JPAService jpaService = Services.get().get(JPAService.class);
                if (jpaService != null) {
                    try {
                        BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(insertList, null, null);
                    }
                    catch (JPAExecutorException je) {
                        throw new CommandException(je);
                    }
                }
                else {
                    LOG.error(ErrorCode.E0610);
                    return null;
                }

                return workflow.getId();
            }
            else {
                // Checking variable substitution for dryrun
                ActionExecutorContext context = new ActionXCommand.ActionExecutorContext(workflow, null, false, false);
                Element workflowXml = XmlUtils.parseXml(app.getDefinition());
                removeSlaElements(workflowXml);
                String workflowXmlString = XmlUtils.removeComments(XmlUtils.prettyPrint(workflowXml).toString());
                workflowXmlString = context.getELEvaluator().evaluate(workflowXmlString, String.class);
                workflowXml = XmlUtils.parseXml(workflowXmlString);

                Iterator<Element> it = workflowXml.getDescendants(new ElementFilter("job-xml"));

                // Checking all variable substitutions in job-xml files
                while (it.hasNext()) {
                    Element e = it.next();
                    String jobXml = e.getTextTrim();
                    Path xmlPath = new Path(workflow.getAppPath(), jobXml);
                    Configuration jobXmlConf = new XConfiguration(fs.open(xmlPath));


                    String jobXmlConfString = XmlUtils.prettyPrint(jobXmlConf).toString();
                    jobXmlConfString = XmlUtils.removeComments(jobXmlConfString);
                    context.getELEvaluator().evaluate(jobXmlConfString, String.class);
                }

                return "OK";
            }
        }
        catch (WorkflowException ex) {
            throw new CommandException(ex);
        }
        catch (HadoopAccessorException ex) {
            throw new CommandException(ex);
        }
        catch (Exception ex) {
            throw new CommandException(ErrorCode.E0803, ex.getMessage(), ex);
        }
    }

    /**
     * Resolving variables from config-default, which might be referencing into conf/defaultConf
     * @param defaultConf config-default.xml
     * @return resolved config-default configuration.
     */
    private Configuration resolveDefaultConfVariables(Configuration defaultConf) {
        XConfiguration resolveDefaultConf = new XConfiguration();
        for (Map.Entry<String, String> entry : defaultConf) {
            String defaultConfKey = entry.getKey();
            String defaultConfValue = entry.getValue();
            // if value is referencing some other key, first check within the default config to resolve,
            // then job.properties (conf)
            if (defaultConfValue.contains("$") && defaultConf.get(defaultConfKey).contains("$")) {
                resolveDefaultConf.set(defaultConfKey, conf.get(defaultConfKey));
            } else {
                resolveDefaultConf.set(defaultConfKey, defaultConf.get(defaultConfKey));
            }
        }
        return resolveDefaultConf;
    }

    private void removeSlaElements(Element eWfJob) {
        Element sla = XmlUtils.getSLAElement(eWfJob);
        if (sla != null) {
            eWfJob.removeChildren(sla.getName(), sla.getNamespace());
        }

        for (Element action : (List<Element>) eWfJob.getChildren("action", eWfJob.getNamespace())) {
            sla = XmlUtils.getSLAElement(action);
            if (sla != null) {
                action.removeChildren(sla.getName(), sla.getNamespace());
            }
        }
    }
    private String verifySlaElements(Element eWfJob, ELEvaluator evalSla) throws CommandException {
        String jobSlaXml = "";
        // Validate WF job
        Element eSla = XmlUtils.getSLAElement(eWfJob);
        if (eSla != null) {
            jobSlaXml = resolveSla(eSla, evalSla);
        }

        // Validate all actions
        for (Element action : (List<Element>) eWfJob.getChildren("action", eWfJob.getNamespace())) {
            eSla = XmlUtils.getSLAElement(action);
            if (eSla != null) {
                resolveSla(eSla, evalSla);
            }
        }
        return jobSlaXml;
    }

    private void writeSLARegistration(Element eWfJob, String slaXml, String jobId, String parentId, String user,
            String group, String appName, XLog log, ELEvaluator evalSla) throws CommandException {
        try {
            if (slaXml != null && slaXml.length() > 0) {
                Element eSla = XmlUtils.parseXml(slaXml);
                SLAEventBean slaEvent = SLADbOperations.createSlaRegistrationEvent(eSla, jobId,
                        SlaAppType.WORKFLOW_JOB, user, group, log);
                if(slaEvent != null) {
                    insertList.add(slaEvent);
                }
                // insert into new table
                SLAOperations.createSlaRegistrationEvent(eSla, jobId, parentId, AppType.WORKFLOW_JOB, user, appName,
                        log, false);
            }
            // Add sla for wf actions
            for (Element action : (List<Element>) eWfJob.getChildren("action", eWfJob.getNamespace())) {
                Element actionSla = XmlUtils.getSLAElement(action);
                if (actionSla != null) {
                    String actionSlaXml = SubmitXCommand.resolveSla(actionSla, evalSla);
                    actionSla = XmlUtils.parseXml(actionSlaXml);
                    String actionId = Services.get().get(UUIDService.class)
                            .generateChildId(jobId, action.getAttributeValue("name") + "");
                    SLAOperations.createSlaRegistrationEvent(actionSla, actionId, jobId, AppType.WORKFLOW_ACTION,
                            user, appName, log, false);
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new CommandException(ErrorCode.E1007, "workflow " + jobId, e.getMessage(), e);
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
            throw new CommandException(ErrorCode.E1004, "Validation error :" + e.getMessage(), e);
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
