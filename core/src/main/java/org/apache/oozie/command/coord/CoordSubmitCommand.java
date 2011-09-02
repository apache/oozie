/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.command.coord;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Validator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.CoordinatorJob.Execution;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.coord.CoordELEvaluator;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.coord.CoordUtils;
import org.apache.oozie.coord.CoordinatorJobException;
import org.apache.oozie.coord.TimeUnit;
import org.apache.oozie.service.DagXLogInfoService;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.SchemaService;
import org.apache.oozie.service.Service;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.service.SchemaService.SchemaName;
import org.apache.oozie.service.UUIDService.ApplicationType;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.PropertiesUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.workflow.WorkflowException;
import org.jdom.Attribute;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.xml.sax.SAXException;

/**
 * This class provides the functionalities to resolve a coordinator job XML and write the job information into a DB
 * table. <p/> Specifically it performs the following functions: 1. Resolve all the variables or properties using job
 * configurations. 2. Insert all datasets definition as part of the <data-in> and <data-out> tags. 3. Validate the XML
 * at runtime.
 */
public class CoordSubmitCommand extends CoordinatorCommand<String> {

    private Configuration conf;
    private String authToken;
    private boolean dryrun;

    public static final String CONFIG_DEFAULT = "coord-config-default.xml";
    public static final String COORDINATOR_XML_FILE = "coordinator.xml";

    private static final Set<String> DISALLOWED_USER_PROPERTIES = new HashSet<String>();
    private static final Set<String> DISALLOWED_DEFAULT_PROPERTIES = new HashSet<String>();
    /**
     * Default timeout for normal jobs, in minutes, after which coordinator input check will timeout
     */
    public static final String CONF_DEFAULT_TIMEOUT_NORMAL = Service.CONF_PREFIX + "coord.normal.default.timeout";

    private XLog log = XLog.getLog(getClass());
    private ELEvaluator evalFreq = null;
    private ELEvaluator evalNofuncs = null;
    private ELEvaluator evalData = null;
    private ELEvaluator evalInst = null;
    private ELEvaluator evalSla = null;

    static {
        String[] badUserProps = {PropertiesUtils.YEAR, PropertiesUtils.MONTH, PropertiesUtils.DAY,
                PropertiesUtils.HOUR, PropertiesUtils.MINUTE, PropertiesUtils.DAYS, PropertiesUtils.HOURS,
                PropertiesUtils.MINUTES, PropertiesUtils.KB, PropertiesUtils.MB, PropertiesUtils.GB,
                PropertiesUtils.TB, PropertiesUtils.PB, PropertiesUtils.RECORDS, PropertiesUtils.MAP_IN,
                PropertiesUtils.MAP_OUT, PropertiesUtils.REDUCE_IN, PropertiesUtils.REDUCE_OUT, PropertiesUtils.GROUPS};
        PropertiesUtils.createPropertySet(badUserProps, DISALLOWED_USER_PROPERTIES);

        String[] badDefaultProps = {PropertiesUtils.HADOOP_USER, PropertiesUtils.HADOOP_UGI,
                WorkflowAppService.HADOOP_JT_KERBEROS_NAME, WorkflowAppService.HADOOP_NN_KERBEROS_NAME};
        PropertiesUtils.createPropertySet(badUserProps, DISALLOWED_DEFAULT_PROPERTIES);
        PropertiesUtils.createPropertySet(badDefaultProps, DISALLOWED_DEFAULT_PROPERTIES);
    }

    /**
     * Constructor to create the Coordinator Submit Command.
     *
     * @param conf : Configuration for Coordinator job
     * @param authToken : To be used for authentication
     */
    public CoordSubmitCommand(Configuration conf, String authToken) {
        super("coord_submit", "coord_submit", 1, XLog.STD);
        this.conf = ParamChecker.notNull(conf, "conf");
        this.authToken = ParamChecker.notEmpty(authToken, "authToken");
    }

    public CoordSubmitCommand(boolean dryrun, Configuration conf, String authToken) {
        super("coord_submit", "coord_submit", 1, XLog.STD, dryrun);
        this.conf = ParamChecker.notNull(conf, "conf");
        this.authToken = ParamChecker.notEmpty(authToken, "authToken");
        this.dryrun = dryrun;
        // TODO Auto-generated constructor stub
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.oozie.command.Command#call(org.apache.oozie.store.Store)
     */
    @Override
    protected String call(CoordinatorStore store) throws StoreException, CommandException {
        String jobId = null;
        log.info("STARTED Coordinator Submit");
        incrJobCounter(1);
        CoordinatorJobBean coordJob = new CoordinatorJobBean();
        try {
            XLog.Info.get().setParameter(DagXLogInfoService.TOKEN, conf.get(OozieClient.LOG_TOKEN));
            mergeDefaultConfig();

            String appXml = readAndValidateXml();
            coordJob.setOrigJobXml(appXml);
            log.debug("jobXml after initial validation " + XmlUtils.prettyPrint(appXml).toString());
            appXml = XmlUtils.removeComments(appXml);
            initEvaluators();
            Element eJob = basicResolveAndIncludeDS(appXml, conf, coordJob);
            log.debug("jobXml after all validation " + XmlUtils.prettyPrint(eJob).toString());

            jobId = storeToDB(eJob, store, coordJob);
            // log JOB info for coordinator jobs
            setLogInfo(coordJob);
            log = XLog.getLog(getClass());

            if (!dryrun) {
                // submit a command to materialize jobs for the next 1 hour (3600 secs)
                // so we don't wait 10 mins for the Service to run.
                queueCallable(new CoordJobMatLookupCommand(jobId, 3600), 100);
            }
            else {
                Date startTime = coordJob.getStartTime();
                long startTimeMilli = startTime.getTime();
                long endTimeMilli = startTimeMilli + (3600 * 1000);
                Date jobEndTime = coordJob.getEndTime();
                Date endTime = new Date(endTimeMilli);
                if (endTime.compareTo(jobEndTime) > 0) {
                    endTime = jobEndTime;
                }
                jobId = coordJob.getId();
                log.info("[" + jobId + "]: Update status to PREMATER");
                coordJob.setStatus(CoordinatorJob.Status.PREMATER);
                CoordActionMaterializeCommand coordActionMatCom = new CoordActionMaterializeCommand(jobId, startTime,
                                                                                                    endTime);
                Configuration jobConf = null;
                try {
                    jobConf = new XConfiguration(new StringReader(coordJob.getConf()));
                }
                catch (IOException e1) {
                    log.warn("Configuration parse error. read from DB :" + coordJob.getConf(), e1);
                }
                String action = coordActionMatCom.materializeJobs(true, coordJob, jobConf, null);
                String output = coordJob.getJobXml() + System.getProperty("line.separator")
                        + "***actions for instance***" + action;
                return output;
            }
        }
        catch (CoordinatorJobException ex) {
            log.warn("ERROR:  ", ex);
            throw new CommandException(ex);
        }
        catch (IllegalArgumentException iex) {
            log.warn("ERROR:  ", iex);
            throw new CommandException(ErrorCode.E1003, iex);
        }
        catch (Exception ex) {// TODO
            log.warn("ERROR:  ", ex);
            throw new CommandException(ErrorCode.E0803, ex);
        }
        log.info("ENDED Coordinator Submit jobId=" + jobId);
        return jobId;
    }

    /**
     * Read the application XML and validate against coordinator Schema
     *
     * @return validated coordinator XML
     * @throws CoordinatorJobException
     */
    private String readAndValidateXml() throws CoordinatorJobException {
        String appPath = ParamChecker.notEmpty(conf.get(OozieClient.COORDINATOR_APP_PATH),
                                               OozieClient.COORDINATOR_APP_PATH);// TODO: COORDINATOR_APP_PATH
        String coordXml = readDefinition(appPath);
        validateXml(coordXml);
        return coordXml;
    }

    /**
     * Validate against Coordinator XSD file
     *
     * @param xmlContent : Input coordinator xml
     * @throws CoordinatorJobException
     */
    private void validateXml(String xmlContent) throws CoordinatorJobException {
        javax.xml.validation.Schema schema = Services.get().get(SchemaService.class).getSchema(SchemaName.COORDINATOR);
        Validator validator = schema.newValidator();
        // log.warn("XML " + xmlContent);
        try {
            validator.validate(new StreamSource(new StringReader(xmlContent)));
        }
        catch (SAXException ex) {
            log.warn("SAXException :", ex);
            throw new CoordinatorJobException(ErrorCode.E0701, ex.getMessage(), ex);
        }
        catch (IOException ex) {
            // ex.printStackTrace();
            log.warn("IOException :", ex);
            throw new CoordinatorJobException(ErrorCode.E0702, ex.getMessage(), ex);
        }
    }

    /**
     * Merge default configuration with user-defined configuration.
     *
     * @throws CommandException
     */
    protected void mergeDefaultConfig() throws CommandException {
        Path configDefault = new Path(conf.get(OozieClient.COORDINATOR_APP_PATH), CONFIG_DEFAULT);
        // Configuration fsConfig = new Configuration();
        // log.warn("CONFIG :" + configDefault.toUri());
        Configuration fsConfig = CoordUtils.getHadoopConf(conf);
        FileSystem fs;
        // TODO: which conf?
        try {
            String user = ParamChecker.notEmpty(conf.get(OozieClient.USER_NAME), OozieClient.USER_NAME);
            String group = ParamChecker.notEmpty(conf.get(OozieClient.GROUP_NAME), OozieClient.GROUP_NAME);
            fs = Services.get().get(HadoopAccessorService.class).createFileSystem(user, group, configDefault.toUri(),
                                                                                  new Configuration());
            if (fs.exists(configDefault)) {
                Configuration defaultConf = new XConfiguration(fs.open(configDefault));
                PropertiesUtils.checkDisallowedProperties(defaultConf, DISALLOWED_DEFAULT_PROPERTIES);
                XConfiguration.injectDefaults(defaultConf, conf);
            }
            else {
                log.info("configDefault Doesn't exist " + configDefault);
            }
            PropertiesUtils.checkDisallowedProperties(conf, DISALLOWED_USER_PROPERTIES);
        }
        catch (IOException e) {
            throw new CommandException(ErrorCode.E0702, e.getMessage() + " : Problem reading default config "
                    + configDefault, e);
        }
        catch (HadoopAccessorException e) {
            throw new CommandException(e);
        }
        log.debug("Merged CONF :" + XmlUtils.prettyPrint(conf).toString());
    }

    /**
     * The method resolve all the variables that are defined in configuration. It also include the data set definition
     * from dataset file into XML.
     *
     * @param appXml : Original job XML
     * @param conf : Configuration of the job
     * @param coordJob : Coordinator job bean to be populated.
     * @return : Resolved and modified job XML element.
     * @throws Exception
     */
    public Element basicResolveAndIncludeDS(String appXml, Configuration conf, CoordinatorJobBean coordJob)
            throws CoordinatorJobException, Exception {
        Element basicResolvedApp = resolveInitial(conf, appXml, coordJob);
        includeDataSets(basicResolvedApp, conf);
        return basicResolvedApp;
    }

    /**
     * Insert data set into data-in and data-out tags.
     *
     * @param eAppXml : coordinator application XML
     * @param eDatasets : DataSet XML
     * @return updated application
     */
    private void insertDataSet(Element eAppXml, Element eDatasets) {
        // Adding DS definition in the coordinator XML
        Element inputList = eAppXml.getChild("input-events", eAppXml.getNamespace());
        if (inputList != null) {
            for (Element dataIn : (List<Element>) inputList.getChildren("data-in", eAppXml.getNamespace())) {
                Element eDataset = findDataSet(eDatasets, dataIn.getAttributeValue("dataset"));
                dataIn.getContent().add(0, eDataset);
            }
        }
        Element outputList = eAppXml.getChild("output-events", eAppXml.getNamespace());
        if (outputList != null) {
            for (Element dataOut : (List<Element>) outputList.getChildren("data-out", eAppXml.getNamespace())) {
                Element eDataset = findDataSet(eDatasets, dataOut.getAttributeValue("dataset"));
                dataOut.getContent().add(0, eDataset);
            }
        }
    }

    /**
     * Find a specific dataset from a list of Datasets.
     *
     * @param eDatasets : List of data sets
     * @param name : queried data set name
     * @return one Dataset element. otherwise throw Exception
     */
    private static Element findDataSet(Element eDatasets, String name) {
        for (Element eDataset : (List<Element>) eDatasets.getChildren("dataset", eDatasets.getNamespace())) {
            if (eDataset.getAttributeValue("name").equals(name)) {
                eDataset = (Element) eDataset.clone();
                eDataset.detach();
                return eDataset;
            }
        }
        throw new RuntimeException("undefined dataset: " + name);
    }

    /**
     * Initialize all the required EL Evaluators.
     */
    protected void initEvaluators() {
        evalFreq = CoordELEvaluator.createELEvaluatorForGroup(conf, "coord-job-submit-freq");
        evalNofuncs = CoordELEvaluator.createELEvaluatorForGroup(conf, "coord-job-submit-nofuncs");
        evalInst = CoordELEvaluator.createELEvaluatorForGroup(conf, "coord-job-submit-instances");
        evalSla = CoordELEvaluator.createELEvaluatorForGroup(conf, "coord-sla-submit");
    }

    /**
     * Resolve basic entities using job Configuration.
     *
     * @param conf :Job configuration
     * @param appXml : Original job XML
     * @param coordJob : Coordinator job bean to be populated.
     * @return Resolved job XML element.
     * @throws Exception
     */
    protected Element resolveInitial(Configuration conf, String appXml, CoordinatorJobBean coordJob)
            throws CoordinatorJobException, Exception {
        Element eAppXml = XmlUtils.parseXml(appXml);
        // job's main attributes
        // frequency
        String val = resolveAttribute("frequency", eAppXml, evalFreq);
        int ival = ParamChecker.checkInteger(val, "frequency");
        ParamChecker.checkGTZero(ival, "frequency");
        coordJob.setFrequency(ival);
        TimeUnit tmp = (evalFreq.getVariable("timeunit") == null) ? TimeUnit.MINUTE : ((TimeUnit) evalFreq
                .getVariable("timeunit"));
        addAnAttribute("freq_timeunit", eAppXml, tmp.toString()); // TODO: Store
        // TimeUnit
        coordJob.setTimeUnit(CoordinatorJob.Timeunit.valueOf(tmp.toString()));
        // End Of Duration
        tmp = evalFreq.getVariable("endOfDuration") == null ? TimeUnit.NONE : ((TimeUnit) evalFreq
                .getVariable("endOfDuration"));
        addAnAttribute("end_of_duration", eAppXml, tmp.toString());
        // coordJob.setEndOfDuration(tmp) // TODO: Add new attribute in Job bean

        // start time
        val = resolveAttribute("start", eAppXml, evalNofuncs);
        ParamChecker.checkUTC(val, "start");
        coordJob.setStartTime(DateUtils.parseDateUTC(val));
        // end time
        val = resolveAttribute("end", eAppXml, evalNofuncs);
        ParamChecker.checkUTC(val, "end");
        coordJob.setEndTime(DateUtils.parseDateUTC(val));
        // Time zone
        val = resolveAttribute("timezone", eAppXml, evalNofuncs);
        ParamChecker.checkTimeZone(val, "timezone");
        coordJob.setTimeZone(val);

        // controls
        val = resolveTagContents("timeout", eAppXml.getChild("controls", eAppXml.getNamespace()), evalNofuncs);
        if (val == "") {
            val = Services.get().getConf().get(CONF_DEFAULT_TIMEOUT_NORMAL);
        }

        ival = ParamChecker.checkInteger(val, "timeout");
        // ParamChecker.checkGEZero(ival, "timeout");
        coordJob.setTimeout(ival);
        val = resolveTagContents("concurrency", eAppXml.getChild("controls", eAppXml.getNamespace()), evalNofuncs);
        if (val == "") {
            val = "-1";
        }
        ival = ParamChecker.checkInteger(val, "concurrency");
        // ParamChecker.checkGEZero(ival, "concurrency");
        coordJob.setConcurrency(ival);
        val = resolveTagContents("execution", eAppXml.getChild("controls", eAppXml.getNamespace()), evalNofuncs);
        if (val == "") {
            val = Execution.FIFO.toString();
        }
        coordJob.setExecution(Execution.valueOf(val));
        String[] acceptedVals = {Execution.LIFO.toString(), Execution.FIFO.toString(), Execution.LAST_ONLY.toString()};
        ParamChecker.isMember(val, acceptedVals, "execution");

        // datasets
        resolveTagContents("include", eAppXml.getChild("datasets", eAppXml.getNamespace()), evalNofuncs);
        // for each data set
        resolveDataSets(eAppXml);
        HashMap<String, String> dataNameList = new HashMap<String, String>();
        resolveIOEvents(eAppXml, dataNameList);

        resolveTagContents("app-path", eAppXml.getChild("action", eAppXml.getNamespace()).getChild("workflow",
                                                                                                   eAppXml.getNamespace()), evalNofuncs);
        // TODO: If action or workflow tag is missing, NullPointerException will
        // occur
        Element configElem = eAppXml.getChild("action", eAppXml.getNamespace()).getChild("workflow",
                                                                                         eAppXml.getNamespace()).getChild("configuration", eAppXml.getNamespace());
        evalData = CoordELEvaluator.createELEvaluatorForDataEcho(conf, "coord-job-submit-data", dataNameList);
        if (configElem != null) {
            for (Element propElem : (List<Element>) configElem.getChildren("property", configElem.getNamespace())) {
                resolveTagContents("name", propElem, evalData);
                // log.warn("Value :");
                // Want to check the data-integrity but don't want to modify the
                // XML
                // for properties only
                Element tmpProp = (Element) propElem.clone();
                resolveTagContents("value", tmpProp, evalData);
                // val = resolveTagContents("value", propElem, evalData);
                // log.warn("Value OK :" + val);
            }
        }
        resolveSLA(eAppXml, coordJob);
        return eAppXml;
    }

    private void resolveSLA(Element eAppXml, CoordinatorJobBean coordJob) throws CommandException {
        // String prefix = XmlUtils.getNamespacePrefix(eAppXml,
        // SchemaService.SLA_NAME_SPACE_URI);
        Element eSla = eAppXml.getChild("action", eAppXml.getNamespace()).getChild("info",
                                                                                   Namespace.getNamespace(SchemaService.SLA_NAME_SPACE_URI));

        if (eSla != null) {
            String slaXml = XmlUtils.prettyPrint(eSla).toString();
            try {
                // EL evaluation
                slaXml = evalSla.evaluate(slaXml, String.class);
                // Validate against semantic SXD
                XmlUtils.validateData(slaXml, SchemaName.SLA_ORIGINAL);
            }
            catch (Exception e) {
                throw new CommandException(ErrorCode.E1004, "Validation ERROR :" + e.getMessage(), e);
            }
        }
    }

    /**
     * Resolve input-events/data-in and output-events/data-out tags.
     *
     * @param eJob : Job element
     * @throws CoordinatorJobException
     */
    private void resolveIOEvents(Element eJobOrg, HashMap<String, String> dataNameList) throws CoordinatorJobException {
        // Resolving input-events/data-in
        // Clone the job and don't update anything in the original
        Element eJob = (Element) eJobOrg.clone();
        Element inputList = eJob.getChild("input-events", eJob.getNamespace());
        if (inputList != null) {
            TreeSet<String> eventNameSet = new TreeSet<String>();
            for (Element dataIn : (List<Element>) inputList.getChildren("data-in", eJob.getNamespace())) {
                String dataInName = dataIn.getAttributeValue("name");
                dataNameList.put(dataInName, "data-in");
                // check whether there is any duplicate data-in name
                if (eventNameSet.contains(dataInName)) {
                    throw new RuntimeException("Duplicate dataIn name " + dataInName);
                }
                else {
                    eventNameSet.add(dataInName);
                }
                resolveTagContents("instance", dataIn, evalInst);
                resolveTagContents("start-instance", dataIn, evalInst);
                resolveTagContents("end-instance", dataIn, evalInst);
            }
        }
        // Resolving output-events/data-out
        Element outputList = eJob.getChild("output-events", eJob.getNamespace());
        if (outputList != null) {
            TreeSet<String> eventNameSet = new TreeSet<String>();
            for (Element dataOut : (List<Element>) outputList.getChildren("data-out", eJob.getNamespace())) {
                String dataOutName = dataOut.getAttributeValue("name");
                dataNameList.put(dataOutName, "data-out");
                // check whether there is any duplicate data-out name
                if (eventNameSet.contains(dataOutName)) {
                    throw new RuntimeException("Duplicate dataIn name " + dataOutName);
                }
                else {
                    eventNameSet.add(dataOutName);
                }
                resolveTagContents("instance", dataOut, evalInst);
            }
        }

    }

    /**
     * Add an attribute into XML element.
     *
     * @param attrName :attribute name
     * @param elem : Element to add attribute
     * @param value :Value of attribute
     */
    private void addAnAttribute(String attrName, Element elem, String value) {
        elem.setAttribute(attrName, value);
    }

    /**
     * Resolve Data set using job configuration.
     *
     * @param eAppXml : Job Element XML
     * @throws Exception
     */
    private void resolveDataSets(Element eAppXml) throws Exception {
        Element datasetList = eAppXml.getChild("datasets", eAppXml.getNamespace());
        if (datasetList != null) {

            List<Element> dsElems = datasetList.getChildren("dataset", eAppXml.getNamespace());
            resolveDataSets(dsElems);
            resolveTagContents("app-path", eAppXml.getChild("action", eAppXml.getNamespace()).getChild("workflow",
                                                                                                       eAppXml.getNamespace()), evalNofuncs);
        }
    }

    /**
     * Resolve Data set using job configuration.
     *
     * @param dsElems : Data set XML element.
     * @throws CoordinatorJobException
     * @throws Exception
     */
    private void resolveDataSets(List<Element> dsElems) throws CoordinatorJobException /*
                                                                                        * throws
                                                                                        * Exception
                                                                                        */ {
        for (Element dsElem : dsElems) {
            // Setting up default TimeUnit and EndOFDuraion
            evalFreq.setVariable("timeunit", TimeUnit.MINUTE);
            evalFreq.setVariable("endOfDuration", TimeUnit.NONE);

            String val = resolveAttribute("frequency", dsElem, evalFreq);
            int ival = ParamChecker.checkInteger(val, "frequency");
            ParamChecker.checkGTZero(ival, "frequency");
            addAnAttribute("freq_timeunit", dsElem, evalFreq.getVariable("timeunit") == null ? TimeUnit.MINUTE
                    .toString() : ((TimeUnit) evalFreq.getVariable("timeunit")).toString());
            addAnAttribute("end_of_duration", dsElem, evalFreq.getVariable("endOfDuration") == null ? TimeUnit.NONE
                    .toString() : ((TimeUnit) evalFreq.getVariable("endOfDuration")).toString());
            val = resolveAttribute("initial-instance", dsElem, evalNofuncs);
            ParamChecker.checkUTC(val, "initial-instance");
            val = resolveAttribute("timezone", dsElem, evalNofuncs);
            ParamChecker.checkTimeZone(val, "timezone");
            resolveTagContents("uri-template", dsElem, evalNofuncs);
            resolveTagContents("done-flag", dsElem, evalNofuncs);
        }
    }

    /**
     * Resolve the content of a tag.
     *
     * @param tagName : Tag name of job XML i.e. <timeout> 10 </timeout>
     * @param elem : Element where the tag exists.
     * @param eval :
     * @return Resolved tag content.
     * @throws CoordinatorJobException
     */
    private String resolveTagContents(String tagName, Element elem, ELEvaluator eval) throws CoordinatorJobException {
        String ret = "";
        if (elem != null) {
            for (Element tagElem : (List<Element>) elem.getChildren(tagName, elem.getNamespace())) {
                if (tagElem != null) {
                    String updated;
                    try {
                        updated = CoordELFunctions.evalAndWrap(eval, tagElem.getText().trim());

                    }
                    catch (Exception e) {
                        // e.printStackTrace();
                        throw new CoordinatorJobException(ErrorCode.E1004, e.getMessage(), e);
                    }
                    tagElem.removeContent();
                    tagElem.addContent(updated);
                    ret += updated;
                }
                /*
                 * else { //TODO: unlike event }
                 */
            }
        }
        return ret;
    }

    /**
     * Resolve an attribute value.
     *
     * @param attrName : Attribute name.
     * @param elem : XML Element where attribute is defiend
     * @param eval : ELEvaluator used to resolve
     * @return Resolved attribute value
     * @throws CoordinatorJobException
     */
    private String resolveAttribute(String attrName, Element elem, ELEvaluator eval) throws CoordinatorJobException {
        Attribute attr = elem.getAttribute(attrName);
        String val = null;
        if (attr != null) {
            try {
                val = CoordELFunctions.evalAndWrap(eval, attr.getValue().trim());

            }
            catch (Exception e) {
                // e.printStackTrace();
                throw new CoordinatorJobException(ErrorCode.E1004, e.getMessage(), e);
            }
            attr.setValue(val);
        }
        return val;
    }

    /**
     * Include referred Datasets into XML.
     *
     * @param resolvedXml : Job XML element.
     * @param conf : Job configuration
     * @throws CoordinatorJobException
     */
    protected void includeDataSets(Element resolvedXml, Configuration conf) throws CoordinatorJobException
        /* throws Exception */ {
        Element datasets = resolvedXml.getChild("datasets", resolvedXml.getNamespace());
        Element allDataSets = new Element("all_datasets", resolvedXml.getNamespace());
        List<String> dsList = new ArrayList<String>();
        if (datasets != null) {
            for (Element includeElem : (List<Element>) datasets.getChildren("include", datasets.getNamespace())) {
                String incDSFile = includeElem.getTextTrim();
                // log.warn(" incDSFile " + incDSFile);
                includeOneDSFile(incDSFile, dsList, allDataSets, datasets.getNamespace());
            }
            for (Element e : (List<Element>) datasets.getChildren("dataset", datasets.getNamespace())) {
                String dsName = (String) e.getAttributeValue("name");
                if (dsList.contains(dsName)) {// Override with this DS
                    // Remove old DS
                    removeDataSet(allDataSets, dsName);
                    // throw new RuntimeException("Duplicate Dataset " +
                    // dsName);
                }
                else {
                    dsList.add(dsName);
                }
                allDataSets.addContent((Element) e.clone());
            }
        }
        insertDataSet(resolvedXml, allDataSets);
        resolvedXml.removeChild("datasets", resolvedXml.getNamespace());
    }

    /**
     * Include One Dataset file.
     *
     * @param incDSFile : Include data set filename.
     * @param dsList :List of dataset names to verify the duplicate.
     * @param allDataSets : Element that includes all dataset definitions.
     * @param dsNameSpace : Data set name space
     * @throws CoordinatorJobException
     * @throws Exception
     */
    private void includeOneDSFile(String incDSFile, List<String> dsList, Element allDataSets, Namespace dsNameSpace)
            throws CoordinatorJobException {
        Element tmpDataSets = null;
        try {
            String dsXml = readDefinition(incDSFile);
            log.debug("DSFILE :" + incDSFile + "\n" + dsXml);
            tmpDataSets = XmlUtils.parseXml(dsXml);
        }
        /*
         * catch (IOException iex) {XLog.getLog(getClass()).warn(
         * "Error reading included dataset file [{0}].  Message [{1}]",
         * incDSFile, iex.getMessage()); throw new
         * CommandException(ErrorCode.E0803, iex.getMessage()); }
         */
        catch (JDOMException e) {
            log.warn("Error parsing included dataset [{0}].  Message [{1}]", incDSFile, e.getMessage());
            throw new CoordinatorJobException(ErrorCode.E0700, e.getMessage());
        }
        resolveDataSets((List<Element>) tmpDataSets.getChildren("dataset"));
        for (Element e : (List<Element>) tmpDataSets.getChildren("dataset")) {
            String dsName = (String) e.getAttributeValue("name");
            if (dsList.contains(dsName)) {
                throw new RuntimeException("Duplicate Dataset " + dsName);
            }
            dsList.add(dsName);
            Element tmp = (Element) e.clone();
            // TODO: Don't like to over-write the external/include DS's
            // namespace
            tmp.setNamespace(dsNameSpace);// TODO:
            tmp.getChild("uri-template").setNamespace(dsNameSpace);
            if (e.getChild("done-flag") != null) {
                tmp.getChild("done-flag").setNamespace(dsNameSpace);
            }
            allDataSets.addContent(tmp);
        }
        // nested include
        for (Element includeElem : (List<Element>) tmpDataSets.getChildren("include", tmpDataSets.getNamespace())) {
            String incFile = includeElem.getTextTrim();
            // log.warn("incDSFile "+ incDSFile);
            includeOneDSFile(incFile, dsList, allDataSets, dsNameSpace);
        }
    }

    /**
     * Remove a dataset from a list of dataset.
     *
     * @param eDatasets : List of dataset
     * @param name : Dataset name to be removed.
     */
    private static void removeDataSet(Element eDatasets, String name) {
        for (Element eDataset : (List<Element>) eDatasets.getChildren("dataset", eDatasets.getNamespace())) {
            if (eDataset.getAttributeValue("name").equals(name)) {
                eDataset.detach();
            }
        }
        throw new RuntimeException("undefined dataset: " + name);
    }

    /**
     * Read workflow definition.
     *
     * @param appPath application path.
     * @param user user name.
     * @param group group name.
     * @param autToken authentication token.
     * @return workflow definition.
     * @throws WorkflowException thrown if the definition could not be read.
     */
    protected String readDefinition(String appPath) throws CoordinatorJobException {
        String user = ParamChecker.notEmpty(conf.get(OozieClient.USER_NAME), OozieClient.USER_NAME);
        String group = ParamChecker.notEmpty(conf.get(OozieClient.GROUP_NAME), OozieClient.GROUP_NAME);
        Configuration confHadoop = CoordUtils.getHadoopConf(conf);
        try {
            URI uri = new URI(appPath);
            log.debug("user =" + user + " group =" + group);
            FileSystem fs = Services.get().get(HadoopAccessorService.class).createFileSystem(user, group, uri,
                                                                                             new Configuration());
            Path p = new Path(uri.getPath());

            // Reader reader = new InputStreamReader(fs.open(new Path(uri
            // .getPath(), fileName)));
            Reader reader = new InputStreamReader(fs.open(p));// TODO
            StringWriter writer = new StringWriter();
            IOUtils.copyCharStream(reader, writer);
            return writer.toString();
        }
        catch (IOException ex) {
            log.warn("IOException :" + XmlUtils.prettyPrint(confHadoop), ex);
            throw new CoordinatorJobException(ErrorCode.E1001, ex.getMessage(), ex); // TODO:
        }
        catch (URISyntaxException ex) {
            log.warn("URISyException :" + ex.getMessage());
            throw new CoordinatorJobException(ErrorCode.E1002, appPath, ex.getMessage(), ex);// TODO:
        }
        catch (HadoopAccessorException ex) {
            throw new CoordinatorJobException(ex);
        }
        catch (Exception ex) {
            log.warn("Exception :", ex);
            throw new CoordinatorJobException(ErrorCode.E1001, ex.getMessage(), ex);// TODO:
        }
    }

    /**
     * Write a Coordinator Job into database
     *
     * @param eJob : XML element of job
     * @param store : Coordinator Store to write.
     * @param coordJob : Coordinator job bean
     * @return Job if.
     * @throws StoreException
     */
    private String storeToDB(Element eJob, CoordinatorStore store, CoordinatorJobBean coordJob) throws StoreException {
        String jobId = Services.get().get(UUIDService.class).generateId(ApplicationType.COORDINATOR);
        coordJob.setId(jobId);
        coordJob.setAuthToken(this.authToken);
        coordJob.setAppName(eJob.getAttributeValue("name"));
        coordJob.setAppPath(conf.get(OozieClient.COORDINATOR_APP_PATH));
        coordJob.setStatus(CoordinatorJob.Status.PREP);
        coordJob.setCreatedTime(new Date()); // TODO: Do we need that?
        coordJob.setUser(conf.get(OozieClient.USER_NAME));
        coordJob.setGroup(conf.get(OozieClient.GROUP_NAME));
        coordJob.setConf(XmlUtils.prettyPrint(conf).toString());
        coordJob.setJobXml(XmlUtils.prettyPrint(eJob).toString());
        coordJob.setLastActionNumber(0);
        coordJob.setLastModifiedTime(new Date());

        if (!dryrun) {
            store.insertCoordinatorJob(coordJob);
        }
        return jobId;
    }

    /**
     * For unit-testing only. Will ultimately go away
     *
     * @param args
     * @throws Exception
     * @throws JDOMException
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        // Configuration conf = new XConfiguration(IOUtils.getResourceAsReader(
        // "org/apache/oozie/coord/conf.xml", -1));

        Configuration conf = new XConfiguration();

        // base case
        // conf.set(OozieClient.COORDINATOR_APP_PATH,
        // "file:///Users/danielwo/oozie/workflows/coord/test1/");

        // no input datasets
        // conf.set(OozieClient.COORDINATOR_APP_PATH,
        // "file:///Users/danielwo/oozie/workflows/coord/coord_noinput/");
        // conf.set(OozieClient.COORDINATOR_APP_PATH,
        // "file:///Users/danielwo/oozie/workflows/coord/coord_use_apppath/");

        // only 1 instance
        // conf.set(OozieClient.COORDINATOR_APP_PATH,
        // "file:///Users/danielwo/oozie/workflows/coord/coord_oneinstance/");

        // no local props in xml
        // conf.set(OozieClient.COORDINATOR_APP_PATH,
        // "file:///Users/danielwo/oozie/workflows/coord/coord_noprops/");

        conf.set(OozieClient.COORDINATOR_APP_PATH,
                 "file:///homes/test/workspace/sandbox_krishna/oozie-main/core/src/main/java/org/apache/oozie/coord/");
        conf.set(OozieClient.USER_NAME, "test");
        // conf.set(OozieClient.USER_NAME, "danielwo");
        conf.set(OozieClient.GROUP_NAME, "other");
        // System.out.println("appXml :"+ appXml + "\n conf :"+ conf);
        new Services().init();
        try {
            CoordSubmitCommand sc = new CoordSubmitCommand(conf, "TESTING");
            String jobId = sc.call();
            System.out.println("Job Id " + jobId);
            Thread.sleep(80000);
        }
        finally {
            Services.get().destroy();
        }
    }
}
