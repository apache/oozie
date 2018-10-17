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

package org.apache.oozie.command.coord;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Validator;

import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.CoordinatorJob.Execution;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.SubmitTransitionXCommand;
import org.apache.oozie.command.bundle.BundleStatusUpdateXCommand;
import org.apache.oozie.coord.CoordELEvaluator;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.coord.CoordUtils;
import org.apache.oozie.coord.CoordinatorJobException;
import org.apache.oozie.coord.TimeUnit;
import org.apache.oozie.coord.input.logic.CoordInputLogicEvaluator;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.CoordMaterializeTriggerService;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.SchemaService;
import org.apache.oozie.service.Service;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.service.SchemaService.SchemaName;
import org.apache.oozie.service.UUIDService.ApplicationType;
import org.apache.oozie.util.ConfigUtils;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.ELUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.InstrumentUtils;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.ParameterVerifier;
import org.apache.oozie.util.ParameterVerifierException;
import org.apache.oozie.util.PropertiesUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Attribute;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.xml.sax.SAXException;

/**
 * This class provides the functionalities to resolve a coordinator job XML and write the job information into a DB
 * table.
 * <p>
 * Specifically it performs the following functions: 1. Resolve all the variables or properties using job
 * configurations. 2. Insert all datasets definition as part of the &lt;data-in&gt; and &lt;data-out&gt; tags. 3. Validate the XML
 * at runtime.
 */
public class CoordSubmitXCommand extends SubmitTransitionXCommand {

    protected Configuration conf;
    protected final String bundleId;
    protected final String coordName;
    protected boolean dryrun;
    protected JPAService jpaService = null;
    private CoordinatorJob.Status prevStatus = CoordinatorJob.Status.PREP;

    public static final String CONFIG_DEFAULT = "coord-config-default.xml";
    public static final String COORDINATOR_XML_FILE = "coordinator.xml";
    public final String COORD_INPUT_EVENTS ="input-events";
    public final String COORD_OUTPUT_EVENTS = "output-events";
    public final String COORD_INPUT_EVENTS_DATA_IN ="data-in";
    public final String COORD_OUTPUT_EVENTS_DATA_OUT = "data-out";

    private static final Set<String> DISALLOWED_USER_PROPERTIES = new HashSet<>();
    private static final Set<String> DISALLOWED_DEFAULT_PROPERTIES = new HashSet<>();

    protected CoordinatorJobBean coordJob = null;
    /**
     * Default timeout for normal jobs, in minutes, after which coordinator input check will timeout
     */
    public static final String CONF_DEFAULT_TIMEOUT_NORMAL = Service.CONF_PREFIX + "coord.normal.default.timeout";

    public static final String CONF_DEFAULT_CONCURRENCY = Service.CONF_PREFIX + "coord.default.concurrency";

    public static final String CONF_DEFAULT_THROTTLE = Service.CONF_PREFIX + "coord.default.throttle";

    public static final String CONF_MAT_THROTTLING_FACTOR = Service.CONF_PREFIX
            + "coord.materialization.throttling.factor";

    /**
     * Default MAX timeout in minutes, after which coordinator input check will timeout
     */
    public static final String CONF_DEFAULT_MAX_TIMEOUT = Service.CONF_PREFIX + "coord.default.max.timeout";

    public static final String CONF_QUEUE_SIZE = Service.CONF_PREFIX + "CallableQueueService.queue.size";

    public static final String CONF_CHECK_MAX_FREQUENCY = Service.CONF_PREFIX + "coord.check.maximum.frequency";

    private ELEvaluator evalFreq = null;
    private ELEvaluator evalNofuncs = null;
    private ELEvaluator evalData = null;
    private ELEvaluator evalInst = null;
    private ELEvaluator evalAction = null;
    private ELEvaluator evalSla = null;
    private ELEvaluator evalTimeout = null;
    private ELEvaluator evalInitialInstance = null;

    static {
        String[] badUserProps = { PropertiesUtils.YEAR, PropertiesUtils.MONTH, PropertiesUtils.DAY,
                PropertiesUtils.HOUR, PropertiesUtils.MINUTE, PropertiesUtils.DAYS, PropertiesUtils.HOURS,
                PropertiesUtils.MINUTES, PropertiesUtils.KB, PropertiesUtils.MB, PropertiesUtils.GB,
                PropertiesUtils.TB, PropertiesUtils.PB, PropertiesUtils.RECORDS, PropertiesUtils.MAP_IN,
                PropertiesUtils.MAP_OUT, PropertiesUtils.REDUCE_IN, PropertiesUtils.REDUCE_OUT, PropertiesUtils.GROUPS };
        PropertiesUtils.createPropertySet(badUserProps, DISALLOWED_USER_PROPERTIES);
    }

    /**
     * Constructor to create the Coordinator Submit Command.
     *
     * @param conf : Configuration for Coordinator job
     */
    public CoordSubmitXCommand(Configuration conf) {
        super("coord_submit", "coord_submit", 1);
        this.conf = ParamChecker.notNull(conf, "conf");
        this.bundleId = null;
        this.coordName = null;
    }

    /**
     * Constructor to create the Coordinator Submit Command by bundle job.
     *
     * @param conf : Configuration for Coordinator job
     * @param bundleId : bundle id
     * @param coordName : coord name
     */
    protected CoordSubmitXCommand(Configuration conf, String bundleId, String coordName) {
        super("coord_submit", "coord_submit", 1);
        this.conf = ParamChecker.notNull(conf, "conf");
        this.bundleId = ParamChecker.notEmpty(bundleId, "bundleId");
        this.coordName = ParamChecker.notEmpty(coordName, "coordName");
    }

    /**
     * Constructor to create the Coordinator Submit Command.
     *
     * @param dryrun : if dryrun
     * @param conf : Configuration for Coordinator job
     */
    public CoordSubmitXCommand(boolean dryrun, Configuration conf) {
        this(conf);
        this.dryrun = dryrun;
    }

    @Override
    protected String submit() throws CommandException {
        LOG.info("STARTED Coordinator Submit");
        String jobId = submitJob();
        LOG.info("ENDED Coordinator Submit jobId=" + jobId);
        return jobId;
    }

    protected String submitJob() throws CommandException {
        String jobId;
        InstrumentUtils.incrJobCounter(getName(), 1, getInstrumentation());

        boolean exceptionOccured = false;
        try {
            mergeDefaultConfig();

            String appXml = readAndValidateXml();
            coordJob.setOrigJobXml(appXml);
            LOG.debug("jobXml after initial validation " + XmlUtils.prettyPrint(appXml).toString());

            Element eXml = XmlUtils.parseXml(appXml);

            String appNamespace = readAppNamespace(eXml);
            coordJob.setAppNamespace(appNamespace);

            ParameterVerifier.verifyParameters(conf, eXml);

            appXml = XmlUtils.removeComments(appXml);
            initEvaluators();
            Element eJob = basicResolveAndIncludeDS(appXml, conf, coordJob);

            validateCoordinatorJob();

            // checking if the coordinator application data input/output events
            // specify multiple data instance values in erroneous manner
            checkMultipleTimeInstances(eJob, COORD_INPUT_EVENTS, COORD_INPUT_EVENTS_DATA_IN);
            checkMultipleTimeInstances(eJob, COORD_OUTPUT_EVENTS, COORD_OUTPUT_EVENTS_DATA_OUT);

            LOG.debug("jobXml after all validation " + XmlUtils.prettyPrint(eJob).toString());

            jobId = storeToDB(appXml, eJob, coordJob);
            // log job info for coordinator job
            LogUtils.setLogInfo(coordJob);

            if (!dryrun) {
                queueMaterializeTransitionXCommand(jobId);
            }
            else {
                return getDryRun(coordJob);
            }
        }
        catch (JDOMException jex) {
            exceptionOccured = true;
            LOG.warn("ERROR: ", jex);
            throw new CommandException(ErrorCode.E0700, jex.getMessage(), jex);
        }
        catch (CoordinatorJobException cex) {
            exceptionOccured = true;
            LOG.warn("ERROR:  ", cex);
            throw new CommandException(cex);
        }
        catch (ParameterVerifierException pex) {
            exceptionOccured = true;
            LOG.warn("ERROR: ", pex);
            throw new CommandException(pex);
        }
        catch (IllegalArgumentException iex) {
            exceptionOccured = true;
            LOG.warn("ERROR:  ", iex);
            throw new CommandException(ErrorCode.E1003, iex.getMessage(), iex);
        }
        catch (Exception ex) {
            exceptionOccured = true;
            LOG.warn("ERROR:  ", ex);
            throw new CommandException(ErrorCode.E0803, ex.getMessage(), ex);
        }
        finally {
            if (exceptionOccured) {
                if (coordJob.getId() == null || coordJob.getId().equalsIgnoreCase("")) {
                    coordJob.setStatus(CoordinatorJob.Status.FAILED);
                    coordJob.resetPending();
                }
            }
        }
        return jobId;
    }

    /**
     * Gets the dryrun output.
     *
     * @param coordJob the coordinatorJobBean
     * @return the dry run
     * @throws Exception the exception
     */
    protected String getDryRun(CoordinatorJobBean coordJob) throws Exception{
        int materializationWindow = ConfigurationService
                .getInt(CoordMaterializeTriggerService.CONF_MATERIALIZATION_WINDOW);
        Date startTime = coordJob.getStartTime();
        long startTimeMilli = startTime.getTime();
        long endTimeMilli = startTimeMilli + (materializationWindow * 1000);
        Date jobEndTime = coordJob.getEndTime();
        Date endTime = new Date(endTimeMilli);
        if (endTime.compareTo(jobEndTime) > 0) {
            endTime = jobEndTime;
        }
        String jobId = coordJob.getId();
        LOG.info("[" + jobId + "]: Update status to RUNNING");
        coordJob.setStatus(Job.Status.RUNNING);
        coordJob.setPending();
        Configuration jobConf = null;
        try {
            jobConf = new XConfiguration(new StringReader(coordJob.getConf()));
        }
        catch (IOException e1) {
            LOG.warn("Configuration parse error. read from DB :" + coordJob.getConf(), e1);
        }
        String action = new CoordMaterializeTransitionXCommand(coordJob, materializationWindow, startTime,
                endTime).materializeActions(true);

        return coordJob.getJobXml() + System.getProperty("line.separator") + "***actions for instance***" + action;
    }

    /**
     * Queue MaterializeTransitionXCommand
     */
    protected void queueMaterializeTransitionXCommand(String jobId) {
        int materializationWindow = ConfigurationService
                .getInt(CoordMaterializeTriggerService.CONF_MATERIALIZATION_WINDOW);
        queue(new CoordMaterializeTransitionXCommand(jobId, materializationWindow), 100);
    }

    /**
     * Method that validates values in the definition for correctness. Placeholder to add more.
     */
    private void validateCoordinatorJob() throws Exception {
        // check if startTime < endTime
        if (!coordJob.getStartTime().before(coordJob.getEndTime())) {
            throw new IllegalArgumentException("Coordinator Start Time must be earlier than End Time.");
        }

        try {
            // Check if a coord job with cron frequency will materialize actions
            int freq = Integer.parseInt(coordJob.getFrequency());

            // Check if the frequency is faster than 5 min if enabled
            if (ConfigurationService.getBoolean(CONF_CHECK_MAX_FREQUENCY)) {
                CoordinatorJob.Timeunit unit = coordJob.getTimeUnit();
                if (freq == 0 || (freq < 5 && unit == CoordinatorJob.Timeunit.MINUTE)) {
                    throw new IllegalArgumentException("Coordinator job with frequency [" + freq +
                            "] minutes is faster than allowed maximum of 5 minutes ("
                            + CONF_CHECK_MAX_FREQUENCY + " is set to true)");
                }
            }
        } catch (NumberFormatException e) {
            Date start = coordJob.getStartTime();
            Calendar cal = Calendar.getInstance();
            cal.setTime(start);
            cal.add(Calendar.MINUTE, -1);
            start = cal.getTime();

            Date nextTime = CoordCommandUtils.getNextValidActionTimeForCronFrequency(start, coordJob);
            if (nextTime == null) {
                throw new IllegalArgumentException("Invalid coordinator cron frequency: " + coordJob.getFrequency());
            }
            if (!nextTime.before(coordJob.getEndTime())) {
                throw new IllegalArgumentException("Coordinator job with frequency '" +
                        coordJob.getFrequency() + "' materializes no actions between start and end time.");
            }
        }
    }

  /*
  * Check against multiple data instance values inside a single <instance> <start-instance> or <end-instance> tag
  * If found, the job is not submitted and user is informed to correct the error,
  *  instead of defaulting to the first instance value in the list
  */
    private void checkMultipleTimeInstances(Element eCoordJob, String eventType, String dataType) throws CoordinatorJobException {
        Element eventsSpec, dataSpec, instance;
        List<Element> instanceSpecList;
        Namespace ns = eCoordJob.getNamespace();
        String instanceValue;
        eventsSpec = eCoordJob.getChild(eventType, ns);
        if (eventsSpec != null) {
            dataSpec = eventsSpec.getChild(dataType, ns);
            if (dataSpec != null) {
                // In case of input-events, there can be multiple child <instance> datasets.
                // Iterating to ensure none of them have errors
                instanceSpecList = dataSpec.getChildren("instance", ns);
                Iterator instanceIter = instanceSpecList.iterator();
                while(instanceIter.hasNext()) {
                    instance = ((Element) instanceIter.next());
                    if(instance.getContentSize() == 0) { //empty string or whitespace
                        throw new CoordinatorJobException(ErrorCode.E1021, "<instance> tag within " + eventType + " is empty!");
                    }
                    instanceValue = instance.getContent(0).toString();
                    boolean isInvalid = false;
                    try {
                        isInvalid = evalAction.checkForExistence(instanceValue, ",");
                    } catch (Exception e) {
                        handleELParseException(eventType, dataType, instanceValue);
                    }
                    if (isInvalid) { // reaching this block implies instance is not empty i.e. length > 0
                        handleExpresionWithMultipleInstances(eventType, dataType, instanceValue);
                    }
                }

                // In case of input-events, there can be multiple child <start-instance> datasets.
                // Iterating to ensure none of them have errors
                instanceSpecList = dataSpec.getChildren("start-instance", ns);
                instanceIter = instanceSpecList.iterator();
                while(instanceIter.hasNext()) {
                    instance = ((Element) instanceIter.next());
                    if(instance.getContentSize() == 0) { //empty string or whitespace
                        throw new CoordinatorJobException(ErrorCode.E1021, "<start-instance> tag within " + eventType
                                + " is empty!");
                    }
                    instanceValue = instance.getContent(0).toString();
                    boolean isInvalid = false;
                    try {
                        isInvalid = evalAction.checkForExistence(instanceValue, ",");
                    } catch (Exception e) {
                        handleELParseException(eventType, dataType, instanceValue);
                    }
                    if (isInvalid) { // reaching this block implies start instance is not empty i.e. length > 0
                        handleExpresionWithStartMultipleInstances(eventType, dataType, instanceValue);
                    }
                }

                // In case of input-events, there can be multiple child <end-instance> datasets.
                // Iterating to ensure none of them have errors
                instanceSpecList = dataSpec.getChildren("end-instance", ns);
                instanceIter = instanceSpecList.iterator();
                while(instanceIter.hasNext()) {
                    instance = ((Element) instanceIter.next());
                    if(instance.getContentSize() == 0) { //empty string or whitespace
                        throw new CoordinatorJobException(ErrorCode.E1021, "<end-instance> tag within " + eventType + " is empty!");
                    }
                    instanceValue = instance.getContent(0).toString();
                    boolean isInvalid = false;
                    try {
                        isInvalid = evalAction.checkForExistence(instanceValue, ",");
                    } catch (Exception e) {
                        handleELParseException(eventType, dataType, instanceValue);
                    }
                    if (isInvalid) { // reaching this block implies instance is not empty i.e. length > 0
                        handleExpresionWithMultipleEndInstances(eventType, dataType, instanceValue);
                    }
                }

            }
        }
    }

    private void handleELParseException(String eventType, String dataType, String instanceValue)
            throws CoordinatorJobException {
        String correctAction = null;
        if(dataType.equals(COORD_INPUT_EVENTS_DATA_IN)) {
            correctAction = "Coordinator app definition should have valid <instance> tag for data-in";
        } else if(dataType.equals(COORD_OUTPUT_EVENTS_DATA_OUT)) {
            correctAction = "Coordinator app definition should have valid <instance> tag for data-out";
        }
        throw new CoordinatorJobException(ErrorCode.E1021, eventType + " instance '" + instanceValue
                + "' is not valid. Coordinator job NOT SUBMITTED. " + correctAction);
    }

    private void handleExpresionWithMultipleInstances(String eventType, String dataType, String instanceValue)
            throws CoordinatorJobException {
        String correctAction = null;
        if(dataType.equals(COORD_INPUT_EVENTS_DATA_IN)) {
            correctAction = "Coordinator app definition should have separate <instance> tag per data-in instance";
        } else if(dataType.equals(COORD_OUTPUT_EVENTS_DATA_OUT)) {
            correctAction = "Coordinator app definition can have only one <instance> tag per data-out instance";
        }
        throw new CoordinatorJobException(ErrorCode.E1021, eventType + " instance '" + instanceValue
                + "' contains more than one date instance. Coordinator job NOT SUBMITTED. " + correctAction);
    }

    private void handleExpresionWithStartMultipleInstances(String eventType, String dataType, String instanceValue)
            throws CoordinatorJobException {
        String correctAction = "Coordinator app definition should not have multiple start-instances";
        throw new CoordinatorJobException(ErrorCode.E1021, eventType + " start-instance '" + instanceValue
                + "' contains more than one date start-instance. Coordinator job NOT SUBMITTED. " + correctAction);
    }

    private void handleExpresionWithMultipleEndInstances(String eventType, String dataType, String instanceValue)
            throws CoordinatorJobException {
        String correctAction = "Coordinator app definition should not have multiple end-instances";
        throw new CoordinatorJobException(ErrorCode.E1021, eventType + " end-instance '" + instanceValue
                + "' contains more than one date end-instance. Coordinator job NOT SUBMITTED. " + correctAction);
    }

    /**
     * Read the application XML and validate against coordinator Schema
     *
     * @return validated coordinator XML
     * @throws CoordinatorJobException thrown if unable to read or validate coordinator xml
     */
    protected String readAndValidateXml() throws CoordinatorJobException {
        String appPath = ParamChecker.notEmpty(conf.get(OozieClient.COORDINATOR_APP_PATH),
                OozieClient.COORDINATOR_APP_PATH);
        String coordXml = readDefinition(appPath);
        validateXml(coordXml);
        return coordXml;
    }

    /**
     * Validate against Coordinator XSD file
     *
     * @param xmlContent : Input coordinator xml
     * @throws CoordinatorJobException thrown if unable to validate coordinator xml
     */
    private void validateXml(String xmlContent) throws CoordinatorJobException {
        try {
            Validator validator = Services.get().get(SchemaService.class).getValidator(SchemaName.COORDINATOR);
            validator.validate(new StreamSource(new StringReader(xmlContent)));
        }
        catch (SAXException ex) {
            LOG.warn("SAXException :", ex);
            throw new CoordinatorJobException(ErrorCode.E0701, ex.getMessage(), ex);
        }
        catch (IOException ex) {
            LOG.warn("IOException :", ex);
            throw new CoordinatorJobException(ErrorCode.E0702, ex.getMessage(), ex);
        }
    }

    /**
     * Read the application XML schema namespace
     *
     * @param coordXmlElement input coordinator xml Element
     * @return app xml namespace
     * @throws CoordinatorJobException
     */
    private String readAppNamespace(Element coordXmlElement) throws CoordinatorJobException {
        Namespace ns = coordXmlElement.getNamespace();
        if (ns != null && bundleId != null && ns.getURI().equals(SchemaService.COORDINATOR_NAMESPACE_URI_1)) {
            throw new CoordinatorJobException(ErrorCode.E1319, "bundle app can not submit coordinator namespace "
                    + SchemaService.COORDINATOR_NAMESPACE_URI_1 + ", please use 0.2 or later");
        }
        if (ns != null) {
            return ns.getURI();
        }
        else {
            throw new CoordinatorJobException(ErrorCode.E0700, "the application xml namespace is not given");
        }
    }

    /**
     * Merge default configuration with user-defined configuration.
     *
     * @throws CommandException thrown if failed to read or merge configurations
     */
    protected void mergeDefaultConfig() throws CommandException {
        Path configDefault = null;
        try {
            String coordAppPathStr = conf.get(OozieClient.COORDINATOR_APP_PATH);
            Path coordAppPath = new Path(coordAppPathStr);
            String user = ParamChecker.notEmpty(conf.get(OozieClient.USER_NAME), OozieClient.USER_NAME);
            HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
            Configuration fsConf = has.createConfiguration(coordAppPath.toUri().getAuthority());
            FileSystem fs = has.createFileSystem(user, coordAppPath.toUri(), fsConf);

            // app path could be a directory
            if (!fs.isFile(coordAppPath)) {
                configDefault = new Path(coordAppPath, CONFIG_DEFAULT);
            } else {
                configDefault = new Path(coordAppPath.getParent(), CONFIG_DEFAULT);
            }

            if (fs.exists(configDefault)) {
                Configuration defaultConf = new XConfiguration(fs.open(configDefault));
                PropertiesUtils.checkDisallowedProperties(defaultConf, DISALLOWED_USER_PROPERTIES);
                PropertiesUtils.checkDefaultDisallowedProperties(defaultConf);
                XConfiguration.injectDefaults(defaultConf, conf);
            }
            else {
                LOG.info("configDefault Doesn't exist " + configDefault);
            }
            PropertiesUtils.checkDisallowedProperties(conf, DISALLOWED_USER_PROPERTIES);

            // Resolving all variables in the job properties.
            // This ensures the Hadoop Configuration semantics is preserved.
            XConfiguration resolvedVarsConf = new XConfiguration();
            for (Map.Entry<String, String> entry : conf) {
                resolvedVarsConf.set(entry.getKey(), conf.get(entry.getKey()));
            }
            conf = resolvedVarsConf;
        }
        catch (IOException e) {
            throw new CommandException(ErrorCode.E0702, e.getMessage() + " : Problem reading default config "
                    + configDefault, e);
        }
        catch (HadoopAccessorException e) {
            throw new CommandException(e);
        }
        LOG.debug("Merged CONF :" + XmlUtils.prettyPrint(conf).toString());
    }

    /**
     * The method resolve all the variables that are defined in configuration. It also include the data set definition
     * from dataset file into XML.
     *
     * @param appXml : Original job XML
     * @param conf : Configuration of the job
     * @param coordJob : Coordinator job bean to be populated.
     * @return Resolved and modified job XML element.
     * @throws CoordinatorJobException thrown if failed to resolve basic entities or include referred datasets
     * @throws Exception thrown if failed to resolve basic entities or include referred datasets
     */
    public Element basicResolveAndIncludeDS(String appXml, Configuration conf, CoordinatorJobBean coordJob)
            throws Exception {
        Element basicResolvedApp = resolveInitial(conf, appXml, coordJob);
        includeDataSets(basicResolvedApp, conf);
        return basicResolvedApp;
    }

    /**
     * Insert data set into data-in and data-out tags.
     *
     * @param eAppXml : coordinator application XML
     * @param eDatasets : DataSet XML
     */
    @SuppressWarnings("unchecked")
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
    @SuppressWarnings("unchecked")
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
        evalAction = CoordELEvaluator.createELEvaluatorForGroup(conf, "coord-action-start");
        evalTimeout = CoordELEvaluator.createELEvaluatorForGroup(conf, "coord-job-wait-timeout");
        evalInitialInstance = CoordELEvaluator.createELEvaluatorForGroup(conf, "coord-job-submit-initial-instance");
    }

    /**
     * Resolve basic entities using job Configuration.
     *
     * @param conf :Job configuration
     * @param appXml : Original job XML
     * @param coordJob : Coordinator job bean to be populated.
     * @return Resolved job XML element.
     * @throws CoordinatorJobException thrown if failed to resolve basic entities
     * @throws Exception thrown if failed to resolve basic entities
     */
    @SuppressWarnings("unchecked")
    protected Element resolveInitial(Configuration conf, String appXml, CoordinatorJobBean coordJob)
            throws Exception {
        Element eAppXml = XmlUtils.parseXml(appXml);
        // job's main attributes
        // frequency
        String val = resolveAttribute("frequency", eAppXml, evalFreq);

        val = ParamChecker.checkFrequency(val);
        coordJob.setFrequency(val);
        TimeUnit tmp = (evalFreq.getVariable("timeunit") == null) ? TimeUnit.MINUTE : ((TimeUnit) evalFreq
                .getVariable("timeunit"));
        try {
            Integer.parseInt(val);
        }
        catch (NumberFormatException ex) {
            tmp=TimeUnit.CRON;
        }

        addAnAttribute("freq_timeunit", eAppXml, tmp.toString());
        // TimeUnit
        coordJob.setTimeUnit(CoordinatorJob.Timeunit.valueOf(tmp.toString()));
        // End Of Duration
        tmp = evalFreq.getVariable("endOfDuration") == null ? TimeUnit.NONE : ((TimeUnit) evalFreq
                .getVariable("endOfDuration"));
        addAnAttribute("end_of_duration", eAppXml, tmp.toString());
        // coordJob.setEndOfDuration(tmp) // TODO: Add new attribute in Job bean

        // Application name
        if (this.coordName == null) {
            String name = ELUtils.resolveAppName(eAppXml.getAttribute("name").getValue(), conf);
            coordJob.setAppName(name);
        }
        else {
            // this coord job is created from bundle
            coordJob.setAppName(this.coordName);
        }

        // start time
        val = resolveAttribute("start", eAppXml, evalNofuncs);
        ParamChecker.checkDateOozieTZ(val, "start");
        coordJob.setStartTime(DateUtils.parseDateOozieTZ(val));
        // end time
        val = resolveAttribute("end", eAppXml, evalNofuncs);
        ParamChecker.checkDateOozieTZ(val, "end");
        coordJob.setEndTime(DateUtils.parseDateOozieTZ(val));
        // Time zone
        val = resolveAttribute("timezone", eAppXml, evalNofuncs);
        ParamChecker.checkTimeZone(val, "timezone");
        coordJob.setTimeZone(val);

        // controls
        val = resolveTagContents("timeout", eAppXml.getChild("controls", eAppXml.getNamespace()), evalTimeout);
        if (val != null && val != "") {
            int t = Integer.parseInt(val);
            tmp = (evalTimeout.getVariable("timeunit") == null) ? TimeUnit.MINUTE : ((TimeUnit) evalTimeout
                    .getVariable("timeunit"));
            switch (tmp) {
                case HOUR:
                    val = String.valueOf(t * 60);
                    break;
                case DAY:
                    val = String.valueOf(t * 60 * 24);
                    break;
                case MONTH:
                    val = String.valueOf(t * 60 * 24 * 30);
                    break;
                default:
                    break;
            }
        }
        else {
            val = ConfigurationService.get(CONF_DEFAULT_TIMEOUT_NORMAL);
        }

        int ival = ParamChecker.checkInteger(val, "timeout");
        if (ival < 0 || ival > ConfigurationService.getInt(CONF_DEFAULT_MAX_TIMEOUT)) {
            ival = ConfigurationService.getInt(CONF_DEFAULT_MAX_TIMEOUT);
        }
        coordJob.setTimeout(ival);

        val = resolveTagContents("concurrency", eAppXml.getChild("controls", eAppXml.getNamespace()), evalNofuncs);
        if (val == null || val.isEmpty()) {
            val = ConfigurationService.get(CONF_DEFAULT_CONCURRENCY);
        }
        ival = ParamChecker.checkInteger(val, "concurrency");
        coordJob.setConcurrency(ival);

        val = resolveTagContents("throttle", eAppXml.getChild("controls", eAppXml.getNamespace()), evalNofuncs);
        if (val == null || val.isEmpty()) {
            ival = ConfigurationService.getInt(CONF_DEFAULT_THROTTLE);
        }
        else {
            ival = ParamChecker.checkInteger(val, "throttle");
        }
        int maxQueue = ConfigurationService.getInt(CONF_QUEUE_SIZE);
        float factor = ConfigurationService.getFloat(CONF_MAT_THROTTLING_FACTOR);
        int maxThrottle = (int) (maxQueue * factor);
        if (ival > maxThrottle || ival < 1) {
            ival = maxThrottle;
        }
        LOG.debug("max throttle " + ival);
        coordJob.setMatThrottling(ival);

        val = resolveTagContents("execution", eAppXml.getChild("controls", eAppXml.getNamespace()), evalNofuncs);
        if (val == "") {
            val = Execution.FIFO.toString();
        }
        coordJob.setExecutionOrder(Execution.valueOf(val));
        String[] acceptedVals = { Execution.LIFO.toString(), Execution.FIFO.toString(), Execution.LAST_ONLY.toString(),
            Execution.NONE.toString()};
        ParamChecker.isMember(val, acceptedVals, "execution");

        // datasets
        resolveTagContents("include", eAppXml.getChild("datasets", eAppXml.getNamespace()), evalNofuncs);
        // for each data set
        resolveDataSets(eAppXml);
        HashMap<String, String> dataNameList = new HashMap<String, String>();
        resolveIODataset(eAppXml);
        resolveIOEvents(eAppXml, dataNameList);

        if (CoordUtils.isInputLogicSpecified(eAppXml)) {
            resolveInputLogic(eAppXml.getChild(CoordInputLogicEvaluator.INPUT_LOGIC, eAppXml.getNamespace()), evalInst,
                    dataNameList);
        }

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
                // Want to check the data-integrity but don't want to modify the
                // XML
                // for properties only
                Element tmpProp = (Element) propElem.clone();
                resolveTagContents("value", tmpProp, evalData);
            }
        }
        evalSla = CoordELEvaluator.createELEvaluatorForDataAndConf(conf, "coord-sla-submit", dataNameList);
        resolveSLA(eAppXml, coordJob);
        return eAppXml;
    }

    /**
     * Resolve SLA events
     *
     * @param eAppXml job XML
     * @param coordJob coordinator job bean
     * @throws CommandException thrown if failed to resolve sla events
     */
    private void resolveSLA(Element eAppXml, CoordinatorJobBean coordJob) throws CommandException {
        Element eSla = XmlUtils.getSLAElement(eAppXml.getChild("action", eAppXml.getNamespace()));

        if (eSla != null) {
            resolveSLAContent(eSla);
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
     * Resolve an SLA value.
     *
     * @param elem : XML Element where attribute is defiend
     */
    private void resolveSLAContent(Element elem) {
        for (Element tagElem : (List<Element>) elem.getChildren()) {
            if (tagElem != null) {
                try {
                    String val = CoordELFunctions.evalAndWrap(evalNofuncs, tagElem.getText().trim());
                    tagElem.setText(val);
                }
                catch (Exception e) {
                    LOG.warn("Variable is not defined in job.properties. Here is the message: {0}", e.getMessage());
                    continue;
                }
            }
        }
    }

    /**
     * Resolve input-events/data-in and output-events/data-out tags.
     *
     * @param eJobOrg : Job element
     * @throws CoordinatorJobException thrown if failed to resolve input and output events
     */
    @SuppressWarnings("unchecked")
    private void resolveIOEvents(Element eJobOrg, HashMap<String, String> dataNameList) throws CoordinatorJobException {
        // Resolving input-events/data-in
        // Clone the job and don't update anything in the original
        Element eJob = (Element) eJobOrg.clone();
        Element inputList = eJob.getChild("input-events", eJob.getNamespace());
        if (inputList != null) {
            TreeSet<String> eventNameSet = new TreeSet<>();
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
            TreeSet<String> eventNameSet = new TreeSet<>();
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

    private void resolveInputLogic(Element root, ELEvaluator evalInputLogic, HashMap<String, String> dataNameList)
            throws Exception {
        for (Object event : root.getChildren()) {
            Element inputElement = (Element) event;
            resolveAttribute("dataset", inputElement, evalInputLogic);
            String name=resolveAttribute("name", inputElement, evalInputLogic);
            resolveAttribute("or", inputElement, evalInputLogic);
            resolveAttribute("and", inputElement, evalInputLogic);
            resolveAttribute("combine", inputElement, evalInputLogic);

            if (name != null) {
                dataNameList.put(name, "data-in");
            }

            if (!inputElement.getChildren().isEmpty()) {
                resolveInputLogic(inputElement, evalInputLogic, dataNameList);
            }
        }
    }

    /**
     * Resolve input-events/dataset and output-events/dataset tags.
     *
     * @param eAppXml : Job element
     * @throws CoordinatorJobException thrown if failed to resolve input and output events
     */
    @SuppressWarnings("unchecked")
    private void resolveIODataset(Element eAppXml) throws CoordinatorJobException {
        // Resolving input-events/data-in
        Element inputList = eAppXml.getChild("input-events", eAppXml.getNamespace());
        if (inputList != null) {
            for (Element dataIn : (List<Element>) inputList.getChildren("data-in", eAppXml.getNamespace())) {
                resolveAttribute("dataset", dataIn, evalInst);

            }
        }
        // Resolving output-events/data-out
        Element outputList = eAppXml.getChild("output-events", eAppXml.getNamespace());
        if (outputList != null) {
            for (Element dataOut : (List<Element>) outputList.getChildren("data-out", eAppXml.getNamespace())) {
                resolveAttribute("dataset", dataOut, evalInst);
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
     * Resolve datasets using job configuration.
     *
     * @param eAppXml : Job Element XML
     * @throws Exception thrown if failed to resolve datasets
     */
    @SuppressWarnings("unchecked")
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
     * Resolve datasets using job configuration.
     *
     * @param dsElems : Data set XML element.
     * @throws CoordinatorJobException thrown if failed to resolve datasets
     */
    private void resolveDataSets(List<Element> dsElems) throws CoordinatorJobException {
        for (Element dsElem : dsElems) {
            // Setting up default TimeUnit and EndOFDuraion
            evalFreq.setVariable("timeunit", TimeUnit.MINUTE);
            evalFreq.setVariable("endOfDuration", TimeUnit.NONE);

            String val = resolveAttribute("frequency", dsElem, evalFreq);
            int ival = ParamChecker.checkInteger(val, "frequency");
            ParamChecker.checkGTZero(ival, "frequency");
            addAnAttribute("freq_timeunit", dsElem, evalFreq.getVariable("timeunit") == null ? TimeUnit.MINUTE
                    .toString() : evalFreq.getVariable("timeunit").toString());
            addAnAttribute("end_of_duration", dsElem, evalFreq.getVariable("endOfDuration") == null ? TimeUnit.NONE
                    .toString() : evalFreq.getVariable("endOfDuration").toString());
            val = resolveAttribute("initial-instance", dsElem, evalInitialInstance);
            ParamChecker.checkDateOozieTZ(val, "initial-instance");
            checkInitialInstance(val);
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
     * @param eval : EL evealuator
     * @return Resolved tag content.
     * @throws CoordinatorJobException thrown if failed to resolve tag content
     */
    @SuppressWarnings("unchecked")
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
                        throw new CoordinatorJobException(ErrorCode.E1004, e.getMessage(), e);
                    }
                    tagElem.removeContent();
                    tagElem.addContent(updated);
                    ret += updated;
                }
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
     * @throws CoordinatorJobException thrown if failed to resolve an attribute value
     */
    private String resolveAttribute(String attrName, Element elem, ELEvaluator eval) throws CoordinatorJobException {
        Attribute attr = elem.getAttribute(attrName);
        String val = null;
        if (attr != null) {
            try {
                val = CoordELFunctions.evalAndWrap(eval, attr.getValue().trim());
            }
            catch (Exception e) {
                throw new CoordinatorJobException(ErrorCode.E1004, e.getMessage(), e);
            }
            attr.setValue(val);
        }
        return val;
    }

    /**
     * Include referred datasets into XML.
     *
     * @param resolvedXml : Job XML element.
     * @param conf : Job configuration
     * @throws CoordinatorJobException thrown if failed to include referred datasets into XML
     */
    @SuppressWarnings("unchecked")
    protected void includeDataSets(Element resolvedXml, Configuration conf) throws CoordinatorJobException {
        Element datasets = resolvedXml.getChild("datasets", resolvedXml.getNamespace());
        Element allDataSets = new Element("all_datasets", resolvedXml.getNamespace());
        List<String> dsList = new ArrayList<>();
        if (datasets != null) {
            for (Element includeElem : (List<Element>) datasets.getChildren("include", datasets.getNamespace())) {
                String incDSFile = includeElem.getTextTrim();
                includeOneDSFile(incDSFile, dsList, allDataSets, datasets.getNamespace());
            }
            for (Element e : (List<Element>) datasets.getChildren("dataset", datasets.getNamespace())) {
                String dsName = e.getAttributeValue("name");
                if (dsList.contains(dsName)) {// Override with this DS
                    // Remove duplicate
                    removeDataSet(allDataSets, dsName);
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
     * Include one dataset file.
     *
     * @param incDSFile : Include data set filename.
     * @param dsList :List of dataset names to verify the duplicate.
     * @param allDataSets : Element that includes all dataset definitions.
     * @param dsNameSpace : Data set name space
     * @throws CoordinatorJobException thrown if failed to include one dataset file
     */
    @SuppressWarnings("unchecked")
    private void includeOneDSFile(String incDSFile, List<String> dsList, Element allDataSets, Namespace dsNameSpace)
    throws CoordinatorJobException {
        Element tmpDataSets;
        try {
            String dsXml = readDefinition(incDSFile);
            LOG.debug("DSFILE :" + incDSFile + "\n" + dsXml);
            tmpDataSets = XmlUtils.parseXml(dsXml);
        }
        catch (JDOMException e) {
            LOG.warn("Error parsing included dataset [{0}].  Message [{1}]", incDSFile, e.getMessage());
            throw new CoordinatorJobException(ErrorCode.E0700, e.getMessage());
        }
        resolveDataSets(tmpDataSets.getChildren("dataset"));
        for (Element e : (List<Element>) tmpDataSets.getChildren("dataset")) {
            String dsName = e.getAttributeValue("name");
            if (dsList.contains(dsName)) {
                throw new RuntimeException("Duplicate Dataset " + dsName);
            }
            dsList.add(dsName);
            Element tmp = (Element) e.clone();
            // TODO: Don't like to over-write the external/include DS's namespace
            tmp.setNamespace(dsNameSpace);
            tmp.getChild("uri-template").setNamespace(dsNameSpace);
            if (e.getChild("done-flag") != null) {
                tmp.getChild("done-flag").setNamespace(dsNameSpace);
            }
            allDataSets.addContent(tmp);
        }
        // nested include
        for (Element includeElem : (List<Element>) tmpDataSets.getChildren("include", tmpDataSets.getNamespace())) {
            String incFile = includeElem.getTextTrim();
            includeOneDSFile(incFile, dsList, allDataSets, dsNameSpace);
        }
    }

    /**
     * Remove a dataset from a list of dataset.
     *
     * @param eDatasets : List of dataset
     * @param name : Dataset name to be removed.
     */
    @SuppressWarnings("unchecked")
    private static void removeDataSet(Element eDatasets, String name) {
        for (Element eDataset : (List<Element>) eDatasets.getChildren("dataset", eDatasets.getNamespace())) {
            if (eDataset.getAttributeValue("name").equals(name)) {
                eDataset.detach();
                return;
            }
        }
        throw new RuntimeException("undefined dataset: " + name);
    }

    /**
     * Read coordinator definition.
     *
     * @param appPath application path.
     * @return coordinator definition.
     * @throws CoordinatorJobException thrown if the definition could not be read.
     */
    protected String readDefinition(String appPath) throws CoordinatorJobException {
        String user = ParamChecker.notEmpty(conf.get(OozieClient.USER_NAME), OozieClient.USER_NAME);
        // Configuration confHadoop = CoordUtils.getHadoopConf(conf);
        try {
            URI uri = new URI(appPath);
            LOG.debug("user =" + user);
            HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
            Configuration fsConf = has.createConfiguration(uri.getAuthority());
            FileSystem fs = has.createFileSystem(user, uri, fsConf);
            Path appDefPath;

            // app path could be a directory
            Path path = new Path(uri.getPath());
            // check file exists for dataset include file, app xml already checked
            if (!fs.exists(path)) {
                throw new URISyntaxException(path.toString(), "path not existed : " + path.toString());
            }
            if (!fs.isFile(path)) {
                appDefPath = new Path(path, COORDINATOR_XML_FILE);
            } else {
                appDefPath = path;
            }

            Reader reader = new InputStreamReader(fs.open(appDefPath), Charsets.UTF_8);
            StringWriter writer = new StringWriter();
            IOUtils.copyCharStream(reader, writer);
            return writer.toString();
        }
        catch (IOException ex) {
            LOG.warn("IOException :" + XmlUtils.prettyPrint(conf), ex);
            throw new CoordinatorJobException(ErrorCode.E1001, ex.getMessage(), ex);
        }
        catch (URISyntaxException ex) {
            LOG.warn("URISyException :" + ex.getMessage());
            throw new CoordinatorJobException(ErrorCode.E1002, appPath, ex.getMessage(), ex);
        }
        catch (HadoopAccessorException ex) {
            throw new CoordinatorJobException(ex);
        }
        catch (Exception ex) {
            LOG.warn("Exception :", ex);
            throw new CoordinatorJobException(ErrorCode.E1001, ex.getMessage(), ex);
        }
    }

    /**
     * Write a coordinator job into database
     *
     *@param appXML : Coordinator definition xml
     * @param eJob : XML element of job
     * @param coordJob : Coordinator job bean
     * @return Job id
     * @throws CommandException thrown if unable to save coordinator job to db
     */
    protected String storeToDB(String appXML, Element eJob, CoordinatorJobBean coordJob) throws CommandException {
        String jobId = Services.get().get(UUIDService.class).generateId(ApplicationType.COORDINATOR);
        coordJob.setId(jobId);

        coordJob.setAppPath(conf.get(OozieClient.COORDINATOR_APP_PATH));
        coordJob.setCreatedTime(new Date());
        coordJob.setUser(conf.get(OozieClient.USER_NAME));
        String group = ConfigUtils.getWithDeprecatedCheck(conf, OozieClient.JOB_ACL, OozieClient.GROUP_NAME, null);
        coordJob.setGroup(group);
        coordJob.setConf(XmlUtils.prettyPrint(conf).toString());
        coordJob.setJobXml(XmlUtils.prettyPrint(eJob).toString());
        coordJob.setLastActionNumber(0);
        coordJob.setLastModifiedTime(new Date());

        if (!dryrun) {
            coordJob.setLastModifiedTime(new Date());
            try {
                CoordJobQueryExecutor.getInstance().insert(coordJob);
            }
            catch (JPAExecutorException jpaee) {
                coordJob.setId(null);
                coordJob.setStatus(CoordinatorJob.Status.FAILED);
                throw new CommandException(jpaee);
            }
        }
        return jobId;
    }

    /*
     * this method checks if the initial-instance specified for a particular
       is not a date earlier than the oozie server default Jan 01, 1970 00:00Z UTC
     */
    private void checkInitialInstance(String val) throws CoordinatorJobException, IllegalArgumentException {
        Date initialInstance, givenInstance;
        try {
            initialInstance = DateUtils.parseDateUTC("1970-01-01T00:00Z");
            givenInstance = DateUtils.parseDateOozieTZ(val);
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Unable to parse dataset initial-instance string '" + val +
                                               "' to Date object. ",e);
        }
        if(givenInstance.compareTo(initialInstance) < 0) {
            throw new CoordinatorJobException(ErrorCode.E1021, "Dataset initial-instance " + val +
                    " is earlier than the default initial instance " + DateUtils.formatDateOozieTZ(initialInstance));
        }
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
    protected void loadState() throws CommandException {
        jpaService = Services.get().get(JPAService.class);
        if (jpaService == null) {
            throw new CommandException(ErrorCode.E0610);
        }
        coordJob = new CoordinatorJobBean();
        if (this.bundleId != null) {
            // this coord job is created from bundle
            coordJob.setBundleId(this.bundleId);
            // first use bundle id if submit thru bundle
            LogUtils.setLogInfo(this.bundleId);
        }
        if (this.coordName != null) {
            // this coord job is created from bundle
            coordJob.setAppName(this.coordName);
        }
        setJob(coordJob);
    }

    @Override
    protected void verifyPrecondition() throws CommandException {
    }

    @Override
    public void notifyParent() throws CommandException {
        // update bundle action
        if (coordJob.getBundleId() != null) {
            LOG.debug("Updating bundle record: " + coordJob.getBundleId() + " for coord id: " + coordJob.getId());
            BundleStatusUpdateXCommand bundleStatusUpdate = new BundleStatusUpdateXCommand(coordJob, prevStatus);
            bundleStatusUpdate.call();
        }
    }

    @Override
    public void updateJob() throws CommandException {
    }

    @Override
    public Job getJob() {
        return coordJob;
    }

    @Override
    public void performWrites() throws CommandException {
    }
}
