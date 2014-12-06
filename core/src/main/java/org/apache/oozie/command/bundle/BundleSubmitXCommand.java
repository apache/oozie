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

package org.apache.oozie.command.bundle;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Validator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.SubmitTransitionXCommand;
import org.apache.oozie.executor.jpa.BundleJobQueryExecutor;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.SchemaService;
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
import org.apache.oozie.util.PropertiesUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.util.ParameterVerifier;
import org.jdom.Attribute;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.xml.sax.SAXException;

/**
 * This Command will submit the bundle.
 */
public class BundleSubmitXCommand extends SubmitTransitionXCommand {

    private Configuration conf;
    public static final String CONFIG_DEFAULT = "bundle-config-default.xml";
    public static final String BUNDLE_XML_FILE = "bundle.xml";
    private final BundleJobBean bundleBean = new BundleJobBean();
    private String jobId;
    private static final Set<String> DISALLOWED_USER_PROPERTIES = new HashSet<String>();
    private static final Set<String> DISALLOWED_DEFAULT_PROPERTIES = new HashSet<String>();

    static {
        String[] badUserProps = { PropertiesUtils.YEAR, PropertiesUtils.MONTH, PropertiesUtils.DAY,
                PropertiesUtils.HOUR, PropertiesUtils.MINUTE, PropertiesUtils.DAYS, PropertiesUtils.HOURS,
                PropertiesUtils.MINUTES, PropertiesUtils.KB, PropertiesUtils.MB, PropertiesUtils.GB,
                PropertiesUtils.TB, PropertiesUtils.PB, PropertiesUtils.RECORDS, PropertiesUtils.MAP_IN,
                PropertiesUtils.MAP_OUT, PropertiesUtils.REDUCE_IN, PropertiesUtils.REDUCE_OUT, PropertiesUtils.GROUPS };
        PropertiesUtils.createPropertySet(badUserProps, DISALLOWED_USER_PROPERTIES);

        String[] badDefaultProps = { PropertiesUtils.HADOOP_USER};
        PropertiesUtils.createPropertySet(badUserProps, DISALLOWED_DEFAULT_PROPERTIES);
        PropertiesUtils.createPropertySet(badDefaultProps, DISALLOWED_DEFAULT_PROPERTIES);
    }

    /**
     * Constructor to create the bundle submit command.
     *
     * @param conf configuration for bundle job
     */
    public BundleSubmitXCommand(Configuration conf) {
        super("bundle_submit", "bundle_submit", 1);
        this.conf = ParamChecker.notNull(conf, "conf");
    }

    /**
     * Constructor to create the bundle submit command.
     *
     * @param dryrun true if dryrun is enable
     * @param conf configuration for bundle job
     */
    public BundleSubmitXCommand(boolean dryrun, Configuration conf) {
        this(conf);
        this.dryrun = dryrun;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.SubmitTransitionXCommand#submit()
     */
    @Override
    protected String submit() throws CommandException {
        LOG.info("STARTED Bundle Submit");
        try {
            InstrumentUtils.incrJobCounter(getName(), 1, getInstrumentation());

            ParameterVerifier.verifyParameters(conf, XmlUtils.parseXml(bundleBean.getOrigJobXml()));

            String jobXmlWithNoComment = XmlUtils.removeComments(this.bundleBean.getOrigJobXml().toString());
            // Resolving all variables in the job properties.
            // This ensures the Hadoop Configuration semantics is preserved.
            XConfiguration resolvedVarsConf = new XConfiguration();
            for (Map.Entry<String, String> entry : conf) {
                resolvedVarsConf.set(entry.getKey(), conf.get(entry.getKey()));
            }
            conf = resolvedVarsConf;

            String resolvedJobXml = resolvedVars(jobXmlWithNoComment, conf);

            //verify the uniqueness of coord names
            verifyCoordNameUnique(resolvedJobXml);
            this.jobId = storeToDB(bundleBean, resolvedJobXml);
            LogUtils.setLogInfo(bundleBean);

            if (dryrun) {
                Date startTime = bundleBean.getStartTime();
                long startTimeMilli = startTime.getTime();
                long endTimeMilli = startTimeMilli + (3600 * 1000);
                Date jobEndTime = bundleBean.getEndTime();
                Date endTime = new Date(endTimeMilli);
                if (endTime.compareTo(jobEndTime) > 0) {
                    endTime = jobEndTime;
                }
                jobId = bundleBean.getId();
                LOG.info("[" + jobId + "]: Update status to PREP");
                bundleBean.setStatus(Job.Status.PREP);
                try {
                    new XConfiguration(new StringReader(bundleBean.getConf()));
                }
                catch (IOException e1) {
                    LOG.warn("Configuration parse error. read from DB :" + bundleBean.getConf(), e1);
                }
                String output = bundleBean.getJobXml() + System.getProperty("line.separator");
                return output;
            }
            else {
                if (bundleBean.getKickoffTime() == null) {
                    // If there is no KickOffTime, default kickoff is NOW.
                    LOG.debug("Since kickoff time is not defined for job id " + jobId
                            + ". Queuing and BundleStartXCommand immediately after submission");
                    queue(new BundleStartXCommand(jobId));
                }
            }
        }
        catch (Exception ex) {
            throw new CommandException(ErrorCode.E1310, ex.getMessage(), ex);
        }
        LOG.info("ENDED Bundle Submit");
        return this.jobId;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#notifyParent()
     */
    @Override
    public void notifyParent() throws CommandException {
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    public String getEntityKey() {
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#isLockRequired()
     */
    @Override
    protected boolean isLockRequired() {
        return false;
    }

    @Override
    protected void loadState() throws CommandException {
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }

    @Override
    protected void eagerLoadState() throws CommandException {
    }

    @Override
    protected void eagerVerifyPrecondition() throws CommandException, PreconditionException {
        try {
            mergeDefaultConfig();
            String appXml = readAndValidateXml();
            bundleBean.setOrigJobXml(appXml);
            LOG.debug("jobXml after initial validation " + XmlUtils.prettyPrint(appXml).toString());
        }
        catch (BundleJobException ex) {
            LOG.warn("BundleJobException:  ", ex);
            throw new CommandException(ex);
        }
        catch (IllegalArgumentException iex) {
            LOG.warn("IllegalArgumentException:  ", iex);
            throw new CommandException(ErrorCode.E1310, iex.getMessage(), iex);
        }
        catch (Exception ex) {
            LOG.warn("Exception:  ", ex);
            throw new CommandException(ErrorCode.E1310, ex.getMessage(), ex);
        }
    }

    /**
     * Merge default configuration with user-defined configuration.
     *
     * @throws CommandException thrown if failed to merge configuration
     */
    protected void mergeDefaultConfig() throws CommandException {
        Path configDefault = null;
        try {
            String bundleAppPathStr = conf.get(OozieClient.BUNDLE_APP_PATH);
            Path bundleAppPath = new Path(bundleAppPathStr);
            String user = ParamChecker.notEmpty(conf.get(OozieClient.USER_NAME), OozieClient.USER_NAME);
            HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
            Configuration fsConf = has.createJobConf(bundleAppPath.toUri().getAuthority());
            FileSystem fs = has.createFileSystem(user, bundleAppPath.toUri(), fsConf);

            // app path could be a directory
            if (!fs.isFile(bundleAppPath)) {
                configDefault = new Path(bundleAppPath, CONFIG_DEFAULT);
            } else {
                configDefault = new Path(bundleAppPath.getParent(), CONFIG_DEFAULT);
            }

            if (fs.exists(configDefault)) {
                Configuration defaultConf = new XConfiguration(fs.open(configDefault));
                PropertiesUtils.checkDisallowedProperties(defaultConf, DISALLOWED_DEFAULT_PROPERTIES);
                XConfiguration.injectDefaults(defaultConf, conf);
            }
            else {
                LOG.info("configDefault Doesn't exist " + configDefault);
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
        LOG.debug("Merged CONF :" + XmlUtils.prettyPrint(conf).toString());
    }

    /**
     * Read the application XML and validate against bundle Schema
     *
     * @return validated bundle XML
     * @throws BundleJobException thrown if failed to read or validate xml
     */
    private String readAndValidateXml() throws BundleJobException {
        String appPath = ParamChecker.notEmpty(conf.get(OozieClient.BUNDLE_APP_PATH), OozieClient.BUNDLE_APP_PATH);
        String bundleXml = readDefinition(appPath);
        validateXml(bundleXml);
        return bundleXml;
    }

    /**
     * Read bundle definition.
     *
     * @param appPath application path.
     * @param user user name.
     * @param group group name.
     * @return bundle definition.
     * @throws BundleJobException thrown if the definition could not be read.
     */
    protected String readDefinition(String appPath) throws BundleJobException {
        String user = ParamChecker.notEmpty(conf.get(OozieClient.USER_NAME), OozieClient.USER_NAME);
        //Configuration confHadoop = CoordUtils.getHadoopConf(conf);
        try {
            URI uri = new URI(appPath);
            LOG.debug("user =" + user);
            HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
            Configuration fsConf = has.createJobConf(uri.getAuthority());
            FileSystem fs = has.createFileSystem(user, uri, fsConf);
            Path appDefPath = null;

            // app path could be a directory
            Path path = new Path(uri.getPath());
            if (!fs.isFile(path)) {
                appDefPath = new Path(path, BUNDLE_XML_FILE);
            } else {
                appDefPath = path;
            }

            Reader reader = new InputStreamReader(fs.open(appDefPath));
            StringWriter writer = new StringWriter();
            IOUtils.copyCharStream(reader, writer);
            return writer.toString();
        }
        catch (IOException ex) {
            LOG.warn("IOException :" + XmlUtils.prettyPrint(conf), ex);
            throw new BundleJobException(ErrorCode.E1301, ex.getMessage(), ex);
        }
        catch (URISyntaxException ex) {
            LOG.warn("URISyException :" + ex.getMessage());
            throw new BundleJobException(ErrorCode.E1302, appPath, ex.getMessage(), ex);
        }
        catch (HadoopAccessorException ex) {
            throw new BundleJobException(ex);
        }
        catch (Exception ex) {
            LOG.warn("Exception :", ex);
            throw new BundleJobException(ErrorCode.E1301, ex.getMessage(), ex);
        }
    }

    /**
     * Validate against Bundle XSD file
     *
     * @param xmlContent input bundle xml
     * @throws BundleJobException thrown if failed to validate xml
     */
    private void validateXml(String xmlContent) throws BundleJobException {
        javax.xml.validation.Schema schema = Services.get().get(SchemaService.class).getSchema(SchemaName.BUNDLE);
        Validator validator = schema.newValidator();
        try {
            validator.validate(new StreamSource(new StringReader(xmlContent)));
        }
        catch (SAXException ex) {
            LOG.warn("SAXException :", ex);
            throw new BundleJobException(ErrorCode.E0701, ex.getMessage(), ex);
        }
        catch (IOException ex) {
            LOG.warn("IOException :", ex);
            throw new BundleJobException(ErrorCode.E0702, ex.getMessage(), ex);
        }
    }

    /**
     * Write a Bundle Job into database
     *
     * @param Bundle job bean
     * @return job id
     * @throws CommandException thrown if failed to store bundle job bean to db
     */
    private String storeToDB(BundleJobBean bundleJob, String resolvedJobXml) throws CommandException {
        try {
            jobId = Services.get().get(UUIDService.class).generateId(ApplicationType.BUNDLE);

            bundleJob.setId(jobId);
            String name = XmlUtils.parseXml(bundleBean.getOrigJobXml()).getAttributeValue("name");
            name = ELUtils.resolveAppName(name, conf);
            bundleJob.setAppName(name);
            bundleJob.setAppPath(conf.get(OozieClient.BUNDLE_APP_PATH));
            // bundleJob.setStatus(BundleJob.Status.PREP); //This should be set in parent class.
            bundleJob.setCreatedTime(new Date());
            bundleJob.setUser(conf.get(OozieClient.USER_NAME));
            String group = ConfigUtils.getWithDeprecatedCheck(conf, OozieClient.JOB_ACL, OozieClient.GROUP_NAME, null);
            bundleJob.setGroup(group);
            bundleJob.setConf(XmlUtils.prettyPrint(conf).toString());
            bundleJob.setJobXml(resolvedJobXml);
            Element jobElement = XmlUtils.parseXml(resolvedJobXml);
            Element controlsElement = jobElement.getChild("controls", jobElement.getNamespace());
            if (controlsElement != null) {
                Element kickoffTimeElement = controlsElement.getChild("kick-off-time", jobElement.getNamespace());
                if (kickoffTimeElement != null && !kickoffTimeElement.getValue().isEmpty()) {
                    Date kickoffTime = DateUtils.parseDateOozieTZ(kickoffTimeElement.getValue());
                    bundleJob.setKickoffTime(kickoffTime);
                }
            }
            bundleJob.setLastModifiedTime(new Date());

            if (!dryrun) {
                BundleJobQueryExecutor.getInstance().insert(bundleJob);
            }
        }
        catch (Exception ex) {
            throw new CommandException(ErrorCode.E1301, ex.getMessage(), ex);
        }
        return jobId;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#getJob()
     */
    @Override
    public Job getJob() {
        return bundleBean;
    }

    /**
     * Resolve job xml with conf
     *
     * @param bundleXml bundle job xml
     * @param conf job configuration
     * @return resolved job xml
     * @throws BundleJobException thrown if failed to resolve variables
     */
    private String resolvedVars(String bundleXml, Configuration conf) throws BundleJobException {
        try {
            ELEvaluator eval = createEvaluator(conf);
            return eval.evaluate(bundleXml, String.class);
        }
        catch (Exception e) {
            throw new BundleJobException(ErrorCode.E1004, e.getMessage(), e);
        }
    }

    /**
     * Create ELEvaluator
     *
     * @param conf job configuration
     * @return ELEvaluator the evaluator for el function
     * @throws BundleJobException thrown if failed to create evaluator
     */
    public ELEvaluator createEvaluator(Configuration conf) throws BundleJobException {
        ELEvaluator eval;
        ELEvaluator.Context context;
        try {
            context = new ELEvaluator.Context();
            eval = new ELEvaluator(context);
            for (Map.Entry<String, String> entry : conf) {
                eval.setVariable(entry.getKey(), entry.getValue());
            }
        }
        catch (Exception e) {
            throw new BundleJobException(ErrorCode.E1004, e.getMessage(), e);
        }
        return eval;
    }

    /**
     * Verify the uniqueness of coordinator names
     *
     * @param resolved job xml
     * @throws CommandException thrown if failed to verify the uniqueness of coordinator names
     */
    @SuppressWarnings("unchecked")
    private Void verifyCoordNameUnique(String resolvedJobXml) throws CommandException {
        Set<String> set = new HashSet<String>();
        try {
            Element bAppXml = XmlUtils.parseXml(resolvedJobXml);
            List<Element> coordElems = bAppXml.getChildren("coordinator", bAppXml.getNamespace());
            for (Element elem : coordElems) {
                Attribute name = elem.getAttribute("name");
                if (name != null) {
                    String coordName = name.getValue();
                    try {
                        coordName = ELUtils.resolveAppName(name.getValue(), conf);
                    }
                    catch (Exception e) {
                        throw new CommandException(ErrorCode.E1321, e.getMessage(), e);
                    }
                    if (set.contains(coordName)) {
                        throw new CommandException(ErrorCode.E1304, name);
                    }
                    set.add(coordName);
                }
                else {
                    throw new CommandException(ErrorCode.E1305);
                }
            }
        }
        catch (JDOMException jex) {
            throw new CommandException(ErrorCode.E1301, jex.getMessage(), jex);
        }

        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#updateJob()
     */
    @Override
    public void updateJob() throws CommandException {
    }

    @Override
    public void performWrites() throws CommandException {
    }
}
