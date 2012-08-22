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
import java.io.StringReader;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.StartTransitionXCommand;
import org.apache.oozie.command.coord.CoordSubmitXCommand;
import org.apache.oozie.executor.jpa.BulkUpdateInsertJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.JobUtils;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Attribute;
import org.jdom.Element;
import org.jdom.JDOMException;

/**
 * The command to start Bundle job
 */
public class BundleStartXCommand extends StartTransitionXCommand {
    private final String jobId;
    private BundleJobBean bundleJob;
    private JPAService jpaService = null;

    /**
     * The constructor for class {@link BundleStartXCommand}
     *
     * @param jobId the bundle job id
     */
    public BundleStartXCommand(String jobId) {
        super("bundle_start", "bundle_start", 1);
        this.jobId = ParamChecker.notEmpty(jobId, "jobId");
    }

    /**
     * The constructor for class {@link BundleStartXCommand}
     *
     * @param jobId the bundle job id
     * @param dryrun true if dryrun is enable
     */
    public BundleStartXCommand(String jobId, boolean dryrun) {
        super("bundle_start", "bundle_start", 1, dryrun);
        this.jobId = ParamChecker.notEmpty(jobId, "jobId");
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    public String getEntityKey() {
        return jobId;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#isLockRequired()
     */
    @Override
    protected boolean isLockRequired() {
        return true;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        eagerVerifyPrecondition();
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#eagerVerifyPrecondition()
     */
    @Override
    protected void eagerVerifyPrecondition() throws CommandException, PreconditionException {
        if (bundleJob.getStatus() != Job.Status.PREP) {
            String msg = "Bundle " + bundleJob.getId() + " is not in PREP status. It is in : " + bundleJob.getStatus();
            LOG.info(msg);
            throw new PreconditionException(ErrorCode.E1100, msg);
        }
    }
    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#loadState()
     */
    @Override
    public void loadState() throws CommandException {
        eagerLoadState();
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#eagerLoadState()
     */
    @Override
    public void eagerLoadState() throws CommandException {
        try {
            jpaService = Services.get().get(JPAService.class);

            if (jpaService != null) {
                this.bundleJob = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
                LogUtils.setLogInfo(bundleJob, logInfo);
                super.setJob(bundleJob);

            }
            else {
                throw new CommandException(ErrorCode.E0610);
            }
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.StartTransitionXCommand#StartChildren()
     */
    @Override
    public void StartChildren() throws CommandException {
        LOG.debug("Started coord jobs for the bundle=[{0}]", jobId);
        insertBundleActions();
        startCoordJobs();
        LOG.debug("Ended coord jobs for the bundle=[{0}]", jobId);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#notifyParent()
     */
    @Override
    public void notifyParent() {
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.StartTransitionXCommand#performWrites()
     */
    @Override
    public void performWrites() throws CommandException {
        try {
            jpaService.execute(new BulkUpdateInsertJPAExecutor(updateList, insertList));
        }
        catch (JPAExecutorException e) {
            throw new CommandException(e);
        }
    }

    /**
     * Insert bundle actions
     *
     * @throws CommandException thrown if failed to create bundle actions
     */
    @SuppressWarnings("unchecked")
    private void insertBundleActions() throws CommandException {
        if (bundleJob != null) {
            Map<String, Boolean> map = new HashMap<String, Boolean>();
            try {
                Element bAppXml = XmlUtils.parseXml(bundleJob.getJobXml());
                List<Element> coordElems = bAppXml.getChildren("coordinator", bAppXml.getNamespace());
                for (Element elem : coordElems) {
                    Attribute name = elem.getAttribute("name");
                    Attribute critical = elem.getAttribute("critical");
                    if (name != null) {
                        if (map.containsKey(name.getValue())) {
                            throw new CommandException(ErrorCode.E1304, name);
                        }
                        boolean isCritical = false;
                        if (critical != null && Boolean.parseBoolean(critical.getValue())) {
                            isCritical = true;
                        }
                        map.put(name.getValue(), isCritical);
                    }
                    else {
                        throw new CommandException(ErrorCode.E1305);
                    }
                }
            }
            catch (JDOMException jex) {
                throw new CommandException(ErrorCode.E1301, jex);
            }

            // if there is no coordinator for this bundle, failed it.
            if (map.isEmpty()) {
                bundleJob.setStatus(Job.Status.FAILED);
                bundleJob.resetPending();
                try {
                    jpaService.execute(new BundleJobUpdateJPAExecutor(bundleJob));
                }
                catch (JPAExecutorException jex) {
                    throw new CommandException(jex);
                }

                LOG.debug("No coord jobs for the bundle=[{0}], failed it!!", jobId);
                throw new CommandException(ErrorCode.E1318, jobId);
            }

            for (Entry<String, Boolean> coordName : map.entrySet()) {
                BundleActionBean action = createBundleAction(jobId, coordName.getKey(), coordName.getValue());
                insertList.add(action);
            }
        }
        else {
            throw new CommandException(ErrorCode.E0604, jobId);
        }
    }

    private BundleActionBean createBundleAction(String jobId, String coordName, boolean isCritical) {
        BundleActionBean action = new BundleActionBean();
        action.setBundleActionId(jobId + "_" + coordName);
        action.setBundleId(jobId);
        action.setCoordName(coordName);
        action.setStatus(Job.Status.PREP);
        action.setLastModifiedTime(new Date());
        if (isCritical) {
            action.setCritical();
        }
        else {
            action.resetCritical();
        }
        return action;
    }

    /**
     * Start Coord Jobs
     *
     * @throws CommandException thrown if failed to start coord jobs
     */
    @SuppressWarnings("unchecked")
    private void startCoordJobs() throws CommandException {
        if (bundleJob != null) {
            try {
                Element bAppXml = XmlUtils.parseXml(bundleJob.getJobXml());
                List<Element> coordElems = bAppXml.getChildren("coordinator", bAppXml.getNamespace());
                for (Element coordElem : coordElems) {
                    Attribute name = coordElem.getAttribute("name");
                    Configuration coordConf = mergeConfig(coordElem);
                    coordConf.set(OozieClient.BUNDLE_ID, jobId);

                    queue(new CoordSubmitXCommand(coordConf, bundleJob.getAuthToken(), bundleJob.getId(), name.getValue()));

                }
                updateBundleAction();
            }
            catch (JDOMException jex) {
                throw new CommandException(ErrorCode.E1301, jex);
            }
            catch (JPAExecutorException je) {
                throw new CommandException(je);
            }
        }
        else {
            throw new CommandException(ErrorCode.E0604, jobId);
        }
    }

    private void updateBundleAction() throws JPAExecutorException {
        for(JsonBean bAction : insertList) {
            BundleActionBean action = (BundleActionBean) bAction;
            action.incrementAndGetPending();
            action.setLastModifiedTime(new Date());
        }
    }

    /**
     * Merge Bundle job config and the configuration from the coord job to pass
     * to Coord Engine
     *
     * @param coordElem the coordinator configuration
     * @return Configuration merged configuration
     * @throws CommandException thrown if failed to merge configuration
     */
    private Configuration mergeConfig(Element coordElem) throws CommandException {
        String jobConf = bundleJob.getConf();
        // Step 1: runConf = jobConf
        Configuration runConf = null;
        try {
            runConf = new XConfiguration(new StringReader(jobConf));
        }
        catch (IOException e1) {
            LOG.warn("Configuration parse error in:" + jobConf);
            throw new CommandException(ErrorCode.E1306, e1.getMessage(), e1);
        }
        // Step 2: Merge local properties into runConf
        // extract 'property' tags under 'configuration' block in the coordElem
        // convert Element to XConfiguration
        Element localConfigElement = coordElem.getChild("configuration", coordElem.getNamespace());

        if (localConfigElement != null) {
            String strConfig = XmlUtils.prettyPrint(localConfigElement).toString();
            Configuration localConf;
            try {
                localConf = new XConfiguration(new StringReader(strConfig));
            }
            catch (IOException e1) {
                LOG.warn("Configuration parse error in:" + strConfig);
                throw new CommandException(ErrorCode.E1307, e1.getMessage(), e1);
            }

            // copy configuration properties in the coordElem to the runConf
            XConfiguration.copy(localConf, runConf);
        }

        // Step 3: Extract value of 'app-path' in coordElem, save it as a
        // new property called 'oozie.coord.application.path', and normalize.
        String appPath = coordElem.getChild("app-path", coordElem.getNamespace()).getValue();
        runConf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
        // Normalize coordinator appPath here;
        try {
            JobUtils.normalizeAppPath(runConf.get(OozieClient.USER_NAME), runConf.get(OozieClient.GROUP_NAME), runConf);
        }
        catch (IOException e) {
            throw new CommandException(ErrorCode.E1001, runConf.get(OozieClient.COORDINATOR_APP_PATH));
        }
        return runConf;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#getJob()
     */
    @Override
    public Job getJob() {
        return bundleJob;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#updateJob()
     */
    @Override
    public void updateJob() throws CommandException {
        updateList.add(bundleJob);
    }
}
