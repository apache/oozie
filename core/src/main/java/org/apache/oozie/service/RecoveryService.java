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
package org.apache.oozie.service;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.coord.CoordActionInputCheckXCommand;
import org.apache.oozie.command.coord.CoordActionReadyXCommand;
import org.apache.oozie.command.coord.CoordActionStartXCommand;
import org.apache.oozie.command.coord.CoordKillXCommand;
import org.apache.oozie.command.coord.CoordResumeXCommand;
import org.apache.oozie.command.coord.CoordSubmitXCommand;
import org.apache.oozie.command.coord.CoordSuspendXCommand;
import org.apache.oozie.command.wf.ActionEndXCommand;
import org.apache.oozie.command.wf.ActionStartXCommand;
import org.apache.oozie.command.wf.KillXCommand;
import org.apache.oozie.command.wf.ResumeXCommand;
import org.apache.oozie.command.wf.SignalXCommand;
import org.apache.oozie.command.wf.SuspendXCommand;
import org.apache.oozie.executor.jpa.BundleActionsGetWaitingOlderJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionsGetForRecoveryJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionsGetReadyGroupbyJobIDJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionsGetPendingJPAExecutor;
import org.apache.oozie.util.JobUtils;
import org.apache.oozie.util.XCallable;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Attribute;
import org.jdom.Element;
import org.jdom.JDOMException;

/**
 * The Recovery Service checks for pending actions and premater coordinator jobs older than a configured age and then
 * queues them for execution.
 */
public class RecoveryService implements Service {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "RecoveryService.";
    public static final String CONF_PREFIX_WF_ACTIONS = Service.CONF_PREFIX + "wf.actions.";
    public static final String CONF_PREFIX_COORD = Service.CONF_PREFIX + "coord.";
    public static final String CONF_PREFIX_BUNDLE = Service.CONF_PREFIX + "bundle.";
    /**
     * Time interval, in seconds, at which the recovery service will be scheduled to run.
     */
    public static final String CONF_SERVICE_INTERVAL = CONF_PREFIX + "interval";
    /**
     * The number of callables to be queued in a batch.
     */
    public static final String CONF_CALLABLE_BATCH_SIZE = CONF_PREFIX + "callable.batch.size";
    /**
     * Age of actions to queue, in seconds.
     */
    public static final String CONF_WF_ACTIONS_OLDER_THAN = CONF_PREFIX_WF_ACTIONS + "older.than";
    /**
     * Age of coordinator jobs to recover, in seconds.
     */
    public static final String CONF_COORD_OLDER_THAN = CONF_PREFIX_COORD + "older.than";

    /**
     * Age of Bundle jobs to recover, in seconds.
     */
    public static final String CONF_BUNDLE_OLDER_THAN = CONF_PREFIX_BUNDLE + "older.than";

    private static final String INSTRUMENTATION_GROUP = "recovery";
    private static final String INSTR_RECOVERED_ACTIONS_COUNTER = "actions";
    private static final String INSTR_RECOVERED_COORD_ACTIONS_COUNTER = "coord_actions";
    private static final String INSTR_RECOVERED_BUNDLE_ACTIONS_COUNTER = "bundle_actions";


    /**
     * RecoveryRunnable is the Runnable which is scheduled to run with the configured interval, and takes care of the
     * queuing of commands.
     */
    static class RecoveryRunnable implements Runnable {
        private final long olderThan;
        private final long coordOlderThan;
        private final long bundleOlderThan;
        private long delay = 0;
        private List<XCallable<?>> callables;
        private List<XCallable<?>> delayedCallables;
        private StringBuilder msg = null;
        private JPAService jpaService = null;

        public RecoveryRunnable(long olderThan, long coordOlderThan,long bundleOlderThan) {
            this.olderThan = olderThan;
            this.coordOlderThan = coordOlderThan;
            this.bundleOlderThan = bundleOlderThan;
        }

        public void run() {
            XLog.Info.get().clear();
            XLog log = XLog.getLog(getClass());
            msg = new StringBuilder();
            jpaService = Services.get().get(JPAService.class);
            runWFRecovery();
            runCoordActionRecovery();
            runCoordActionRecoveryForReady();
            runBundleRecovery();
            log.debug("QUEUING [{0}] for potential recovery", msg.toString());
            boolean ret = false;
            if (null != callables) {
                ret = Services.get().get(CallableQueueService.class).queueSerial(callables);
                if (ret == false) {
                    log.warn("Unable to queue the callables commands for RecoveryService. "
                            + "Most possibly command queue is full. Queue size is :"
                            + Services.get().get(CallableQueueService.class).queueSize());
                }
                callables = null;
            }
            if (null != delayedCallables) {
                ret = Services.get().get(CallableQueueService.class).queueSerial(delayedCallables, this.delay);
                if (ret == false) {
                    log.warn("Unable to queue the delayedCallables commands for RecoveryService. "
                            + "Most possibly Callable queue is full. Queue size is :"
                            + Services.get().get(CallableQueueService.class).queueSize());
                }
                delayedCallables = null;
                this.delay = 0;
            }
        }

        private void runBundleRecovery(){
            XLog.Info.get().clear();
            XLog log = XLog.getLog(getClass());

            try {
                List<BundleActionBean> bactions = jpaService.execute(new BundleActionsGetWaitingOlderJPAExecutor(bundleOlderThan));
                msg.append(", BUNDLE_ACTIONS : " + bactions.size());
                for (BundleActionBean baction : bactions) {
                    Services.get().get(InstrumentationService.class).get().incr(INSTRUMENTATION_GROUP,
                            INSTR_RECOVERED_BUNDLE_ACTIONS_COUNTER, 1);
                    if(baction.getStatus() == Job.Status.PREP){
                        BundleJobBean bundleJob = null;
                        try {
                            if (jpaService != null) {
                                bundleJob = jpaService.execute(new BundleJobGetJPAExecutor(baction.getBundleId()));
                            }
                            if(bundleJob != null){
                                Element bAppXml = XmlUtils.parseXml(bundleJob.getJobXml());
                                List<Element> coordElems = bAppXml.getChildren("coordinator", bAppXml.getNamespace());
                                for (Element coordElem : coordElems) {
                                    Attribute name = coordElem.getAttribute("name");
                                    if (name.getValue().equals(baction.getCoordName())) {
                                        Configuration coordConf = mergeConfig(coordElem,bundleJob);
                                        coordConf.set(OozieClient.BUNDLE_ID, baction.getBundleId());
                                        queueCallable(new CoordSubmitXCommand(coordConf, bundleJob.getAuthToken(), bundleJob.getId(), name.getValue()));
                                    }
                                }
                            }
                        }
                        catch (JDOMException jex) {
                            throw new CommandException(ErrorCode.E1301, jex);
                        }
                        catch (JPAExecutorException je) {
                            throw new CommandException(je);
                        }
                    }
                    else if(baction.getStatus() == Job.Status.KILLED){
                        queueCallable(new CoordKillXCommand(baction.getCoordId()));
                    }
                    else if(baction.getStatus() == Job.Status.SUSPENDED){
                        queueCallable(new CoordSuspendXCommand(baction.getCoordId()));
                    }
                    else if(baction.getStatus() == Job.Status.RUNNING){
                        queueCallable(new CoordResumeXCommand(baction.getCoordId()));
                    }
                }
            }
            catch (Exception ex) {
                log.error("Exception, {0}", ex.getMessage(), ex);
            }
        }

        /**
         * Recover coordinator actions that are staying in WAITING or SUBMITTED too long
         */
        private void runCoordActionRecovery() {
            XLog.Info.get().clear();
            XLog log = XLog.getLog(getClass());

            try {
                List<CoordinatorActionBean> cactions = jpaService.execute(new CoordActionsGetForRecoveryJPAExecutor(coordOlderThan));
                msg.append(", COORD_ACTIONS : " + cactions.size());
                for (CoordinatorActionBean caction : cactions) {
                    Services.get().get(InstrumentationService.class).get().incr(INSTRUMENTATION_GROUP,
                                                                                INSTR_RECOVERED_COORD_ACTIONS_COUNTER, 1);
                    if (caction.getStatus() == CoordinatorActionBean.Status.WAITING) {
						queueCallable(new CoordActionInputCheckXCommand(
								caction.getId(), caction.getJobId()));

						log.info("Recover a WAITTING coord action and resubmit CoordActionInputCheckXCommand :"
								+ caction.getId());
                    }
                    else if (caction.getStatus() == CoordinatorActionBean.Status.SUBMITTED) {
                        CoordinatorJobBean coordJob = jpaService.execute(new CoordJobGetJPAExecutor(caction.getJobId()));
						queueCallable(new CoordActionStartXCommand(
								caction.getId(), coordJob.getUser(),
								coordJob.getAuthToken(), caction.getJobId()));

                        log.info("Recover a SUBMITTED coord action and resubmit CoordActionStartCommand :" + caction.getId());
                    }
                    else if (caction.getStatus() == CoordinatorActionBean.Status.SUSPENDED) {
                        if (caction.getExternalId() != null) {
                            queueCallable(new SuspendXCommand(caction.getExternalId()));
                            log.debug("Recover a SUSPENDED coord action and resubmit SuspendXCommand :" + caction.getId());
                        }
                    }
                    else if (caction.getStatus() == CoordinatorActionBean.Status.KILLED) {
                        if (caction.getExternalId() != null) {
                            queueCallable(new KillXCommand(caction.getExternalId()));
                            log.debug("Recover a KILLED coord action and resubmit KillXCommand :" + caction.getId());
                        }
                    }
                    else if (caction.getStatus() == CoordinatorActionBean.Status.RUNNING) {
                        if (caction.getExternalId() != null) {
                            queueCallable(new ResumeXCommand(caction.getExternalId()));
                            log.debug("Recover a RUNNING coord action and resubmit ResumeXCommand :" + caction.getId());
                        }
                    }
                }
            }
            catch (Exception ex) {
                log.error("Exception, {0}", ex.getMessage(), ex);
            }
        }

        /**
         * Recover coordinator actions that are staying in READY too long
         */
        private void runCoordActionRecoveryForReady() {
            XLog.Info.get().clear();
            XLog log = XLog.getLog(getClass());

            try {
                List<String> jobids = jpaService.execute(new CoordActionsGetReadyGroupbyJobIDJPAExecutor(coordOlderThan));
                msg.append(", COORD_READY_JOBS : " + jobids.size());
                for (String jobid : jobids) {
                        queueCallable(new CoordActionReadyXCommand(jobid));

                    log.info("Recover READY coord actions for jobid :" + jobid);
                }
            }
            catch (Exception ex) {
                log.error("Exception, {0}", ex.getMessage(), ex);
            }
        }

        /**
         * Recover wf actions
         */
        private void runWFRecovery() {
            XLog.Info.get().clear();
            XLog log = XLog.getLog(getClass());
            // queue command for action recovery
            try {
                List<WorkflowActionBean> actions = null;
                try {
                    actions = jpaService.execute(new WorkflowActionsGetPendingJPAExecutor(olderThan));
                }
                catch (JPAExecutorException ex) {
                    log.warn("Exception while reading pending actions from storage", ex);
                }
                //log.debug("QUEUING[{0}] pending wf actions for potential recovery", actions.size());
                msg.append(" WF_ACTIONS " + actions.size());

                for (WorkflowActionBean action : actions) {
                    Services.get().get(InstrumentationService.class).get().incr(INSTRUMENTATION_GROUP,
                            INSTR_RECOVERED_ACTIONS_COUNTER, 1);
                    if (action.getStatus() == WorkflowActionBean.Status.PREP
                            || action.getStatus() == WorkflowActionBean.Status.START_MANUAL) {
						queueCallable(new ActionStartXCommand(action.getId(),
								action.getType()));
                    }
                    else if (action.getStatus() == WorkflowActionBean.Status.START_RETRY) {
                        Date nextRunTime = action.getPendingAge();
                            queueCallable(new ActionStartXCommand(action.getId(), action.getType()), nextRunTime.getTime()
                                    - System.currentTimeMillis());
                    }
                    else if (action.getStatus() == WorkflowActionBean.Status.DONE
                            || action.getStatus() == WorkflowActionBean.Status.END_MANUAL) {
                            queueCallable(new ActionEndXCommand(action.getId(), action.getType()));
                    }
                    else if (action.getStatus() == WorkflowActionBean.Status.END_RETRY) {
                        Date nextRunTime = action.getPendingAge();
						queueCallable(new ActionEndXCommand(action.getId(),
								action.getType()), nextRunTime.getTime()
								- System.currentTimeMillis());

                    }
                    else if (action.getStatus() == WorkflowActionBean.Status.OK
                            || action.getStatus() == WorkflowActionBean.Status.ERROR) {
						queueCallable(new SignalXCommand(action.getJobId(),
								action.getId()));
                    }
                    else if (action.getStatus() == WorkflowActionBean.Status.USER_RETRY) {
						queueCallable(new ActionStartXCommand(action.getId(),
								action.getType()));
                    }
                }
            }
            catch (Exception ex) {
                log.error("Exception, {0}", ex.getMessage(), ex);
            }
        }

        /**
         * Adds callables to a list. If the number of callables in the list reaches {@link
         * RecoveryService#CONF_CALLABLE_BATCH_SIZE}, the entire batch is queued and the callables list is reset.
         *
         * @param callable the callable to queue.
         */
        private void queueCallable(XCallable<?> callable) {
            if (callables == null) {
                callables = new ArrayList<XCallable<?>>();
            }
            callables.add(callable);
            if (callables.size() == Services.get().getConf().getInt(CONF_CALLABLE_BATCH_SIZE, 10)) {
                boolean ret = Services.get().get(CallableQueueService.class).queueSerial(callables);
                if (ret == false) {
                    XLog.getLog(getClass()).warn(
                            "Unable to queue the callables commands for RecoveryService. "
                                    + "Most possibly command queue is full. Queue size is :"
                                    + Services.get().get(CallableQueueService.class).queueSize());
                }
                callables = new ArrayList<XCallable<?>>();
            }
        }

        /**
         * Adds callables to a list. If the number of callables in the list reaches {@link
         * RecoveryService#CONF_CALLABLE_BATCH_SIZE}, the entire batch is queued with the delay set to the maximum delay
         * of the callables in the list. The callables list and the delay is reset.
         *
         * @param callable the callable to queue.
         * @param delay the delay for the callable.
         */
        private void queueCallable(XCallable<?> callable, long delay) {
            if (delayedCallables == null) {
                delayedCallables = new ArrayList<XCallable<?>>();
            }
            this.delay = Math.max(this.delay, delay);
            delayedCallables.add(callable);
            if (delayedCallables.size() == Services.get().getConf().getInt(CONF_CALLABLE_BATCH_SIZE, 10)) {
                boolean ret = Services.get().get(CallableQueueService.class).queueSerial(delayedCallables, this.delay);
                if (ret == false) {
                    XLog.getLog(getClass()).warn("Unable to queue the delayedCallables commands for RecoveryService. "
                            + "Most possibly Callable queue is full. Queue size is :"
                            + Services.get().get(CallableQueueService.class).queueSize());
                }
                delayedCallables = new ArrayList<XCallable<?>>();
                this.delay = 0;
            }
        }
    }

    /**
     * Initializes the RecoveryService.
     *
     * @param services services instance.
     */
    @Override
    public void init(Services services) {
        Configuration conf = services.getConf();
        Runnable recoveryRunnable = new RecoveryRunnable(conf.getInt(CONF_WF_ACTIONS_OLDER_THAN, 120), conf.getInt(
                CONF_COORD_OLDER_THAN, 600),conf.getInt(CONF_BUNDLE_OLDER_THAN, 600));
        services.get(SchedulerService.class).schedule(recoveryRunnable, 10, conf.getInt(CONF_SERVICE_INTERVAL, 600),
                                                      SchedulerService.Unit.SEC);
    }

    /**
     * Destroy the Recovery Service.
     */
    @Override
    public void destroy() {
    }

    /**
     * Return the public interface for the Recovery Service.
     *
     * @return {@link RecoveryService}.
     */
    @Override
    public Class<? extends Service> getInterface() {
        return RecoveryService.class;
    }

    /**
     * Merge Bundle job config and the configuration from the coord job to pass
     * to Coord Engine
     *
     * @param coordElem the coordinator configuration
     * @return Configuration merged configuration
     * @throws CommandException thrown if failed to merge configuration
     */
    private static Configuration mergeConfig(Element coordElem,BundleJobBean bundleJob) throws CommandException {
        XLog.Info.get().clear();
        XLog log = XLog.getLog("RecoveryService");

        String jobConf = bundleJob.getConf();
        // Step 1: runConf = jobConf
        Configuration runConf = null;
        try {
            runConf = new XConfiguration(new StringReader(jobConf));
        }
        catch (IOException e1) {
            log.warn("Configuration parse error in:" + jobConf);
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
                log.warn("Configuration parse error in:" + strConfig);
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
}
