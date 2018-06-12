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

package org.apache.oozie.sla;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.AppType;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.event.JobEvent;
import org.apache.oozie.client.event.SLAEvent.SLAStatus;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.executor.jpa.BatchQueryExecutor.UpdateEntry;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.SLARegistrationQueryExecutor;
import org.apache.oozie.executor.jpa.SLARegistrationQueryExecutor.SLARegQuery;
import org.apache.oozie.executor.jpa.SLASummaryQueryExecutor;
import org.apache.oozie.executor.jpa.SLASummaryQueryExecutor.SLASummaryQuery;
import org.apache.oozie.executor.jpa.sla.SLASummaryGetRecordsOnRestartJPAExecutor;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.service.InstrumentationService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.SchedulerService;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.service.SLAService;

import com.google.common.annotations.VisibleForTesting;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.Pair;
import org.apache.oozie.util.XLog;

/**
 * Implementation class for SLACalculator that calculates SLA related to
 * start/end/duration of jobs using a memory-based map
 */
public class SLACalculatorMemory implements SLACalculator {

    private static XLog LOG = XLog.getLog(SLACalculatorMemory.class);
    // TODO optimization priority based insertion/processing/bumping up-down
    private Map<String, SLACalcStatus> slaMap;
    protected Set<String> historySet;
    private static int capacity;
    private static JPAService jpaService;
    protected EventHandlerService eventHandler;
    private static int modifiedAfter;
    private static long jobEventLatency;
    private Instrumentation instrumentation;
    public static final String INSTRUMENTATION_GROUP = "sla-calculator";
    public static final String SLA_MAP = "sla-map";
    private int maxRetryCount;

    @Override
    public void init(Configuration conf) throws ServiceException {
        capacity = ConfigurationService.getInt(conf, SLAService.CONF_CAPACITY);
        jobEventLatency = ConfigurationService.getInt(conf, SLAService.CONF_JOB_EVENT_LATENCY);
        maxRetryCount = ConfigurationService.getInt(conf, SLAService.CONF_MAXIMUM_RETRY_COUNT);
        slaMap = new ConcurrentHashMap<String, SLACalcStatus>();
        historySet = Collections.synchronizedSet(new HashSet<String>());
        jpaService = Services.get().get(JPAService.class);
        eventHandler = Services.get().get(EventHandlerService.class);
        instrumentation = Services.get().get(InstrumentationService.class).get();
        // load events modified after
        modifiedAfter = conf.getInt(SLAService.CONF_EVENTS_MODIFIED_AFTER, 7);
        loadOnRestart();
        Runnable purgeThread = new HistoryPurgeWorker();
        // schedule runnable by default 1 hours
        Services.get()
                .get(SchedulerService.class)
                .schedule(purgeThread, 3600, Services.get().getConf().getInt(SLAService.CONF_SLA_HISTORY_PURGE_INTERVAL, 3600),
                        SchedulerService.Unit.SEC);
    }

    public class HistoryPurgeWorker extends Thread {

        public HistoryPurgeWorker() {
        }

        @Override
        public void run() {
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
            Iterator<String> jobItr = historySet.iterator();
            while (jobItr.hasNext()) {
                String jobId = jobItr.next();
                LOG.debug(" Running HistoryPurgeWorker for " + jobId);
                try {
                    boolean isDone = SLAXCommandFactory.getSLAJobHistoryXCommand(jobId).call();
                    if (isDone) {
                        LOG.debug("[{0}] job is finished and processed. Removing from history");
                        jobItr.remove();
                    }
                }
                catch (CommandException e) {
                    if (e.getErrorCode().equals(ErrorCode.E0604) || e.getErrorCode().equals(ErrorCode.E0605)) {
                        LOG.warn("Job is not found in db: " + jobId, e);
                        jobItr.remove();
                    }
                    else {
                        LOG.error("Failed to fetch the job: " + jobId, e);
                    }
                }
            }
        }
    }

    private void loadOnRestart() {
        try {
            List<SLASummaryBean> summaryBeans = jpaService
                    .execute(new SLASummaryGetRecordsOnRestartJPAExecutor(modifiedAfter));
            for (SLASummaryBean summaryBean : summaryBeans) {
                String jobId = summaryBean.getId();
                putAndIncrement(jobId, new SLACalcStatus(summaryBean));
            }
            LOG.info("Loaded {0} SLASummary object after restart", slaMap.size());
        }
        catch (Exception e) {
            LOG.warn("Failed to retrieve SLASummary records on restart", e);
        }
    }

    @Override
    public int size() {
        return slaMap.size();
    }

    @VisibleForTesting
    public Set<String> getHistorySet(){
        return historySet;
    }

    @Override
    public SLACalcStatus get(String jobId) throws JPAExecutorException {
        SLACalcStatus memObj;
        memObj = slaMap.get(jobId);
        if (memObj == null && historySet.contains(jobId)) {
            memObj = new SLACalcStatus(SLASummaryQueryExecutor.getInstance()
                    .get(SLASummaryQuery.GET_SLA_SUMMARY, jobId), SLARegistrationQueryExecutor.getInstance().get(
                    SLARegQuery.GET_SLA_REG_ON_RESTART, jobId));
        }
        return memObj;
    }

    /**
     * Get SLACalcStatus from map if SLARegistration is not null, else create a new SLACalcStatus
     * This function deosn't update  slaMap
     * @param jobId
     * @return SLACalcStatus returns SLACalcStatus from map if SLARegistration is not null,
     * else create a new SLACalcStatus
     * @throws JPAExecutorException
     */
    private SLACalcStatus getOrCreateSLACalcStatus(String jobId) throws JPAExecutorException {
        SLACalcStatus memObj;
        memObj = slaMap.get(jobId);
        // if the request came from immediately after restart don't use map SLACalcStatus.
        if (memObj == null || memObj.getSLARegistrationBean() == null) {
            SLARegistrationBean registrationBean = SLARegistrationQueryExecutor.getInstance()
                    .get(SLARegQuery.GET_SLA_REG_ON_RESTART, jobId);
            SLASummaryBean summaryBean = memObj == null
                    ? SLASummaryQueryExecutor.getInstance().get(SLASummaryQuery.GET_SLA_SUMMARY, jobId)
                    : memObj.getSLASummaryBean();
            return new SLACalcStatus(summaryBean, registrationBean);
        }
        return memObj;
    }

    @Override
    public Iterator<String> iterator() {
        return slaMap.keySet().iterator();
    }

    @Override
    public boolean isEmpty() {
        return slaMap.isEmpty();
    }

    @Override
    public void clear() {
        final int originalSize = slaMap.size();
        slaMap.clear();
        historySet.clear();
        instrumentation.decr(INSTRUMENTATION_GROUP, SLA_MAP, originalSize);
    }

    /**
     * Invoked via periodic run, update the SLA for registered jobs.
     * <p>
     * Track the number of times the {@link SLACalcStatus} entry has not been processed successfully, and when a preconfigured
     * {code oozie.sla.service.SLAService.maximum.retry.count} is reached, remove any {@link SLACalculatorMemory#slaMap} entries
     * that are causing {@code JPAExecutorException}s of certain {@link ErrorCode}s.
     * @param jobId the workflow or coordinator job or action ID the SLA is tracked against
     */
    void updateJobSla(String jobId) throws Exception {
        SLACalcStatus slaCalc = slaMap.get(jobId);

        if (slaCalc == null) {
            // job might be processed and removed from map by addJobStatus
            return;
        }
        boolean firstCheckAfterRetstart = checkAndUpdateSLACalcAfterRestart(slaCalc);
        // get eventProcessed on DB for validation in HA
        SLASummaryBean summaryBean = null;
        try {
            summaryBean = SLASummaryQueryExecutor.getInstance()
                    .get(SLASummaryQuery.GET_SLA_SUMMARY_EVENTPROCESSED_LAST_MODIFIED, jobId);
            resetRetryCount(jobId);
        }
        catch (final JPAExecutorException e) {
            if (e.getErrorCode().equals(ErrorCode.E0603)
                    || e.getErrorCode().equals(ErrorCode.E0604)
                    || e.getErrorCode().equals(ErrorCode.E0605)) {
                LOG.debug("job [{0}] is not in DB, removing from Memory", jobId);
                incrementRetryCountAndRemove(jobId);
                return;
            }
            throw e;
        }
        byte eventProc = summaryBean.getEventProcessed();
        slaCalc.setEventProcessed(eventProc);
        if (eventProc >= 7) {
            if (eventProc == 7) {
                historySet.add(jobId);
            }
            removeAndDecrement(jobId);
            LOG.trace("Removed Job [{0}] from map as SLA processed", jobId);
        }
        else {
            if (!slaCalc.getLastModifiedTime().equals(summaryBean.getLastModifiedTime())) {
                // Update last modified time.
                slaCalc.setLastModifiedTime(summaryBean.getLastModifiedTime());
                reloadExpectedTimeAndConfig(slaCalc);
                LOG.debug("Last modified time has changed for job " + jobId + " reloading config from DB");
            }
            if (firstCheckAfterRetstart || isChanged(slaCalc)) {
                LOG.debug("{0} job has SLA event change. EventProc = {1}, status = {2}", slaCalc.getId(),
                        slaCalc.getEventProcessed(), slaCalc.getJobStatus());
                try {
                    SLAXCommandFactory.getSLAEventXCommand(slaCalc).call();
                    checkEventProc(slaCalc);
                }
                catch (XException e) {
                    if (e.getErrorCode().equals(ErrorCode.E0604) || e.getErrorCode().equals(ErrorCode.E0605)) {
                        LOG.debug("job [{0}] is is not in DB, removing from Memory", slaCalc.getId());
                        removeAndDecrement(jobId);
                    }
                    else {
                        if (firstCheckAfterRetstart) {
                            slaCalc.setSLARegistrationBean(null);
                        }
                    }
                }
            }
        }
    }

    private boolean isChanged(SLACalcStatus slaCalc) {
        SLARegistrationBean reg = slaCalc.getSLARegistrationBean();
        byte eventProc = slaCalc.getEventProcessed();

        if ((eventProc & 1) == 0) { // first bit (start-processed) unset
            if (reg.getExpectedStart() != null) {
                if (reg.getExpectedStart().getTime() + jobEventLatency < System.currentTimeMillis()) {
                    return true;
                }
            }
            else {
                return true;
            }
        }
        if (eventProc != 8 && ((eventProc >> 1) & 1) == 0) {
            if (reg.getExpectedDuration() == -1) {
                return true;
            }
            else if (slaCalc.getActualStart() != null) {
                if ((reg.getExpectedDuration() + jobEventLatency) < (System.currentTimeMillis() - slaCalc
                        .getActualStart().getTime())) {
                    return true;
                }
            }
        }
        if (eventProc < 4) {
            if (reg.getExpectedEnd().getTime() + jobEventLatency < System.currentTimeMillis()) {
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("rawtypes")
    private void updateDBSlaConfig(SLACalcStatus slaCalc, List<UpdateEntry> updateList) throws JPAExecutorException {
        updateList.add(new UpdateEntry<SLARegQuery>(SLARegQuery.UPDATE_SLA_CONFIG, slaCalc.getSLARegistrationBean()));
        slaCalc.setLastModifiedTime(new Date());
        updateList.add(new UpdateEntry<SLASummaryQuery>(SLASummaryQuery.UPDATE_SLA_SUMMARY_LAST_MODIFIED_TIME,
                new SLASummaryBean(slaCalc)));
    }

    @SuppressWarnings("rawtypes")
    private void updateDBSlaExpectedValues(SLACalcStatus slaCalc, List<UpdateEntry> updateList)
            throws JPAExecutorException {
        slaCalc.setLastModifiedTime(new Date());
        updateList.add(new UpdateEntry<SLARegQuery>(SLARegQuery.UPDATE_SLA_EXPECTED_VALUE, slaCalc
                .getSLARegistrationBean()));
        updateList.add(new UpdateEntry<SLASummaryQuery>(SLASummaryQuery.UPDATE_SLA_SUMMARY_FOR_EXPECTED_TIMES,
                new SLASummaryBean(slaCalc)));
    }

    @SuppressWarnings("rawtypes")
    private void executeBatchQuery(List<UpdateEntry> updateList) throws JPAExecutorException {
        BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(null, updateList, null);
    }

    /**
     * Periodically run by the SLAService worker threads to update SLA status by
     * iterating through all the jobs in the map
     */
    @Override
    public void updateAllSlaStatus() {
        LOG.info("Running periodic SLA check");
        Iterator<String> iterator = slaMap.keySet().iterator();
        while (iterator.hasNext()) {
            String jobId = iterator.next();
            try {
                LOG.trace("Processing SLA for jobid={0}", jobId);
                updateJobSla(jobId);
            }
            catch (Exception e) {
                setLogPrefix(jobId);
                LOG.error("Exception in SLA processing for job [{0}]", jobId, e);
                LogUtils.clearLogPrefix();
            }
        }
    }

    /**
     * Register a new job into the map for SLA tracking
     * @return true if successful
     */
    @Override
    public boolean addRegistration(String jobId, SLARegistrationBean reg) throws JPAExecutorException {
        try {
            if (slaMap.size() < capacity) {
                SLACalcStatus slaCalc = new SLACalcStatus(reg);
                slaCalc.setSLAStatus(SLAStatus.NOT_STARTED);
                slaCalc.setJobStatus(getJobStatus(reg.getAppType()));
                List<JsonBean> insertList = new ArrayList<JsonBean>();
                final SLASummaryBean summaryBean = new SLASummaryBean(slaCalc);
                final Timestamp currentTime = DateUtils.convertDateToTimestamp(new Date());
                reg.setCreatedTimestamp(currentTime);
                summaryBean.setCreatedTimestamp(currentTime);
                insertList.add(reg);
                insertList.add(summaryBean);
                BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(insertList, null, null);
                putAndIncrement(jobId, slaCalc);
                LOG.trace("SLA Registration Event - Job:" + jobId);
                return true;
            }
            else {
                setLogPrefix(reg.getId());
                LOG.error(
                        "SLACalculator memory capacity reached. Cannot add or update new SLA Registration entry for job [{0}]",
                        reg.getId());
                LogUtils.clearLogPrefix();
            }
        }
        catch (JPAExecutorException jpa) {
            throw jpa;
        }
        return false;
    }

    private String getJobStatus(AppType appType) {
        String status = null;
        switch (appType) {
            case COORDINATOR_ACTION:
                status = CoordinatorAction.Status.WAITING.name();
                break;
            case WORKFLOW_ACTION:
                status = WorkflowAction.Status.PREP.name();
                break;
            case WORKFLOW_JOB:
                status = WorkflowJob.Status.PREP.name();
                break;
            default:
                break;
        }
        return status;
    }

    /**
     * Update job into the map for SLA tracking
     */
    @Override
    public boolean updateRegistration(String jobId, SLARegistrationBean reg) throws JPAExecutorException {
        try {
            if (slaMap.size() < capacity) {
                SLACalcStatus slaCalc = new SLACalcStatus(reg);
                slaCalc.setSLAStatus(SLAStatus.NOT_STARTED);
                slaCalc.setJobStatus(getJobStatus(reg.getAppType()));

                @SuppressWarnings("rawtypes")
                List<UpdateEntry> updateList = new ArrayList<UpdateEntry>();
                updateList.add(new UpdateEntry<SLARegQuery>(SLARegQuery.UPDATE_SLA_REG_ALL, reg));
                updateList.add(new UpdateEntry<SLASummaryQuery>(SLASummaryQuery.UPDATE_SLA_SUMMARY_ALL,
                        new SLASummaryBean(slaCalc)));
                BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(null, updateList, null);
                putAndIncrement(jobId, slaCalc);
                LOG.trace("SLA Registration Event - Job:" + jobId);
                return true;
            }
            else {
                setLogPrefix(reg.getId());
                LOG.error(
                        "SLACalculator memory capacity reached. Cannot add or update new SLA Registration entry for job [{0}]",
                        reg.getId());
                LogUtils.clearLogPrefix();
            }
        }
        catch (JPAExecutorException jpa) {
            throw jpa;
        }
        return false;
    }

    /**
     * Remove job from being tracked in map
     */
    @Override
    public void removeRegistration(String jobId) {
        if (!removeAndDecrement(jobId)) {
            historySet.remove(jobId);
        }
    }

    /**
     * Triggered after receiving Job status change event, update SLA status
     * accordingly
     */
    @Override
    public boolean addJobStatus(String jobId, String jobStatus, JobEvent.EventStatus jobEventStatus, Date startTime,
            Date endTime) throws JPAExecutorException, ServiceException {
        LOG.debug(
                "Received addJobStatus request for job  [{0}] jobStatus = [{1}], jobEventStatus = [{2}], startTime = [{3}], "
                        + "endTime = [{4}] ", jobId, jobStatus, jobEventStatus, startTime, endTime);
        SLACalcStatus slaCalc = slaMap.get(jobId);
        boolean firstCheckAfterRetstart = checkAndUpdateSLACalcAfterRestart(slaCalc);
        if (slaCalc == null) {
            SLARegistrationBean slaRegBean = SLARegistrationQueryExecutor.getInstance().get(
                    SLARegQuery.GET_SLA_REG_ALL, jobId);
            if (slaRegBean != null) { // filter out jobs picked by SLA job event listener
                                      // but not actually configured for SLA
                SLASummaryBean slaSummaryBean = SLASummaryQueryExecutor.getInstance().get(
                        SLASummaryQuery.GET_SLA_SUMMARY, jobId);
                slaCalc = new SLACalcStatus(slaSummaryBean, slaRegBean);
                putAndIncrement(jobId, slaCalc);
            }
        }
        else {
            SLASummaryBean summaryBean = ((SLASummaryQueryExecutor) SLASummaryQueryExecutor.getInstance()).get(
                    SLASummaryQuery.GET_SLA_SUMMARY_EVENTPROCESSED_LAST_MODIFIED, jobId);
            byte eventProc = summaryBean.getEventProcessed();
            if (!slaCalc.getLastModifiedTime().equals(summaryBean.getLastModifiedTime())) {
                // Update last modified time.
                slaCalc.setLastModifiedTime(summaryBean.getLastModifiedTime());
                reloadExpectedTimeAndConfig(slaCalc);
                LOG.debug("Last modified time has changed for job " + jobId + " reloading config from DB");

            }
            slaCalc.setEventProcessed(eventProc);
        }
        if (slaCalc != null) {
            try {
                SLAXCommandFactory.getSLAEventXCommand(slaCalc,
                        ConfigurationService.getLong(SLAService.CONF_SLA_CALC_LOCK_TIMEOUT, 20 * 1000)).call();
                checkEventProc(slaCalc);
            }
            catch (XException e) {
                if (firstCheckAfterRetstart) {
                    slaCalc.setSLARegistrationBean(null);
                }
                LOG.error(e);
                throw new ServiceException(e);
            }
            return true;
        }
        else {
            return false;
        }
    }

    private void checkEventProc(SLACalcStatus slaCalc){
        byte eventProc = slaCalc.getEventProcessed();
        if (slaCalc.getEventProcessed() >= 8) {
            removeAndDecrement(slaCalc.getId());
            LOG.debug("Removed Job [{0}] from map after Event-processed=8", slaCalc.getId());
        }
        if (eventProc == 7) {
            historySet.add(slaCalc.getId());
            removeAndDecrement(slaCalc.getId());
            LOG.debug("Removed Job [{0}] from map after Event-processed=7", slaCalc.getId());
        }
    }

    public void reloadExpectedTimeAndConfig(SLACalcStatus slaCalc) throws JPAExecutorException {
        SLARegistrationBean regBean = SLARegistrationQueryExecutor.getInstance().get(
                SLARegQuery.GET_SLA_EXPECTED_VALUE_CONFIG, slaCalc.getId());

        if (regBean.getExpectedDuration() > 0) {
            slaCalc.getSLARegistrationBean().setExpectedDuration(regBean.getExpectedDuration());
        }
        if (regBean.getExpectedEnd() != null) {
            slaCalc.getSLARegistrationBean().setExpectedEnd(regBean.getExpectedEnd());
        }
        if (regBean.getExpectedStart() != null) {
            slaCalc.getSLARegistrationBean().setExpectedStart(regBean.getExpectedStart());
        }
        if (regBean.getSLAConfigMap().containsKey(OozieClient.SLA_DISABLE_ALERT)) {
            slaCalc.getSLARegistrationBean().addToSLAConfigMap(OozieClient.SLA_DISABLE_ALERT,
                    regBean.getSLAConfigMap().get(OozieClient.SLA_DISABLE_ALERT));
        }
        if (regBean.getNominalTime() != null) {
            slaCalc.getSLARegistrationBean().setNominalTime(regBean.getNominalTime());
        }
    }

    @VisibleForTesting
    public boolean isJobIdInSLAMap(String jobId) {
        return this.slaMap.containsKey(jobId);
    }

    @VisibleForTesting
    public boolean isJobIdInHistorySet(String jobId) {
        return this.historySet.contains(jobId);
    }

    private void setLogPrefix(String jobId) {
        LOG = LogUtils.setLogInfo(LOG, jobId, null, null);
    }

    @Override
    public boolean enableAlert(List<String> jobIds) throws JPAExecutorException, ServiceException {
        boolean isJobFound = false;
        @SuppressWarnings("rawtypes")
        List<UpdateEntry> updateList = new ArrayList<BatchQueryExecutor.UpdateEntry>();
        for (String jobId : jobIds) {
            SLACalcStatus slaCalc = getOrCreateSLACalcStatus(jobId);
            slaCalc.getSLARegistrationBean().removeFromSLAConfigMap(OozieClient.SLA_DISABLE_ALERT);
            updateDBSlaConfig(slaCalc, updateList);
            isJobFound = true;
        }
        executeBatchQuery(updateList);
        return isJobFound;
    }

    @Override
    public boolean enableChildJobAlert(List<String> parentJobIds) throws JPAExecutorException, ServiceException {
        return enableAlert(getSLAJobsforParents(parentJobIds));
    }

    @Override
    public boolean disableAlert(List<String> jobIds) throws JPAExecutorException, ServiceException {
        boolean isJobFound = false;
        @SuppressWarnings("rawtypes")
        List<UpdateEntry> updateList = new ArrayList<BatchQueryExecutor.UpdateEntry>();

        for (String jobId : jobIds) {
            SLACalcStatus slaCalc = getOrCreateSLACalcStatus(jobId);
            slaCalc.getSLARegistrationBean().addToSLAConfigMap(OozieClient.SLA_DISABLE_ALERT, Boolean.toString(true));
            updateDBSlaConfig(slaCalc, updateList);
            isJobFound = true;
        }
        executeBatchQuery(updateList);
        return isJobFound;
    }

    @Override
    public boolean disableChildJobAlert(List<String> parentJobIds) throws JPAExecutorException, ServiceException {
        return disableAlert(getSLAJobsforParents(parentJobIds));
    }

    @Override
    public boolean changeDefinition(List<Pair<String, Map<String, String>>> jobIdsSLAPair) throws JPAExecutorException,
            ServiceException {
        boolean isJobFound = false;
        @SuppressWarnings("rawtypes")
        List<UpdateEntry> updateList = new ArrayList<BatchQueryExecutor.UpdateEntry>();
        for (Pair<String, Map<String, String>> jobIdSLAPair : jobIdsSLAPair) {
            SLACalcStatus slaCalc = getOrCreateSLACalcStatus(jobIdSLAPair.getFirst());
            updateParams(slaCalc, jobIdSLAPair.getSecond());
            updateDBSlaExpectedValues(slaCalc, updateList);
            isJobFound = true;
        }
        executeBatchQuery(updateList);
        return isJobFound;
    }

    private void updateParams(SLACalcStatus slaCalc, Map<String, String> newParams) throws ServiceException {
        SLARegistrationBean reg = slaCalc.getSLARegistrationBean();
        if (newParams != null) {
            try {
                Date newNominal = SLAOperations.setNominalTime(newParams.get(RestConstants.SLA_NOMINAL_TIME), reg);
                SLAOperations.setExpectedStart(newParams.get(RestConstants.SLA_SHOULD_START), newNominal, reg);
                SLAOperations.setExpectedEnd(newParams.get(RestConstants.SLA_SHOULD_END), newNominal, reg);
                SLAOperations.setExpectedDuration(newParams.get(RestConstants.SLA_MAX_DURATION), reg);
            }
            catch (CommandException ce) {
                throw new ServiceException(ce);
            }
        }
    }

    private List<String> getSLAJobsforParents(List<String> parentJobIds) throws JPAExecutorException {
        List<String> childJobIds = new ArrayList<String>();
        for (String jobId : parentJobIds) {
            List<SLARegistrationBean> registrationBeanList = SLARegistrationQueryExecutor.getInstance().getList(
                    SLARegQuery.GET_SLA_REG_FOR_PARENT_ID, jobId);
            for (SLARegistrationBean bean : registrationBeanList) {
                childJobIds.add(bean.getId());
            }
        }
        return childJobIds;
    }

    private boolean checkAndUpdateSLACalcAfterRestart(SLACalcStatus slaCalc) throws JPAExecutorException {
        if (slaCalc != null && slaCalc.getSLARegistrationBean() == null) {
            return updateSLARegistartion(slaCalc);
        }
        return false;
    }

    public boolean updateSLARegistartion(SLACalcStatus slaCalc) throws JPAExecutorException {
        if (slaCalc.getSLARegistrationBean() == null) {
            synchronized (slaCalc) {
                if (slaCalc.getSLARegistrationBean() == null) {
                    SLARegistrationBean slaRegBean = SLARegistrationQueryExecutor.getInstance()
                            .get(SLARegQuery.GET_SLA_REG_ON_RESTART, slaCalc.getId());
                    slaCalc.updateSLARegistrationBean(slaRegBean);
                    return true;
                }
            }
        }
        return false;
    }

    private boolean putAndIncrement(final String jobId, final SLACalcStatus newStatus) {
        if (slaMap.put(jobId, newStatus) == null) {
            LOG.trace("Added a new item to SLA map. [jobId={0}]", jobId);
            instrumentation.incr(INSTRUMENTATION_GROUP, SLA_MAP, 1);
            return true;
        }

        LOG.trace("Updated an existing item in SLA map. [jobId={0}]", jobId);
        return false;
    }

    private boolean removeAndDecrement(final String jobId) {
        if (slaMap.remove(jobId) != null) {
            LOG.trace("Removed an existing item from SLA map. [jobId={0}]", jobId);
            instrumentation.decr(INSTRUMENTATION_GROUP, SLA_MAP, 1);
            return true;
        }

        LOG.trace("Tried to remove a non-existing item from SLA map. [jobId={0}]", jobId);
        return false;
    }

    private void resetRetryCount(final String jobId) {
        if (slaMap.containsKey(jobId)) {
            LOG.debug("Resetting retry count on [{0}]", jobId);
            final SLACalcStatus existingStatus = slaMap.get(jobId);
            existingStatus.resetRetryCount();
            putAndIncrement(jobId, existingStatus);
        }
    }

    private void incrementRetryCountAndRemove(final String jobId) {
        LOG.debug("Checking SLA calculator status [{0}] for retry count", jobId);
        if (slaMap.containsKey(jobId)) {
            final SLACalcStatus existingStatus = slaMap.get(jobId);
            if (existingStatus.getRetryCount() < maxRetryCount) {
                existingStatus.incrementRetryCount();
                LOG.debug("Retrying with SLA calculator status [{0}] retry count [{1}]", jobId, existingStatus.getRetryCount());
                putAndIncrement(jobId, existingStatus);
            }
            else {
                LOG.debug("Removing [{0}] from SLA map as maximum retry count reached", jobId);
                removeAndDecrement(jobId);
            }
        }
    }
}
