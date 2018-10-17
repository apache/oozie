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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.Date;

import com.google.common.base.Charsets;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.ConfigUtils;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.diff.EditList;
import org.eclipse.jgit.diff.HistogramDiff;
import org.eclipse.jgit.diff.RawText;
import org.eclipse.jgit.diff.RawTextComparator;
import org.jdom.Element;

/**
 * This class provides the functionalities to update coordinator job XML and properties. It uses CoordSubmitXCommand
 * functionality to validate XML and resolve all the variables or properties using job configurations.
 */
public class CoordUpdateXCommand extends CoordSubmitXCommand {

    private final String jobId;
    private boolean showDiff = true;
    private boolean isConfChange = false;

    //This properties are set in coord jobs by bundle. An update command should not overide it.
    final static String[] bundleConfList = new String[] { OozieClient.BUNDLE_ID };

    StringBuffer diff = new StringBuffer();
    CoordinatorJobBean oldCoordJob = null;

    public CoordUpdateXCommand(boolean dryrun, Configuration conf, String jobId) {
        super(dryrun, conf);
        this.jobId = jobId;
        isConfChange = conf.size() != 0;
    }

    public CoordUpdateXCommand(boolean dryrun, Configuration conf, String jobId, boolean showDiff) {
        super(dryrun, conf);
        this.jobId = jobId;
        this.showDiff = showDiff;
        isConfChange = conf.size() != 0;
    }

    @Override
    protected String storeToDB(String xmlElement, Element eJob, CoordinatorJobBean coordJob) throws CommandException {
        check(oldCoordJob, coordJob);

        ConfigUtils.checkAndSetDisallowedProperties(conf,
                this.oldCoordJob.getUser(),
                new CommandException(ErrorCode.E1003,
                        String.format("%s=%s", OozieClient.USER_NAME, conf.get(OozieClient.USER_NAME))),
                true);

        computeDiff(eJob);
        oldCoordJob.setAppPath(conf.get(OozieClient.COORDINATOR_APP_PATH));
        if (isConfChange) {
            oldCoordJob.setConf(XmlUtils.prettyPrint(conf).toString());
        }
        oldCoordJob.setMatThrottling(coordJob.getMatThrottling());
        oldCoordJob.setOrigJobXml(xmlElement);
        oldCoordJob.setConcurrency(coordJob.getConcurrency());
        oldCoordJob.setExecution(coordJob.getExecution());
        oldCoordJob.setTimeout(coordJob.getTimeout());
        oldCoordJob.setJobXml(XmlUtils.prettyPrint(eJob).toString());

        if (!dryrun) {
            oldCoordJob.setLastModifiedTime(new Date());
            // Should log the changes, this should be useful for debugging.
            LOG.info("Coord update changes : " + diff.toString());
            try {
                CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, oldCoordJob);
            }
            catch (JPAExecutorException jpaee) {
                throw new CommandException(jpaee);
            }
        }
        return jobId;
    }

    @Override
    protected void loadState() throws CommandException {
        super.loadState();
        jpaService = Services.get().get(JPAService.class);
        if (jpaService == null) {
            throw new CommandException(ErrorCode.E0610);
        }
        coordJob = new CoordinatorJobBean();
        try {
            oldCoordJob = CoordJobQueryExecutor.getInstance().get(CoordJobQuery.GET_COORD_JOB, jobId);
        }
        catch (JPAExecutorException e) {
            throw new CommandException(e);
        }

        LogUtils.setLogInfo(oldCoordJob);
        if (!isConfChange || StringUtils.isEmpty(conf.get(OozieClient.COORDINATOR_APP_PATH))) {
            try {
                XConfiguration jobConf = new XConfiguration(new StringReader(oldCoordJob.getConf()));

                if (!isConfChange) {
                    conf = jobConf;
                }
                else {
                    for (String bundleConfKey : bundleConfList) {
                        if (jobConf.get(bundleConfKey) != null) {
                            conf.set(bundleConfKey, jobConf.get(bundleConfKey));
                        }
                    }
                    if (StringUtils.isEmpty(conf.get(OozieClient.COORDINATOR_APP_PATH))) {
                        conf.set(OozieClient.COORDINATOR_APP_PATH, jobConf.get(OozieClient.COORDINATOR_APP_PATH));
                    }
                }
            }
            catch (Exception e) {
                throw new CommandException(ErrorCode.E1023, e.getMessage(), e);
            }
        }
        coordJob.setConf(XmlUtils.prettyPrint(conf).toString());
        setJob(coordJob);
        LogUtils.setLogInfo(coordJob);
    }

    @Override
    protected void verifyPrecondition() throws CommandException {
        if (coordJob.getStatus() == CoordinatorJob.Status.SUCCEEDED
                || coordJob.getStatus() == CoordinatorJob.Status.DONEWITHERROR) {
            LOG.info("Can't update coord job. Job has finished processing");
            throw new CommandException(ErrorCode.E1023, "Can't update coord job. Job has finished processing");
        }
    }

    /**
     * Gets the difference of job definition and properties.
     *
     * @param eJob the e job
     * @return the diff
     */
    private void computeDiff(Element eJob) {
        try {
            diff.append("**********Job definition changes**********").append(System.getProperty("line.separator"));
            diff.append(getDiffinGitFormat(oldCoordJob.getJobXml(), XmlUtils.prettyPrint(eJob).toString()));
            diff.append("******************************************").append(System.getProperty("line.separator"));
            diff.append("**********Job conf changes****************").append(System.getProperty("line.separator"));
            if (isConfChange) {
                diff.append(getDiffinGitFormat(oldCoordJob.getConf(), XmlUtils.prettyPrint(conf).toString()));
            }
            else {
                diff.append("No conf update requested").append(System.getProperty("line.separator"));
            }
            diff.append("******************************************").append(System.getProperty("line.separator"));
        }
        catch (IOException e) {
            diff.append("Error computing diff. Error " + e.getMessage());
            LOG.warn("Error computing diff.", e);
        }
    }

    /**
     * Get the differences in git format.
     *
     * @param string1 the string1
     * @param string2 the string2
     * @return the diff
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private String getDiffinGitFormat(String string1, String string2) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RawText rt1 = new RawText(string1.getBytes(Charsets.UTF_8));
        RawText rt2 = new RawText(string2.getBytes(Charsets.UTF_8));
        EditList diffList = new EditList();
        diffList.addAll(new HistogramDiff().diff(RawTextComparator.DEFAULT, rt1, rt2));
        new DiffFormatter(out).format(diffList, rt1, rt2);
        return out.toString(Charsets.UTF_8.name());
    }

    @Override
    protected String submit() throws CommandException {
        LOG.info("STARTED Coordinator update");
        submitJob();
        LOG.info("ENDED Coordinator update");
        if (showDiff) {
            return diff.toString();
        }
        else {
            return "";
        }
    }

    /**
     * Check. Frequency can't be changed. EndTime can't be changed. StartTime can't be changed. AppName can't be changed
     * Timeunit can't be changed. Timezone can't be changed
     *
     * @param oldCoord the old coord
     * @param newCoord the new coord
     * @throws CommandException the command exception
     */
    public void check(CoordinatorJobBean oldCoord, CoordinatorJobBean newCoord) throws CommandException {
        if (!oldCoord.getFrequency().equals(newCoord.getFrequency())) {
            throw new CommandException(ErrorCode.E1023, "Frequency can't be changed. Old frequency = "
                    + oldCoord.getFrequency() + " new frequency = " + newCoord.getFrequency());
        }

        if (!oldCoord.getEndTime().equals(newCoord.getEndTime())) {
            throw new CommandException(ErrorCode.E1023, "End time can't be changed. Old end time = "
                    + oldCoord.getEndTime() + " new end time = " + newCoord.getEndTime());
        }

        if (!oldCoord.getStartTime().equals(newCoord.getStartTime())) {
            throw new CommandException(ErrorCode.E1023, "Start time can't be changed. Old start time = "
                    + oldCoord.getStartTime() + " new start time = " + newCoord.getStartTime());
        }

        if (!oldCoord.getAppName().equals(newCoord.getAppName())) {
            throw new CommandException(ErrorCode.E1023, "Coord name can't be changed. Old name = "
                    + oldCoord.getAppName() + " new name = " + newCoord.getAppName());
        }

        if (!oldCoord.getTimeUnitStr().equals(newCoord.getTimeUnitStr())) {
            throw new CommandException(ErrorCode.E1023, "Timeunit can't be changed. Old Timeunit = "
                    + oldCoord.getTimeUnitStr() + " new Timeunit = " + newCoord.getTimeUnitStr());
        }

        if (!oldCoord.getTimeZone().equals(newCoord.getTimeZone())) {
            throw new CommandException(ErrorCode.E1023, "TimeZone can't be changed. Old timeZone = "
                    + oldCoord.getTimeZone() + " new timeZone = " + newCoord.getTimeZone());
        }

    }

    @Override
    protected void queueMaterializeTransitionXCommand(String jobId) {
    }

    @Override
    public void notifyParent() throws CommandException {
    }

    @Override
    protected boolean isLockRequired() {
        return true;
    }

    @Override
    public String getEntityKey() {
        return jobId;
    }

    @Override
    public void transitToNext() {
    }

    @Override
    public String getKey() {
        return getName() + "_" + jobId;
    }

    @Override
    public String getDryRun(CoordinatorJobBean job) throws Exception{
        return super.getDryRun(oldCoordJob);
    }
}
