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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.SLAAlertsXCommand;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.service.ServiceException;

public abstract class BundleSLAAlertsXCommand extends SLAAlertsXCommand {

    private String actions;

    private String dates;

    private String childIds;

    public BundleSLAAlertsXCommand(String jobId, String actions, String dates, String childIds) {
        super(jobId, "SLA.command", "SLA.command");
        this.actions = actions;
        this.dates = dates;
        this.childIds = childIds;

    }

    @Override
    protected void loadState() throws CommandException {
    }

    /**
     * Gets the coord jobs from bundle.
     *
     * @param id the bundle id
     * @param coords the coords name/id
     * @return the coord jobs from bundle
     * @throws CommandException the command exception
     */
    protected Set<String> getCoordJobsFromBundle(String id, String coords) throws CommandException {
        Set<String> jobs = new HashSet<String>();
        List<CoordinatorJobBean> coordJobs;
        try {
            if (coords == null) {
                coordJobs = CoordJobQueryExecutor.getInstance()
                        .getList(CoordJobQuery.GET_COORD_JOBS_WITH_PARENT_ID, id);
            }
            else {
                coordJobs = CoordJobQueryExecutor.getInstance().getList(
                        CoordJobQuery.GET_COORD_JOBS_FOR_BUNDLE_BY_APPNAME_ID, Arrays.asList(coords.split(",")), id);
            }
        }
        catch (XException e) {
            throw new CommandException(e);
        }
        for (CoordinatorJobBean jobBean : coordJobs) {
            jobs.add(jobBean.getId());
        }
        return jobs;

    }

    /**
     * Gets the coord jobs.
     *
     * @return the coord jobs
     */
    protected String getCoordJobs() {
        return childIds;
    }

    /**
     * Gets the actions.
     *
     * @return the actions
     */
    protected String getActions() {
        return actions;
    }

    /**
     * Gets the dates.
     *
     * @return the dates
     */
    protected String getDates() {
        return dates;
    }

    protected boolean isJobRequest() {
        return true;

    }

    @Override
    protected boolean executeSlaCommand() throws ServiceException, CommandException {
        StringBuffer report = new StringBuffer();

        Set<String> coordJobs = getCoordJobsFromBundle(getJobId(), getCoordJobs());

        if (coordJobs.isEmpty()) {
            throw new CommandException(ErrorCode.E1026, "No record found");
        }
        else {
            for (String job : coordJobs) {
                try {
                    executeCoordCommand(job, getActions(), getDates());
                }
                catch (Exception e) {
                    // Ignore exception for coords.
                    String errorMsg = "SLA command for coord job " + job + " failed. Error message is  : " + e.getMessage();
                    LOG.error(errorMsg, e);
                    report.append(errorMsg).append(System.getProperty("line.separator"));
                }
            }
            if (!report.toString().isEmpty()) {
                throw new CommandException(ErrorCode.E1026, report.toString());
            }
            return true;
        }
    }

    protected abstract void executeCoordCommand(String id, String actions, String dates) throws ServiceException,
            CommandException;

}
