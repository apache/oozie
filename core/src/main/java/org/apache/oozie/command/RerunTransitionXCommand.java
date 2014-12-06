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

package org.apache.oozie.command;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.Job;
import org.apache.oozie.util.StatusUtils;

/**
 * Transition command for rerun the job. The derived class has to override these following functions:
 * <p/>
 * updateJob() : update job status and attributes
 * rerunChildren() : submit or queue commands to rerun children
 * notifyParent() : update the status to upstream if any
 *
 * @param <T>
 */
public abstract class RerunTransitionXCommand<T> extends TransitionXCommand<T> {
    protected String jobId;
    protected T ret;
    protected Job.Status prevStatus;

    /**
     * The constructor for abstract class {@link RerunTransitionXCommand}
     *
     * @param name the command name
     * @param type the command type
     * @param priority the command priority
     */
    public RerunTransitionXCommand(String name, String type, int priority) {
        super(name, type, priority);
    }

    /**
     * The constructor for abstract class {@link RerunTransitionXCommand}
     *
     * @param name the command name
     * @param type the command type
     * @param priority the command priority
     * @param dryrun true if dryrun is enable
     */
    public RerunTransitionXCommand(String name, String type, int priority, boolean dryrun) {
        super(name, type, priority, dryrun);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#transitToNext()
     */
    @Override
    public void transitToNext() {
        if (job == null) {
            job = this.getJob();
        }
        prevStatus = job.getStatus();
        if (prevStatus == Job.Status.SUCCEEDED || prevStatus == Job.Status.PAUSED
                || prevStatus == Job.Status.SUSPENDED || prevStatus == Job.Status.RUNNING) {
            job.setStatus(Job.Status.RUNNING);
        }
        else {
            // Check for backward compatibility
            job.setStatus(StatusUtils.getStatusIfBackwardSupportTrue(Job.Status.RUNNINGWITHERROR));
        }
        job.setPending();
    }

    /**
     * Rerun actions associated with the job
     *
     * @throws CommandException thrown if failed to rerun actions
     */
    public abstract void rerunChildren() throws CommandException;

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#execute()
     */
    @Override
    protected T execute() throws CommandException {
        getLog().info("STARTED " + getClass().getSimpleName() + " for jobId=" + jobId);
        try {
            transitToNext();
            rerunChildren();
            updateJob();
            performWrites();
        }
        finally {
            notifyParent();
        }
        getLog().info("ENDED " + getClass().getSimpleName() + " for jobId=" + jobId);
        return ret;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        eagerVerifyPrecondition();
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#eagerLoadState()
     */
    @Override
    protected void eagerLoadState() throws CommandException {
        loadState();
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#eagerVerifyPrecondition()
     */
    @Override
    protected void eagerVerifyPrecondition() throws CommandException, PreconditionException {
        if (getJob().getStatus() == Job.Status.KILLED || getJob().getStatus() == Job.Status.FAILED
                || getJob().getStatus() == Job.Status.PREP || getJob().getStatus() == Job.Status.PREPPAUSED
                || getJob().getStatus() == Job.Status.PREPSUSPENDED) {
            getLog().warn(
                    "RerunCommand is not able to run because job status=" + getJob().getStatus() + ", jobid="
                            + getJob().getId());
            throw new PreconditionException(ErrorCode.E1100, "Not able to rerun the job Id= " + getJob().getId()
                    + ". job is in wrong state= " + getJob().getStatus());
        }
    }

    /**
     * This method will return the previous status.
     *
     * @return JOB Status
     */
    public Job.Status getPrevStatus() {
        if (prevStatus != null) {
            return prevStatus;
        }
        else {
            return job.getStatus();
        }
    }
}
