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

import java.util.ArrayList;
import java.util.List;

import org.apache.oozie.client.Job;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.util.ParamChecker;

/**
 * This is the base commands for all the jobs related commands . This will drive the statuses for all the jobs and all
 * the jobs will follow the same state machine.
 *
 * @param <T>
 */
public abstract class TransitionXCommand<T> extends XCommand<T> {

    protected Job job;
    protected List<JsonBean> updateList = new ArrayList<JsonBean>();
    protected List<JsonBean> insertList = new ArrayList<JsonBean>();

    public TransitionXCommand(String name, String type, int priority) {
        super(name, type, priority);
    }

    public TransitionXCommand(String name, String type, int priority, boolean dryrun) {
        super(name, type, priority, dryrun);
    }

    /**
     * Transit to the next status based on the result of the Job.
     *
     * @throws CommandException
     */
    public abstract void transitToNext() throws CommandException;

    /**
     * Update the parent job.
     *
     * @throws CommandException
     */
    public abstract void updateJob() throws CommandException;

    /**
     * This will be used to notify the parent about the status of that perticular job.
     *
     * @throws CommandException
     */
    public abstract void notifyParent() throws CommandException;

    /**
     * This will be used to perform atomically all the writes within this command.
     *
     * @throws CommandException
     */
    public abstract void performWrites() throws CommandException;

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#execute()
     */
    @Override
    protected T execute() throws CommandException {
        transitToNext();
        updateJob();
        notifyParent();
        return null;
    }

    /**
     * Get the Job for the command.
     *
     * @return the job
     */
    public Job getJob() {
        return job;
    }

    /**
     * Set the Job for the command.
     *
     * @param job the job
     */
    public void setJob(Job job) {
        this.job = ParamChecker.notNull(job, "job");
    }

}
