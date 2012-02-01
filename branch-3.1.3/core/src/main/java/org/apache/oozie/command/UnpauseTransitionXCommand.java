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

/**
 * Transition command for unpause the job. The derived class has to override these following functions:
 * <p/>
 * updateJob() : update job status and attributes
 * unpauseChildren() : submit or queue commands to unpause children
 * notifyParent() : update the status to upstream if any
 *
 * @param <T>
 */
public abstract class UnpauseTransitionXCommand extends TransitionXCommand<Void> {
    /**
     * The constructor for abstract class {@link UnpauseTransitionXCommand}
     *
     * @param name the command name
     * @param type the command type
     * @param priority the command priority
     */
    public UnpauseTransitionXCommand(String name, String type, int priority) {
        super(name, type, priority);
    }

    /**
     * Unpause actions associated with the job
     *
     * @throws CommandException thrown if failed to unpause actions
     */
    public abstract void unpauseChildren() throws CommandException;

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#transitToNext()
     */
    @Override
    public final void transitToNext() throws CommandException {
        if (job == null) {
            job = this.getJob();
        }

        if (job.getStatus() == Job.Status.PAUSED) {
            job.setStatus(Job.Status.RUNNING);
        }
        else if (job.getStatus() == Job.Status.PAUSEDWITHERROR) {
            job.setStatus(Job.Status.RUNNINGWITHERROR);
        }
        else if (job.getStatus() == Job.Status.PREPPAUSED) {
            job.setStatus(Job.Status.PREP);
        }
        else {
            throw new CommandException(ErrorCode.E1316, job.getId());
        }

        //TODO: to be revisited;
        //job.setPending();
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#execute()
     */
    @Override
    protected Void execute() throws CommandException {
        try {
            transitToNext();
            updateJob();
            unpauseChildren();
        }
        finally {
            notifyParent();
        }
        return null;
    }
}
