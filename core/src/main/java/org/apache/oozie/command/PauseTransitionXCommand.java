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
 * Transition command for pause the job. The derived class has to override these following functions:
 * <p/>
 * updateJob() : update job status and attributes
 * pauseChildren() : submit or queue commands to pause children
 * notifyParent() : update the status to upstream if any
 *
 * @param <T>
 */
public abstract class PauseTransitionXCommand extends TransitionXCommand<Void> {
    /**
     * The constructor for abstract class {@link PauseTransitionXCommand}
     *
     * @param name the command name
     * @param type the command type
     * @param priority the command priority
     */
    public PauseTransitionXCommand(String name, String type, int priority) {
        super(name, type, priority);
    }

    /**
     * pause actions associated with the job
     *
     * @throws CommandException thrown if failed to pause actions
     */
    public abstract void pauseChildren() throws CommandException;

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#transitToNext()
     */
    @Override
    public final void transitToNext() throws CommandException {
        if (job == null) {
            job = this.getJob();
        }

        if (job.getStatus() == Job.Status.RUNNING) {
            job.setStatus(Job.Status.PAUSED);
        }
        else if (job.getStatus() == Job.Status.RUNNINGWITHERROR) {
            job.setStatus(Job.Status.PAUSEDWITHERROR);
        }
        else if (job.getStatus() == Job.Status.PREP) {
            job.setStatus(Job.Status.PREPPAUSED);
        }
        else {
            throw new CommandException(ErrorCode.E1315, job.getId());
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
            pauseChildren();
        }
        finally {
            notifyParent();
        }
        return null;
    }
}
