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

import org.apache.oozie.client.Job;

public abstract class ResumeTransitionXCommand extends TransitionXCommand<Void> {

    /**
     * Resume all children of the job
     *
     * @throws CommandException
     */
    public abstract void resumeChildren() throws CommandException;

    public ResumeTransitionXCommand(String name, String type, int priority) {
        super(name, type, priority);
    }

    public ResumeTransitionXCommand(String name, String type, int priority, boolean dryrun) {
        super(name, type, priority, dryrun);
    }

    /**
     * Transit job to PREP from PREPSUSPENDED or to RUNNING from SUSPENDED.
     *
     * @see org.apache.oozie.command.TransitionXCommand#transitToNext()
     */
    @Override
    public void transitToNext() {
        if (job == null) {
            job = this.getJob();
        }
        if (job.getStatus() == Job.Status.PREPSUSPENDED) {
            job.setStatus(Job.Status.PREP);
        }
        else if (job.getStatus() == Job.Status.SUSPENDED) {
            job.setStatus(Job.Status.RUNNING);
        }
        job.setPending();
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#execute()
     */
    @Override
    protected Void execute() throws CommandException {
        transitToNext();
        try {
            resumeChildren();
            updateJob();
        } finally {
            notifyParent();
        }
        return null;
    }
}
