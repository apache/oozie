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
import org.apache.oozie.executor.jpa.JPAExecutorException;

/**
 * StatusTransitXCommand is super class for Status Transit Command, it defines layout for Status Transit Commands. It
 * tries change job status change after acquiring lock with zero timeout. StatusTransit Commands are not requeued.
 */
abstract public class StatusTransitXCommand extends XCommand<Void> {

    /**
     * Instantiates a new status transit x command.
     *
     * @param name the name
     * @param type the type
     * @param priority the priority
     */
    public StatusTransitXCommand(String name, String type, int priority) {
        super(name, type, priority);
    }

    @Override
    final protected long getLockTimeOut() {
        return 0L;
    }

    @Override
    final protected boolean isReQueueRequired() {
        return false;
    }

    @Override
    final protected boolean isLockRequired() {
        return true;
    }

    @Override
    protected Void execute() throws CommandException {

        final Job.Status jobStatus = getJobStatus();
        try {
            if (jobStatus != null) {
                updateJobStatus(jobStatus);
            }
        }
        catch (JPAExecutorException e) {
            throw new CommandException(e);
        }

        return null;

    }

    /**
     * Gets the job status.
     *
     * @return the job status
     * @throws CommandException the command exception
     */
    protected Job.Status getJobStatus() throws CommandException {
        if (isTerminalState()) {
            return getTerminalStatus();
        }
        if (isPausedState()) {
            return getPausedState();
        }
        if (isSuspendedState()) {
            return getSuspendedStatus();
        }
        if (isRunningState()) {
            return getRunningState();
        }
        return null;
    }

    /**
     * Checks if job is in terminal state.
     *
     * @return true, if is terminal state
     */
    protected abstract boolean isTerminalState();

    /**
     * Gets the job terminal status.
     *
     * @return the terminal status
     */
    protected abstract Job.Status getTerminalStatus();

    /**
     * Checks if job is in paused state.
     *
     * @return true, if job is in paused state
     */
    protected abstract boolean isPausedState();

    /**
     * Gets the job pause state.
     *
     * @return the paused state
     */
    protected abstract Job.Status getPausedState();

    /**
     * Checks if is in suspended state.
     *
     * @return true, if job is in suspended state
     */
    protected abstract boolean isSuspendedState();

    /**
     * Gets the suspended status.
     *
     * @return the suspended status
     */
    protected abstract Job.Status getSuspendedStatus();

    /**
     * Checks if job is in running state.
     *
     * @return true, if job is in running state
     */
    protected abstract boolean isRunningState();

    /**
     * Gets the job running state.
     *
     * @return the running state
     */
    protected abstract Job.Status getRunningState();

    /**
     * Update job status.
     *
     * @param status the status
     * @throws JPAExecutorException the JPA executor exception
     * @throws CommandException the command exception
     */
    protected abstract void updateJobStatus(Job.Status status) throws JPAExecutorException, CommandException;

}
