package org.apache.oozie.command;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.Job;
import org.apache.oozie.util.XLog;

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
    public final void transitToNext() {
        if (job == null) {
            job = this.getJob();
        }
        job.setStatus(Job.Status.RUNNING);
        job.resetPending();
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
        transitToNext();
        updateJob();
        rerunChildren();
        notifyParent();
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
        if (getJob().getStatus() == Job.Status.KILLED || getJob().getStatus() == Job.Status.FAILED) {
            getLog().warn(
                    "RerunCommand is not able to run because job status=" + getJob().getStatus() + ", jobid="
                            + getJob().getId());
            throw new PreconditionException(ErrorCode.E1100, "Not able to rerun the job Id= " + getJob().getId()
                    + ". job is in wrong state= " + getJob().getStatus());
        }
    }

    /**
     * Get XLog object
     *
     * @return log object
     */
    public XLog getLog() {
        return null;
    }
}
