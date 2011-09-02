package org.apache.oozie.command.bundle;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.oozie.client.Job;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.RerunTransitionXCommand;
import org.apache.oozie.command.coord.CoordRerunXCommand;
import org.apache.oozie.executor.jpa.BundleActionUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.BundleActionsGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;
import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;

/**
 * Rerun bundle coordinator jobs by a list of coordinator names or dates. User can specify if refresh or noCleanup.
 * <p/>
 * The "refresh" is used to indicate if user wants to refresh an action's input/outpur dataset urls
 * <p/>
 * The "noCleanup" is used to indicate if user wants to cleanup output events for given rerun actions
 */
public class BundleRerunXCommand extends RerunTransitionXCommand<Void> {

    protected final XLog LOG = XLog.getLog(BundleRerunXCommand.class);
    private String coordScope;
    private String dateScope;
    private boolean refresh;
    private boolean noCleanup;
    private BundleJobBean bundleJob;
    private List<BundleActionBean> bundleActions;

    private JPAService jpaService = null;

    /**
     * The constructor for class {@link BundleRerunXCommand}
     *
     * @param jobId the bundle job id
     * @param coordScope the rerun scope for coordinator job names separated by ","
     * @param dateScope the rerun scope for coordinator nominal times separated by ","
     * @param refresh true if user wants to refresh input/outpur dataset urls
     * @param noCleanup false if user wants to cleanup output events for given rerun actions
     */
    public BundleRerunXCommand(String jobId, String coordScope, String dateScope, boolean refresh, boolean noCleanup) {
        super("bundle_rerun", "bundle_rerun", 1);
        this.jobId = ParamChecker.notEmpty(jobId, "jobId");
        this.coordScope = coordScope;
        this.dateScope = ParamChecker.notEmpty(dateScope, "scope");
        this.refresh = refresh;
        this.noCleanup = noCleanup;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#loadState()
     */
    @Override
    protected void loadState() throws CommandException {
        try {
            jpaService = Services.get().get(JPAService.class);

            if (jpaService != null) {
                this.bundleJob = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
                this.bundleActions = jpaService.execute(new BundleActionsGetJPAExecutor(jobId));
                LogUtils.setLogInfo(bundleJob, logInfo);
                super.setJob(bundleJob);
            }
            else {
                throw new CommandException(ErrorCode.E0610);
            }
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }

    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.RerunTransitionXCommand#rerunChildren()
     */
    @Override
    public void rerunChildren() throws CommandException {
        Map<String, BundleActionBean> coordNameToBAMapping = new HashMap<String, BundleActionBean>();
        if (bundleActions != null) {
            for (BundleActionBean action : bundleActions) {
                if (action.getCoordName() != null) {
                    coordNameToBAMapping.put(action.getCoordName(),action);
                }
            }
        }

        if(coordScope != null && !coordScope.isEmpty()) {
            String[] list = coordScope.split(",");
            for (String s : list) {
                s = s.trim();
                if (coordNameToBAMapping.keySet().contains(s)) {
                    String id = coordNameToBAMapping.get(s).getCoordId();
                    if (id == null) {
                        LOG.info("No coord id found. Therefore, nothing to queue for coord rerun for coordname: " + s);
                        continue;
                    }
                    LOG.debug("Queuing rerun for coord id " + id + " of bundle " + bundleJob.getId());
                    queue(new CoordRerunXCommand(id, RestConstants.JOB_COORD_RERUN_DATE, dateScope, refresh, noCleanup));
                    updateBundleAction(coordNameToBAMapping.get(s));
                }
                else {
                    LOG.info("Rerun for coord " + s + " NOT performed because it is not in bundle ", bundleJob.getId());
                }
            }
        }
        else {
            if (bundleActions != null) {
                for (BundleActionBean action : bundleActions) {
                    if (action.getCoordId() == null) {
                        LOG.info("No coord id found. Therefore nothing to queue for coord rerun with coord name "
                                + action.getCoordName());
                        continue;
                    }
                    LOG.debug("Queuing rerun for coord id :" + action.getCoordId());
                    queue(new CoordRerunXCommand(action.getCoordId(), RestConstants.JOB_COORD_RERUN_DATE, dateScope, refresh, noCleanup));
                    updateBundleAction(action);
                }
            }
        }

        LOG.info("Rerun coord jobs for the bundle=[{0}]", jobId);
    }

    /**
     * Update bundle action
     *
     * @param action the bundle action
     * @throws CommandException thrown if failed to update bundle action
     */
    private void updateBundleAction(BundleActionBean action) throws CommandException {
        action.incrementAndGetPending();
        action.setLastModifiedTime(new Date());
        try {
            jpaService.execute(new BundleActionUpdateJPAExecutor(action));
        }
        catch (JPAExecutorException je) {
            throw new CommandException(je);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#updateJob()
     */
    @Override
    public void updateJob() throws CommandException {
        try {
            bundleJob.setPending();
            jpaService.execute(new BundleJobUpdateJPAExecutor(bundleJob));
        }
        catch (JPAExecutorException je) {
            throw new CommandException(je);
        }

    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    protected String getEntityKey() {
        return jobId;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#isLockRequired()
     */
    @Override
    protected boolean isLockRequired() {
        return true;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#getJob()
     */
    @Override
    public Job getJob() {
        return bundleJob;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#notifyParent()
     */
    @Override
    public void notifyParent() throws CommandException {

    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.RerunTransitionXCommand#getLog()
     */
    @Override
    public XLog getLog() {
        return LOG;
    }

}
