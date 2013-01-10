package org.apache.oozie.command.coord;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.Job;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.executor.jpa.CoordActionGetForInputCheckJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionUpdatePushInputCheckJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.MetadataServiceException;
import org.apache.oozie.service.PartitionDependencyManagerService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.PartitionWrapper;
import org.apache.oozie.util.StatusUtils;

public class CoordActionUpdatePushMissingDependency extends CoordinatorXCommand<Void> {

    private String actionId;
    private JPAService jpaService = null;
    private CoordinatorActionBean coordAction = null;
    private CoordinatorJobBean coordJob = null;

    public CoordActionUpdatePushMissingDependency(String actionId) {
        super("coord_action_push_md", "coord_action_push_md", 0);
        this.actionId = actionId;
    }

    @Override
    protected Void execute() throws CommandException {
        LOG.info("STARTED for Action id [{0}]", actionId);
       // TODO: Get the list of action from Available map
        PartitionDependencyManagerService pdms = Services.get().get(PartitionDependencyManagerService.class);
        if (pdms == null) {
            throw new CommandException(ErrorCode.E1024, "PartitionDependencyManagerService");
        }
        List<PartitionWrapper> availPartitionList = pdms.getAvailablePartitions(actionId);
        pdms.getAvailablePartitions(actionId);
        if (availPartitionList == null || availPartitionList.size() == 0) {
            LOG.info("There is no available partitionList of action ID: [{0}]", actionId);
            handleTimeout();
            return null;
        }
        String missPartitions = coordAction.getPushMissingDependencies();
        LOG.debug("Updating action Id " + actionId + " for available partition of " + availPartitionList.toString()
                + "missing parts :" + missPartitions);
        String newMissPartitions = removePartitions(missPartitions, availPartitionList);
        LOG.trace("In CoordActionUpdatePushMissingDependency: New missing partitions " +newMissPartitions);
        // TODO - Check if new miss partitions are updated, then only persist, do the same in CoordInputCheck
        coordAction.setPushMissingDependencies(newMissPartitions);
        String otherDeps = coordAction.getMissingDependencies();
        if ((newMissPartitions == null || newMissPartitions.trim().length() == 0)
                && (otherDeps == null || otherDeps.trim().length() == 0)) {
            coordAction.setStatus(CoordinatorAction.Status.READY);
            // pass jobID to the CoordActionReadyXCommand
            LOG.trace("Queuing READY for "+actionId);
            queue(new CoordActionReadyXCommand(coordAction.getJobId()), 100);
        }
        else {
            handleTimeout();
        }
        coordAction.setLastModifiedTime(new Date());
        if (jpaService != null) {
            try {
                jpaService.execute(new CoordActionUpdatePushInputCheckJPAExecutor(coordAction));
                // remove available partitions for the action as the push dependencies are persisted
                if (pdms.removeAvailablePartitions(availPartitionList, actionId)) {
                    LOG.debug("Succesfully removed partitions for actionId: [{0}] from available Map ", actionId);
                }
                else {
                    LOG.warn("Unable to remove partitions for actionId: [{0}] from available Map ", actionId);
                }
            }
            catch (JPAExecutorException jex) {
                throw new CommandException(ErrorCode.E1023, jex.getMessage(), jex);
            }
            catch (MetadataServiceException e) {
                throw new CommandException(ErrorCode.E0902, e.getMessage(), e);
            }
        }
        LOG.info("ENDED for Action id [{0}]", actionId);
        return null;
    }

    // TODO: Revisit efficiency
    private String removePartitions(String missPartitions, List<PartitionWrapper> availPartitionList)
            throws CommandException {
        if (missPartitions == null || missPartitions.length() == 0) {
            LOG.warn("Missing dependency from db is empty. availableMap contains :" + availPartitionList);
            return null;
        }
        List<PartitionWrapper> missPartList = createPartitionWrapper(missPartitions);
        StringBuilder ret = new StringBuilder();
        for (PartitionWrapper part : availPartitionList) {
            if (missPartList.contains(part)) {
                missPartList.remove(part);
                LOG.debug("Removing partition " + part);
            }
            else {
                LOG.warn("NOT found partition [{0}] in missingList: [{1}] of action: [{2}]", part, missPartList, actionId);
            }
        }
        for (PartitionWrapper missParts : missPartList) {
            if (ret.length() > 0) {
                ret.append(CoordELFunctions.INSTANCE_SEPARATOR);
            }
            ret.append(missParts.toString());
        }
        return ret.toString();
    }

    private List<PartitionWrapper> createPartitionWrapper(String missPartitions) throws CommandException {
        String[] parts = missPartitions.split(CoordELFunctions.INSTANCE_SEPARATOR);
        List<PartitionWrapper> ret = new ArrayList<PartitionWrapper>();
        for (String partURI : parts) {
            try {
                ret.add(new PartitionWrapper(partURI));
            }
            catch (URISyntaxException e) {
                throw new CommandException(ErrorCode.E1025, e);
            }
        }
        return ret;
    }

    private void handleTimeout() {
        long waitingTime = (new Date().getTime() - Math.max(coordAction.getNominalTime().getTime(), coordAction
                .getCreatedTime().getTime()))
                / (60 * 1000);
        int timeOut = coordAction.getTimeOut();
        if ((timeOut >= 0) && (waitingTime > timeOut)) {
            queue(new CoordActionTimeOutXCommand(coordAction), 100);
        }
    }

    @Override
    protected boolean isLockRequired() {
        return true;
    }

    @Override
    public String getEntityKey() {
        return actionId;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getKey()
     */
    @Override
    public String getKey(){
        return getName() + "_" + actionId;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#eagerLoadState()
     */
    @Override
    protected void eagerLoadState() throws CommandException {
        try {
            jpaService = Services.get().get(JPAService.class);
            if (jpaService != null) {
                coordAction = jpaService.execute(new CoordActionGetForInputCheckJPAExecutor(actionId));
                coordJob = jpaService.execute(new CoordJobGetJPAExecutor(coordAction.getJobId()));
                LogUtils.setLogInfo(coordAction, logInfo);
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
     * @see org.apache.oozie.command.XCommand#eagerVerifyPrecondition()
     */
    @Override
    protected void eagerVerifyPrecondition() throws CommandException, PreconditionException {
        if (coordAction.getStatus() != CoordinatorActionBean.Status.WAITING) {
            throw new PreconditionException(ErrorCode.E1100, "[" + actionId
                    + "]::CoordActionUpdatePushDependency:: Ignoring action. Should be in WAITING state, but state="
                    + coordAction.getStatus());
        }
        // if eligible to do action input check when running with backward
        // support is true
        if (StatusUtils.getStatusForCoordActionInputCheck(coordJob)) {
            return;
        }

        if (coordJob.getStatus() != Job.Status.RUNNING && coordJob.getStatus() != Job.Status.RUNNINGWITHERROR
                && coordJob.getStatus() != Job.Status.PAUSED && coordJob.getStatus() != Job.Status.PAUSEDWITHERROR) {
            throw new PreconditionException(ErrorCode.E1100, "[" + actionId
                    + "]::CoordActionUpdatePushDependency:: Ignoring action."
                    + " Coordinator job is not in RUNNING/RUNNINGWITHERROR/PAUSED/PAUSEDWITHERROR state, but state="
                    + coordJob.getStatus());
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.command.XCommand#loadState()
     */
    @Override
    protected void loadState() throws CommandException {
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }

}
