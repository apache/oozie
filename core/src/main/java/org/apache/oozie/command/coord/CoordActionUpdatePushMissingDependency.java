package org.apache.oozie.command.coord;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.executor.jpa.CoordActionGetForInputCheckJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionUpdatePushInputCheckJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.PartitionDependencyManagerService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.PartitionWrapper;

public class CoordActionUpdatePushMissingDependency extends CoordinatorXCommand<Void> {

    private String actionId;
    private JPAService jpaService = null;
    private CoordinatorActionBean coordAction = null;

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
        coordAction.setPushMissingDependencies(newMissPartitions);
        String otherDeps = coordAction.getMissingDependencies();
        if ((newMissPartitions == null || newMissPartitions.trim().length() == 0)
                && (otherDeps == null || otherDeps.trim().length() == 0)) {
            coordAction.setStatus(CoordinatorAction.Status.READY);
            // pass jobID to the CoordActionReadyXCommand
            queue(new CoordActionReadyXCommand(coordAction.getJobId()), 100);
        }
        else {
            handleTimeout();
        }
        coordAction.setLastModifiedTime(new Date());
        if (jpaService != null) {
            try {
                jpaService.execute(new CoordActionUpdatePushInputCheckJPAExecutor(coordAction));
            }
            catch (JPAExecutorException jex) {
                throw new CommandException(ErrorCode.E1023, jex.getMessage(), jex);
            }
            finally {
                // remove from Available map as it is being persisted
                if (pdms.removeActionFromAvailPartitions(actionId)) {
                    LOG.debug("Succesfully removed actionId: [{0}] from available Map ", actionId);
                }
                else {
                    LOG.warn("Unable to remove actionId: [{0}] from available Map ", actionId);
                }
            }
        }
        LOG.info("ENDED for Action id [{0}]", actionId);
        return null;
    }

    // TODO: Revisit efficiency
    private String removePartitions(String missPartitions, List<PartitionWrapper> availPartitionList)
            throws CommandException {
        if (missPartitions == null || missPartitions.length() == 0) {
            LOG.warn("Missing dependency is empty. avaialableMap is :" + availPartitionList);
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
                LOG.warn("NOT found partition [{0}] into missingList: [{1}] ", part, missPartList);
            }
        }
        for (PartitionWrapper missParts : missPartList) {
            if (ret.length() > 0) {
                ret.append(CoordELFunctions.DIR_SEPARATOR);
            }
            ret.append(missParts.toString());
        }
        return ret.toString();
    }

    private List<PartitionWrapper> createPartitionWrapper(String missPartitions) throws CommandException {
        String[] parts = missPartitions.split(CoordELFunctions.DIR_SEPARATOR);
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

    @Override
    protected void loadState() throws CommandException {
        try {
            jpaService = Services.get().get(JPAService.class);
            if (jpaService != null) {
                coordAction = jpaService.execute(new CoordActionGetForInputCheckJPAExecutor(actionId));
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

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if (coordAction.getStatus() != CoordinatorActionBean.Status.WAITING) {
            throw new PreconditionException(ErrorCode.E1100, "[" + actionId
                    + "]::CoordActionInputCheck:: Ignoring action. Should be in WAITING state, but state="
                    + coordAction.getStatus());
        }
        // TODO: check the parent coordinator job?
    }

}
