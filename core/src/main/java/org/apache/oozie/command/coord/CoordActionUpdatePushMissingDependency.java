package org.apache.oozie.command.coord;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.dependency.DependencyChecker;
import org.apache.oozie.dependency.URIHandler;
import org.apache.oozie.service.PartitionDependencyManagerService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.URIHandlerService;

public class CoordActionUpdatePushMissingDependency extends CoordPushDependencyCheckXCommand {

    public CoordActionUpdatePushMissingDependency(String actionId) {
        super("coord_action_push_md", actionId);
    }

    @Override
    protected Void execute() throws CommandException {
        LOG.info("STARTED for Action id [{0}]", actionId);
        String pushMissingDeps = coordAction.getPushMissingDependencies();
        if (pushMissingDeps == null || pushMissingDeps.length() == 0) {
            LOG.info("Nothing to check. Empty push missing dependency");
            return null;
        }

        PartitionDependencyManagerService pdms = Services.get().get(PartitionDependencyManagerService.class);
        Collection<String> availDepList = pdms.getAvailableDependencyURIs(actionId);
        if (availDepList == null || availDepList.size() == 0) {
            LOG.info("There is no available dependency List of action ID: [{0}]", actionId);
            if (isTimeout()) { // Poll and check as one last try
                queue(new CoordPushDependencyCheckXCommand(coordAction.getId()), 100);
            }
            return null;
        }

        LOG.debug("Updating action Id " + actionId + " for available partition of " + availDepList.toString()
                + "missing parts :" + pushMissingDeps);

        String[] missingDepsArray = DependencyChecker.dependenciesAsArray(pushMissingDeps);
        List<String> missingDepsAfterCheck = new ArrayList<String>();
        for (String missingDep : missingDepsArray) {
            if (!availDepList.contains(missingDep)) {
                missingDepsAfterCheck.add(missingDep);
            }
        }
        boolean isChangeInDependency = true;
        if (missingDepsAfterCheck.size() == 0) { // All push-based dependencies are available
            onAllPushDependenciesAvailable();
        }
        else {
            if (missingDepsAfterCheck.size() == missingDepsArray.length) {
                isChangeInDependency = false;
            }
            else {
                String stillMissingDeps = DependencyChecker.dependenciesAsString(missingDepsAfterCheck);
                coordAction.setPushMissingDependencies(stillMissingDeps);
            }
            if (isTimeout()) { // Poll and check as one last try
                queue(new CoordPushDependencyCheckXCommand(coordAction.getId()), 100);
            }
        }
        updateCoordAction(coordAction, isChangeInDependency);
        unregisterAvailableDependencies(availDepList);
        LOG.info("ENDED for Action id [{0}]", actionId);
        return null;
    }

    private void unregisterAvailableDependencies(Collection<String> availDepList) {
        URIHandlerService uriService = Services.get().get(URIHandlerService.class);
        for (String availDepURI : availDepList) {
            try {
                URI availableURI = new URI(availDepURI);
                URIHandler handler = uriService.getURIHandler(availableURI);
                if (handler.unregisterFromNotification(availableURI, actionId)) {
                    LOG.debug("Succesfully unregistered uri [{0}] for actionId: [{1}] from notifications",
                            availableURI, actionId);
                }
                else {
                    LOG.warn("Unable to unregister uri [{0}] for actionId: [{1}] from notifications", availableURI,
                            actionId);
                }
            }
            catch (Exception e) {
                LOG.warn("Exception while unregistering uri for actionId: [{0}] from notifications", actionId, e);
            }
        }
    }

    @Override
    public String getEntityKey() {
        return actionId;
    }

}
