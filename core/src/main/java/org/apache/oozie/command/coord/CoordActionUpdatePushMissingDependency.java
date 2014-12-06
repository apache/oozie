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

package org.apache.oozie.command.coord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.dependency.DependencyChecker;
import org.apache.oozie.service.PartitionDependencyManagerService;
import org.apache.oozie.service.Services;

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
        }
        else {
            PartitionDependencyManagerService pdms = Services.get().get(PartitionDependencyManagerService.class);
            Collection<String> availDepList = pdms.getAvailableDependencyURIs(actionId);
            if (availDepList == null || availDepList.size() == 0) {
                LOG.info("There are no available dependencies");
                if (isTimeout()) { // Poll and check as one last try
                    queue(new CoordPushDependencyCheckXCommand(coordAction.getId()), 100);
                }
            }
            else {
                LOG.debug("Updating with available uris=[{0}] where missing uris=[{1}]", availDepList.toString(),
                        pushMissingDeps);

                String[] missingDepsArray = DependencyChecker.dependenciesAsArray(pushMissingDeps);
                List<String> stillMissingDepsList = new ArrayList<String>(Arrays.asList(missingDepsArray));
                stillMissingDepsList.removeAll(availDepList);
                boolean isChangeInDependency = true;
                if (stillMissingDepsList.size() == 0) {
                    // All push-based dependencies are available
                    onAllPushDependenciesAvailable();
                }
                else {
                    if (stillMissingDepsList.size() == missingDepsArray.length) {
                        isChangeInDependency = false;
                    }
                    else {
                        String stillMissingDeps = DependencyChecker.dependenciesAsString(stillMissingDepsList);
                        coordAction.setPushMissingDependencies(stillMissingDeps);
                    }
                    if (isTimeout()) { // Poll and check as one last try
                        queue(new CoordPushDependencyCheckXCommand(coordAction.getId()), 100);
                    }
                }
                updateCoordAction(coordAction, isChangeInDependency);
                removeAvailableDependencies(pdms, availDepList);
                LOG.info("ENDED for Action id [{0}]", actionId);
            }
        }
        return null;
    }

    private void removeAvailableDependencies(PartitionDependencyManagerService pdms, Collection<String> availDepList) {
        if (pdms.removeAvailableDependencyURIs(actionId, availDepList)) {
            LOG.debug("Successfully removed uris [{0}] from available list", availDepList.toString());
        }
        else {
            LOG.warn("Failed to remove uris [{0}] from available list", availDepList.toString(), actionId);
        }
    }

    @Override
    public String getEntityKey() {
        return actionId;
    }

}
