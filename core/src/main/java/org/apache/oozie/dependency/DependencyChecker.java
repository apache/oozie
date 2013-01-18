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
package org.apache.oozie.dependency;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.URIAccessorException;
import org.apache.oozie.service.URIHandlerService;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;

public class DependencyChecker {

    private static XLog LOG = XLog.getLog(DependencyChecker.class);

    /**
     * Return a string of missing dependencies concatenated by CoordELFunctions.INSTANCE_SEPARATOR
     *
     * @param missingDependencies list of missing dependencies
     * @return missing dependencies as a string
     */
    public static String dependenciesAsString(List<String> missingDependencies) {
        return StringUtils.join(missingDependencies, CoordELFunctions.INSTANCE_SEPARATOR);
    }

    /**
     * Return a array of missing dependencies
     *
     * @param missingDependencies missing dependencies concatenated by
     *        CoordELFunctions.INSTANCE_SEPARATOR
     * @return missing dependencies as a array
     */
    public static String[] dependenciesAsArray(String missingDependencies) {
        return missingDependencies.split(CoordELFunctions.INSTANCE_SEPARATOR);
    }

    /**
     * Get the currently missing and available dependencies after checking the list of known missing
     * dependencies against the source.
     *
     * @param missingDependencies known missing dependencies
     * @param actionConf Configuration for the action
     * @param stopOnFirstMissing Does not continue check for the rest of list if there is a missing
     *        dependency
     * @return ActionDependency which has the list of missing and available dependencies
     * @throws CommandException
     */
    public static ActionDependency checkForAvailability(String missingDependencies, Configuration actionConf,
            boolean stopOnFirstMissing) throws CommandException {
        return checkForAvailability(dependenciesAsArray(missingDependencies), actionConf, stopOnFirstMissing);
    }

    /**
     * Get the currently missing and available dependencies after checking the list of known missing
     * dependencies against the source.
     *
     * @param missingDependencies known missing dependencies
     * @param actionConf Configuration for the action
     * @param stopOnFirstMissing Does not continue check for the rest of list if there is a missing
     *        dependency
     * @return ActionDependency which has the list of missing and available dependencies
     * @throws CommandException
     */
    public static ActionDependency checkForAvailability(String[] missingDependencies, Configuration actionConf,
            boolean stopOnFirstMissing) throws CommandException {
        String user = ParamChecker.notEmpty(actionConf.get(OozieClient.USER_NAME), OozieClient.USER_NAME);
        List<String> missingDeps = new ArrayList<String>();
        Map<URIHandler, List<URI>> availableDeps = new HashMap<URIHandler, List<URI>>();
        URIHandlerService uriService = Services.get().get(URIHandlerService.class);
        int index = 0;
        for (;index < missingDependencies.length; index++) {
            String dependency = missingDependencies[index];
            try {
                URI uri = new URI(dependency);
                URIHandler uriHandler = uriService.getURIHandler(uri);
                LOG.debug("Checking for the availability of {0} ", dependency);
                if (uriHandler.exists(uri, actionConf, user)) {
                    List<URI> availableURIs = availableDeps.get(uriHandler);
                    if (availableURIs == null) {
                        availableURIs = new ArrayList<URI>();
                        availableDeps.put(uriHandler, availableURIs);
                    }
                    availableURIs.add(uri);
                }
                else {
                    missingDeps.add(dependency);
                    if (stopOnFirstMissing) {
                        index++;
                        break;
                    }
                }
            }
            catch (URISyntaxException e) {
                throw new CommandException(ErrorCode.E0906, e.getMessage(), e);
            }
            catch (URIAccessorException e) {
                throw new CommandException(e);
            }
        }
        if (stopOnFirstMissing) {
            for (;index < missingDependencies.length; index++) {
                missingDeps.add(missingDependencies[index]);
            }
        }
        return new ActionDependency(missingDeps, availableDeps);
    }

    public static class ActionDependency {
        private List<String> missingDependencies;
        private Map<URIHandler, List<URI>> availableDependencies;

        public ActionDependency(List<String> missingDependencies, Map<URIHandler, List<URI>> availableDependencies) {
            this.missingDependencies = missingDependencies;
            this.availableDependencies = availableDependencies;
        }

        public List<String> getMissingDependencies() {
            return missingDependencies;
        }

        public Map<URIHandler, List<URI>> getAvailableDependencies() {
            return availableDependencies;
        }

    }

}
