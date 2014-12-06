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
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.URIHandlerService;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;

public class DependencyChecker {

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
        final XLog LOG = XLog.getLog(DependencyChecker.class); //OOZIE-1251. Don't initialize as static variable.
        String user = ParamChecker.notEmpty(actionConf.get(OozieClient.USER_NAME), OozieClient.USER_NAME);
        List<String> missingDeps = new ArrayList<String>();
        List<String> availableDeps = new ArrayList<String>();
        URIHandlerService uriService = Services.get().get(URIHandlerService.class);
        boolean continueChecking = true;
        try {
            for (int index = 0; index < missingDependencies.length; index++) {
                if (continueChecking) {
                    String dependency = missingDependencies[index];

                    URI uri = new URI(dependency);
                    URIHandler uriHandler = uriService.getURIHandler(uri);
                    LOG.debug("Checking for the availability of dependency [{0}] ", dependency);
                    if (uriHandler.exists(uri, actionConf, user)) {
                        LOG.debug("Dependency [{0}] is available", dependency);
                        availableDeps.add(dependency);
                    }
                    else {
                        LOG.debug("Dependency [{0}] is missing", dependency);
                        missingDeps.add(dependency);
                        if (stopOnFirstMissing) {
                            continueChecking = false;
                        }
                    }

                }
                else {
                    missingDeps.add(missingDependencies[index]);
                }
            }
        }
        catch (URISyntaxException e) {
            throw new CommandException(ErrorCode.E0906, e.getMessage(), e);
        }
        catch (URIHandlerException e) {
            throw new CommandException(e);
        }
        return new ActionDependency(missingDeps, availableDeps);
    }
}
