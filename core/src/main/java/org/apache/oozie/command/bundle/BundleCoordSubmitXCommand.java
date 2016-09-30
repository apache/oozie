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

package org.apache.oozie.command.bundle;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.coord.CoordSubmitXCommand;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;

public class BundleCoordSubmitXCommand extends CoordSubmitXCommand {

    private String coordId;

    public BundleCoordSubmitXCommand(Configuration conf, String bundleId, String coordName) {
        super(conf, bundleId, coordName);
    }

    @Override
    public String getEntityKey() {
        return bundleId + "_" + coordName;
    }

    @Override
    protected boolean isLockRequired() {
        return true;
    }

    @Override
    protected void verifyPrecondition() throws CommandException {
        super.verifyPrecondition();
        if (coordId != null) {
            LOG.warn("Coord [{0}] is already submitted for bundle [{1}]", coordId, bundleId);
            throw new CommandException(ErrorCode.E1304, coordName);
        }
    }

    protected void loadState() throws CommandException {
        super.loadState();
        try {
            CoordinatorJobBean coordJobs = CoordJobQueryExecutor.getInstance().getIfExist(
                    CoordJobQuery.GET_COORD_JOBS_FOR_BUNDLE_BY_APPNAME_ID, coordName, bundleId);

            if (coordJobs != null) {
                coordId = coordJobs.getId();
            }
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

    @Override
    public String getKey() {
        return getName() + "_" + getEntityKey();
    }

}
