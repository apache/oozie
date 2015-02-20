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

import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.coord.CoordSLAAlertsEnableXCommand;
import org.apache.oozie.service.ServiceException;

public class BundleSLAAlertsEnableXCommand extends BundleSLAAlertsXCommand {

    public BundleSLAAlertsEnableXCommand(String jobId, String actions, String dates, String childIds) {
        super(jobId, actions, dates, childIds);

    }

    @Override
    protected void loadState() throws CommandException {
    }

    @Override
    protected void executeCoordCommand(String id, String actions, String dates) throws ServiceException,
            CommandException {
        new CoordSLAAlertsEnableXCommand(id, actions, dates).call();
    }

    @Override
    protected void updateJob() throws CommandException {
    }
}
