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

package org.apache.oozie.command.wf;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.WorkflowIdGetForExternalIdJPAExecutor;

public class ExternalIdXCommand extends WorkflowXCommand<String> {
    private String externalId;

    public ExternalIdXCommand(String externalId) {
        super("externalId", "externalId", 1);
        this.externalId = ParamChecker.notEmpty(externalId, "externalId");
    }

    @Override
    protected boolean isLockRequired() {
        return false;
    }

    @Override
    public String getEntityKey() {
        return this.externalId;
    }

    @Override
    protected void loadState() throws CommandException {
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }

    @Override
    protected String execute() throws CommandException {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            String wfId = null;
            if (jpaService != null) {
                wfId = jpaService.execute(new WorkflowIdGetForExternalIdJPAExecutor(externalId));
            }
            else {
                LOG.error(ErrorCode.E0610);
            }
            return wfId;
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

}
