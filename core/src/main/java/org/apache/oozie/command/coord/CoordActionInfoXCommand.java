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

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.CoordActionGetForInfoJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.ParamChecker;

public class CoordActionInfoXCommand extends CoordinatorXCommand<CoordinatorActionBean> {
    /**
     * This class gets the Coordinator action info based on coordinator action id.
     */
    private final String id;

    public CoordActionInfoXCommand(String id) {
        super("action.info", "action.info", 1);
        this.id = ParamChecker.notEmpty(id, "id");
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#execute()
     */
    @Override
    protected CoordinatorActionBean execute() throws CommandException {
        JPAService jpaService = Services.get().get(JPAService.class);
        if (jpaService != null) {
            CoordinatorActionBean action;
            try {
                action = jpaService.execute(new CoordActionGetForInfoJPAExecutor(this.id));
            }
            catch (JPAExecutorException e) {
                throw new CommandException(e);
            }
            return action;
        }
        else {
            LOG.error(ErrorCode.E0610);
            return null;
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    public String getEntityKey() {
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#loadState()
     */
    @Override
    protected void loadState() throws CommandException {
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#isLockRequired()
     */
    @Override
    protected boolean isLockRequired() {
        return false;
    }
}
