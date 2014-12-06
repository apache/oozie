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

import java.util.List;
import java.util.Map;

import org.apache.oozie.CoordinatorJobInfo;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.CoordJobInfoGetJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;

/**
 * The command to get a job info for a list of coordinator jobs by given filters.
 */
public class CoordJobsXCommand extends CoordinatorXCommand<CoordinatorJobInfo> {
    private Map<String, List<String>> filter;
    private int start = 1;
    private int len = 50;

    public CoordJobsXCommand(Map<String, List<String>> filter, int start, int length) {
        super("coord.job.info", "coord.job.info", 1);
        this.filter = filter;
        this.start = start;
        this.len = length;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#isLockRequired()
     */
    @Override
    protected boolean isLockRequired() {
        return false;
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
     * @see org.apache.oozie.command.XCommand#execute()
     */
    @Override
    protected CoordinatorJobInfo execute() throws CommandException {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            CoordinatorJobInfo coordInfo = null;
            if (jpaService != null) {
                coordInfo = jpaService.execute(new CoordJobInfoGetJPAExecutor(filter, start, len));
            }
            else {
                LOG.error(ErrorCode.E0610);
            }
            return coordInfo;
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

}
