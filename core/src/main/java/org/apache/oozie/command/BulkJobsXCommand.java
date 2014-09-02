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

package org.apache.oozie.command;

import java.util.List;
import java.util.Map;

import org.apache.oozie.BulkResponseInfo;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.XCommand;
import org.apache.oozie.executor.jpa.BulkJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;

/**
 * The command to get a job info for a list of bundle jobs by given filters.
 */
public class BulkJobsXCommand extends XCommand<BulkResponseInfo> {
    private Map<String,List<String>> bulkParams;
    private int start = 1;
    private int len = 50;

    /**
     * The constructor for BundleJobsXCommand
     *
     * @param filter the filter string
     * @param start start location for paging
     * @param len total length to get
     */
    public BulkJobsXCommand(Map<String,List<String>> filter, int start, int length) {
        super("bundle.job.info", "bundle.job.info", 1);
        this.bulkParams = filter;
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
    protected BulkResponseInfo execute() throws CommandException {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            BulkResponseInfo bulk = null;
            if (jpaService != null) {
                bulk = jpaService.execute(new BulkJPAExecutor(bulkParams, start, len));
            }
            else {
                LOG.error(ErrorCode.E0610);
            }
            return bulk;
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

}
