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

import org.apache.oozie.ErrorCode;
import org.apache.oozie.SLAEventBean;
import org.apache.oozie.XException;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.XCommand;
import org.apache.oozie.executor.jpa.SLAEventsGetForSeqIdJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;

/**
 * The command to get a list of SLAEvents which are greater than given seqId.
 *
 */
public class SLAEventsXCommand extends XCommand<List<SLAEventBean>> {

    private long seqId = 0;
    private int maxNoEvents = 100; // Default
    private long lastSeqId = -1;

    public SLAEventsXCommand(long seqId, int maxNoEvnts) {
        super("SLAEventsXCommand", "SLAEventsXCommand", 1);
        this.seqId = seqId;
        this.maxNoEvents = maxNoEvnts;
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
        return Long.toString(seqId);
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
    protected List<SLAEventBean> execute() throws CommandException {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            List<SLAEventBean> slaEventList = null;
            long lastSeqId[] = new long[1];
            if (jpaService != null) {
                slaEventList = jpaService.execute(new SLAEventsGetForSeqIdJPAExecutor(seqId, maxNoEvents, lastSeqId));
            }
            else {
                LOG.error(ErrorCode.E0610);
            }
            setLastSeqId(lastSeqId[0]);
            return slaEventList;
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

    /**
     * Set lastSeqId
     *
     * @param lastSeqId
     */
    public void setLastSeqId(long lastSeqId) {
        this.lastSeqId = lastSeqId;
    }

    /**
     * Get lastSeqId
     *
     * @return lastSeqId
     */
    public long getLastSeqId() {
        return lastSeqId;
    }

}
