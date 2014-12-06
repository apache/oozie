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

import org.apache.oozie.ErrorCode;
import org.apache.oozie.SLAEventBean;
import org.apache.oozie.XException;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.XCommand;
import org.apache.oozie.executor.jpa.SLAEventsGetForFilterJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Service;
import org.apache.oozie.service.Services;

/**
 * The command to get a list of SLAEvents which are greater than given seqId.
 *
 */
@SuppressWarnings("deprecation")
public class SLAEventsXCommand extends XCommand<List<SLAEventBean>> {

    private long seqId = 0;
    private int maxNoEvents = 100; // Default
    private long lastSeqId = -1;
    private final Map<String, List<String>> filter;

    public static final String SLA_DEFAULT_MAXEVENTS = Service.CONF_PREFIX + "sla.default.maxevents";

    public SLAEventsXCommand(long seqId, int maxNoEvnts, Map<String, List<String>> filter) {
        super("SLAEventsXCommand", "SLAEventsXCommand", 1);
        this.seqId = seqId;
        int sysMax = Services.get().getConf().getInt(SLA_DEFAULT_MAXEVENTS, 1000);
        this.maxNoEvents = maxNoEvnts > sysMax ? sysMax : maxNoEvnts;
        this.filter = filter;
    }

    @Override
    protected boolean isLockRequired() {
        return false;
    }

    @Override
    public String getEntityKey() {
        return Long.toString(seqId);
    }

    @Override
    protected void loadState() throws CommandException {
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }

    @Override
    protected List<SLAEventBean> execute() throws CommandException {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            List<SLAEventBean> slaEventList = null;
            long lastSeqId[] = new long[1];
            if (jpaService != null) {
                slaEventList = jpaService.execute(new SLAEventsGetForFilterJPAExecutor(seqId, maxNoEvents, filter, lastSeqId));
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
