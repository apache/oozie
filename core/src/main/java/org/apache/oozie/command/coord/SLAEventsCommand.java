/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.command.coord;

import java.util.List;

import org.apache.oozie.SLAEventBean;
import org.apache.oozie.command.Command;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.store.SLAStore;
import org.apache.oozie.store.Store;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.util.XLog;

public class SLAEventsCommand extends Command<List<SLAEventBean>, SLAStore> {

    private long seqId;
    private int maxNoEvents;
    private long lastSeqId = -1;

    public SLAEventsCommand(long seqId, int maxNoEvnts) {
        super("SLAEventsCommand", "SLAEventsCommand", 0, XLog.OPS);
        this.seqId = seqId;
        this.maxNoEvents = maxNoEvnts;
    }

    @Override
    protected List<SLAEventBean> call(SLAStore store) throws StoreException, CommandException {
        long lsId[] = new long[1];
        List<SLAEventBean> slaEvntList = store.getSLAEventListNewerSeqLimited(seqId, maxNoEvents, lsId);
        store.getEntityManager().clear();
        setLastSeqId(lsId[0]);
        return slaEvntList;
    }

    public void setLastSeqId(long lastSeqId) {
        this.lastSeqId = lastSeqId;
    }

    public long getLastSeqId() {
        return lastSeqId;
    }

    @Override
    public Class<? extends Store> getStoreClass() {
        // TODO Auto-generated method stub
        return SLAStore.class;
    }

}
