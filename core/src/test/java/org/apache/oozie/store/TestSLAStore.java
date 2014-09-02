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

package org.apache.oozie.store;

import java.util.List;

import org.apache.oozie.SLAEventBean;
import org.apache.oozie.service.SLAStoreService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XTestCase;

public class TestSLAStore extends XTestCase {
    Services services;
    SLAStore store;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        store = Services.get().get(SLAStoreService.class).create();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testSLAStore() throws StoreException {
        String slaId = "1";
        try {
            _testInsertEvent(slaId);
            //_testGetSlaEventSeqNewer(0);
            //_testGetSlaEventSeqNewerLimited(0, 10);
        }
        finally {

        }
    }

    private void _testGetSlaEventSeqNewerLimited(long seqId, int limitLen) {
        // store.beginTrx();
        try {
            long lastSeqId[] = new long[1];
            List<SLAEventBean> slaEvents = store
                    .getSLAEventListNewerSeqLimited(seqId, limitLen, lastSeqId);
            //System.out.println("AAA " + slaEvents.size() + " : " + lastSeqId[0]);
            if (slaEvents.size() == 0) {
                fail("Unable to GET Get any record of sequence id greater than ="
                        + seqId);
            }
            /*for (int i = 0; i < slaEvents.size(); i++) {
                SLAEventBean event = (SLAEventBean) slaEvents.get(i);
                System.out.println("Limit  seq_id " + event.getEvent_id()
                        + " SLA IS: " + event.getSlaId());
            }*/
            //  store.commitTrx();
        }
        catch (Exception ex) {
            //store.rollbackTrx();
            ex.printStackTrace();
            fail("Unable to GET Get record of sequence id greater than ="
                    + seqId);
        }
    }

/*    private void _testGetSlaEventSeqNewer(long seqId) {
        store.beginTrx();
        try {
            List<SLAEventBean> slaEvents = store.getSLAEventListNewerSeq(seqId);
            System.out.println("Total # of Records " + slaEvents.size());
            if (slaEvents.size() == 0) {
                fail("Unable to GET Get any record of sequence id greater than ="
                        + seqId);
            }
            store.commitTrx();
        }
        catch (Exception ex) {
            store.rollbackTrx();
            ex.printStackTrace();
            fail("Unable to GET Get record of sequence id greater than ="
                    + seqId);
        }
    }*/

    private void _testInsertEvent(String slaId) {
        SLAEventBean sla = createSLAEvent(slaId);
        store.beginTrx();
        try {
            store.insertSLAEvent(sla);
            store.commitTrx();
        }
        catch (Exception ex) {
            store.rollbackTrx();
            ex.printStackTrace();
            fail("Unable to insert a record into COORD Job ");
        }
    }

    private SLAEventBean createSLAEvent(String slaId) {
        SLAEventBean sla = new SLAEventBean();
        sla.setSlaId(slaId);
        // sla.setClientId("GMS");

        return sla;
    }
}
