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

package org.apache.oozie.service;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.store.SLAStore;
import org.apache.oozie.store.Store;
import org.apache.oozie.store.StoreException;

public class SLAStoreService implements Service {

    @Override
    public void destroy() {
        // TODO Auto-generated method stub

    }

    @Override
    public Class<? extends Service> getInterface() {
        // TODO Auto-generated method stub
        return SLAStoreService.class;
    }

    /**
     * Return a SLA store instance with a fresh transaction. <p/> The LSA store has to be committed and then closed to
     * commit changes, if only close it rolls back.
     *
     * @return a SLA store.
     * @throws StoreException thrown if the SLA store could not be created.
     */
    public SLAStore create() throws StoreException {
        try {
            return new SLAStore();
        }
        catch (Exception ex) {
            throw new StoreException(ErrorCode.E0600, ex.getMessage(), ex);
        }
    }

    /**
     * Return a SLA store instance with an existing transaction. <p/> The SLA store has to be committed and then closed
     * to commit changes, if only close it rolls back.
     *
     * @return a SLA store.
     * @throws StoreException thrown if the SLA store could not be created.
     */
    public <S extends Store> SLAStore create(S store) throws StoreException {
        try {
            return new SLAStore(store);
        }
        catch (Exception ex) {
            throw new StoreException(ErrorCode.E0600, ex.getMessage(), ex);// TODO:
            // Error
            // CODE
        }
    }

    @Override
    public void init(Services services) throws ServiceException {
        // TODO Auto-generated method stub

    }

}
