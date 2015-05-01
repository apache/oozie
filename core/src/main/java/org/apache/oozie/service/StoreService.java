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

import org.apache.oozie.store.StoreException;
import org.apache.oozie.store.SLAStore;
import org.apache.oozie.store.Store;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.ErrorCode;
import javax.persistence.EntityManager;

/**
 * Base service for persistency of jobs and actions.
 */
public class StoreService implements Service {

    /**
     * Return instance of store.
     *
     * @return {@link Store}.
     */
    @SuppressWarnings("unchecked")
    public <S extends Store> S getStore(Class<S> klass) throws StoreException {
        if (WorkflowStore.class.equals(klass)) {
            return (S) Services.get().get(WorkflowStoreService.class).create();
        }
        else if (SLAStore.class.equals(klass)) {
            return (S) Services.get().get(SLAStoreService.class).create();
        }
        // to do add checks for other stores - coordinator and SLA stores
        throw new StoreException(ErrorCode.E0607, " can not get store StoreService.getStore(Class)", "");
    }

    /**
     * Return instance of store with an EntityManager pointing to an existing Store.
     *
     * @return {@link Store}.
     */
    @SuppressWarnings("unchecked")
    public <S extends Store, T extends Store> S getStore(Class<S> klass, T store) throws StoreException {
        if (WorkflowStore.class.equals(klass)) {
            return (S) Services.get().get(WorkflowStoreService.class).create(store);
        }
        else if (SLAStore.class.equals(klass)) {
            return (S) Services.get().get(SLAStoreService.class).create(store);
        }
        throw new StoreException(ErrorCode.E0607, " StoreService.getStore(Class, store)", "");
    }

    /**
     * Return the public interface of the service.
     *
     * @return {@link StoreService}.
     */
    public Class<? extends Service> getInterface() {
        return StoreService.class;
    }

    private JPAService jpaService;

    /**
     * Initializes the {@link StoreService}.
     *
     * @param services services instance.
     */
    public void init(Services services) throws ServiceException {
        jpaService = Services.get().get(JPAService.class);
        if (jpaService == null) {
            throw new ServiceException(ErrorCode.E0610);
        }
    }

    /**
     * Destroy the StoreService
     */
    public void destroy() {
    }

    /**
     * Return EntityManager
     */
    public EntityManager getEntityManager() {
        return jpaService.getEntityManager();
    }
}
