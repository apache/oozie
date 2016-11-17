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

package org.apache.oozie.test;

import org.apache.commons.logging.LogFactory;
import org.apache.oozie.*;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.StoreService;
import org.apache.oozie.sla.SLARegistrationBean;
import org.apache.oozie.sla.SLASummaryBean;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.util.XLog;

import javax.persistence.EntityManager;
import javax.persistence.FlushModeType;
import javax.persistence.TypedQuery;
import java.util.List;

public class TestDbCleaner {
    private static final XLog log = new XLog(LogFactory.getLog(TestDbCleaner.class));

    /**
     * Minimal set of require Services for cleaning up the database ({@link JPAService} and {@link StoreService})
     */
    private static final String MINIMAL_SERVICES_FOR_DB_CLEANUP = JPAService.class.getName() + "," + StoreService.class.getName();

    private EntityManager entityManager;

    void cleanDbTables() throws StoreException, ServiceException {
        // If the Services are already loaded, then a test is likely calling this for something specific and we shouldn't mess with
        // the Services; so just cleanup the database
        if (Services.get() != null) {
            performCleanDbTables();
        }
        else {
            // Otherwise, this is probably being called during setup() and we should just load the minimal set of required Services
            // needed to cleanup the database and shut them down when done; the test will likely start its own Services later and
            // we don't want to interfere
            try {
                final Services services = new Services();
                services.getConf().set(Services.CONF_SERVICE_CLASSES, MINIMAL_SERVICES_FOR_DB_CLEANUP);
                services.init();
                performCleanDbTables();
            } finally {
                if (Services.get() != null) {
                    Services.get().destroy();
                }
            }
        }
    }

    private void performCleanDbTables() throws StoreException {
        ensureEntityManager().setFlushMode(FlushModeType.COMMIT);
        ensureEntityManager().getTransaction().begin();

        final int wfjSize = removeAllByQueryName("GET_WORKFLOWS", WorkflowJobBean.class);
        final int wfaSize = removeAllByQueryName("GET_ACTIONS", WorkflowActionBean.class);
        final int cojSize = removeAllByQueryName("GET_COORD_JOBS", CoordinatorJobBean.class);
        final int coaSize = removeAllByQueryName("GET_COORD_ACTIONS", CoordinatorActionBean.class);
        final int bjSize = removeAllByQueryName("GET_BUNDLE_JOBS", BundleJobBean.class);
        final int baSize = removeAllByQueryName("GET_BUNDLE_ACTIONS", BundleActionBean.class);
        final int slaSize = removeAllByQueryName("GET_SLA_EVENTS", SLAEventBean.class);
        final int ssSize = removeAllByQueryName("GET_SLA_EVENTS", SLAEventBean.class);
        final int slaRegSize = removeAllByHql("select OBJECT(w) from SLARegistrationBean w", SLARegistrationBean.class);
        final int slaSumSize = removeAllByHql("select OBJECT(w) from SLASummaryBean w", SLASummaryBean.class);

        ensureEntityManager().getTransaction().commit();
        ensureEntityManager().close();

        log.info(wfjSize + " entries in WF_JOBS removed from DB!");
        log.info(wfaSize + " entries in WF_ACTIONS removed from DB!");
        log.info(cojSize + " entries in COORD_JOBS removed from DB!");
        log.info(coaSize + " entries in COORD_ACTIONS removed from DB!");
        log.info(bjSize + " entries in BUNDLE_JOBS removed from DB!");
        log.info(baSize + " entries in BUNDLE_ACTIONS removed from DB!");
        log.info(slaSize + " entries in SLA_EVENTS removed from DB!");
        log.info(ssSize + " entries in SLA_SUMMARY removed from DB!");
        log.info(slaRegSize + " entries in SLA_REGISTRATION removed from DB!");
        log.info(slaSumSize + " entries in SLA_SUMMARY removed from DB!");
    }

    private <E> int removeAllByQueryName(final String queryName, final Class<E> entityClass) {
        return removeAll(ensureEntityManager().createNamedQuery(queryName, entityClass));
    }

    private <E> int removeAllByHql(final String hql, final Class<E> entityClass) {
        return removeAll(ensureEntityManager().createQuery(hql, entityClass));
    }

    private <E> int removeAll(final TypedQuery<E> query) {
        final List<E> entitiesToRemove = query.getResultList();
        final int removedEntitiedCount = entitiesToRemove.size();

        for (final E entityToRemove : entitiesToRemove) {
            ensureEntityManager().remove(entityToRemove);
        }

        return removedEntitiedCount;
    }

    private EntityManager ensureEntityManager() {
        if (entityManager == null) {
            entityManager = Services.get().get(JPAService.class).getEntityManager();
        }

        return entityManager;
    }
}
