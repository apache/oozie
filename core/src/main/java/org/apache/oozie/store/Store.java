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

//import javax.persistence.EntityManagerFactory;

import javax.persistence.EntityManager;
import javax.persistence.FlushModeType;
import javax.persistence.PersistenceUnit;
/*
 import javax.persistence.Persistence;
 import org.apache.oozie.CoordinatorActionBean;
 import org.apache.oozie.CoordinatorJobBean;
 import org.apache.oozie.WorkflowActionBean;
 import org.apache.oozie.WorkflowJobBean;
 import org.apache.oozie.SLAEventBean;
 import org.apache.oozie.client.rest.JsonCoordinatorAction;
 import org.apache.oozie.client.rest.JsonCoordinatorJob;
 import org.apache.oozie.client.rest.JsonWorkflowAction;
 import org.apache.oozie.client.rest.JsonWorkflowJob;
 import org.apache.oozie.client.rest.JsonSLAEvent;
 */
import org.apache.oozie.service.Services;
import org.apache.oozie.service.StoreService;
import org.apache.oozie.util.XLog;
import org.apache.openjpa.persistence.OpenJPAEntityManager;
import org.apache.openjpa.persistence.OpenJPAPersistence;

import java.sql.Connection;
import java.sql.SQLException;

@PersistenceUnit(unitName = "oozie")
/**
 * <code>Store</code> Abstract class to separate Entities from Actual store implementation
 */
public abstract class Store {

    private EntityManager entityManager;

    /**
     * create a fresh transaction
     */
    public Store() {
        entityManager = Services.get().get(StoreService.class).getEntityManager();
    }

    /**
     * Use an existing transaction for cross store operations
     */
    public Store(Store store) {
        entityManager = store.getEntityManager();
    }

    /**
     * Return EntityManager
     */
    public EntityManager getEntityManager() {
        return entityManager;
    }

    /**
     * Invoke transaction on the EntityManager
     */
    public void beginTrx() {
        entityManager.setFlushMode(FlushModeType.COMMIT);
        entityManager.getTransaction().begin();
    }

    /**
     * Commit current transaction
     */
    public void commitTrx() {
        entityManager.getTransaction().commit();
    }

    /**
     * Close current transaction <p/> Before close transaction, it needs to be committed.
     */
    public void closeTrx() {
        entityManager.close();
    }

    /**
     * Rollback transaction
     */
    public void rollbackTrx() {
        entityManager.getTransaction().rollback();
    }

    /**
     * Check if transaction is active
     *
     * @return boolean
     */
    public boolean isActive() {
        return entityManager.getTransaction().isActive();
    }

    public String getConnection() {
        OpenJPAEntityManager kem = OpenJPAPersistence.cast(entityManager);
        Connection conn = (Connection) kem.getConnection();
        return conn.toString();
    }

    public boolean isDetached(Object o) {
        OpenJPAEntityManager kem = OpenJPAPersistence.cast(entityManager);
        return kem.isDetached(o);
    }

    public boolean isClosed() {
        OpenJPAEntityManager kem = OpenJPAPersistence.cast(entityManager);
        Connection conn = (Connection) kem.getConnection();
        try {
            return conn.isClosed();
        }
        catch (SQLException e) {
            XLog.getLog(getClass()).info(XLog.STD, e.getMessage(), e);
        }
        return true;
    }

    public boolean contains(Object entity) {
        return entityManager.contains(entity);
    }

    public String getFlushMode() {
        return entityManager.getFlushMode().toString();
    }
}
