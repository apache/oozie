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

package org.apache.oozie.executor.jpa;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;

/**
 * Base Class of Query Executor
 */
public abstract class QueryExecutor<T, E extends Enum<E>> {

    protected QueryExecutor() {
    }

    public abstract int executeUpdate(E namedQuery, T jobBean) throws JPAExecutorException;

    public void insert(final JsonBean bean) throws JPAExecutorException {
        if (bean != null) {
            JPAService jpaService = Services.get().get(JPAService.class);
            jpaService.execute(new JsonBeanPersisterExecutor(bean));
        }
    }

    public abstract T get(E namedQuery, Object... parameters) throws JPAExecutorException;

    public abstract List<T> getList(E namedQuery, Object... parameters) throws JPAExecutorException;

    public abstract Query getUpdateQuery(E namedQuery, T wfBean, EntityManager em) throws JPAExecutorException;

    public abstract Query getSelectQuery(E namedQuery, EntityManager em, Object... parameters)
            throws JPAExecutorException;

    public abstract Object getSingleValue(E namedQuery, Object... parameters)
            throws JPAExecutorException;

    public abstract T getIfExist(E namedQuery, Object... parameters) throws JPAExecutorException;

}
