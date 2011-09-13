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

import javax.persistence.EntityManager;

/**
 * Executor pattern interface that gives access to an {@link EntityManager}.
 * <p/>
 * Implementations are executed by the {@link org.apache.oozie.service.JPAService}.
 */
public interface JPAExecutor<T> {

    /**
     * Return the name of the JPA executor. Used for logging and instrumentation.
     *
     * @return the name of the JPA executor.
     */
    public String getName();

    /**
     * Method that encapsulates JPA access operations.
     * <p/>
     * Implementations should not close the received {@link EntityManager}.
     * <p/>
     * Implementations should commit any transaction before ending, else the transaction will be rolled back.
     *
     * @param em an active {@link EntityManager}
     * @return a return value if any.
     * @throws JPAExecutorException thrown if a jpa executor failed
     */
    public T execute(EntityManager em) throws JPAExecutorException;

}
