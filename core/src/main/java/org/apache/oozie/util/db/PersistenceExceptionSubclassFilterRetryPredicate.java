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

package org.apache.oozie.util.db;

import com.google.common.collect.Sets;
import org.apache.oozie.util.XLog;

import javax.persistence.EntityExistsException;
import javax.persistence.EntityNotFoundException;
import javax.persistence.LockTimeoutException;
import javax.persistence.NoResultException;
import javax.persistence.NonUniqueResultException;
import javax.persistence.OptimisticLockException;
import javax.persistence.PersistenceException;
import javax.persistence.PessimisticLockException;
import javax.persistence.QueryTimeoutException;
import javax.persistence.TransactionRequiredException;
import java.util.Set;

/**
 * A {@link DatabaseRetryPredicate} which applies when a given {@link Exception} (or its causes) are NOT blacklisted.
 * <p/>
 * Blacklisted exceptions in this class do not indicate a network failure, therefore no retry should take place.
 */
public class PersistenceExceptionSubclassFilterRetryPredicate extends DatabaseRetryPredicate {
    private static final XLog LOG = XLog.getLog(PersistenceExceptionSubclassFilterRetryPredicate.class);
    private static final Set<Class<? extends PersistenceException>> BLACKLIST = Sets.newHashSet(
            EntityExistsException.class,
            EntityNotFoundException.class,
            LockTimeoutException.class,
            NoResultException.class,
            NonUniqueResultException.class,
            OptimisticLockException.class,
            PessimisticLockException.class,
            QueryTimeoutException.class,
            TransactionRequiredException.class
    );

    @Override
    public boolean apply(final Throwable throwable) {
        LOG.trace("Retry predicate investigation started. [throwable.class={0}]", throwable.getClass().getName());

        boolean applies = true;

        for (final Class<?> classDownTheStackTrace : getAllExceptions(throwable)) {
            for (final Class<? extends PersistenceException> blacklistElement : BLACKLIST) {
                if (blacklistElement.isAssignableFrom(classDownTheStackTrace)) {
                    applies = false;
                }
            }
        }

        LOG.trace("Retry predicate investigation finished. [applies={0}]", applies);

        return applies;
    }
}
