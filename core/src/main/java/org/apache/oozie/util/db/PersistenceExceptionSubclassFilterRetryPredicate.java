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
import org.apache.oozie.executor.jpa.JPAExecutorException;
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
 * <p>
 * Blacklisted exceptions in this class do not indicate a network failure, therefore no retry should take place.
 */
public class PersistenceExceptionSubclassFilterRetryPredicate extends DatabaseRetryPredicate {
    private static final XLog LOG = XLog.getLog(PersistenceExceptionSubclassFilterRetryPredicate.class);

    /**
     * If the {@code Throwable} to be checked has a cause chain, these {@code Exception} classes are used as blacklist: if one of
     * them appear either at the top level, or down the cause chain, no retry will happen.
     */
    @SuppressWarnings("unchecked")
    private static final Set<Class<? extends PersistenceException>> BLACKLIST_WITH_CAUSE = Sets.newHashSet(
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

    /**
     * If the {@code Throwable} to be checked doesn't have a cause, these {@code Exception} classes are used as blacklist: if one of
     * them is assignable from the one to be checked, no retry will happen.
     * <p>
     * Note that this blacklist is different from {@link #BLACKLIST_WITH_CAUSE} because this handles the use case where
     * {@code Exception}s are inserted by a failure injection framework or piece of code rather than the database layer that is
     * failing.
     */
    @SuppressWarnings("unchecked")
    private static final Set<Class<? extends Exception>> BLACKLIST_WITHOUT_CAUSE = Sets.newHashSet(
            JPAExecutorException.class,
            RuntimeException.class
    );

    @Override
    public boolean apply(final Throwable throwable) {
        LOG.trace("Retry predicate investigation started. [throwable.class={0}]", throwable.getClass().getName());

        boolean applies = true;

        if ((throwable.getCause() == null) && BLACKLIST_WITHOUT_CAUSE.contains(throwable.getClass())) {
            applies = false;
        }
        else {
            for (final Class<?> classDownTheStackTrace : getAllExceptions(throwable)) {
                for (final Class<? extends PersistenceException> blacklistElement : BLACKLIST_WITH_CAUSE) {
                    if (blacklistElement.isAssignableFrom(classDownTheStackTrace)) {
                        applies = false;
                    }
                }
            }
        }

        LOG.trace("Retry predicate investigation finished. [applies={0}]", applies);

        return applies;
    }
}
