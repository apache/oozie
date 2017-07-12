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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;

import java.util.concurrent.Callable;

import org.apache.commons.lang.mutable.MutableInt;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Predicate;

/**
 * Conventions used in the tests:
 *  <ul>
 *      <li>{@code RuntimeException} indicates an SQL error (network failure) where we should retry</li>
 *      <li>{@code IllegalStateException} is a different SQL error where we should not retry</li>
 *  </ul>
 */
public class TestOperationRetryHandler {

    private final Predicate<Throwable> testRetryPredicate = new Predicate<Throwable>() {
        @Override
        public boolean apply(Throwable input) {
            return input.getClass() == RuntimeException.class;
        }
    };

    private final OperationRetryHandler retryHandler = new OperationRetryHandler(3, 1, 1, testRetryPredicate);

    @Test
    public void testWhenConfiguredNotToRetryThenTriesOnce() throws Exception {
        tryOnceAndAssert(new OperationRetryHandler(0, 1, 100, testRetryPredicate));
        tryOnceAndAssert(new OperationRetryHandler(10, 1, 0, testRetryPredicate));
        tryOnceAndAssert(new OperationRetryHandler(0, 1, 0, testRetryPredicate));
    }

    private void tryOnceAndAssert(final OperationRetryHandler tryingOnceRetryHandler) throws Exception {
        @SuppressWarnings("unchecked")
        final Callable<String> operation = mock(Callable.class);
        final MutableInt callCount = new MutableInt(0);

        willAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                callCount.increment();
                return null;
            }
        }).given(operation).call();

        tryingOnceRetryHandler.executeWithRetry(operation);

        assertCallCount(1, callCount);
        assertNoRetryAttemptsAreInProgressOrExhausted();
    }

    @Test
    public void testNoRetry() throws Exception {
        @SuppressWarnings("unchecked")
        Callable<String> operation = mock(Callable.class);
        given(operation.call()).willReturn("dummy");

        retryHandler.executeWithRetry(operation);
    }

    @Test
    public void testRetriesOnFailure() throws Exception {
        @SuppressWarnings("unchecked")
        Callable<String> operation = mock(Callable.class);
        final MutableInt callCount = new MutableInt(0);

        willAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                callCount.increment();
                throw new RuntimeException();
            }
        }).given(operation).call();

        boolean exceptionThrown = false;
        try {
            retryHandler.executeWithRetry(operation);
        } catch (RuntimeException e) {
            exceptionThrown = true;
        }

        assertTrue("Exception was not thrown", exceptionThrown);
        assertCallCount(3, callCount);
        assertNoRetryAttemptsAreInProgressOrExhausted();
    }

    @Test
    public void testRetriesOnFailureRecovery() throws Exception {
        @SuppressWarnings("unchecked")
        Callable<String> operation = mock(Callable.class);
        final MutableInt callCount = new MutableInt(0);

        willAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                callCount.increment();
                if (callCount.intValue() < 2) {
                    throw new RuntimeException();
                }
                return null;
            }
        }).given(operation).call();

        retryHandler.executeWithRetry(operation);

        assertCallCount(2, callCount);
        assertNoRetryAttemptsAreInProgressOrExhausted();
    }

    @Test
    public void testNoRetriesOnNonSQLError() throws Exception {
        @SuppressWarnings("unchecked")
        Callable<String> operation = mock(Callable.class);
        final MutableInt callCount = new MutableInt(0);

        willAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                callCount.increment();
                throw new IllegalStateException();
            }
        }).given(operation).call();

        boolean exceptionThrown = false;
        try {
            retryHandler.executeWithRetry(operation);
        } catch (IllegalStateException e) {
            exceptionThrown = true;
        }

        assertTrue("Exception was not thrown", exceptionThrown);
        assertCallCount(1, callCount);
        assertNoRetryAttemptsAreInProgressOrExhausted();
    }

    @Test
    public void testEmbeddedRetryHandlersWhenInnerHandlerThrowsSQLError() throws Exception {
        @SuppressWarnings("unchecked")
        final Callable<String> innerOperation = mock(Callable.class);
        final MutableInt callCount = new MutableInt(0);

        // internal operation which will be retried
        willAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                callCount.increment();
                throw new RuntimeException();
            }
        }).given(innerOperation).call();

        @SuppressWarnings("unchecked")
        Callable<String> outerOperation = mock(Callable.class);

        // the outer operation which calls the inner one and this is not
        // supposed to be retried
        willAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                try {
                    retryHandler.executeWithRetry(innerOperation);
                } catch (Exception e) {
                    throw new RuntimeException();
                }
                return null;
            }
        }).given(outerOperation).call();

        boolean exceptionThrown = false;
        try {
            retryHandler.executeWithRetry(outerOperation);
        } catch (Exception e) {
            exceptionThrown = true;
        }

        assertTrue("Exception was not thrown", exceptionThrown);
        assertCallCount(3, callCount);
        assertNoRetryAttemptsAreInProgressOrExhausted();
    }

    private void assertCallCount(int expected, MutableInt callCount) {
        assertEquals("Number of retries", expected, callCount.intValue());
    }

    private void assertNoRetryAttemptsAreInProgressOrExhausted() {
        assertEquals("Nesting level", 0,
                OperationRetryHandler.RETRY_ATTEMPT_STATE.getInProgressCount());
        assertEquals("Retries performed", false,
                OperationRetryHandler.RETRY_ATTEMPT_STATE.isExhausted());
    }
}
