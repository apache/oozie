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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.oozie.service.AsyncXCommandExecutor.AccessibleRunnableScheduledFuture;
import org.apache.oozie.service.AsyncXCommandExecutor.PriorityComparator;
import org.apache.oozie.service.AsyncXCommandExecutor.ScheduledXCallable;
import org.apache.oozie.service.CallableQueueService.CallableWrapper;
import org.apache.oozie.util.XCallable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Sets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.times;
import static org.mockito.Matchers.same;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;

@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("deprecation")
public class TestAsyncXCommandExecutor {
    private static final String DEFAULT_TYPE = "test";
    private static final int DEFAULT_MAX_ACTIVE_COMMANDS = 5;
    private static final boolean DEFAULT_ENABLE_CONCURRENCY_CHECK = true;
    private static final long DEFAULT_MAXWAIT = 30_000;
    private static final int TEST_PRIORITIES = 5;
    private static final int MAX_PRIORITY = TEST_PRIORITIES - 1;
    private static final int AWAIT_TERMINATION_TIMEOUT_SECONDS = 1;

    @Mock
    private ThreadPoolExecutor executor;

    @Mock
    private ScheduledThreadPoolExecutor scheduledExecutor;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private CallableWrapper<?> callableWrapper;

    @Mock
    private CallableQueueService callableQueueService;

    private PriorityBlockingQueue<CallableWrapper<?>> priorityBlockingQueue;
    private BlockingQueue<AccessibleRunnableScheduledFuture<ScheduledXCallable>> delayQueue;
    private ConcurrentHashMap<String, Set<CallableWrapper<?>>> pendingCommandsPerType;
    private AtomicInteger activeCommands;
    private AsyncXCommandExecutor asyncExecutor;

    @Before
    public void setup() {
        activeCommands = new AtomicInteger(0);
        priorityBlockingQueue = new PriorityBlockingQueue<>(100, new PriorityComparator());
        pendingCommandsPerType = new ConcurrentHashMap<>();
        delayQueue = new LinkedBlockingQueue<>();  // in reality it's not LBQ, but it's fine here
        asyncExecutor = createExecutor(DEFAULT_ENABLE_CONCURRENCY_CHECK, DEFAULT_MAX_ACTIVE_COMMANDS, DEFAULT_MAXWAIT,
                TEST_PRIORITIES, AWAIT_TERMINATION_TIMEOUT_SECONDS);
        when(callableWrapper.filterDuplicates()).thenReturn(true);
        when(callableWrapper.getElement().getKey()).thenReturn("key");
        when(callableWrapper.getElement().getType()).thenReturn(DEFAULT_TYPE);
    }

    @Test
    public void testSubmitCallableWithNoDelay() {
        boolean result = asyncExecutor.queue(callableWrapper, false);

        verify(executor).execute(same(callableWrapper));
        verifyZeroInteractions(scheduledExecutor);
        assertEquals("Active commands", 1, asyncExecutor.getSize());
        assertTrue("Queuing result", result);
    }

    @Test
    public void testSubmitCallableWithDelay() {
        when(callableWrapper.getInitialDelay()).thenReturn(111L);
        when(callableWrapper.getDelay(eq(TimeUnit.MILLISECONDS))).thenReturn(222L);

        boolean result = asyncExecutor.queue(callableWrapper, false);

        verify(scheduledExecutor).schedule(any(ScheduledXCallable.class), eq(222L), eq(TimeUnit.MILLISECONDS));
        verifyZeroInteractions(executor);
        assertEquals("Active commands", 1, asyncExecutor.getSize());
        assertTrue("Queuing result", result);
    }

    @Test
    public void testSubmissionSuccessfulAfterDelay() {
        when(callableWrapper.getInitialDelay()).thenReturn(100L);
        when(callableWrapper.getDelay(eq(TimeUnit.MILLISECONDS))).thenReturn(50L);
        when(callableQueueService.canSubmitCallable(any(XCallable.class))).thenReturn(true);
        configureMockScheduler();

        asyncExecutor.queue(callableWrapper, false);

        verify(scheduledExecutor).schedule(any(ScheduledXCallable.class), eq(50L),
                eq(TimeUnit.MILLISECONDS));
        verify(executor).execute(callableWrapper);
    }

    @Test
    public void testSubmissionFailsAfterDelay() {
        when(callableWrapper.getInitialDelay()).thenReturn(100L);
        when(callableWrapper.getDelay(eq(TimeUnit.MILLISECONDS))).thenReturn(50L);
        configureMockScheduler();

        asyncExecutor.queue(callableWrapper, false);

        verify(scheduledExecutor).schedule(any(ScheduledXCallable.class), eq(50L),
                eq(TimeUnit.MILLISECONDS));
        verifyZeroInteractions(executor);
    }

    @Test
    public void testSubmissionSuccessfulAfterDelayWhenMaxConcurrencyCheckDisabled() {
        asyncExecutor = createExecutor(false, 2, DEFAULT_MAXWAIT, TEST_PRIORITIES, AWAIT_TERMINATION_TIMEOUT_SECONDS);
        when(callableWrapper.getInitialDelay()).thenReturn(100L);
        when(callableWrapper.getDelay(eq(TimeUnit.MILLISECONDS))).thenReturn(50L);
        XCallable<?> wrappedCommand = mock(XCallable.class);
        Mockito.<XCallable<?>>when(callableWrapper.getElement()).thenReturn(wrappedCommand);
        configureMockScheduler();

        asyncExecutor.queue(callableWrapper, false);

        verify(scheduledExecutor).schedule(any(ScheduledXCallable.class), eq(50L),
                eq(TimeUnit.MILLISECONDS));
        verify(executor).execute(eq(callableWrapper));
    }

    @Test
    public void testCannotSubmitDueToFiltering() {
        when(callableWrapper.filterDuplicates()).thenReturn(false);

        boolean result = asyncExecutor.queue(callableWrapper, false);

        verifyZeroInteractions(scheduledExecutor);
        verifyZeroInteractions(executor);
        assertEquals("Active commands", 0, asyncExecutor.getSize());
        assertTrue("Queuing result", result);
    }

    @Test
    public void testExceptionThrownDuringSubmission() {
        doThrow(new RuntimeException()).when(executor).execute(any(Runnable.class));

        boolean exceptionThrown = false;
        try {
            asyncExecutor.queue(callableWrapper, false);
        } catch (RuntimeException e) {
            exceptionThrown = true;
        }

        assertTrue("Exception was not thrown", exceptionThrown);
        verify(callableWrapper).removeFromUniqueCallables();
        verifyZeroInteractions(scheduledExecutor);
    }

    @Test
    public void testSubmitWithNegativePriority() {
        testIllegalPriority(-1);
    }

    @Test
    public void testSubmitWithTooHighPriority() {
        testIllegalPriority(MAX_PRIORITY + 1);
    }

    @Test
    public void testQueueSizeWhenCommandIsFinished() {
        CallableWrapper<?> delayedCommand = mock(CallableWrapper.class);
        when(delayedCommand.getInitialDelay()).thenReturn(100L);
        when(delayedCommand.filterDuplicates()).thenReturn(true);

        asyncExecutor.queue(callableWrapper, false);
        asyncExecutor.queue(delayedCommand, false);
        int sizeAfterQueue = asyncExecutor.getSize();
        asyncExecutor.commandFinished();
        asyncExecutor.commandFinished();

        assertEquals("Size after queue", 2, sizeAfterQueue);
        assertEquals("Active commands", 0, asyncExecutor.getSize());
    }

    @Test
    public void testQueueSizeWhenQueueIsFullDuringMaxConcurrencyCheck() {
        XCallable<?> callable = mock(XCallable.class);
        when(callable.getType()).thenReturn(DEFAULT_TYPE);
        Mockito.<XCallable<?>>when(callableWrapper.getElement()).thenReturn(callable);
        when(callableQueueService.canSubmitCallable(eq(callable))).thenReturn(true);
        asyncExecutor.handleConcurrencyExceeded(callableWrapper);
        activeCommands.set(20);

        asyncExecutor.checkMaxConcurrency(DEFAULT_TYPE);

        assertEquals("Active commands", 19, activeCommands.get());
    }

    @Test
    public void testSubmissionWhenQueueIsFull() {
        asyncExecutor = createExecutor(true, 2, DEFAULT_MAXWAIT, TEST_PRIORITIES, AWAIT_TERMINATION_TIMEOUT_SECONDS);
        callableWrapper = mock(CallableWrapper.class, Mockito.RETURNS_DEEP_STUBS);
        when(callableWrapper.filterDuplicates()).thenReturn(true);
        when(callableWrapper.getElement().getKey()).thenReturn("key");

        asyncExecutor.queue(callableWrapper, false);
        asyncExecutor.queue(callableWrapper, false);
        boolean finalResult = asyncExecutor.queue(callableWrapper, false);

        assertFalse("Last submission shouldn't have succeeded", finalResult);
        verify(executor, times(2)).execute(same(callableWrapper));
    }

    @Test
    public void testSubmissionWhenQueueSizeIsIgnored() {
        asyncExecutor = createExecutor(true, 2, DEFAULT_MAXWAIT, TEST_PRIORITIES, AWAIT_TERMINATION_TIMEOUT_SECONDS);
        callableWrapper = mock(CallableWrapper.class, Mockito.RETURNS_DEEP_STUBS);
        when(callableWrapper.filterDuplicates()).thenReturn(true);
        when(callableWrapper.getElement().getKey()).thenReturn("key");

        asyncExecutor.queue(callableWrapper, false);
        asyncExecutor.queue(callableWrapper, false);
        boolean finalResult = asyncExecutor.queue(callableWrapper, true);

        assertTrue("Last submission should have succeeded", finalResult);
        verify(executor, times(3)).execute(same(callableWrapper));
    }

    @Test
    public void testPendingCommandSubmission() {
        XCallable<?> callable = mock(XCallable.class);
        when(callable.getType()).thenReturn(DEFAULT_TYPE);
        Mockito.<XCallable<?>>when(callableWrapper.getElement()).thenReturn(callable);
        when(callableQueueService.canSubmitCallable(eq(callable))).thenReturn(true);

        asyncExecutor.handleConcurrencyExceeded(callableWrapper);
        asyncExecutor.checkMaxConcurrency(DEFAULT_TYPE);

        verify(executor).execute(eq(callableWrapper));
        assertEquals("Number of pending commands", 1, pendingCommandsPerType.size());
        Set<CallableWrapper<?>> pendingCommandsList = pendingCommandsPerType.get(DEFAULT_TYPE);
        assertNotNull("List of pending commands doesn't exist", pendingCommandsList);
        assertEquals("List of pending commands should be empty", 0, pendingCommandsList.size());
    }

    @Test
    public void testPendingCommandsWithSameType() {
        XCallable<?> callable = mock(XCallable.class);
        when(callable.getType()).thenReturn(DEFAULT_TYPE);
        Mockito.<XCallable<?>>when(callableWrapper.getElement()).thenReturn(callable);

        XCallable<?> secondCallable = mock(XCallable.class);
        when(secondCallable.getType()).thenReturn(DEFAULT_TYPE);
        CallableWrapper<?> secondWrapper = mock(CallableWrapper.class);
        Mockito.<XCallable<?>>when(secondWrapper.getElement()).thenReturn(secondCallable);

        asyncExecutor.handleConcurrencyExceeded(callableWrapper);
        asyncExecutor.handleConcurrencyExceeded(secondWrapper);

        assertEquals("Number of pending commands", 1, pendingCommandsPerType.size());
        Set<CallableWrapper<?>> pendingCommandsList = pendingCommandsPerType.get(DEFAULT_TYPE);
        assertNotNull("List of pending commands doesn't exist", pendingCommandsList);
        assertEquals("List of pending commands", 2, pendingCommandsList.size());
    }

    @Test
    public void testPendingCommandSubmissionWhenQueueIsFull() {
        XCallable<?> callable = mock(XCallable.class);
        when(callable.getType()).thenReturn(DEFAULT_TYPE);
        Mockito.<XCallable<?>>when(callableWrapper.getElement()).thenReturn(callable);

        when(callableQueueService.canSubmitCallable(eq(callable))).thenReturn(true);

        activeCommands.set(10);
        asyncExecutor.handleConcurrencyExceeded(callableWrapper);
        asyncExecutor.checkMaxConcurrency(DEFAULT_TYPE);

        verifyZeroInteractions(executor);
        assertEquals("Number of pending commands", 1, pendingCommandsPerType.size());
        Set<CallableWrapper<?>> pendingCommandsList = pendingCommandsPerType.get(DEFAULT_TYPE);
        assertNotNull("List of pending commands doesn't exist", pendingCommandsList);
        assertEquals("List of pending commands should be empty", 0, pendingCommandsList.size());
    }

    @Test
    public void testPendingCommandSubmissionWhenMaxConcurrencyReached() {
        XCallable<?> callable = mock(XCallable.class);
        when(callable.getType()).thenReturn(DEFAULT_TYPE);
        Mockito.<XCallable<?>>when(callableWrapper.getElement()).thenReturn(callable);
        when(callableQueueService.canSubmitCallable(eq(callable))).thenReturn(false);

        asyncExecutor.handleConcurrencyExceeded(callableWrapper);
        asyncExecutor.checkMaxConcurrency(DEFAULT_TYPE);

        verifyZeroInteractions(executor);
        assertEquals("Number of pending commands", 1, pendingCommandsPerType.size());
        Set<CallableWrapper<?>> pendingCommandsList = pendingCommandsPerType.get(DEFAULT_TYPE);
        assertNotNull("List of pending commands doesn't exist", pendingCommandsList);
        assertEquals("List of pending commands list should not be empty", 1, pendingCommandsList.size());
    }

    @Test
    public void testQueueDump() {
        CallableWrapper<?> pendingCallable = mock(CallableWrapper.class);
        CallableWrapper<?> waitingCallable = mock(CallableWrapper.class);
        ScheduledXCallable delayedXCallable = mock(ScheduledXCallable.class);
        @SuppressWarnings("unchecked")
        AccessibleRunnableScheduledFuture<ScheduledXCallable> asrf = mock(AccessibleRunnableScheduledFuture.class);
        Mockito.<CallableWrapper<?>>when(delayedXCallable.getCallableWrapper()).thenReturn(waitingCallable);
        when(asrf.getTask()).thenReturn(delayedXCallable);
        when(pendingCallable.toString()).thenReturn("pendingCallable");
        when(waitingCallable.toString()).thenReturn("waitingCallable");
        when(callableWrapper.toString()).thenReturn("callableWrapper");

        priorityBlockingQueue.add(callableWrapper);
        delayQueue.add(asrf);
        pendingCommandsPerType.put(DEFAULT_TYPE, Sets.newHashSet(pendingCallable));

        List<String> queueDump = asyncExecutor.getQueueDump();
        assertEquals("Size", 3, queueDump.size());
        assertTrue("PendingCallable not found", queueDump.contains("pendingCallable"));
        assertTrue("WaitingCallable not found", queueDump.contains("waitingCallable"));
        assertTrue("CallableWrapper not found", queueDump.contains("callableWrapper"));
    }

    @Test
    public void testAntiStarvationWhenDelayIsAboveMaxWait() {
        asyncExecutor = createExecutor(DEFAULT_ENABLE_CONCURRENCY_CHECK, DEFAULT_MAX_ACTIVE_COMMANDS, 500, TEST_PRIORITIES,
                AWAIT_TERMINATION_TIMEOUT_SECONDS);
        when(callableWrapper.getDelay(eq(TimeUnit.MILLISECONDS))).thenReturn(-40000L);
        when(callableWrapper.getPriority()).thenReturn(0);
        pendingCommandsPerType.put(DEFAULT_TYPE, Sets.newHashSet(callableWrapper));

        asyncExecutor.checkMaxConcurrency(DEFAULT_TYPE);

        verify(callableWrapper).setPriority(1);
        verify(callableWrapper).setDelay(eq(0L), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testAntiStarvationWhenDelayIsBelowMaxWait() {
        when(callableWrapper.getDelay(eq(TimeUnit.MILLISECONDS))).thenReturn(-200L);
        when(callableWrapper.getPriority()).thenReturn(0);
        pendingCommandsPerType.put(DEFAULT_TYPE, Sets.newHashSet(callableWrapper));

        asyncExecutor.checkMaxConcurrency(DEFAULT_TYPE);

        verify(callableWrapper, never()).setPriority(anyInt());
        verify(callableWrapper, never()).setDelay(anyLong(), any(TimeUnit.class));
    }

    @Test
    public void testAntiStarvationWhenPriorityIsHighest() {
        asyncExecutor = createExecutor(DEFAULT_ENABLE_CONCURRENCY_CHECK, DEFAULT_MAX_ACTIVE_COMMANDS, 500, TEST_PRIORITIES,
                AWAIT_TERMINATION_TIMEOUT_SECONDS);
        when(callableWrapper.getDelay(eq(TimeUnit.MILLISECONDS))).thenReturn(-1000L);
        when(callableWrapper.getPriority()).thenReturn(MAX_PRIORITY);
        pendingCommandsPerType.put(DEFAULT_TYPE, Sets.newHashSet(callableWrapper));

        asyncExecutor.checkMaxConcurrency(DEFAULT_TYPE);

        verify(callableWrapper, never()).setPriority(anyInt());
        verify(callableWrapper, never()).setDelay(anyLong(), any(TimeUnit.class));
    }

    @Test
    public void testShutDown() throws InterruptedException {
        when(executor.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(true);
        when(scheduledExecutor.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(true);
        asyncExecutor.shutdown();

        verify(executor).shutdown();
        verify(executor).awaitTermination(eq(1000L), eq(TimeUnit.MILLISECONDS));
        verify(scheduledExecutor).shutdown();
        verify(scheduledExecutor).awaitTermination(eq(1000L), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testPriorityHandling() {
        asyncExecutor = createExecutor(DEFAULT_ENABLE_CONCURRENCY_CHECK, 100, DEFAULT_MAXWAIT, 100,
                AWAIT_TERMINATION_TIMEOUT_SECONDS);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                CallableWrapper<?> wrapper = (CallableWrapper<?>) invocation.getArguments()[0];
                priorityBlockingQueue.add(wrapper);
                return null;
            }
        }).when(executor).execute(any(Runnable.class));

        List<CallableWrapper<?>> mockedWrappers = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            CallableWrapper<?> wrapper = mock(CallableWrapper.class, Mockito.RETURNS_DEEP_STUBS);
            when(wrapper.getPriority()).thenReturn(i);
            when(wrapper.getInitialDelay()).thenReturn(0L);
            when(wrapper.filterDuplicates()).thenReturn(true);
            when(wrapper.getElement().getName()).thenReturn(String.valueOf(i));
            mockedWrappers.add(wrapper);
        }

        for (CallableWrapper<?> callable : mockedWrappers) {
            asyncExecutor.queue(callable, false);
        }

        CallableWrapper<?> firstElement = priorityBlockingQueue.poll();

        CallableWrapper<?> lastElement = null;
        CallableWrapper<?> previous = null;

        do {
            previous = lastElement;
            lastElement = priorityBlockingQueue.poll();
        } while (lastElement != null);
        lastElement = previous;

        assertEquals("Priority - first element", 99, firstElement.getPriority());
        assertEquals("Priority - last element", 0, lastElement.getPriority());
    }

    private void testIllegalPriority(int prio) {
        when(callableWrapper.getPriority()).thenReturn(prio);

        boolean exceptionThrown = false;
        Throwable cause = null;
        try {
            asyncExecutor.queue(callableWrapper, false);
        } catch (RuntimeException e) {
            exceptionThrown = true;
            cause = e.getCause();
        }

        assertTrue("Exception was not thrown", exceptionThrown);
        verifyZeroInteractions(scheduledExecutor);
        verifyZeroInteractions(executor);
        assertTrue("Illegal exception", cause instanceof IllegalArgumentException);
        verify(callableWrapper).removeFromUniqueCallables();
    }

    private void configureMockScheduler() {
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                ScheduledXCallable target = (ScheduledXCallable) invocation.getArguments()[0];
                target.run();
                return null;
            }
        }).when(scheduledExecutor).schedule(any(ScheduledXCallable.class), any(Long.class),
                any(TimeUnit.class));
    }

    private AsyncXCommandExecutor createExecutor(boolean needMaxConcurrencyCheck, int maxActiveCallables,
            long maxWait, int priorities, int awaitTerminationTimeoutSeconds) {
        return new AsyncXCommandExecutor(needMaxConcurrencyCheck,
                callableQueueService,
                maxActiveCallables,
                executor,
                scheduledExecutor,
                priorityBlockingQueue,
                delayQueue,
                pendingCommandsPerType,
                activeCommands,
                maxWait,
                priorities,
                awaitTerminationTimeoutSeconds);
    }
}
