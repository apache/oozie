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
package org.apache.oozie.util;

import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A Queue implementation that support queuing elements into the future and priority queuing.
 * <p/>
 * The {@link PriorityDelayQueue} avoids starvation by raising elements priority as they age.
 * <p/>
 * To support queuing elements into the future, the JDK <code>DelayQueue</code> is used.
 * <p/>
 * To support priority queuing, an array of <code>DelayQueue</code> sub-queues is used. Elements are consumed from the
 * higher priority sub-queues first. From a sub-queue, elements are available based on their age.
 * <p/>
 * To avoid starvation, there is is maximum wait time for an an element in a sub-queue, after the maximum wait time has
 * elapsed, the element is promoted to the next higher priority sub-queue. Eventually it will reach the maximum priority
 * sub-queue and it will be consumed when it is the oldest element in the that sub-queue.
 * <p/>
 * Every time an element is promoted to a higher priority sub-queue, a new maximum wait time applies.
 * <p/>
 * This class does not use a separate thread for anti-starvation check, instead, the check is performed on polling and
 * seeking operations. This check is performed, the most every 1/2 second.
 */
public class PriorityDelayQueue<E> extends AbstractQueue<PriorityDelayQueue.QueueElement<E>>
        implements BlockingQueue<PriorityDelayQueue.QueueElement<E>> {

    /**
     * Element wrapper required by the queue.
     * <p/>
     * This wrapper keeps track of the priority and the age of a queue element.
     */
    public static class QueueElement<E> implements Delayed {
        private E element;
        private int priority;
        private long baseTime;
        boolean inQueue;

        /**
         * Create an Element wrapper.
         *
         * @param element element.
         * @param priority priority of the element.
         * @param delay delay of the element.
         * @param unit time unit of the delay.
         *
         * @throws IllegalArgumentException if the element is <tt>NULL</tt>, the priority is negative or if the delay is
         * negative.
         */
        public QueueElement(E element, int priority, long delay, TimeUnit unit) {
            if (element == null) {
                throw new IllegalArgumentException("element cannot be null");
            }
            if (priority < 0) {
                throw new IllegalArgumentException("priority cannot be negative, [" + element + "]");
            }
            if (delay < 0) {
                throw new IllegalArgumentException("delay cannot be negative");
            }
            this.element = element;
            this.priority = priority;
            setDelay(delay, unit);
        }

        /**
         * Create an Element wrapper with no delay and minimum priority.
         *
         * @param element element.
         */
        public QueueElement(E element) {
            this(element, 0, 0, TimeUnit.MILLISECONDS);
        }

        /**
         * Return the element from the wrapper.
         *
         * @return the element.
         */
        public E getElement() {
            return element;
        }

        /**
         * Return the priority of the element.
         *
         * @return the priority of the element.
         */
        public int getPriority() {
            return priority;
        }

        /**
         * Set the delay of the element.
         *
         * @param delay delay of the element.
         * @param unit time unit of the delay.
         */
        public void setDelay(long delay, TimeUnit unit) {
            baseTime = System.currentTimeMillis() + unit.toMillis(delay);
        }

        /**
         * Return the delay of the element.
         *
         * @param unit time unit of the delay.
         *
         * @return the delay in the specified time unit.
         */
        public long getDelay(TimeUnit unit) {
            return unit.convert(baseTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        /**
         * Compare the age of this wrapper element with another. The priority is not used for the comparision.
         *
         * @param o the other wrapper element to compare with.
         *
         * @return less than zero if this wrapper is older, zero if both wrapper elements have the same age, greater
         *         than zero if the parameter wrapper element is older.
         */
        public int compareTo(Delayed o) {
            long diff = (getDelay(TimeUnit.MILLISECONDS) - o.getDelay(TimeUnit.MILLISECONDS));
            if(diff > 0) {
                return 1;
            } else if(diff < 0) {
                return -1;
            } else {
                return 0;
            }
        }

        /**
         * Return the string representation of the wrapper element.
         *
         * @return the string representation of the wrapper element.
         */
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("[").append(element).append("] priority=").append(priority).append(" delay=").
                    append(getDelay(TimeUnit.MILLISECONDS));
            return sb.toString();
        }

    }

    /**
     * Frequency, in milliseconds, of the anti-starvation check.
     */
    public static final long ANTI_STARVATION_INTERVAL = 500;

    protected int priorities;
    protected DelayQueue<QueueElement<E>>[] queues;
    protected transient final ReentrantLock lock = new ReentrantLock();
    private transient long lastAntiStarvationCheck = 0;
    private long maxWait;
    private int maxSize;
    protected AtomicInteger currentSize;

    /**
     * Create a <code>PriorityDelayQueue</code>.
     *
     * @param priorities number of priorities the queue will support.
     * @param maxWait max wait time for elements before they are promoted to the next higher priority.
     * @param unit time unit of the max wait time.
     * @param maxSize maximum size of the queue, -1 means unbounded.
     */
    @SuppressWarnings("unchecked")
    public PriorityDelayQueue(int priorities, long maxWait, TimeUnit unit, int maxSize) {
        if (priorities < 1) {
            throw new IllegalArgumentException("priorities must be 1 or more");
        }
        if (maxWait < 0) {
            throw new IllegalArgumentException("maxWait must be greater than 0");
        }
        if (maxSize < -1 || maxSize == 0) {
            throw new IllegalArgumentException("maxSize must be -1 or greater than 0");
        }
        this.priorities = priorities;
        queues = new DelayQueue[priorities];
        for (int i = 0; i < priorities; i++) {
            queues[i] = new DelayQueue<QueueElement<E>>();
        }
        this.maxWait = unit.toMillis(maxWait);
        this.maxSize = maxSize;
        if (maxSize != -1) {
            currentSize = new AtomicInteger();
        }
    }

    /**
     * Return number of priorities the queue supports.
     *
     * @return number of priorities the queue supports.
     */
    public int getPriorities() {
        return priorities;
    }

    /**
     * Return the max wait time for elements before they are promoted to the next higher priority.
     *
     * @param unit time unit of the max wait time.
     *
     * @return the max wait time in the specified time unit.
     */
    public long getMaxWait(TimeUnit unit) {
        return unit.convert(maxWait, TimeUnit.MILLISECONDS);
    }

    /**
     * Return the maximum queue size.
     *
     * @return the maximum queue size. If <code>-1</code> the queue is unbounded.
     */
    public long getMaxSize() {
        return maxSize;
    }

    /**
     * Return an iterator over all the {@link QueueElement} elements (both expired and unexpired) in this queue. The
     * iterator does not return the elements in any particular order.  The returned <tt>Iterator</tt> is a "weakly
     * consistent" iterator that will never throw {@link ConcurrentModificationException}, and guarantees to traverse
     * elements as they existed upon construction of the iterator, and may (but is not guaranteed to) reflect any
     * modifications subsequent to construction.
     *
     * @return an iterator over the {@link QueueElement} elements in this queue.
     */
    @Override
    @SuppressWarnings("unchecked")
    public Iterator<QueueElement<E>> iterator() {
        QueueElement[][] queueElements = new QueueElement[queues.length][];
        lock.lock();
        try {
            for (int i = 0; i < queues.length; i++) {
                queueElements[i] = queues[i].toArray(new QueueElement[0]);
            }
        }
        finally {
            lock.unlock();
        }
        List<QueueElement<E>> list = new ArrayList<QueueElement<E>>();
        for (QueueElement[] elements : queueElements) {
            list.addAll(Arrays.asList((QueueElement<E>[]) elements));
        }
        return list.iterator();
    }

    /**
     * Return the number of elements in the queue.
     *
     * @return the number of elements in the queue.
     */
    @Override
    public int size() {
        int size = 0;
        for (DelayQueue<QueueElement<E>> queue : queues) {
            size += queue.size();
        }
        return size;
    }

    /**
     * Return the number of elements on each priority sub-queue.
     *
     * @return the number of elements on each priority sub-queue.
     */
    public int[] sizes() {
        int[] sizes = new int[queues.length];
        for (int i = 0; i < queues.length; i++) {
            sizes[i] = queues[i].size();
        }
        return sizes;
    }

    /**
     * Inserts the specified element into this queue if it is possible to do
     * so immediately without violating capacity restrictions, returning
     * <tt>true</tt> upon success and throwing an
     * <tt>IllegalStateException</tt> if no space is currently available.
     * When using a capacity-restricted queue, it is generally preferable to
     * use {@link #offer(Object) offer}.
     *
     * @param queueElement the {@link QueueElement} element to add.
     * @return <tt>true</tt> (as specified by {@link Collection#add})
     * @throws IllegalStateException if the element cannot be added at this
     *         time due to capacity restrictions
     * @throws ClassCastException if the class of the specified element
     *         prevents it from being added to this queue
     * @throws NullPointerException if the specified element is null
     * @throws IllegalArgumentException if some property of the specified
     *         element prevents it from being added to this queue
     */
    @Override
    public boolean add(QueueElement<E> queueElement) {
        return offer(queueElement, false);
    }

    /**
     * Insert the specified {@link QueueElement} element into the queue.
     *
     * @param queueElement the {@link QueueElement} element to add.
     * @param ignoreSize if the queue is bound to a maximum size and the maximum size is reached, this parameter (if set
     * to <tt>true</tt>) allows to ignore the maximum size and add the element to the queue.
     *
     * @return <tt>true</tt> if the element has been inserted, <tt>false</tt> if the element was not inserted (the queue
     *         has reached its maximum size).
     *
     * @throws NullPointerException if the specified element is null
     */
    boolean offer(QueueElement<E> queueElement, boolean ignoreSize) {
        if (queueElement == null) {
            throw new NullPointerException("queueElement is NULL");
        }
        if (queueElement.getPriority() < 0 || queueElement.getPriority() >= priorities) {
            throw new IllegalArgumentException("priority out of range: " + queueElement);
        }
        if (queueElement.inQueue) {
            throw new IllegalStateException("queueElement already in a queue: " + queueElement);
        }
        if (!ignoreSize && currentSize != null && currentSize.get() >= maxSize) {
            return false;
        }
        boolean accepted;
        lock.lock();
        try {
            accepted = queues[queueElement.getPriority()].offer(queueElement);
            debug("offer([{0}]), to P[{1}] delay[{2}ms] accepted[{3}]", queueElement.getElement().toString(),
                  queueElement.getPriority(), queueElement.getDelay(TimeUnit.MILLISECONDS), accepted);
            if (accepted) {
                if (currentSize != null) {
                    currentSize.incrementAndGet();
                }
                queueElement.inQueue = true;
            }
        } finally {
            lock.unlock();
        }
        return accepted;
    }

    /**
     * Insert the specified element into the queue.
     * <p/>
     * The element is added with minimun priority and no delay.
     *
     * @param queueElement the element to add.
     *
     * @return <tt>true</tt> if the element has been inserted, <tt>false</tt> if the element was not inserted (the queue
     *         has reached its maximum size).
     *
     * @throws NullPointerException if the specified element is null
     */
    @Override
    public boolean offer(QueueElement<E> queueElement) {
        return offer(queueElement, false);
    }

    /**
     * Retrieve and remove the head of this queue, or return <tt>null</tt> if this queue has no elements with an expired
     * delay.
     * <p/>
     * The retrieved element is the oldest one from the highest priority sub-queue.
     * <p/>
     * Invocations to this method run the anti-starvation (once every interval check).
     *
     * @return the head of this queue, or <tt>null</tt> if this queue has no elements with an expired delay.
     */
    @Override
    public QueueElement<E> poll() {
        lock.lock();
        try {
            antiStarvation();
            QueueElement<E> e = null;
            int i = priorities;
            for (; e == null && i > 0; i--) {
                e = queues[i - 1].poll();
            }
            if (e != null) {
                if (currentSize != null) {
                    currentSize.decrementAndGet();
                }
                e.inQueue = false;
                debug("poll(): [{0}], from P[{1}]", e.getElement().toString(), i);
            }
            return e;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Retrieve, but does not remove, the head of this queue, or returns <tt>null</tt> if this queue is empty.  Unlike
     * <tt>poll</tt>, if no expired elements are available in the queue, this method returns the element that will
     * expire next, if one exists.
     *
     * @return the head of this queue, or <tt>null</tt> if this queue is empty.
     */
    @Override
    public QueueElement<E> peek() {
        lock.lock();
        try {
            antiStarvation();
            QueueElement<E> e = null;

            QueueElement<E> [] seeks = new QueueElement[priorities];
            boolean foundElement = false;
            for (int i = priorities - 1; i > -1; i--) {
                e = queues[i].peek();
                debug("peek(): considering [{0}] from P[{1}]", e, i);
                seeks[priorities - i - 1] = e;
                foundElement |= e != null;
            }
            if (foundElement) {
                e = null;
                for (int i = 0; e == null && i < priorities; i++) {
                    if (seeks[i] != null && seeks[i].getDelay(TimeUnit.MILLISECONDS) > 0) {
                        debug("peek, ignoring [{0}]", seeks[i]);
                    }
                    else {
                        e = seeks[i];
                    }
                }
                if (e != null) {
                    debug("peek(): choosing [{0}]", e);
                }
                if (e == null) {
                    int first;
                    for (first = 0; e == null && first < priorities; first++) {
                        e = seeks[first];
                    }
                    if (e != null) {
                        debug("peek(): initial choosing [{0}]", e);
                    }
                    for (int i = first; i < priorities; i++) {
                        QueueElement<E> ee = seeks[i];
                        if (ee != null && ee.getDelay(TimeUnit.MILLISECONDS) < e.getDelay(TimeUnit.MILLISECONDS)) {
                            debug("peek(): choosing [{0}] over [{1}]", ee, e);
                            e = ee;
                        }
                    }
                }
            }
            if (e != null) {
                debug("peek(): [{0}], from P[{1}]", e.getElement().toString(), e.getPriority());
            }
            else {
                debug("peek(): NULL");
            }
            return e;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Run the anti-starvation check every {@link #ANTI_STARVATION_INTERVAL} milliseconds.
     * <p/>
     * It promotes elements beyond max wait time to the next higher priority sub-queue.
     */
    protected void antiStarvation() {
        long now = System.currentTimeMillis();
        if (now - lastAntiStarvationCheck > ANTI_STARVATION_INTERVAL) {
            for (int i = 0; i < queues.length - 1; i++) {
                antiStarvation(queues[i], queues[i + 1], "from P[" + i + "] to P[" + (i + 1) + "]");
            }
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < queues.length; i++) {
                sb.append("P[").append(i).append("]=").append(queues[i].size()).append(" ");
            }
            debug("sub-queue sizes: {0}", sb.toString());
            lastAntiStarvationCheck = System.currentTimeMillis();
        }
    }

    /**
     * Promote elements beyond max wait time from a lower priority sub-queue to a higher priority sub-queue.
     *
     * @param lowerQ lower priority sub-queue.
     * @param higherQ higher priority sub-queue.
     * @param msg sub-queues msg (from-to) for debugging purposes.
     */
    private void antiStarvation(DelayQueue<QueueElement<E>> lowerQ, DelayQueue<QueueElement<E>> higherQ, String msg) {
        int moved = 0;
        QueueElement<E> e = lowerQ.poll();
        while (e != null && e.getDelay(TimeUnit.MILLISECONDS) < -maxWait) {
            e.setDelay(0, TimeUnit.MILLISECONDS);
            if (!higherQ.offer(e)) {
                throw new IllegalStateException("Could not move element to higher sub-queue, element rejected");
            }
            e.priority++;
            e = lowerQ.poll();
            moved++;
        }
        if (e != null) {
            if (!lowerQ.offer(e)) {
                throw new IllegalStateException("Could not reinsert element to current sub-queue, element rejected");
            }
        }
        debug("anti-starvation, moved {0} element(s) {1}", moved, msg);
    }

    /**
     * Method for debugging purposes. This implementation is a <tt>NOP</tt>.
     * <p/>
     * This method should be overriden for logging purposes.
     * <p/>
     * Message templates used by this class are in JDK's <tt>MessageFormat</tt> syntax.
     *
     * @param msgTemplate message template.
     * @param msgArgs arguments for the message template.
     */
    protected void debug(String msgTemplate, Object... msgArgs) {
    }

    /**
     * Insert the specified element into this queue, waiting if necessary
     * for space to become available.
     * <p/>
     * NOTE: This method is to fulfill the <tt>BlockingQueue<tt/> interface. Not implemented in the most optimal way.
     *
     * @param e the element to add
     * @throws InterruptedException if interrupted while waiting
     * @throws ClassCastException if the class of the specified element
     *         prevents it from being added to this queue
     * @throws NullPointerException if the specified element is null
     * @throws IllegalArgumentException if some property of the specified
     *         element prevents it from being added to this queue
     */
    @Override
    public void put(QueueElement<E> e) throws InterruptedException {
        while (!offer(e, true)) {
            Thread.sleep(10);
        }
    }

    /**
     * Insert the specified element into this queue, waiting up to the
     * specified wait time if necessary for space to become available.
     * <p/>
     * IMPORTANT: This implementation forces the addition of the element to the queue regardless
     * of the queue current size. The timeout value is ignored as the element is added immediately.
     * <p/>
     * NOTE: This method is to fulfill the <tt>BlockingQueue<tt/> interface. Not implemented in the most optimal way.
     *
     * @param e the element to add
     * @param timeout how long to wait before giving up, in units of
     *        <tt>unit</tt>
     * @param unit a <tt>TimeUnit</tt> determining how to interpret the
     *        <tt>timeout</tt> parameter
     * @return <tt>true</tt> if successful, or <tt>false</tt> if
     *         the specified waiting time elapses before space is available
     * @throws InterruptedException if interrupted while waiting
     * @throws ClassCastException if the class of the specified element
     *         prevents it from being added to this queue
     * @throws NullPointerException if the specified element is null
     * @throws IllegalArgumentException if some property of the specified
     *         element prevents it from being added to this queue
     */
    @Override
    public boolean offer(QueueElement<E> e, long timeout, TimeUnit unit) throws InterruptedException {
        return offer(e, true);
    }

    /**
     * Retrieve and removes the head of this queue, waiting if necessary
     * until an element becomes available.
     * <p/>
     * IMPORTANT: This implementation has a delay of up to 10ms (when the queue is empty) to detect a new element
     * is available. It is doing a 10ms sleep.
     * <p/>
     * NOTE: This method is to fulfill the <tt>BlockingQueue<tt/> interface. Not implemented in the most optimal way.
     *
     * @return the head of this queue
     * @throws InterruptedException if interrupted while waiting
     */
    @Override
    public QueueElement<E> take() throws InterruptedException {
        QueueElement<E> e = poll();
        while (e == null) {
            Thread.sleep(10);
            e = poll();
        }
        return e;
    }

    /**
     * Retrieve and removes the head of this queue, waiting up to the
     * specified wait time if necessary for an element to become available.
     * <p/>
     * NOTE: This method is to fulfill the <tt>BlockingQueue<tt/> interface. Not implemented in the most optimal way.
     *
     * @param timeout how long to wait before giving up, in units of
     *        <tt>unit</tt>
     * @param unit a <tt>TimeUnit</tt> determining how to interpret the
     *        <tt>timeout</tt> parameter
     * @return the head of this queue, or <tt>null</tt> if the
     *         specified waiting time elapses before an element is available
     * @throws InterruptedException if interrupted while waiting
     */
    @Override
    public QueueElement<E> poll(long timeout, TimeUnit unit) throws InterruptedException {
        QueueElement<E> e = poll();
        long time = System.currentTimeMillis() + unit.toMillis(timeout);
        while (e == null && time > System.currentTimeMillis()) {
            Thread.sleep(10);
            e = poll();
        }
        return poll();
    }

    /**
     * Return the number of additional elements that this queue can ideally
     * (in the absence of memory or resource constraints) accept without
     * blocking, or <tt>Integer.MAX_VALUE</tt> if there is no intrinsic
     * limit.
     *
     * <p>Note that you <em>cannot</em> always tell if an attempt to insert
     * an element will succeed by inspecting <tt>remainingCapacity</tt>
     * because it may be the case that another thread is about to
     * insert or remove an element.
     * <p/>
     * NOTE: This method is to fulfill the <tt>BlockingQueue<tt/> interface. Not implemented in the most optimal way.
     *
     * @return the remaining capacity
     */
    @Override
    public int remainingCapacity() {
        return (maxSize == -1) ? -1 : maxSize - size();
    }

    /**
     * Remove all available elements from this queue and adds them
     * to the given collection.  This operation may be more
     * efficient than repeatedly polling this queue.  A failure
     * encountered while attempting to add elements to
     * collection <tt>c</tt> may result in elements being in neither,
     * either or both collections when the associated exception is
     * thrown.  Attempt to drain a queue to itself result in
     * <tt>IllegalArgumentException</tt>. Further, the behavior of
     * this operation is undefined if the specified collection is
     * modified while the operation is in progress.
     * <p/>
     * NOTE: This method is to fulfill the <tt>BlockingQueue<tt/> interface. Not implemented in the most optimal way.
     *
     * @param c the collection to transfer elements into
     * @return the number of elements transferred
     * @throws UnsupportedOperationException if addition of elements
     *         is not supported by the specified collection
     * @throws ClassCastException if the class of an element of this queue
     *         prevents it from being added to the specified collection
     * @throws NullPointerException if the specified collection is null
     * @throws IllegalArgumentException if the specified collection is this
     *         queue, or some property of an element of this queue prevents
     *         it from being added to the specified collection
     */
    @Override
    public int drainTo(Collection<? super QueueElement<E>> c) {
        int count = 0;
        for (DelayQueue<QueueElement<E>> q : queues) {
            count += q.drainTo(c);
        }
        return count;
    }

    /**
     * Remove at most the given number of available elements from
     * this queue and adds them to the given collection.  A failure
     * encountered while attempting to add elements to
     * collection <tt>c</tt> may result in elements being in neither,
     * either or both collections when the associated exception is
     * thrown.  Attempt to drain a queue to itself result in
     * <tt>IllegalArgumentException</tt>. Further, the behavior of
     * this operation is undefined if the specified collection is
     * modified while the operation is in progress.
     * <p/>
     * NOTE: This method is to fulfill the <tt>BlockingQueue<tt/> interface. Not implemented in the most optimal way.
     *
     * @param c the collection to transfer elements into
     * @param maxElements the maximum number of elements to transfer
     * @return the number of elements transferred
     * @throws UnsupportedOperationException if addition of elements
     *         is not supported by the specified collection
     * @throws ClassCastException if the class of an element of this queue
     *         prevents it from being added to the specified collection
     * @throws NullPointerException if the specified collection is null
     * @throws IllegalArgumentException if the specified collection is this
     *         queue, or some property of an element of this queue prevents
     *         it from being added to the specified collection
     */
    @Override
    public int drainTo(Collection<? super QueueElement<E>> c, int maxElements) {
        int left = maxElements;
        int count = 0;
        for (DelayQueue<QueueElement<E>> q : queues) {
            int drained = q.drainTo(c, left);
            count += drained;
            left -= drained;
        }
        return count;
    }

    /**
     * Removes all of the elements from this queue. The queue will be empty after this call returns.
     */
    @Override
    public void clear() {
        for (DelayQueue<QueueElement<E>> q : queues) {
            q.clear();
        }
    }
}
