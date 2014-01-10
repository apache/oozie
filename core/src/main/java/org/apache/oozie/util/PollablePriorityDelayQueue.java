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

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class PollablePriorityDelayQueue<E> extends PriorityDelayQueue<E> {

    public PollablePriorityDelayQueue(int priorities, long maxWait, TimeUnit unit, int maxSize) {
        super(priorities, maxWait, unit, maxSize);
    }

    /**
     * Retrieve and remove the head of this queue if it is eligible to poll. If not, iterate next element until find
     * the one which is eligible to poll from queue.
     *
     * Return <tt>null</tt> if this queue has no elements eligible to run.
     *
     * <p/>
     * Invocations to this method run the anti-starvation (once every interval check).
     *
     * @return the element of this queue, for which eligibleToPoll is true.
     */
    @Override
    public QueueElement<E> poll() {
        try {
            lock.lock();
            antiStarvation();
            QueueElement<E> e = null;
            int i = priorities;
            for (; e == null && i > 0; i--) {
                e = queues[i - 1].peek();
                if (eligibleToPoll(e)) {
                    e = queues[i - 1].poll();
                }
                else {
                    if (e != null) {
                        debug("poll(): the peek element [{0}], from P[{1}] is not eligible to poll", e.getElement().toString(), i);
                    }
                    e = null;
                    Iterator<QueueElement<E>> iter = queues[i - 1].iterator();
                    while(e == null && iter.hasNext()) {
                        e = iter.next();
                        if (e.getDelay(TimeUnit.MILLISECONDS) <= 0 && eligibleToPoll(e)) {
                            queues[i - 1].remove(e);
                        }
                        else {
                            debug("poll(): the iterator element [{0}], from P[{1}] is not eligible to poll", e.getElement().toString(), i);
                            e = null;
                        }
                    }
                }
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
     * Method for checking the QueueElement eligible to poll before remove it from queue.
     * <p/>
     * This method should be overriden for checking purposes.
     *
     * @param element the element to check
     */
    protected boolean eligibleToPoll(QueueElement<?> element) {
        return true;
    }
}
