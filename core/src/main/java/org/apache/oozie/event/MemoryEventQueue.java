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


package org.apache.oozie.event;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.event.Event;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.util.XLog;

/**
 * An implementation of the EventQueue, defining a memory-based data structure
 * holding the events
 */
public class MemoryEventQueue implements EventQueue {

    private static ConcurrentLinkedQueue<EventQueueElement> eventQueue;
    private static AtomicInteger currentSize;
    private static int maxSize;
    private static XLog LOG;
    private static int batchSize;

    @Override
    public void init(Configuration conf) {
        eventQueue = new ConcurrentLinkedQueue<EventQueueElement>();
        maxSize = ConfigurationService.getInt(conf, EventHandlerService.CONF_QUEUE_SIZE);
        currentSize = new AtomicInteger();
        batchSize = ConfigurationService.getInt(conf, EventHandlerService.CONF_BATCH_SIZE);
        LOG = XLog.getLog(getClass());
        LOG.info("Memory Event Queue initialized with Max size = [{0}], Batch drain size = [{1}]", maxSize, batchSize);
    }

    @Override
    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public void add(Event e) {
        EventQueueElement eqe = new EventQueueElement(e);
        try {
            if (size() <= maxSize) {
                if (eventQueue.add(eqe)) {
                    currentSize.incrementAndGet();
                }
            }
            else {
                LOG.warn("Queue size [{0}] reached max limit. Element [{1}] not added", size(), e);
            }
        }
        catch (IllegalStateException ise) {
            LOG.warn("Unable to add event due to " + ise);
        }
    }

    @Override
    public List<Event> pollBatch() {
        // batch drain
        List<Event> eventBatch = new ArrayList<Event>();
        for (int i = 0; i < batchSize; i++) {
            EventQueueElement polled = eventQueue.poll();
            if (polled != null) {
                currentSize.decrementAndGet();
                eventBatch.add(polled.event);
            }
            else {
                LOG.trace("Current queue size [{0}] less than polling batch size [{1}]", currentSize.get(), batchSize);
                break;
            }
        }
        return eventBatch;
    }

    @Override
    public Event poll() {
        EventQueueElement polled = eventQueue.poll();
        if (polled != null) {
            currentSize.decrementAndGet();
            return polled.event;
        }
        return null;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public int size() {
        return currentSize.intValue();
    }

    @Override
    public Event peek() {
        EventQueueElement peeked = eventQueue.peek();
        if (peeked != null) {
            return peeked.event;
        }
        return null;
    }

    @Override
    public void clear() {
        eventQueue.clear();
        currentSize.set(0);
    }

}
