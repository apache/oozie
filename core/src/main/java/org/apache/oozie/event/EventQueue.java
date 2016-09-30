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

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.event.Event;

/**
 * Interface to define the queue operations for the events system
 */
public interface EventQueue {

    public class EventQueueElement implements Serializable {

        private static final long serialVersionUID = 1L;
        Event event;

        public EventQueueElement(Event e) {
            event = e;
        }
    }

    /**
     * Initialize the event queue
     * @param conf
     */
    void init(Configuration conf);

    /**
     * Add event to queue
     * @param e
     */
    void add(Event e);

    /**
     * Fetch events from queue in batch
     * @return events set
     */
    List<Event> pollBatch();

    /**
    * Fetch single event from queue
    * @return event
    */
   Event poll();

    /**
     * Find out if queue is empty
     * @return boolean
     */
    boolean isEmpty();

    /**
     * Get current queue size
     * @return size
     */
    int size();

    /**
     * Read topmost event from queue but do not pop from it
     * @return event
     */
    Event peek();

    /**
     * Get the batch size used during polling events
     * @return batchSize
     */
    int getBatchSize();

    /**
     * Clear the events queue
     */
    void clear();

}
