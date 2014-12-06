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

package org.apache.oozie.dependency.hcat;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;

public class WaitingActions implements Serializable {

    private static final long serialVersionUID = 1L;
    private Collection<WaitingAction> waitingActions;

    public WaitingActions() {
        waitingActions = new HashSet<WaitingAction>();
    }

    /**
     * Get the list of waiting actions
     * @return waiting actions
     */
    public Collection<WaitingAction> getWaitingActions() {
        return waitingActions;
    }

    /**
     * Add a waiting action
     * @param waitingAction waiting action
     */
    public void add(WaitingAction waitingAction) {
        waitingActions.add(waitingAction);
    }

}
