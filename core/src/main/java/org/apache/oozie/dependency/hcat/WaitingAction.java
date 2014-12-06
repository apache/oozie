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

public class WaitingAction implements Serializable {

    private static final long serialVersionUID = 1L;
    private String actionID;
    private String dependencyURI;

    public WaitingAction(String actionID, String dependencyURI) {
        this.actionID = actionID;
        this.dependencyURI = dependencyURI;
    }

    /**
     * Get the action id
     * @return action id
     */
    public String getActionID() {
        return actionID;
    }

    /**
     * Get the dependency uri
     * @return dependency uri
     */
    public String getDependencyURI() {
        return dependencyURI;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + ((actionID == null) ? 0 : actionID.hashCode());
        result = prime * result + ((dependencyURI == null) ? 0 : dependencyURI.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        WaitingAction other = (WaitingAction) obj;
        return actionID.equals(other.actionID) && dependencyURI.equals(other.dependencyURI);
    }

    @Override
    public String toString() {
        return "WaitingAction [actionID=" + actionID + ", dependencyURI=" + dependencyURI + "]";
    }
}
