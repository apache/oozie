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

package org.apache.oozie.util.graph;

import org.apache.oozie.client.WorkflowAction;

import java.awt.Point;
import java.util.HashMap;
import java.util.Map;

public class WorkflowActionNode {
    private String name;
    private String type;
    private Point loc;
    private final Map<String, Boolean> arcs;
    private WorkflowAction.Status status = null;

    private WorkflowActionNode(final String name,
                               final String type,
                               final HashMap<String, Boolean> arcs,
                               final Point loc,
                               final WorkflowAction.Status status) {
        this.name = name;
        this.type = type;
        this.arcs = arcs;
        this.loc = loc;
        this.status = status;
    }

    WorkflowActionNode(final String name, final String type) {
        this(name, type, new HashMap<String, Boolean>(), new Point(0, 0), null);
    }

    void addArc(final String arc, final boolean isError) {
        arcs.put(arc, isError);
    }

    void addArc(final String arc) {
        addArc(arc, false);
    }

    public void setName(final String name) {
        this.name = name;
    }

    public void setType(final String type) {
        this.type = type;
    }

    public void setLocation(final Point loc) {
        this.loc = loc;
    }

    void setLocation(final double x, final double y) {
        loc.setLocation(x, y);
    }

    public void setStatus(final WorkflowAction.Status status) {
        this.status = status;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    Map<String, Boolean> getArcs() {
        return arcs;
    }

    public Point getLocation() {
        return loc;
    }

    public WorkflowAction.Status getStatus() {
        return status;
    }

    @Override
    public String toString() {
        final StringBuilder s = new StringBuilder();

        s.append("Node: ").append(name).append("\t");
        s.append("Type: ").append(type).append("\t");
        s.append("Location: (").append(loc.getX()).append(", ").append(loc.getY()).append(")\t");
        s.append("Status: ").append(status).append("\n");

        for (final Map.Entry<String, Boolean> entry : arcs.entrySet()) {
            s.append("\t").append(entry.getKey());
            if (entry.getValue()) {
                s.append(" on error\n");
            } else {
                s.append("\n");
            }
        }

        return s.toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final WorkflowActionNode that = (WorkflowActionNode) o;

        if (!name.equals(that.name)) {
            return false;
        }
        if (!type.equals(that.type)) {
            return false;
        }

        return status == that.status;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();

        result = 31 * result + type.hashCode();
        result = 31 * result + (status != null ? status.hashCode() : 0);

        return result;
    }
}
