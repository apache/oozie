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

package org.apache.oozie.coord.input.dependency;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Writable;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.util.WritableUtils;

public class CoordUnResolvedInputDependency implements Writable {

    private boolean isResolved;
    private List<String> dependency = new ArrayList<String>();
    private List<String> resolvedList = new ArrayList<String>();

    public CoordUnResolvedInputDependency(List<String> dependency) {
        this.dependency = dependency;

    }

    public CoordUnResolvedInputDependency() {
    }

    public boolean isResolved() {
        return isResolved;
    }

    public void setResolved(boolean isResolved) {
        this.isResolved = isResolved;
    }

    public List<String> getDependencies() {
        return dependency;
    }

    public List<String> getResolvedList() {
        return resolvedList;
    }

    public void setResolvedList(List<String> resolvedList) {
        this.resolvedList = resolvedList;
    }

    public void addResolvedList(List<String> resolvedList) {
        this.resolvedList.addAll(resolvedList);
    }

    public String getUnResolvedList() {
        if (!isResolved) {
            return StringUtils.join(dependency, CoordELFunctions.INSTANCE_SEPARATOR);
        }
        else
            return "";
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBoolean(isResolved);
        WritableUtils.writeStringList(out, dependency);
        WritableUtils.writeStringList(out, resolvedList);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        isResolved = in.readBoolean();
        dependency = WritableUtils.readStringList(in);
        resolvedList = WritableUtils.readStringList(in);
    }
}
