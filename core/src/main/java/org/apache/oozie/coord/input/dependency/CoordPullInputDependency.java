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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.coord.CoordCommandUtils;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.dependency.ActionDependency;
import org.apache.oozie.util.WritableUtils;
import org.jdom.JDOMException;

public class CoordPullInputDependency extends AbstractCoordInputDependency {
    private Map<String, CoordUnResolvedInputDependency> unResolvedList = new HashMap<String, CoordUnResolvedInputDependency>();

    public CoordPullInputDependency() {
        super();

    }

    public void addResolvedList(String dataSet, String list) {
        unResolvedList.get(dataSet).addResolvedList(Arrays.asList(list.split(",")));
    }

    public CoordUnResolvedInputDependency getUnResolvedDependency(String dataSet) {
        return unResolvedList.get(dataSet);
    }

    public boolean isUnResolvedDependencyMet() {
        for (CoordUnResolvedInputDependency coordUnResolvedDependency : unResolvedList.values()) {
            if (!coordUnResolvedDependency.isResolved()) {
                return false;
            }
        }
        return true;
    }

    public void addUnResolvedList(String dataSet, String dependency) {
        unResolvedList.put(dataSet, new CoordUnResolvedInputDependency(Arrays.asList(dependency.split("#"))));
    }

    public String getMissingDependencies() {
        StringBuffer bf = new StringBuffer(super.getMissingDependencies());
        String unresolvedMissingDependencies = getUnresolvedMissingDependencies();
        if (!StringUtils.isEmpty(unresolvedMissingDependencies)) {
            bf.append(CoordCommandUtils.RESOLVED_UNRESOLVED_SEPARATOR);
            bf.append(unresolvedMissingDependencies);
        }
        return bf.toString();
    }

    public String getUnresolvedMissingDependencies() {
        StringBuffer bf = new StringBuffer();
        if (unResolvedList != null) {
            for (CoordUnResolvedInputDependency coordUnResolvedDependency : unResolvedList.values()) {
                if (!coordUnResolvedDependency.isResolved()) {
                    String unresolvedList = coordUnResolvedDependency.getUnResolvedList();
                    if (bf.length() > 0 && !unresolvedList.isEmpty()) {
                        bf.append(CoordELFunctions.INSTANCE_SEPARATOR);
                    }
                    bf.append(unresolvedList);
                }
            }
        }
        return bf.toString();
    }

    protected void generateDependencies() {
        super.generateDependencies();
    }

    private void writeObject(ObjectOutputStream os) throws IOException, ClassNotFoundException {
        os.writeObject(unResolvedList);
    }

    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        unResolvedList = (Map<String, CoordUnResolvedInputDependency>) in.readObject();
        generateDependencies();
    }

    public boolean isDependencyMet() {
        return isResolvedDependencyMeet() && isUnResolvedDependencyMet();

    }

    public boolean isResolvedDependencyMeet() {
        return super.isDependencyMet();

    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        WritableUtils.writeMap(out, unResolvedList);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        unResolvedList = WritableUtils.readMap(in, CoordUnResolvedInputDependency.class);
    }

    @Override
    public void setMissingDependencies(String join) {
        // We don't have to set this for input logic. Dependency map will have computed missing dependencies
    }

    @Override
    public List<String> getAvailableDependencies(String dataSet) {
        List<String> availableList = new ArrayList<String>();
        availableList.addAll(super.getAvailableDependencies(dataSet));
        if (getUnResolvedDependency(dataSet) != null) {
            availableList.addAll(getUnResolvedDependency(dataSet).getResolvedList());
        }
        return availableList;
    }

    public boolean isDataSetResolved(String dataSet) {
        if(unResolvedList.containsKey(dataSet)){
            return unResolvedList.get(dataSet).isResolved();
        }
        else{
            return super.isDataSetResolved(dataSet);
        }
    }

    @Override
    public Map<String, ActionDependency> getMissingDependencies(CoordinatorActionBean coordAction)
            throws CommandException, IOException, JDOMException {
        Map<String, ActionDependency> missingDependenciesMap = new HashMap<String, ActionDependency>();
        missingDependenciesMap.putAll(super.getMissingDependencies(coordAction));

        for (String key : unResolvedList.keySet()) {
            if (!unResolvedList.get(key).isResolved()) {
                missingDependenciesMap.put(key,
                        new ActionDependency(unResolvedList.get(key).getDependencies(), new ArrayList<String>()));
            }
        }
        return missingDependenciesMap;
    }
}
