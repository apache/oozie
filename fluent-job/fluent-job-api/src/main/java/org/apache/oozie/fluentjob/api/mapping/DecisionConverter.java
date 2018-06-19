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

package org.apache.oozie.fluentjob.api.mapping;

import com.google.common.base.Preconditions;
import org.apache.oozie.fluentjob.api.Condition;
import org.apache.oozie.fluentjob.api.generated.workflow.CASE;
import org.apache.oozie.fluentjob.api.generated.workflow.DECISION;
import org.apache.oozie.fluentjob.api.generated.workflow.DEFAULT;
import org.apache.oozie.fluentjob.api.generated.workflow.ObjectFactory;
import org.apache.oozie.fluentjob.api.generated.workflow.SWITCH;
import org.apache.oozie.fluentjob.api.dag.DagNodeWithCondition;
import org.apache.oozie.fluentjob.api.dag.Decision;
import org.apache.oozie.fluentjob.api.dag.NodeBase;
import org.dozer.DozerConverter;
import org.dozer.Mapper;
import org.dozer.MapperAware;

import java.util.List;

/**
 * A {@link DozerConverter} converting from {@link Decision} to JAXB {@link DECISION}.
 */
public class DecisionConverter extends DozerConverter<Decision, DECISION> implements MapperAware {
    private static final ObjectFactory OBJECT_FACTORY = new ObjectFactory();
    private Mapper mapper;

    public DecisionConverter() {
        super(Decision.class, DECISION.class);
    }

    @Override
    public DECISION convertTo(final Decision source, DECISION destination) {
        destination = ensureDestination(destination);

        mapName(source, destination);

        mapTransitions(source, destination);

        return destination;
    }

    @Override
    public Decision convertFrom(final DECISION source, final Decision destination) {
        throw new UnsupportedOperationException("This mapping is not bidirectional.");
    }

    private Mapper checkAndGetMapper() {
        Preconditions.checkNotNull(mapper, "mapper should be set");
        return mapper;
    }

    @Override
    public void setMapper(final Mapper mapper) {
        this.mapper = mapper;
    }

    private void mapName(final Decision source, final DECISION destination) {
        final String name = source.getName();
        destination.setName(name);
    }

    private void mapTransitions(final Decision source, final DECISION destination) {
        final NodeBase defaultNode = source.getDefaultChild();

        Preconditions.checkState(defaultNode != null, "No default transition found.");

        final NodeBase realDefaultNode = RealChildLocator.findRealChild(defaultNode);

        final DEFAULT defaultCase = OBJECT_FACTORY.createDEFAULT();
        defaultCase.setTo(realDefaultNode.getName());
        destination.getSwitch().setDefault(defaultCase);

        final List<DagNodeWithCondition> childrenIncludingDefault = source.getChildrenWithConditions();

        // The default child is the last on the list, we remove it as we have already handled that.
        final List<DagNodeWithCondition> children = childrenIncludingDefault.subList(0, childrenIncludingDefault.size() - 1);
        final List<CASE> cases = destination.getSwitch().getCase();

        for (final DagNodeWithCondition childWithCondition : children) {
            final NodeBase child = childWithCondition.getNode();
            final NodeBase realChild = RealChildLocator.findRealChild(child);

            final Condition condition = childWithCondition.getCondition();

            final DagNodeWithCondition realChildWithCondition = new DagNodeWithCondition(realChild, condition);

            final CASE mappedCase = checkAndGetMapper().map(realChildWithCondition, CASE.class);
            cases.add(mappedCase);
        }
    }

    private DECISION ensureDestination(final DECISION destination) {
        DECISION result = destination;
        if (result == null) {
            result = OBJECT_FACTORY.createDECISION();
        }

        if (result.getSwitch() == null) {
            final SWITCH _switch = OBJECT_FACTORY.createSWITCH();
            result.setSwitch(_switch);
        }

        return result;
    }
}
