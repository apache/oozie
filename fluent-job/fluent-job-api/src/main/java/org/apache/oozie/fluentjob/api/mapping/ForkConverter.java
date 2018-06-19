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

import org.apache.oozie.fluentjob.api.generated.workflow.FORK;
import org.apache.oozie.fluentjob.api.generated.workflow.FORKTRANSITION;
import org.apache.oozie.fluentjob.api.generated.workflow.ObjectFactory;
import org.apache.oozie.fluentjob.api.dag.Fork;
import org.apache.oozie.fluentjob.api.dag.NodeBase;
import org.dozer.DozerConverter;

import java.util.List;

/**
 * A {@link DozerConverter} converting from {@link Fork} to JAXB {@link FORK}.
 */
public class ForkConverter extends DozerConverter<Fork, FORK> {
    private static final ObjectFactory WORKFLOW_OBJECT_FACTORY = new ObjectFactory();

    public ForkConverter() {
        super(Fork.class, FORK.class);
    }

    @Override
    public FORK convertTo(final Fork source, FORK destination) {
        destination = ensureDestination(destination);

        destination.setName(source.getName());

        final List<FORKTRANSITION> transitions = destination.getPath();
        for (final NodeBase child : source.getChildren()) {
            final NodeBase realChild = RealChildLocator.findRealChild(child);
            transitions.add(convertToFORKTRANSITION(realChild));
        }

        return destination;
    }

    private FORK ensureDestination(FORK destination) {
        if (destination == null) {
            destination = WORKFLOW_OBJECT_FACTORY.createFORK();
        }
        return destination;
    }

    @Override
    public Fork convertFrom(final FORK source, final Fork destination) {
        throw new UnsupportedOperationException("This mapping is not bidirectional.");
    }

    private FORKTRANSITION convertToFORKTRANSITION(final NodeBase source) {
        final FORKTRANSITION destination = WORKFLOW_OBJECT_FACTORY.createFORKTRANSITION();

        final NodeBase realChild = RealChildLocator.findRealChild(source);

        destination.setStart(realChild.getName());

        return destination;
    }
}
