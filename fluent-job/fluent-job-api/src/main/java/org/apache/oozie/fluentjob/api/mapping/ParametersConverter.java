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

import org.apache.oozie.fluentjob.api.generated.workflow.ObjectFactory;
import org.apache.oozie.fluentjob.api.generated.workflow.PARAMETERS;
import org.apache.oozie.fluentjob.api.workflow.Parameter;
import org.apache.oozie.fluentjob.api.workflow.Parameters;
import org.dozer.DozerConverter;

/**
 * A {@link DozerConverter} converting from {@link Parameters} to JAXB {@link PARAMETERS}.
 */
public class ParametersConverter extends DozerConverter<Parameters, PARAMETERS> {
    private static final ObjectFactory OBJECT_FACTORY = new ObjectFactory();

    public ParametersConverter() {
        super(Parameters.class, PARAMETERS.class);
    }

    @Override
    public PARAMETERS convertTo(final Parameters source, PARAMETERS destination) {
        if (source == null) {
            return null;
        }

        destination = ensureDestination(destination);

        mapParameters(source, destination);

        return destination;
    }

    private PARAMETERS ensureDestination(final PARAMETERS destination) {
        if (destination == null) {
            return OBJECT_FACTORY.createPARAMETERS();
        }

        return destination;
    }

    private void mapParameters(final Parameters source, final PARAMETERS destination) {
        for (final Parameter parameter : source.getParameters()) {
            final PARAMETERS.Property property = OBJECT_FACTORY.createPARAMETERSProperty();
            property.setName(parameter.getName());
            property.setValue(parameter.getValue());
            property.setDescription(parameter.getDescription());

            destination.getProperty().add(property);
        }
    }

    @Override
    public Parameters convertFrom(final PARAMETERS source, final Parameters destination) {
        throw new UnsupportedOperationException("This mapping is not bidirectional.");
    }
}
