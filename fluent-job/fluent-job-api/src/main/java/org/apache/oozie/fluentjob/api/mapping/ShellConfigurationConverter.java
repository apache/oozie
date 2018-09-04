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

import org.apache.oozie.fluentjob.api.generated.action.shell.CONFIGURATION;
import org.apache.oozie.fluentjob.api.generated.action.shell.ObjectFactory;
import org.dozer.DozerConverter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A {@link DozerConverter} converting from {@link Map} to JAXB {@link CONFIGURATION}.
 */
public class ShellConfigurationConverter extends DozerConverter<Map, CONFIGURATION> {
    private static final ObjectFactory OBJECT_FACTORY = new ObjectFactory();

    public ShellConfigurationConverter() {
        super(Map.class, CONFIGURATION.class);
    }

    @Override
    public CONFIGURATION convertTo(final Map source, CONFIGURATION destination) {
        if (source == null || source.isEmpty()) {
            return null;
        }

        destination = ensureDestination(destination);

        mapEntries(source, destination);

        return destination;
    }

    private CONFIGURATION ensureDestination(CONFIGURATION destination) {
        if (destination == null) {
            destination = OBJECT_FACTORY.createCONFIGURATION();
        }

        return destination;
    }

    private void mapEntries(final Map source, final CONFIGURATION destination) {
        if (source != null) {
            final List<CONFIGURATION.Property> targetProperties = new ArrayList<>();

            for (final Object objectKey : source.keySet()) {
                final String name = objectKey.toString();
                final String value = source.get(name).toString();
                final CONFIGURATION.Property targetProperty = OBJECT_FACTORY.createCONFIGURATIONProperty();
                targetProperty.setName(name);
                targetProperty.setValue(value);
                targetProperties.add(targetProperty);
            }

            destination.setProperty(targetProperties);
        }
    }

    @Override
    public Map convertFrom(final CONFIGURATION source, final Map destination) {
        throw new UnsupportedOperationException("This mapping is not bidirectional.");
    }
}
