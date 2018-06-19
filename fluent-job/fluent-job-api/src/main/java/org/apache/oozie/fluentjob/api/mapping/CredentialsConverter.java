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

import org.apache.oozie.fluentjob.api.generated.workflow.CREDENTIAL;
import org.apache.oozie.fluentjob.api.generated.workflow.CREDENTIALS;
import org.apache.oozie.fluentjob.api.generated.workflow.ObjectFactory;
import org.apache.oozie.fluentjob.api.workflow.ConfigurationEntry;
import org.apache.oozie.fluentjob.api.workflow.Credential;
import org.apache.oozie.fluentjob.api.workflow.Credentials;
import org.dozer.DozerConverter;

/**
 * A {@link DozerConverter} converting from {@link Credentials} to JAXB {@link CREDENTIALS}.
 */
public class CredentialsConverter extends DozerConverter<Credentials, CREDENTIALS> {
    private static final ObjectFactory OBJECT_FACTORY = new ObjectFactory();

    public CredentialsConverter() {
        super(Credentials.class, CREDENTIALS.class);
    }

    @Override
    public CREDENTIALS convertTo(final Credentials source, CREDENTIALS destination) {
        if (source == null) {
            return null;
        }

        destination = ensureDestination(destination);

        mapCredentials(source, destination);

        return destination;
    }

    private CREDENTIALS ensureDestination(final CREDENTIALS destination) {
        if (destination == null) {
            return OBJECT_FACTORY.createCREDENTIALS();
        }

        return destination;
    }

    private void mapCredentials(final Credentials source, final CREDENTIALS destination) {
        if (source.getCredentials() == null) {
            return;
        }

        for (final Credential credential : source.getCredentials()) {
            final CREDENTIAL mappedCredential = OBJECT_FACTORY.createCREDENTIAL();
            mappedCredential.setName(credential.getName());
            mappedCredential.setType(credential.getType());
            mapConfigurationEntries(credential, mappedCredential);

            destination.getCredential().add(mappedCredential);
        }
    }

    private void mapConfigurationEntries(final Credential source, final CREDENTIAL destination) {
        if (source.getConfigurationEntries() == null) {
            return;
        }

        for (final ConfigurationEntry configurationEntry : source.getConfigurationEntries()) {
            final CREDENTIAL.Property mappedProperty = OBJECT_FACTORY.createCREDENTIALProperty();
            mappedProperty.setName(configurationEntry.getName());
            mappedProperty.setValue(configurationEntry.getValue());
            mappedProperty.setDescription(configurationEntry.getDescription());

            destination.getProperty().add(mappedProperty);
        }
    }

    @Override
    public Credentials convertFrom(final CREDENTIALS source, final Credentials destination) {
        throw new UnsupportedOperationException("This mapping is not bidirectional.");
    }
}
