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

import org.apache.oozie.fluentjob.api.generated.action.spark.LAUNCHER;
import org.apache.oozie.fluentjob.api.generated.action.spark.ObjectFactory;
import org.apache.oozie.fluentjob.api.action.Launcher;
import org.dozer.DozerConverter;

/**
 * A {@link DozerConverter} converting from {@link Launcher} to JAXB {@link LAUNCHER}.
 */
public class SparkLauncherConverter extends DozerConverter<Launcher, LAUNCHER> {
    private static final ObjectFactory OBJECT_FACTORY = new ObjectFactory();

    public SparkLauncherConverter() {
        super(Launcher.class, LAUNCHER.class);
    }

    @Override
    public LAUNCHER convertTo(final Launcher source, LAUNCHER destination) {
        if (source == null) {
            return null;
        }

        destination = ensureDestination(destination);

        mapAttributes(source, destination);

        return destination;
    }

    private LAUNCHER ensureDestination(final LAUNCHER destination) {
        if (destination == null) {
            return OBJECT_FACTORY.createLAUNCHER();
        }

        return destination;
    }

    private void mapAttributes(final Launcher source, final LAUNCHER destination) {
        if (source == null) {
            return;
        }

        destination.getMemoryMbOrVcoresOrJavaOpts().add(OBJECT_FACTORY.createLAUNCHERMemoryMb(source.getMemoryMb()));
        destination.getMemoryMbOrVcoresOrJavaOpts().add(OBJECT_FACTORY.createLAUNCHERVcores(source.getVCores()));
        destination.getMemoryMbOrVcoresOrJavaOpts().add(OBJECT_FACTORY.createLAUNCHERQueue(source.getQueue()));
        destination.getMemoryMbOrVcoresOrJavaOpts().add(OBJECT_FACTORY.createLAUNCHERSharelib(source.getSharelib()));
        destination.getMemoryMbOrVcoresOrJavaOpts().add(OBJECT_FACTORY.createLAUNCHERViewAcl(source.getViewAcl()));
        destination.getMemoryMbOrVcoresOrJavaOpts().add(OBJECT_FACTORY.createLAUNCHERModifyAcl(source.getModifyAcl()));
    }

    @Override
    public Launcher convertFrom(final LAUNCHER source, final Launcher destination) {
        throw new UnsupportedOperationException("This mapping is not bidirectional.");
    }
}
